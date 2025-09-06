mod config;
mod data;
use crate::{config::*, data::*};

use std::sync::Arc;
use std::time::Duration;
use bindings::region::{DbConnection, DbUpdate, SubscriptionHandle};
use bindings::sdk::{DbContext, IntoQueries, SubscriptionHandle as SdkSubscriptionHandle};
use hashbrown::{HashMap, HashSet};
use tokio::sync::{mpsc::{unbounded_channel, UnboundedReceiver}, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, info};

#[derive(Debug, Clone, Copy)]
enum DungeonState { Open, Cleared(u64), Closed(u64) }
#[derive(Debug, Clone)]
struct Dungeon {
    loc: [i32; 2],
    state: DungeonState,
    players: HashSet<String>,
}
type DungeonMap = HashMap<u64, Dungeon>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = match Config::from("config.json") {
        Err(e) => {
            error!("failed to parse configuration file: {:#?}", e);
            return
        }
        Ok(c) if c.is_empty() => {
            error!("please fill out the configuration file (config.json)!");
            return
        },
        Ok(c) => c,
    };

    let (tx, rx) = unbounded_channel();
    let ctx = match DbConnection::builder()
        .configure(&config)
        .with_channel(tx)
        .with_light_mode(true)
        .on_connect(|_, _, _| info!("connected!"))
        .on_disconnect(|_, _| info!("disconnected!"))
        .build() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            error!("failed to build db connection: {:#?}", e);
            return
        }
    };

    let (tx_post, rx_post) = watch::channel(Default::default());
    let (exec, _, _, _) = spawn_threads(&ctx, rx, tx_post, rx_post);

    let _ = exec.await;
}

async fn post(mut rx: watch::Receiver<DungeonMap>) {
    while let Ok(()) = rx.changed().await {
        sleep(Duration::from_millis(500)).await;
        let value = rx.borrow_and_update();

        for (id, dungeon) in value.iter() {
            info!("{}: {:?}", id, dungeon)
        }
    }
}

async fn consume(ctx: Arc<DbConnection>, mut rx: UnboundedReceiver<DbUpdate>, tx: watch::Sender<DungeonMap>) {
    let handle = subscribe(ctx.clone(), [
        "SELECT * FROM dungeon_state;",
        "SELECT p.* FROM portal_state p JOIN location_state l ON p.entity_id = l.entity_id;",
        "SELECT l.* FROM location_state l JOIN portal_state p ON l.entity_id = p.entity_id;",
        "SELECT n.* FROM dimension_network_state n JOIN dungeon_state d ON n.entity_id = d.entity_id;",
    ]);
    let Some(update) = rx.recv().await else {return};
    let _ = handle.unsubscribe();

    let (mut dungeons, lookup) = build_dungeons(update);

    let mut queries = vec![
        "SELECT * FROM player_username_state;".to_string(),
        "SELECT n.* FROM dimension_network_state n JOIN dungeon_state d ON n.entity_id = d.entity_id;".to_string(),
    ];
    for dim in lookup.keys() {
        queries.push(format!("SELECT * FROM mobile_entity_state WHERE dimension = {};", dim))
    }
    subscribe(ctx.clone(), queries);

    let mut players = HashMap::new();
    while let Some(update) = rx.recv().await {
        let mut dirty = false;

        for e in update.player_username_state.inserts {
            players.insert(e.row.entity_id, e.row.username);
        }

        for e in update.dimension_network_state.inserts {
            let id = e.row.entity_id;
            dungeons.get_mut(&id).unwrap().state = e.row.into();

            dirty = true;
        }

        for (dungeon, changes) in entity_loc(update.mobile_entity_state, &lookup) {
            let dungeon = dungeons.get_mut(&dungeon).unwrap();

            for id in changes.insert {
                let Some(name) = players.get(&id) else {continue};
                dirty |= dungeon.players.insert(name.clone());
            }
            for id in changes.delete {
                let Some(name) = players.get(&id) else {continue};
                dirty |= dungeon.players.remove(name);
            }
        }

        if dirty { tx.send(dungeons.clone()).unwrap(); }
    }
}

fn build_dungeons(update: DbUpdate) -> (HashMap<u64, Dungeon>, HashMap<u32, u64>) {
    let mut rooms = HashMap::new();
    let mut dungeons = HashMap::<u64, Dungeon>::new();

    // fill dungeons
    for e in update.dungeon_state.inserts {
        let id = e.row.entity_id;
        rooms.insert(id, HashSet::new());
        dungeons.insert(id, e.row.into());
    }

    // fill state, add starting room
    for e in update.dimension_network_state.inserts {
        let id = e.row.entity_id;
        rooms.get_mut(&id).unwrap().insert(e.row.entrance_dimension_id);
        dungeons.get_mut(&id).unwrap().state = e.row.into();
    }

    // build portal connections for source dim -> target dim
    let portals = portals(update.location_state, update.portal_state);

    // find all reachable dimensions (except for overworld) for each dungeon
    for dungeon in rooms.values_mut() {
        let mut size = 0;
        while dungeon.len() != size {
            size = dungeon.len();
            for room in dungeon.clone() {
                dungeon.extend(portals.get(&room).unwrap());
            }
            dungeon.remove(&1);
        }
    }

    info!("dungeons: {:?}", dungeons);
    info!("rooms: {:?}", rooms);

    // build dim -> dungeon lookup
    let mut lookup = HashMap::new();
    for (id, dimensions) in rooms.drain() {
        for dim in dimensions {
            lookup.insert(dim, id);
        }
    }

    (dungeons, lookup)
}

fn subscribe<Queries: IntoQueries>(ctx: Arc<DbConnection>, queries: Queries) -> SubscriptionHandle {
    ctx.subscription_builder()
        .on_error({
            let ctx = ctx.clone();
            move |_, e| { error!("subscription error: {}", e); let _ = ctx.disconnect(); }
        })
        .subscribe(queries)
}

fn spawn_threads(
    ctx: &Arc<DbConnection>,
    rx: UnboundedReceiver<DbUpdate>,
    tx_post: watch::Sender<DungeonMap>,
    rx_post: watch::Receiver<DungeonMap>,
) -> (JoinHandle<()>, JoinHandle<()>, JoinHandle<()>, JoinHandle<()>) {(
    // database exec
    tokio::spawn({
        let ctx = ctx.clone();
        async move {
            let _ = ctx.run_async().await;
        }
    }),
    // consumer task
    tokio::spawn({
        let ctx = ctx.clone();
        consume(ctx, rx, tx_post)
    }),
    // poster task
    tokio::spawn({
        post(rx_post)
    }),
    // shutdown trigger
    tokio::spawn({
        let ctx = ctx.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            let _ = ctx.disconnect();
        }
    }),
)}