mod config;
use config::{Config, Configurable};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use bindings::region::{DbConnection, DbUpdate, SubscriptionHandle};
use bindings::sdk::{DbContext, IntoQueries, SubscriptionHandle as SdkSubscriptionHandle};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Debug, Clone, Copy)]
enum DungeonState { Open, Cleared(u64), Closed(u64) }
#[derive(Debug, Clone)]
struct Dungeon {
    loc: [i32; 2],
    state: DungeonState,
    players: HashSet<String>,
}

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
    let (exec, _, _) = spawn_threads(&ctx, rx);

    let _ = exec.await;
}

async fn consume(ctx: Arc<DbConnection>, mut rx: UnboundedReceiver<DbUpdate>) {
    let handle = subscribe(ctx.clone(),[
        "SELECT * FROM dungeon_state;",
        concat!("SELECT n.* FROM dimension_network_state n",
                " JOIN dungeon_state d ON n.entity_id = d.entity_id;"),
        concat!("SELECT p.* FROM portal_state p",
                " JOIN location_state l ON p.entity_id = l.entity_id;"),
        concat!("SELECT l.* FROM location_state l",
                " JOIN portal_state p ON l.entity_id = p.entity_id;"),
    ]);

    let Some(update) = rx.recv().await else {return};
    let _ = handle.unsubscribe();

    let (mut dungeons, lookup) = build_dungeons(update);

    let mut queries = Vec::new();
    queries.extend([
        "SELECT * FROM player_username_state;",
        concat!("SELECT n.* FROM dimension_network_state n",
                " JOIN dungeon_state d ON n.entity_id = d.entity_id;"),
    ].into_queries());
    queries.extend(lookup.keys()
        .map(|dim| format!("SELECT * FROM mobile_entity_state WHERE dimension = {};", dim))
        .collect::<Vec<_>>()
        .into_queries());
    subscribe(ctx.clone(), queries);

    let mut players = HashMap::new();
    while let Some(update) = rx.recv().await {
        let mut dirty = false;

        for e in update.dimension_network_state.inserts {
            let dungeon = dungeons.get_mut(&e.row.entity_id).unwrap();
            dungeon.state = match (e.row.collapse_respawn_timestamp, e.row.is_collapsed) {
                (0, false) => DungeonState::Open,
                (n, false) => DungeonState::Cleared(n),
                (n, true) => DungeonState::Closed(n),
            };

            dirty = true;
        }

        for e in update.player_username_state.inserts {
            players.insert(e.row.entity_id, e.row.username);
        }

        let mut deletes = HashSet::new();
        for e in update.mobile_entity_state.deletes {
            let Some(dungeon) = lookup.get(&e.row.dimension) else {continue};
            deletes.insert((e.row.entity_id, dungeon));
        }
        for e in update.mobile_entity_state.inserts {
            let Some(dungeon) = lookup.get(&e.row.dimension) else {continue};
            let Some(name) = players.get(&e.row.entity_id) else {continue};

            if !deletes.remove(&(e.row.entity_id, dungeon)) {
                dirty |= dungeons.get_mut(dungeon).unwrap().players.insert(name.clone());
            }
        }
        for (id, dungeon) in deletes {
            let Some(name) = players.get(&id) else {continue};
            dungeons.get_mut(dungeon).unwrap().players.remove(name);
        }

        if dirty {
            println!();
            for dungeon in dungeons.values() {
                info!("{:?}", dungeon);
            }
        }
    }
}

fn build_dungeons(update: DbUpdate) -> (HashMap<u64, Dungeon>, HashMap<u32, u64>) {
    let mut dungeons = HashMap::new();
    let mut rooms = HashMap::new();

    // fill dungeons
    for e in update.dungeon_state.inserts {
        dungeons.insert(e.row.entity_id, Dungeon {
            loc: [e.row.location.x, e.row.location.z],
            state: DungeonState::Closed(0),
            players: HashSet::new(),
        });
        rooms.insert(e.row.entity_id, HashSet::new());
    }

    // fill state, add starting room
    for e in update.dimension_network_state.inserts {
        let dungeon = dungeons.get_mut(&e.row.entity_id).unwrap();
        dungeon.state = match (e.row.collapse_respawn_timestamp, e.row.is_collapsed) {
            (0, false) => DungeonState::Open,
            (n, false) => DungeonState::Cleared(n),
            (n, true) => DungeonState::Closed(n),
        };

        let rooms = rooms.get_mut(&e.row.entity_id).unwrap();
        rooms.insert(e.row.entrance_dimension_id);
    }

    // build portal connections for source dim -> target dim
    let mut portals = HashMap::new();
    let mut dimensions = HashMap::new();

    for e in update.location_state.inserts {
        dimensions.insert(e.row.entity_id, e.row.dimension);
    }
    for e in update.portal_state.inserts {
        let from = dimensions.get(&e.row.entity_id).unwrap();
        portals.entry(*from)
            .or_insert_with(HashSet::new)
            .insert(e.row.destination_dimension);
    }

    // find all reachable dimensions (except for overworld) for each dungeon
    for dungeon in rooms.values_mut() {
        let mut size = 0;
        while dungeon.len() != size {
            size = dungeon.len();
            for room in dungeon.clone() {
                for next in portals.get(&room).unwrap() {
                    dungeon.insert(*next);
                }
            }
            dungeon.remove(&1);
        }
    }

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

fn spawn_threads(ctx: &Arc<DbConnection>, rx: UnboundedReceiver<DbUpdate>)
    -> (JoinHandle<()>, JoinHandle<()>, JoinHandle<()>) {(
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
        consume(ctx, rx)
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