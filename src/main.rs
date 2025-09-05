mod config;
use config::{Config, Configurable};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use bindings::region::{DbConnection, DbUpdate};
use bindings::sdk::DbContext;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Debug, Clone, Copy)]
enum DungeonState { Open, Cleared(u64), Closed(u64) }
#[derive(Debug, Clone)]
struct Dungeon {
    loc: [i32; 2],
    state: DungeonState,
    rooms: HashSet<u32>,
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

    ctx.subscription_builder()
        .on_error({
            let ctx = ctx.clone();
            move |_, e| { error!("subscription error: {}", e); let _ = ctx.disconnect(); }
        })
        .subscribe([
            "SELECT * FROM dungeon_state;",
            concat!("SELECT n.* FROM dimension_network_state n",
                    " JOIN dungeon_state d ON n.entity_id = d.entity_id;"),
            concat!("SELECT p.* FROM portal_state p",
                    " JOIN location_state l ON p.entity_id = l.entity_id;"),
            concat!("SELECT l.* FROM location_state l",
                    " JOIN portal_state p ON l.entity_id = p.entity_id;"),
        ]);

    let _ = exec.await;
}

async fn consume(ctx: Arc<DbConnection>, mut rx: UnboundedReceiver<DbUpdate>) {
    let mut dungeons = HashMap::new();
    let mut portal = HashMap::new();

    while let Some(update) = rx.recv().await {
        for e in update.dungeon_state.inserts {
            dungeons.insert(e.row.entity_id, Dungeon {
                loc: [e.row.location.x, e.row.location.z],
                state: DungeonState::Closed(0),
                rooms: HashSet::new(),
            });
        }

        for e in update.dimension_network_state.inserts {
            let Some(dungeon) = dungeons.get_mut(&e.row.building_id) else {continue};
            dungeon.state = match (e.row.collapse_respawn_timestamp, e.row.is_collapsed) {
                (0, false) => DungeonState::Open,
                (n, false) => DungeonState::Cleared(n),
                (n, true) => DungeonState::Closed(n),
            };

            if dungeon.rooms.is_empty() { dungeon.rooms.insert(e.row.entrance_dimension_id); }
        }

        let mut dims = HashMap::new();
        for e in update.location_state.inserts {
            dims.insert(e.row.entity_id, e.row.dimension);
        }

        for e in update.portal_state.inserts {
            let Some(from) = dims.get(&e.row.entity_id) else {continue};
            portal.entry(*from)
                .or_insert_with(HashSet::new)
                .insert(e.row.destination_dimension);
        }

        for dungeon in dungeons.values_mut() {
            if dungeon.rooms.len() > 1 {continue};

            let mut size = 0;
            while dungeon.rooms.len() != size {
                size = dungeon.rooms.len();
                for room in dungeon.rooms.clone() {
                    for next in portal.get(&room).unwrap() {
                        dungeon.rooms.insert(*next);
                    }
                }
                dungeon.rooms.remove(&1);
            }

            info!("dungeon: {:?}", dungeon);
        }
    }
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