mod config;
use config::{Config, Configurable};

use std::sync::Arc;
use bindings::region::{DbConnection, DbUpdate};
use bindings::sdk::DbContext;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::task::JoinHandle;
use tracing::{error, info};

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
            concat!("SELECT d.* FROM dungeon_state d"),
            concat!("SELECT n.* FROM dimension_network_state n",
                    "  JOIN dungeon_state d ON n.building_id = d.entity_id"),
        ]);

    let _ = exec.await;
}

async fn consume(mut rx: UnboundedReceiver<DbUpdate>) {
    while let Some(update) = rx.recv().await {
        for row in update.dungeon_state.inserts {
            info!("{:?}", row.row);
        }

        for row in update.dimension_network_state.inserts {
            info!("{:?}", row.row);
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
    tokio::spawn(consume(rx)),
    // shutdown trigger
    tokio::spawn({
        let ctx = ctx.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            let _ = ctx.disconnect();
        }
    }),
)}