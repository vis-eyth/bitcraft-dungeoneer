mod config;
mod data;
use crate::{config::*, data::*};

use std::collections::HashMap as StdHashMap;
use std::sync::Arc;
use std::time::Duration;
use bindings::region::{DbConnection, DbUpdate, EnemyType, SubscriptionHandle};
use bindings::sdk::{DbContext, IntoQueries, SubscriptionHandle as SdkSubscriptionHandle};
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use serde_json::{json, Value};
use tokio::sync::{mpsc::{unbounded_channel, UnboundedReceiver}, watch::{channel, Receiver, Sender}};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, info};

#[derive(Debug, Clone, Copy)]
enum DungeonState { Open, Cleared(u64), Closed(u64) }
#[derive(Debug, Clone)]
enum BossState { Alive(u64), Fighting(u64), Dead(Vec<String>) }
#[derive(Debug, Clone)]
struct Dungeon {
    loc: [i32; 2],
    state: DungeonState,
    players: HashSet<String>,
    boss: BossState,
}
type DungeonMap = HashMap<u64, Dungeon>;

impl DungeonState {
    pub fn title(&self, active: bool, boss: bool) -> String {
        match self {
            DungeonState::Open if boss => "boss fight!".to_string(),
            DungeonState::Open if active => "active!".to_string(),
            DungeonState::Open => "open!".to_string(),
            DungeonState::Cleared(ts) => format!("clear, closing <t:{}:R>", ts / 1000),
            DungeonState::Closed(ts) => format!("closed, opening <t:{}:R>", ts / 1000),
        }
    }
    pub fn color(&self, active: bool, boss: bool) -> i32 {
        match self {
            DungeonState::Open if boss => 14364802,  // #db3082
            DungeonState::Open if active => 5814783, // #58b9ff
            DungeonState::Open => 8107618,           // #7bb662
            DungeonState::Cleared(_) => 16765697,    // #ffd301
            DungeonState::Closed(_) => 14032671,     // #d61f1f
        }
    }
}
impl BossState {
    fn alive(id: u64, fighting: bool) -> Self {
        if fighting { BossState::Fighting(id) }
        else { BossState::Alive(id) }
    }
}
impl Dungeon {
    fn skip_message(&self) -> bool {
        matches!(self.state, DungeonState::Cleared(_)) && !matches!(self.boss, BossState::Dead(_))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Message {
    title: String,
    description: Option<String>,
    color: i32,
}
impl Message {
    pub fn for_dungeon(name: String, state: &Dungeon) -> Self {
        let boss = matches!(&state.boss, BossState::Fighting(_));
        let active = !state.players.is_empty();

        Self {
            title: format!("{} `[{}N {}E]` - {}", name, state.loc[1] / 3, state.loc[0] / 3,
                           state.state.title(active, boss)),
            description: match (&state.boss, &state.state, active) {
                (BossState::Dead(contrib), DungeonState::Cleared(_), _) =>
                    Some(format!("player contribution:\n{}", contrib.iter().join("\n"))),
                (_, _, true) =>
                    Some(format!("current players: `{}`", state.players.iter().join("`, `"))),
                _ =>
                    None,
            },
            color: state.state.color(active, boss),
        }
    }
    pub fn as_json(&self) -> Value {
        match self.description {
            Some(ref d) =>
                json!({"title": self.title, "description": d, "color": self.color}),
            None =>
                json!({"title": self.title, "color": self.color})
        }
    }
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

    let (tx_post, rx_post) = channel(Default::default());
    let (exec, _, _, _) = spawn_threads(&ctx, rx, tx_post, rx_post, config.webhook_url(), config.entity_name());

    let _ = exec.await;
}

async fn post(mut rx: Receiver<DungeonMap>, webhook_url: String, entity_name: StdHashMap<u64, String>) {
    let client = reqwest::Client::new();
    let mut last = HashMap::new(); // last sent message per dungeon

    while let Ok(()) = rx.changed().await {
        let mut post = Vec::new();
        sleep(Duration::from_millis(500)).await;

        for (id, dungeon) in rx.borrow_and_update().iter() {
            if dungeon.skip_message() {continue}

            let name = entity_name.get(&id).map_or("Dungeon".to_string(), |n| n.clone());
            let msg = Message::for_dungeon(name, dungeon);

            if last.get(id).is_none_or(|m| *m != msg) {
                post.push(msg.as_json());
                last.insert(*id, msg);
            }
        }

        if webhook_url.is_empty() {
            if post.is_empty() { info!("received event, no update")}
            for e in post { info!("update: {:?}", e) }
            continue;
        }

        info!("update: {} dungeons", post.len());
        if post.is_empty() {continue}

        let payload = json!({"embeds": post});
        let response = client
            .post(&webhook_url)
            .header("Content-Type", "application/json")
            .body(payload.to_string())
            .send()
            .await;

        match response {
            Ok(response) if !response.status().is_success() => {
                error!("failed to post event: {:#?}", response);
            }
            Err(e) => {
                error!("failed to post event: {:#?}", e);
            }
            _ => {}
        }
    }
}

async fn consume(ctx: Arc<DbConnection>, mut rx: UnboundedReceiver<DbUpdate>, tx: Sender<DungeonMap>) {
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
    for dim in lookup.keys() { queries.extend([
        format!("SELECT * FROM mobile_entity_state WHERE dimension = {};", dim),
        format!("SELECT e.* FROM enemy_state e JOIN mobile_entity_state m ON e.entity_id = m.entity_id WHERE m.dimension = {};", dim),
        format!("SELECT c.* FROM contribution_state c JOIN mobile_entity_state m ON c.enemy_entity_id = m.entity_id WHERE m.dimension = {};", dim),
    ]) }
    subscribe(ctx.clone(), queries);

    let mut players = HashMap::new();
    let mut bosses = HashMap::new();
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

        let mut contributions = HashMap::new();
        for e in update.enemy_state.inserts {
            if !matches!(e.row.enemy_type, EnemyType::Sentinel) {continue};
            bosses.entry(e.row.entity_id).or_insert_with(HashMap::new);
        }
        for e in update.contribution_state.deletes {
            let Some(boss) = bosses.get_mut(&e.row.enemy_entity_id) else {continue};
            boss.remove(&e.row.player_entity_id);

            contributions.entry(e.row.enemy_entity_id).or_insert_with(HashMap::new)
                .insert(e.row.player_entity_id, e.row.contribution);
        }
        for e in update.contribution_state.inserts {
            let Some(boss) = bosses.get_mut(&e.row.enemy_entity_id) else {continue};
            boss.insert(e.row.player_entity_id, e.row.contribution);
        }

        for (dungeon, changes) in entity_loc(update.mobile_entity_state, &lookup) {
            let dungeon = dungeons.get_mut(&dungeon).unwrap();

            for id in changes.insert {
                if let Some(name) = players.get(&id) {
                    dirty |= dungeon.players.insert(name.clone());
                }
                if let Some(c) = bosses.get(&id) {
                    dirty |= update_boss(&mut dungeon.boss, BossState::alive(id, !c.is_empty()));
                }
            }
            for id in changes.delete {
                if let Some(name) = players.get(&id) {
                    dirty |= dungeon.players.remove(name);
                }
                if let Some(c) = contributions.get(&id) {
                    let mut c = c.iter().collect_vec();
                    c.sort_by_key(|(_, v)| **v as i32);
                    c.reverse();

                    let sum = c.iter().map(|(_, v)| *v).sum::<f32>() / 100_f32;
                    let c = c.iter()
                        .map(|(id, v)| format!("- `{}`: `{:.1}%`", players.get(*id).unwrap(), **v / sum))
                        .collect_vec();

                    dirty |= update_boss(&mut dungeon.boss, BossState::Dead(c));
                    bosses.remove(&id);
                }
            }
        }

        for dungeon in dungeons.values_mut() {
            let id = match dungeon.boss {
                BossState::Alive(id) => id,
                BossState::Fighting(id) => id,
                BossState::Dead(_) => {continue},
            };

            if let Some(c) = bosses.get_mut(&id) {
                dirty |= update_boss(&mut dungeon.boss, BossState::alive(id, !c.is_empty()));
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
    tx_post: Sender<DungeonMap>,
    rx_post: Receiver<DungeonMap>,
    webhook_url: String,
    entity_name: StdHashMap<u64, String>,
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
        post(rx_post, webhook_url, entity_name)
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