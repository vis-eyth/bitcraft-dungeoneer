use std::mem::replace;
use hashbrown::{HashMap, HashSet};
use bindings::{region, sdk::__codegen::TableUpdate};
use crate::*;

#[derive(Default)]
pub struct Update<T: Default> {
    pub insert: T,
    pub delete: T,
}

impl Into<Dungeon> for region::DungeonState {
    fn into(self) -> Dungeon {
        Dungeon {
            loc: [self.location.x, self.location.z],
            state: DungeonState::Closed(0),
            players: HashSet::new(),
            boss: BossState::Alive(0, false),
        }
    }
}
impl Into<DungeonState> for region::DimensionNetworkState {
    fn into(self) -> DungeonState {
        match (self.collapse_respawn_timestamp, self.is_collapsed) {
            (0, false) => DungeonState::Open,
            (n, false) => DungeonState::Cleared(n),
            (n, true) => DungeonState::Closed(n),
        }
    }
}

pub fn portals(location: TableUpdate<region::LocationState>, portal: TableUpdate<region::PortalState>)
    -> HashMap<u32, HashSet<u32>> {
    let mut dim = HashMap::new(); // entity id -> loc dim
    for e in location.inserts {
        dim.insert(e.row.entity_id, e.row.dimension);
    }

    let mut result = HashMap::new(); // loc dim -> reachable loc dim
    for e in portal.inserts {
        let src = dim.get(&e.row.entity_id).unwrap();
        let dst = e.row.destination_dimension;

        result.entry(*src).or_insert_with(HashSet::new).insert(dst);
    }

    result
}

pub fn entity_loc(update: TableUpdate<region::MobileEntityState>, cache: &HashMap<u32, u64>)
    -> HashMap<u64, Update<HashSet<u64>>> {
    let (mut insert, mut delete) = (HashSet::new(), HashSet::new());
    for e in update.inserts {
        let dungeon = cache.get(&e.row.dimension).unwrap();
        insert.insert((*dungeon, e.row.entity_id));
    }
    for e in update.deletes {
        let dungeon = cache.get(&e.row.dimension).unwrap();
        delete.insert((*dungeon, e.row.entity_id));
    }

    let mut result = HashMap::<u64, Update<HashSet<u64>>>::new();
    for e in &insert {
        if delete.contains(e) {continue};
        result.entry(e.0).or_insert_with(Default::default).insert.insert(e.1);
    }
    for e in &delete {
        if insert.contains(e) {continue};
        result.entry(e.0).or_insert_with(Default::default).delete.insert(e.1);
    }

    result
}

pub fn update_boss(boss: &mut BossState, new: BossState) -> bool {
    let old = replace(boss, new.clone());
    match (old, new) {
        (BossState::Dead(_), BossState::Dead(_)) => false,
        (BossState::Alive(_, true), BossState::Alive(_, true)) => false,
        (BossState::Alive(_, false), BossState::Alive(_, false)) => false,
        _ => true,
    }
}