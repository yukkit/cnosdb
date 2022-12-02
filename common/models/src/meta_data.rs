use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::schema::{DatabaseSchema, TskvTableSchema};

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Resource {
    pub id: u64,
    pub cpu: u64,
    pub disk: u64,
    pub memory: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct UserInfo {
    pub name: String,
    pub pwd: String,
    pub perm: u64, //read write admin bitmap
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct NodeInfo {
    pub id: u64,
    pub tcp_addr: String,
    pub http_addr: String,
    pub status: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct BucketInfo {
    pub id: u32,
    pub start_time: i64,
    pub end_time: i64,
    pub shard_group: Vec<ReplcationSet>,
}

impl BucketInfo {
    pub fn vnode_for(&self, id: u64) -> ReplcationSet {
        let index = id as usize % self.shard_group.len();

        self.shard_group[index].clone()
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ReplcationSet {
    pub id: u32,
    pub vnodes: Vec<VnodeInfo>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct VnodeInfo {
    pub id: u32,
    pub node_id: u64,
}

// CREATE DATABASE <database_name>
// [WITH [TTL <duration>]
// [SHARD <n>]
// [VNODE_DURATION <duration>]
// [REPLICA <n>]
// [PRECISION {'ms' | 'us' | 'ns'}]]
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct DatabaseInfo {
    // pub name: String,
    // pub shard: u32,
    // pub ttl: i64,
    // pub vnode_duration: i64,
    // pub replications: u32,
    pub schema: DatabaseSchema,

    pub buckets: Vec<BucketInfo>,
    pub tables: HashMap<String, TskvTableSchema>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct TenantMetaData {
    pub version: u64,
    pub users: HashMap<String, UserInfo>,
    pub dbs: HashMap<String, DatabaseInfo>,
    pub data_nodes: HashMap<String, NodeInfo>,
}

impl TenantMetaData {
    pub fn new() -> Self {
        Self {
            version: 0,
            users: HashMap::new(),
            dbs: HashMap::new(),
            data_nodes: HashMap::new(),
        }
    }

    pub fn table_schema(&self, db: &str, tab: &str) -> Option<TskvTableSchema> {
        if let Some(info) = self.dbs.get(db) {
            if let Some(schema) = info.tables.get(tab) {
                return Some(schema.clone());
            }
        }

        None
    }

    pub fn database_min_ts(&self, name: &str) -> Option<i64> {
        if let Some(db) = self.dbs.get(name) {
            let ttl = db.schema.config.ttl_or_default().time_stamp();
            let now = crate::utils::now_timestamp();

            return Some(now - ttl);
        }

        None
    }

    pub fn bucket_by_timestamp(&self, db_name: &str, ts: i64) -> Option<&BucketInfo> {
        if let Some(db) = self.dbs.get(db_name) {
            if let Some(bucket) = db
                .buckets
                .iter()
                .find(|bucket| (ts >= bucket.start_time) && (ts < bucket.end_time))
            {
                return Some(bucket);
            }
        }

        None
    }

    pub fn mapping_bucket(&self, db_name: &str, start: i64, end: i64) -> Vec<BucketInfo> {
        if let Some(db) = self.dbs.get(db_name) {
            let mut result = vec![];
            for item in db.buckets.iter() {
                if end < item.start_time || start > item.end_time {
                    continue;
                }

                result.push(item.clone());
            }

            return result;
        }

        vec![]
    }
}

pub fn get_time_range(ts: i64, duration: i64) -> (i64, i64) {
    if duration <= 0 {
        (std::i64::MIN, std::i64::MAX)
    } else {
        (
            (ts / duration) * duration,
            (ts / duration) * duration + duration,
        )
    }
}

pub fn allocation_replication_set(
    nodes: Vec<NodeInfo>,
    shards: u32,
    replica: u32,
    begin_seq: u32,
) -> (Vec<ReplcationSet>, u32) {
    let node_count = nodes.len() as u32;
    let mut replica = replica;
    if replica == 0 {
        replica = 1
    } else if replica > node_count {
        replica = node_count
    }

    let mut incr_id = begin_seq;

    let mut index = begin_seq;
    let mut group = vec![];
    for _ in 0..shards {
        let mut repl_set = ReplcationSet {
            id: incr_id,
            vnodes: vec![],
        };
        incr_id += 1;

        for _ in 0..replica {
            repl_set.vnodes.push(VnodeInfo {
                id: incr_id,
                node_id: nodes.get((index % node_count) as usize).unwrap().id,
            });
            incr_id += 1;
            index += 1;
        }

        group.push(repl_set);
    }

    (group, incr_id - begin_seq)
}