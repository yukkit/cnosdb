use std::sync::Arc;

use meta::model::MetaRef;
use models::meta_data::{ReplicationSet, VnodeInfo};
use policy::random::RandomReplicaSelectionPolicy;
use policy::topology_aware::TopologyAwareReplicaSelectionPolicy;

mod policy;

pub type DynamicReplicaSelectionerRef = Arc<DynamicReplicaSelectioner>;

/// Dynamically select vnode replica
///
/// Selection strategy:
/// 1. The replica is normal (replica status)
/// 2. The copy is on the same node, same rack, and same computer room as the execution node (NodeSelector)
/// 3. The number of read tasks performed by the storage node is small (resource management)
/// 4. Randomly select a copy (random selection)
pub struct DynamicReplicaSelectioner {
    topology_aware: ReplicaSelectionPolicyRef,
    random: ReplicaSelectionPolicyRef,
}

impl DynamicReplicaSelectioner {
    pub fn new(node_manager: MetaRef) -> Self {
        let topology_aware = Arc::new(TopologyAwareReplicaSelectionPolicy::new(node_manager));
        let random = Arc::new(RandomReplicaSelectionPolicy::new());

        Self {
            topology_aware,
            random,
        }
    }

    /// Select the best replica for reading from the given vnode and its replicas
    pub fn select(&self, shards: Vec<ReplicationSet>) -> Vec<VnodeInfo> {
        let shards = shards.into_iter().map(|e| e.vnodes).collect();

        // TODO 1. 过滤掉不可用的副本
        // 2. 根据拓扑结构获取优先级最高的2个(至多)副本，因为目前未实现<3>，所以这里直接获取2个，防止所有请求都落在一个节点上
        let selected_shards = self.topology_aware.select(shards, 2);
        // TODO 3. 根据资源情况获取优先级最高的副本
        // 4. 从已选择的副本中随机选择一个副本
        let selected_shards = self.random.select(selected_shards, 1);

        filter_first_replica(selected_shards)
    }
}

fn filter_first_replica(shards: Vec<Vec<VnodeInfo>>) -> Vec<VnodeInfo> {
    let mut selected_replicas = Vec::new();
    for replicas in shards {
        if let Some(shard) = replicas.first() {
            selected_replicas.push(shard.to_owned());
        }
    }

    selected_replicas
}

pub type ReplicaSelectionPolicyRef = Arc<dyn ReplicaSelectionPolicy + Send + Sync>;

/// 给定vnode及其副本，选择一个最佳用于读取的副本
pub trait ReplicaSelectionPolicy {
    /// 从给定的vnode副本中选择N个最佳用于读取的副本
    ///
    /// Parameters:
    ///
    /// - shards: vnode副本
    /// - limit: 选择的副本数量, 如果副本数小于limit，则所有副本都会返回，如果limit小于0，则所有副本都会返回
    fn select(&self, shards: Vec<Vec<VnodeInfo>>, limit: isize) -> Vec<Vec<VnodeInfo>>;
}
