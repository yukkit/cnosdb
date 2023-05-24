use meta::model::MetaRef;
use models::meta_data::VnodeInfo;

use crate::reader::replica_selection::ReplicaSelectionPolicy;

/// 基于拓扑感知的vnode副本选择策略
///
/// TODO 目前仅感知是否在同一节点，后续需要考虑机架、机房
pub struct TopologyAwareReplicaSelectionPolicy {
    // TODO 节点管理器
    node_manager: MetaRef,
}

impl TopologyAwareReplicaSelectionPolicy {
    pub fn new(node_manager: MetaRef) -> Self {
        Self { node_manager }
    }
}

impl ReplicaSelectionPolicy for TopologyAwareReplicaSelectionPolicy {
    fn select(&self, shards: Vec<Vec<VnodeInfo>>, limit: isize) -> Vec<Vec<VnodeInfo>> {
        if limit < 0 {
            return shards;
        }

        let is_same_host = |id| self.node_manager.node_id() == id;

        shards
            .into_iter()
            .map(|mut replicas| {
                replicas.sort_by_key(|k| if is_same_host(k.node_id) { 0 } else { i32::MAX });

                replicas
                    .into_iter()
                    .take(limit as usize)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }
}
