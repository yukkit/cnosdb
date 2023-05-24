use std::sync::Arc;

use metrics::count::U64Counter;
use models::meta_data::{NodeId, VnodeInfo};
use tokio::runtime::Runtime;
use trace::debug;
use tskv::query_iterator::QueryOption;
use tskv::EngineRef;

use super::table_scan::local::LocalTskvTableScanStream;
use super::tag_scan::local::LocalTskvTagScanStream;
use super::CombinedRecordBatchStream;
use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::service::CoordServiceMetrics;
use crate::SendableCoordinatorRecordBatchStream;

pub struct QueryExecutor {
    option: QueryOption,

    runtime: Arc<Runtime>,
    kv_inst: Option<EngineRef>,

    data_out: U64Counter,
}

impl QueryExecutor {
    pub fn new(
        option: QueryOption,
        runtime: Arc<Runtime>,
        kv_inst: Option<EngineRef>,
        metrics: Arc<CoordServiceMetrics>,
    ) -> Self {
        let data_out = metrics.data_out(
            option.table_schema.tenant.as_str(),
            option.table_schema.db.as_str(),
        );
        Self {
            option,
            runtime,
            kv_inst,
            data_out,
        }
    }

    pub fn local_node_executor(
        &self,
        vnodes: Vec<VnodeInfo>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        let mut routines = vec![];
        let now = tokio::time::Instant::now();
        for vnode in vnodes.iter() {
            debug!("query local vnode: {:?}, now: {:?}", vnode, now);
            let stream = self.local_vnode_executor(vnode.clone())?;
            routines.push(stream);
        }

        Ok(Box::pin(CombinedRecordBatchStream::new(routines)))
    }

    pub fn local_node_tag_scan(
        &self,
        node_id: NodeId,
        vnode: Vec<VnodeInfo>,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        let kv = self
            .kv_inst
            .as_ref()
            .ok_or(CoordinatorError::KvInstanceNotFound {
                vnode_id: 0,
                node_id,
            })?
            .clone();

        let streams = vnode
            .into_iter()
            .map(|vnode| {
                let stream = LocalTskvTagScanStream::new(
                    vnode.id,
                    self.option.clone(),
                    kv.clone(),
                    self.data_out.clone(),
                );
                Box::pin(stream) as SendableCoordinatorRecordBatchStream
            })
            .collect::<Vec<_>>();
        let stream = CombinedRecordBatchStream::new(streams);

        Ok(Box::pin(stream))
    }

    fn local_vnode_executor(
        &self,
        vnode: VnodeInfo,
    ) -> CoordinatorResult<SendableCoordinatorRecordBatchStream> {
        let kv_inst = self
            .kv_inst
            .as_ref()
            .ok_or(CoordinatorError::KvInstanceNotFound {
                vnode_id: vnode.id,
                node_id: vnode.node_id,
            })?
            .clone();

        let stream = LocalTskvTableScanStream::new(
            vnode.id,
            self.option.clone(),
            kv_inst,
            self.runtime.clone(),
            self.data_out.clone(),
        );

        Ok(Box::pin(stream))
    }
}
