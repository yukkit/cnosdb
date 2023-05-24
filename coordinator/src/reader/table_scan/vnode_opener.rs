use std::sync::Arc;

use meta::model::MetaRef;
use models::meta_data::VnodeInfo;
use tokio::runtime::Runtime;
use tskv::query_iterator::{QueryOption, TskvSourceMetrics};
use tskv::EngineRef;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::reader::table_scan::local::LocalTskvTableScanStream;
use crate::reader::table_scan::remote::TonicTskvRecordBatchStream;
use crate::reader::{VnodeOpenFuture, VnodeOpener};
use crate::service::CoordServiceMetrics;
use crate::SendableCoordinatorRecordBatchStream;

pub struct TemporaryVnodeOpener {
    kv_inst: Option<EngineRef>,
    runtime: Arc<Runtime>,
    meta: MetaRef,
    metrics: TskvSourceMetrics,
    coord_metrics: Arc<CoordServiceMetrics>,
}

impl TemporaryVnodeOpener {
    pub fn new(
        kv_inst: Option<EngineRef>,
        runtime: Arc<Runtime>,
        meta: MetaRef,
        metrics: TskvSourceMetrics,
        coord_metrics: Arc<CoordServiceMetrics>,
    ) -> Self {
        Self {
            kv_inst,
            runtime,
            meta,
            metrics,
            coord_metrics,
        }
    }
}

impl VnodeOpener for TemporaryVnodeOpener {
    fn open(&self, vnode: &VnodeInfo, option: &QueryOption) -> CoordinatorResult<VnodeOpenFuture> {
        let node_id = vnode.node_id;
        let vnode_id = vnode.id;
        let curren_nodet_id = self.meta.node_id();
        let kv_inst = self.kv_inst.clone();
        let runtime = self.runtime.clone();
        let metrics = self.metrics.clone();
        let coord_metrics = self.coord_metrics.clone();
        let option = option.clone();
        let admin_meta = self.meta.admin_meta();

        let future = async move {
            // TODO 请求路由的过程应该由通信框架决定，客户端只关心业务逻辑（请求目标和请求内容）
            if node_id == curren_nodet_id {
                // 路由到进程内的引擎
                let data_out = coord_metrics.data_out(
                    option.table_schema.tenant.as_str(),
                    option.table_schema.db.as_str(),
                );
                let kv_inst =
                    kv_inst.ok_or(CoordinatorError::KvInstanceNotFound { vnode_id, node_id })?;
                let stream =
                    LocalTskvTableScanStream::new(vnode_id, option, kv_inst, runtime, data_out);
                Ok(Box::pin(stream) as SendableCoordinatorRecordBatchStream)
            } else {
                // 路由到远程的引擎
                let request = {
                    let vnode_ids = vec![vnode_id];
                    let req = option
                        .to_query_record_batch_request(vnode_ids)
                        .map_err(CoordinatorError::from)?;
                    tonic::Request::new(req)
                };

                Ok(Box::pin(TonicTskvRecordBatchStream::new(
                    node_id, request, admin_meta, metrics,
                )) as SendableCoordinatorRecordBatchStream)
            }
        };

        Ok(Box::pin(future))
    }
}
