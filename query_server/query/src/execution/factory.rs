use std::sync::Arc;

use datafusion::logical_expr::{Extension, LogicalPlan};
use models::runtime::executor::DedicatedExecutor;
use spi::query::datasource::stream::checker::StreamCheckerManagerRef;
use spi::query::execution::{QueryExecutionFactory, QueryExecutionRef, QueryStateMachineRef};
use spi::query::logical_planner::{Plan, QueryPlan};
use spi::query::optimizer::Optimizer;
use spi::query::scheduler::SchedulerRef;
use spi::QueryError;
use tskv::kv_option::QueryOptions;

use super::query::SqlQueryExecution;
use super::stream::trigger::executor::{TriggerExecutorFactory, TriggerExecutorFactoryRef};
use super::stream::{MicroBatchStreamExecutionBuilder, MicroBatchStreamExecutionDesc};
use super::sys::SystemExecution;
use crate::dispatcher::query_tracker::QueryTracker;
use crate::execution::ddl::DDLExecution;
use crate::extension::logical::plan_node::table_writer_merge::TableWriterMergePlanNode;
use crate::extension::logical::utils::extract_stream_providers;
use crate::extension::utils::downcast_plan_node;

pub struct SqlQueryExecutionFactory {
    optimizer: Arc<dyn Optimizer + Send + Sync>,
    scheduler: SchedulerRef,
    query_tracker: Arc<QueryTracker>,
    trigger_executor_factory: TriggerExecutorFactoryRef,
    runtime: Arc<DedicatedExecutor>,
    stream_checker_manager: StreamCheckerManagerRef,
}

impl SqlQueryExecutionFactory {
    #[inline(always)]
    pub fn new(
        optimizer: Arc<dyn Optimizer + Send + Sync>,
        scheduler: SchedulerRef,
        query_tracker: Arc<QueryTracker>,
        stream_checker_manager: StreamCheckerManagerRef,
        config: Arc<QueryOptions>,
    ) -> Self {
        // Only do periodic scheduling, no need for many threads
        let trigger_executor_runtime =
            DedicatedExecutor::new("stream-trigger", config.stream_trigger_cpu);
        let trigger_executor_factory = Arc::new(TriggerExecutorFactory::new(Arc::new(
            trigger_executor_runtime,
        )));

        // perform stream-related preparations, not actual operator execution
        let runtime = Arc::new(DedicatedExecutor::new(
            "stream-executor",
            config.stream_executor_cpu,
        ));

        Self {
            optimizer,
            scheduler,
            query_tracker,
            trigger_executor_factory,
            runtime,
            stream_checker_manager,
        }
    }
}

pub type QueryExecutionFactoryRef = Arc<dyn QueryExecutionFactory + Send + Sync>;

impl QueryExecutionFactory for SqlQueryExecutionFactory {
    fn create_query_execution(
        &self,
        plan: Plan,
        state_machine: QueryStateMachineRef,
    ) -> Result<QueryExecutionRef, QueryError> {
        match plan {
            Plan::Query(query_plan) => {
                // 获取执行计划中所有涉及到的stream source
                let stream_providers = extract_stream_providers(&query_plan);

                // (含有流表, explain, dml)
                match (
                    !stream_providers.is_empty(),
                    query_plan.is_explain(),
                    is_dml(&query_plan),
                ) {
                    (false, _, _) | (true, true, _) => Ok(Arc::new(SqlQueryExecution::new(
                        state_machine,
                        query_plan,
                        self.optimizer.clone(),
                        self.scheduler.clone(),
                    ))),
                    (true, false, true) => {
                        // 流操作
                        // stream source + dml + !explain
                        let options = state_machine.session.inner().state().config().into();
                        let exec =
                            MicroBatchStreamExecutionBuilder::new(MicroBatchStreamExecutionDesc {
                                plan: Arc::new(query_plan),
                                options,
                            })
                            .with_stream_providers(stream_providers)
                            .build(
                                state_machine,
                                self.scheduler.clone(),
                                self.trigger_executor_factory.clone(),
                                self.runtime.clone(),
                            )?;

                        Ok(Arc::new(exec))
                    }
                    (true, false, false) => {
                        // stream source + !dml + !explain
                        Err(QueryError::NotImplemented {
                            err: "Stream table can only be used as source table in insert select statements.".to_string(),
                        })
                    }
                }
            }
            Plan::DDL(ddl_plan) => Ok(Arc::new(DDLExecution::new(
                state_machine,
                self.stream_checker_manager.clone(),
                ddl_plan,
            ))),
            Plan::SYSTEM(sys_plan) => Ok(Arc::new(SystemExecution::new(
                state_machine,
                sys_plan,
                self.query_tracker.clone(),
            ))),
        }
    }
}

fn is_dml(query_plan: &QueryPlan) -> bool {
    match &query_plan.df_plan {
        LogicalPlan::Dml(_) => true,
        LogicalPlan::Extension(Extension { node }) => {
            downcast_plan_node::<TableWriterMergePlanNode>(node.as_ref()).is_some()
        }
        _ => false,
    }
}

impl Drop for SqlQueryExecutionFactory {
    fn drop(&mut self) {
        self.runtime.shutdown();
    }
}
