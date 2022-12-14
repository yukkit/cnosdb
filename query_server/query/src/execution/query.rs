use std::sync::Arc;

use async_trait::async_trait;
use datafusion::scheduler::Scheduler;
use futures::stream::AbortHandle;
use futures::TryStreamExt;
use parking_lot::Mutex;
use snafu::ResultExt;
use spi::query::dispatcher::{QueryInfo, QueryStatus};
use spi::query::execution::{ExecutionError, Output};
use spi::query::{
    execution::{QueryExecution, QueryStateMachineRef},
    logical_planner::QueryPlan,
    optimizer::Optimizer,
    ScheduleSnafu,
};

use spi::query::{QueryError, Result};
use trace::debug;

pub struct SqlQueryExecution {
    query_state_machine: QueryStateMachineRef,
    plan: QueryPlan,
    optimizer: Arc<dyn Optimizer + Send + Sync>,
    scheduler: Arc<Scheduler>,

    abort_handle: Mutex<Option<AbortHandle>>,
}

impl SqlQueryExecution {
    pub fn new(
        query_state_machine: QueryStateMachineRef,
        plan: QueryPlan,
        optimizer: Arc<dyn Optimizer + Send + Sync>,
        scheduler: Arc<Scheduler>,
    ) -> Self {
        Self {
            query_state_machine,
            plan,
            optimizer,
            scheduler,
            abort_handle: Mutex::new(None),
        }
    }
}

impl SqlQueryExecution {
    async fn start(&self) -> Result<Output> {
        // begin optimize
        self.query_state_machine.begin_optimize();
        let optimized_physical_plan = self
            .optimizer
            .optimize(&self.plan.df_plan, &self.query_state_machine.session)
            .await?;
        self.query_state_machine.end_optimize();

        // begin schedule
        self.query_state_machine.begin_schedule();
        let stream = self
            .scheduler
            .schedule(
                optimized_physical_plan,
                self.query_state_machine.session.inner().task_ctx(),
            )
            .context(ScheduleSnafu)?
            .stream();
        let schema_ref = stream.schema();
        let execution_result =
            stream
                .try_collect::<Vec<_>>()
                .await
                .map_err(|source| QueryError::Execution {
                    source: ExecutionError::Arrow { source },
                })?;
        self.query_state_machine.end_schedule();

        Ok(Output::StreamData(schema_ref, execution_result))
    }
}

#[async_trait]
impl QueryExecution for SqlQueryExecution {
    async fn start(&self) -> Result<Output> {
        let (task, abort_handle) = futures::future::abortable(self.start());

        {
            *self.abort_handle.lock() = Some(abort_handle);
        }

        task.await.map_err(|_| QueryError::Cancel)?
    }

    fn cancel(&self) -> Result<()> {
        debug!(
            "cancel sql query execution: query_id: {:?}, sql: {}, state: {:?}",
            &self.query_state_machine.query_id,
            self.query_state_machine.query.content(),
            self.query_state_machine.state()
        );

        // change state
        self.query_state_machine.cancel();
        // stop future task
        if let Some(e) = self.abort_handle.lock().as_ref() {
            e.abort()
        };

        debug!(
            "canceled sql query execution: query_id: {:?}, sql: {}, state: {:?}",
            &self.query_state_machine.query_id,
            self.query_state_machine.query.content(),
            self.query_state_machine.state()
        );
        Ok(())
    }

    fn info(&self) -> QueryInfo {
        let qsm = &self.query_state_machine;
        QueryInfo::new(
            qsm.query_id,
            qsm.query.content().to_string(),
            *qsm.session.tenant_id(),
            qsm.session.tenant().to_string(),
            qsm.query.context().user_info().desc().clone(),
        )
    }

    fn status(&self) -> QueryStatus {
        QueryStatus::new(
            self.query_state_machine.state().clone(),
            self.query_state_machine.duration(),
        )
    }
}
