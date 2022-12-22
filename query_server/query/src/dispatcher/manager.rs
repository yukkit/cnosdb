use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::scheduler::Scheduler;
use models::oid::Oid;
use snafu::ResultExt;

use spi::query::dispatcher::{QueryInfo, QueryStatus};
use spi::query::execution::Output;
use spi::{
    query::{
        ast::ExtStatement,
        dispatcher::QueryDispatcher,
        execution::{QueryExecutionFactory, QueryStateMachine},
        logical_planner::LogicalPlanner,
        optimizer::Optimizer,
        parser::Parser,
        session::IsiphoSessionCtxFactory,
    },
    service::protocol::{Query, QueryId},
};

use spi::query::QueryError::{self, BuildQueryDispatcher};
use spi::query::{BuildFunctionMetaSnafu, LogicalPlannerSnafu, Result};
use tokio::runtime::Runtime;

use crate::extension::expr::load_all_functions;
use crate::function::simple_func_manager::SimpleFunctionMetadataManager;
use crate::metadata::{ContextProviderExtension, MetadataProvider};
use crate::{
    execution::factory::SqlQueryExecutionFactory, sql::logical::planner::DefaultLogicalPlanner,
};

use super::query_history::manager::QueryHistoryManager;
use super::query_history::HistoryManagerOptionsBuilder;
use super::query_tracker::QueryTracker;

#[derive(Clone)]
pub struct SimpleQueryDispatcher {
    coord: CoordinatorRef,
    session_factory: Arc<IsiphoSessionCtxFactory>,
    // TODO resource manager
    // query tracker
    query_tracker: Arc<QueryTracker>,
    query_history_manager: Arc<QueryHistoryManager>,
    // parser
    parser: Arc<dyn Parser + Send + Sync>,
    // get query execution factory
    query_execution_factory: Arc<dyn QueryExecutionFactory + Send + Sync>,
}

#[async_trait]
impl QueryDispatcher for SimpleQueryDispatcher {
    fn start(&self) {
        // TODO
    }

    fn stop(&self) {
        // TODO
    }

    fn create_query_id(&self) -> QueryId {
        QueryId::next_id()
    }

    fn query_info(&self, _id: &QueryId) {
        // TODO
    }

    async fn execute_query(
        &self,
        tenant_id: Oid,
        query_id: QueryId,
        query: &Query,
    ) -> Result<Output> {
        let session = self
            .session_factory
            .create_isipho_session_ctx(query.context().clone(), tenant_id);

        let mut func_manager = SimpleFunctionMetadataManager::default();
        load_all_functions(&mut func_manager).context(BuildFunctionMetaSnafu)?;
        let scheme_provider = MetadataProvider::new(
            self.coord.clone(),
            func_manager,
            self.query_tracker.clone(),
            session.clone(),
        );

        let logical_planner = DefaultLogicalPlanner::new(scheme_provider);

        let statements = self.parser.parse(query.content())?;

        // not allow multi statement
        if statements.len() > 1 {
            return Err(QueryError::MultiStatement {
                num: statements.len(),
                sql: query.content().to_string(),
            });
        }

        let stmt = statements[0].clone();

        let query_state_machine = Arc::new(QueryStateMachine::begin(
            query_id,
            query.clone(),
            session.clone(),
            self.coord.clone(),
        ));

        let result = self
            .execute_statement(stmt, &logical_planner, query_state_machine)
            .await?;

        Ok(result)
    }

    fn running_query_infos(&self) -> Vec<QueryInfo> {
        self.query_tracker
            .running_queries()
            .iter()
            .map(|e| e.info())
            .collect()
    }

    fn running_query_status(&self) -> Vec<QueryStatus> {
        self.query_tracker
            .running_queries()
            .iter()
            .map(|e| e.status())
            .collect()
    }

    fn cancel_query(&self, id: &QueryId) {
        self.query_tracker.query(id).map(|e| e.cancel());
    }
}

impl SimpleQueryDispatcher {
    async fn execute_statement<S: ContextProviderExtension>(
        &self,
        stmt: ExtStatement,
        logical_planner: &DefaultLogicalPlanner<S>,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> Result<Output> {
        // begin analyze
        query_state_machine.begin_analyze();
        let logical_plan = logical_planner
            .create_logical_plan(stmt.clone(), &query_state_machine.session)
            .context(LogicalPlannerSnafu)?;
        query_state_machine.end_analyze();

        let execution = self
            .query_execution_factory
            .create_query_execution(logical_plan, query_state_machine.clone());

        // TrackedQuery.drop() is called implicitly when the value goes out of scope,
        self.query_tracker
            .try_track_query(query_state_machine.query_id, execution)?
            .start()
            .await
    }
}

#[derive(Default, Clone)]
pub struct SimpleQueryDispatcherBuilder {
    history_runtime: Option<Arc<Runtime>>,
    coord: Option<CoordinatorRef>,
    session_factory: Option<Arc<IsiphoSessionCtxFactory>>,
    parser: Option<Arc<dyn Parser + Send + Sync>>,
    // cnosdb optimizer
    optimizer: Option<Arc<dyn Optimizer + Send + Sync>>,
    // TODO 需要封装 scheduler
    scheduler: Option<Arc<Scheduler>>,

    queries_limit: usize,
}

impl SimpleQueryDispatcherBuilder {
    pub fn with_history_runtime(mut self, history_runtime: Arc<Runtime>) -> Self {
        self.history_runtime = Some(history_runtime);
        self
    }

    pub fn with_coord(mut self, coord: CoordinatorRef) -> Self {
        self.coord = Some(coord);
        self
    }

    pub fn with_session_factory(mut self, session_factory: Arc<IsiphoSessionCtxFactory>) -> Self {
        self.session_factory = Some(session_factory);
        self
    }

    pub fn with_parser(mut self, parser: Arc<dyn Parser + Send + Sync>) -> Self {
        self.parser = Some(parser);
        self
    }

    pub fn with_optimizer(mut self, optimizer: Arc<dyn Optimizer + Send + Sync>) -> Self {
        self.optimizer = Some(optimizer);
        self
    }

    pub fn with_scheduler(mut self, scheduler: Arc<Scheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn with_queries_limit(mut self, limit: u32) -> Self {
        self.queries_limit = limit as usize;
        self
    }

    pub fn build(self) -> Result<SimpleQueryDispatcher> {
        let history_runtime = self.history_runtime.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of history runtime".to_string(),
        })?;

        let coord = self.coord.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of coord".to_string(),
        })?;

        let session_factory = self.session_factory.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of session_factory".to_string(),
        })?;

        let parser = self.parser.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of parser".to_string(),
        })?;

        let optimizer = self.optimizer.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of optimizer".to_string(),
        })?;

        let scheduler = self.scheduler.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of scheduler".to_string(),
        })?;

        let options = HistoryManagerOptionsBuilder::default()
            .build()
            .map_err(|e| BuildQueryDispatcher { err: e.to_string() })?;
        let query_history_manager = Arc::new(QueryHistoryManager::new(
            options,
            history_runtime,
            coord.clone(),
        ));
        let sender = query_history_manager.start();
        let query_tracker = Arc::new(QueryTracker::new(self.queries_limit, sender));

        let query_execution_factory = Arc::new(SqlQueryExecutionFactory::new(
            optimizer,
            scheduler,
            query_tracker.clone(),
        ));

        Ok(SimpleQueryDispatcher {
            coord,
            session_factory,
            parser,
            query_execution_factory,
            query_tracker,
            query_history_manager,
        })
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use coordinator::service::MockCoordinator;
    use spi::query::execution::QueryExecution;

    use crate::{
        dispatcher::{
            query_history::{manager::QueryHistoryManager, HistoryManagerOptionsBuilder},
            query_tracker::QueryTracker,
        },
        execution::QueryExecutionMock,
    };

    #[test]
    fn test() {
        trace::init_default_global_tracing("/tmp", "test_rust.log", "debug");

        let coord = Arc::new(MockCoordinator {});
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(4)
                .build()
                .unwrap(),
        );
        let options = HistoryManagerOptionsBuilder::default()
            .flush_interval(Duration::from_secs(7))
            .build()
            .unwrap();

        let query_history_manager = Arc::new(QueryHistoryManager::new(
            options,
            runtime.clone(),
            coord.clone(),
        ));

        let sender = query_history_manager.start();

        let query_tracker = Arc::new(QueryTracker::new(1_000_000, sender));
        let execution = Arc::new(QueryExecutionMock {});
        let info = execution.info();

        for _ in (0..1_000_000).into_iter() {
            let tracked_query = query_tracker
                .try_track_query(info.query_id(), execution.clone())
                .unwrap();
            drop(tracked_query);
            // tokio::time::sleep(Duration::from_secs(3)).await;
            std::thread::sleep(Duration::from_secs(3));
        }
    }
}
