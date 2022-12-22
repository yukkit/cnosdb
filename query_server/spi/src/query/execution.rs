use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use meta::error::MetaError;
use parking_lot::RwLock;
use snafu::Snafu;

use crate::service::protocol::Query;
use crate::service::protocol::QueryId;
use meta::meta_client::MetaRef;

use super::dispatcher::{QueryInfo, QueryStatus};
use super::{logical_planner::Plan, session::IsiphoSessionCtx, Result};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ExecutionError {
    #[snafu(display("External err: {}", source))]
    External { source: DataFusionError },

    #[snafu(display("Arrow err: {}", source))]
    Arrow { source: ArrowError },

    #[snafu(display("Metadata operator err: {}", source))]
    Metadata { source: MetaError },

    #[snafu(display("Query not found: {:?}", query_id))]
    QueryNotFound { query_id: QueryId },

    #[snafu(display("Coordinator operator err: {}", source))]
    CoordinatorErr {
        source: coordinator::errors::CoordinatorError,
    },
}

#[async_trait]
pub trait QueryExecution: Send + Sync {
    // 开始
    async fn start(&self) -> Result<Output>;
    // 停止
    fn cancel(&self) -> Result<()>;
    // query状态
    // 查询计划
    // 静态信息
    fn info(&self) -> QueryInfo;
    // 运行时信息
    fn status(&self) -> QueryStatus;
    // sql
    // 资源占用（cpu时间/内存/吞吐量等）
    // ......
}
// pub trait Output {
//     fn as_any(&self) -> &dyn Any;
// }
#[derive(Clone)]
pub enum Output {
    StreamData(SchemaRef, Vec<RecordBatch>),
    Nil(()),
}

impl Output {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::StreamData(schema, _) => schema.clone(),
            Self::Nil(_) => Arc::new(Schema::empty()),
        }
    }

    pub fn chunk_result(&self) -> &[RecordBatch] {
        match self {
            Self::StreamData(_, result) => result,
            Self::Nil(_) => &[],
        }
    }

    pub fn num_rows(&self) -> usize {
        self.chunk_result()
            .iter()
            .map(|e| e.num_rows())
            .reduce(|p, c| p + c)
            .unwrap_or(0)
    }

    /// Returns the number of records affected by the query operation
    ///
    /// If it is a select statement, returns the number of rows in the result set
    ///
    /// -1 means unknown
    ///
    /// panic! when StreamData's number of records greater than i64::Max
    pub fn affected_rows(&self) -> i64 {
        match self {
            Self::StreamData(_, result) => result
                .iter()
                .map(|e| e.num_rows())
                .reduce(|p, c| p + c)
                .unwrap_or(0) as i64,
            Self::Nil(_) => 0,
        }
    }
}

pub trait QueryExecutionFactory {
    fn create_query_execution(
        &self,
        plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> Arc<dyn QueryExecution>;
}

pub type QueryStateMachineRef = Arc<QueryStateMachine>;

pub struct QueryStateMachine {
    pub session: IsiphoSessionCtx,
    pub query_id: QueryId,
    pub query: Query,
    pub meta: MetaRef,
    pub coord: CoordinatorRef,

    start_time: DateTime<Utc>,
    end_time: RwLock<Option<DateTime<Utc>>>,

    state: AtomicPtr<QueryState>,
}

impl QueryStateMachine {
    pub fn begin(
        query_id: QueryId,
        query: Query,
        session: IsiphoSessionCtx,
        coord: CoordinatorRef,
    ) -> Self {
        let meta = coord.meta_manager();

        Self {
            query_id,
            session,
            query,
            meta,
            coord,
            start_time: Utc::now(),
            end_time: Default::default(),
            state: AtomicPtr::new(Box::into_raw(Box::new(QueryState::ACCEPTING))),
        }
    }

    pub fn begin_analyze(&self) {
        // TODO record time
        self.translate_to(Box::new(QueryState::RUNNING(RUNNING::ANALYZING)));
    }

    pub fn end_analyze(&self) {
        // TODO record time
    }

    pub fn begin_optimize(&self) {
        // TODO record time
        self.translate_to(Box::new(QueryState::RUNNING(RUNNING::OPTMIZING)));
    }

    pub fn end_optimize(&self) {
        // TODO
    }

    pub fn begin_schedule(&self) {
        // TODO
        self.translate_to(Box::new(QueryState::RUNNING(RUNNING::SCHEDULING)));
    }

    pub fn end_schedule(&self) {
        // TODO
    }

    pub fn finish(&self) {
        self.translate_to_done(DONE::FINISHED);
    }

    pub fn cancel(&self) {
        self.translate_to_done(DONE::CANCELLED);
    }

    pub fn fail(&self) {
        self.translate_to_done(DONE::FAILED);
    }

    pub fn state(&self) -> &QueryState {
        unsafe { &*self.state.load(Ordering::Relaxed) }
    }

    /// ms
    pub fn start_time(&self) -> i64 {
        self.start_time.timestamp_millis()
    }

    pub fn duration(&self) -> Duration {
        self.end_time.read().unwrap_or(Utc::now()) - self.start_time
    }

    fn translate_to(&self, state: Box<QueryState>) {
        self.state.store(Box::into_raw(state), Ordering::Relaxed);
    }

    fn translate_to_done(&self, state: DONE) {
        self.translate_to(Box::new(QueryState::DONE(state)));
        // record end time
        let _ = self.end_time.write().replace(Utc::now());
    }
}

#[derive(Debug, Clone)]
pub enum QueryState {
    ACCEPTING,
    RUNNING(RUNNING),
    DONE(DONE),
}

impl AsRef<str> for QueryState {
    fn as_ref(&self) -> &str {
        match self {
            QueryState::ACCEPTING => "ACCEPTING",
            QueryState::RUNNING(e) => e.as_ref(),
            QueryState::DONE(e) => e.as_ref(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RUNNING {
    DISPATCHING,
    ANALYZING,
    OPTMIZING,
    SCHEDULING,
}

impl AsRef<str> for RUNNING {
    fn as_ref(&self) -> &str {
        match self {
            Self::DISPATCHING => "DISPATCHING",
            Self::ANALYZING => "ANALYZING",
            Self::OPTMIZING => "OPTMIZING",
            Self::SCHEDULING => "SCHEDULING",
        }
    }
}

#[derive(Debug, Clone)]
pub enum DONE {
    FINISHED,
    FAILED,
    CANCELLED,
}

impl AsRef<str> for DONE {
    fn as_ref(&self) -> &str {
        match self {
            Self::FINISHED => "FINISHED",
            Self::FAILED => "FAILED",
            Self::CANCELLED => "CANCELLED",
        }
    }
}
