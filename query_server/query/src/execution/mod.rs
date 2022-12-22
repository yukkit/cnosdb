use async_trait::async_trait;
use chrono::{Duration, Utc};
use models::auth::user::{UserDesc, UserOptions};
use spi::query::{
    dispatcher::{QueryInfo, QueryStatus, QueryStatusBuilder},
    execution::{Output, QueryExecution, QueryState, RUNNING},
    QueryError,
};

mod ddl;
pub mod factory;
mod query;
mod sys;

pub struct QueryExecutionMock {}

#[async_trait]
impl QueryExecution for QueryExecutionMock {
    async fn start(&self) -> std::result::Result<Output, QueryError> {
        Ok(Output::Nil(()))
    }
    fn cancel(&self) -> std::result::Result<(), QueryError> {
        Ok(())
    }
    fn info(&self) -> QueryInfo {
        let options = UserOptions::default();
        let desc = UserDesc::new(0_u128, "user".to_string(), options, true);
        QueryInfo::new(
            1_u64.into(),
            "test".to_string(),
            0_u128,
            "tenant".to_string(),
            desc,
        )
    }
    fn status(&self) -> QueryStatus {
        unsafe {
            QueryStatusBuilder::default()
                .state(QueryState::RUNNING(RUNNING::SCHEDULING))
                .duration(Duration::hours(1))
                .start_time(Utc::now().timestamp_millis())
                .build()
                .unwrap_unchecked()
        }
    }
}
