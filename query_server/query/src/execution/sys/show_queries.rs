use std::sync::Arc;

use async_trait::async_trait;
use chrono::Duration;
use datafusion::arrow::{
    array::{Int64Builder, StringBuilder},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use snafu::ResultExt;
use spi::{
    query::execution::{ArrowSnafu, ExecutionError, Output, QueryState, QueryStateMachineRef},
    service::protocol::QueryId,
};

use crate::dispatcher::query_tracker::QueryTracker;

use super::SystemTask;

pub struct ShowQueriesTask {
    query_tracker: Arc<QueryTracker>,
}

impl ShowQueriesTask {
    pub fn new(query_tracker: Arc<QueryTracker>) -> Self {
        Self { query_tracker }
    }
}

#[async_trait]
impl SystemTask for ShowQueriesTask {
    async fn execute(
        &self,
        _query_state_machine: QueryStateMachineRef,
    ) -> std::result::Result<Output, ExecutionError> {
        let mut result_builder = ShowQueriesResultBuilder::new();

        self.query_tracker.running_queries().iter().for_each(|e| {
            let info = e.info();
            let status = e.status();
            result_builder.add_column(
                info.query_id(),
                info.user_name(),
                info.query(),
                status.query_state(),
                status.duration(),
            )
        });

        Ok(Output::StreamData(
            result_builder.schema(),
            result_builder.build().context(ArrowSnafu)?,
        ))
    }
}

struct ShowQueriesResultBuilder {
    schema: SchemaRef,

    query_ids: StringBuilder,
    users: StringBuilder,
    queries: StringBuilder,
    states: StringBuilder,
    durations: Int64Builder,
}

impl ShowQueriesResultBuilder {
    fn new() -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("query_id", DataType::Utf8, false),
            Field::new("user", DataType::Utf8, false),
            Field::new("query", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("duration", DataType::Int64, false),
        ]));

        Self {
            schema,
            query_ids: StringBuilder::new(),
            users: StringBuilder::new(),
            queries: StringBuilder::new(),
            states: StringBuilder::new(),
            durations: Int64Builder::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_column(
        &mut self,
        query_id: QueryId,
        user: impl AsRef<str>,
        query: impl AsRef<str>,
        state: &QueryState,
        duration: &Duration,
    ) {
        self.query_ids.append_value(query_id.to_string());
        self.users.append_value(user.as_ref());
        self.queries.append_value(query.as_ref());
        self.states.append_value(state.as_ref());
        self.durations.append_value(duration.num_milliseconds());
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn build(self) -> Result<Vec<RecordBatch>, ArrowError> {
        let ShowQueriesResultBuilder {
            schema,
            mut query_ids,
            mut users,
            mut queries,
            mut states,
            mut durations,
        } = self;

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(query_ids.finish()),
                Arc::new(users.finish()),
                Arc::new(queries.finish()),
                Arc::new(states.finish()),
                Arc::new(durations.finish()),
            ],
        )?;

        Ok(vec![batch])
    }
}
