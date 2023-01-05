use std::sync::Arc;

use async_trait::async_trait;
use coordinator::errors;
use datafusion::{
    arrow::{error::ArrowError, record_batch::RecordBatch},
    common::Result as DFResult,
    datasource::{listing::ListingTable, TableProvider},
    error::DataFusionError,
    execution::context::SessionState,
    parquet::errors::ParquetError,
    physical_plan::{metrics::ExecutionPlanMetricsSet, ExecutionPlan, SendableRecordBatchStream},
};
use futures::StreamExt;
use models::define_result;
use snafu::{ResultExt, Snafu};
use trace::warn;

use crate::{
    extension::physical::plan_node::table_writer::TableWriterExec, table::ClusterTable,
    utils::point_util::PointUtilError,
};

pub mod sink;
pub mod write_exec_ext;

define_result!(DataSourceError);

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum DataSourceError {
    #[snafu(display("Point util error, err: {}", source))]
    PointUtil { source: PointUtilError },

    #[snafu(display("Tskv operator, err: {}", source))]
    Tskv { source: tskv::Error },

    #[snafu(display("Coordinator operator, err: {}", source))]
    Coordinator { source: errors::CoordinatorError },

    #[snafu(display("invalid flatbuffers: {}", source))]
    InvalidFlatbuffer {
        source: flatbuffers::InvalidFlatbuffer,
    },

    #[snafu(display("error msg: {}", msg))]
    CommonError { msg: String },

    #[snafu(display("Failed to fetch record batch, error: {}", source))]
    FetchBatch { source: ArrowError },

    #[snafu(display("Not implemented: {}", msg))]
    Unimplement { msg: String },

    #[snafu(display("Failed to write to object storage, error: {}", source))]
    Write { source: object_store::Error },

    #[snafu(display("Failed to close parquet writer, error: {}", source))]
    CloseWriter { source: ParquetError },

    #[snafu(display("Failed to build parquet writer, error: {}", source))]
    BuildArrowWriter { source: ParquetError },

    #[snafu(display("Failed to serialize data to parquet bytes, error: {}", source))]
    Serialize { source: ParquetError },
}

#[async_trait]
pub trait WriteExecExt: Send + Sync {
    async fn write(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Arc<TableWriterExec>>;
}

#[async_trait]
impl WriteExecExt for dyn TableProvider {
    async fn write(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Arc<TableWriterExec>> {
        let table_write: &dyn WriteExecExt =
            if let Some(table) = self.as_any().downcast_ref::<ClusterTable>() {
                table as _
            } else if let Some(table) = self.as_any().downcast_ref::<ListingTable>() {
                table as _
            } else {
                warn!("Table not support write.");
                return Err(DataFusionError::Plan(
                    "Table not support write.".to_string(),
                ));
            };

        let result = table_write.write(state, input).await?;

        Ok(result)
    }
}

#[async_trait]
pub trait RecordBatchSink: Send + Sync {
    async fn append(&self, record_batch: RecordBatch) -> Result<SinkMetadata>;

    async fn stream_write(&self, mut stream: SendableRecordBatchStream) -> Result<SinkMetadata> {
        let mut meta = SinkMetadata::default();

        while let Some(batch) = stream.next().await {
            let batch: RecordBatch = batch.context(FetchBatchSnafu)?;
            meta.merge(self.append(batch).await?);
        }

        Ok(meta)
    }
}

pub trait RecordBatchSinkProvider: Send + Sync {
    fn create_batch_sink(
        &self,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Box<dyn RecordBatchSink>;
}

#[derive(Default)]
pub struct SinkMetadata {
    rows_writed: usize,
    bytes_writed: usize,
}

impl SinkMetadata {
    pub fn new(rows_writed: usize, bytes_writed: usize) -> Self {
        Self {
            rows_writed,
            bytes_writed,
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.rows_writed += other.rows_writed;
        self.bytes_writed += other.bytes_writed;
    }

    pub fn record_rows_writed(&mut self, rows_writed: usize) {
        self.rows_writed += rows_writed;
    }

    pub fn record_bytes_writed(&mut self, bytes_writed: usize) {
        self.bytes_writed += bytes_writed;
    }

    pub fn rows_writed(&self) -> usize {
        self.rows_writed
    }

    pub fn bytes_writed(&self) -> usize {
        self.bytes_writed
    }
}
