pub mod serializer;

use std::sync::Arc;

use crate::data_source::{DataSourceError, WriteSnafu};
use crate::data_source::{RecordBatchSink, RecordBatchSinkProvider};
use crate::data_source::{Result, SinkMetadata};
use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::{
    arrow::record_batch::RecordBatch, physical_plan::metrics::ExecutionPlanMetricsSet,
};
use object_store::path::Path;
use object_store::DynObjectStore;
use snafu::ResultExt;
use spi::query::datasource::WriteContext;
use trace::debug;

use super::DynRecordBatchSerializer;

pub struct ObjectStoreSink {
    ctx: WriteContext,
    s: Arc<DynRecordBatchSerializer>,
    object_store: Arc<DynObjectStore>,
}

#[async_trait]
impl RecordBatchSink for ObjectStoreSink {
    async fn append(&self, _record_batch: RecordBatch) -> Result<SinkMetadata> {
        Err(DataSourceError::Unimplement {
            msg: "ObjectStoreRecordBatchSink::append".to_string(),
        })
    }

    async fn stream_write(&self, stream: SendableRecordBatchStream) -> Result<SinkMetadata> {
        debug!("Process ObjectStoreRecordBatchSink::stream_write");

        let path = self.ctx.location().child(format!(
            "part-{}{}",
            self.ctx.partition(),
            self.ctx.file_extension()
        ));

        let (rows_writed, data) = self.s.to_bytes(&self.ctx, stream).await?;
        let bytes_writed = data.len();

        self.object_store
            .put(&path, data)
            .await
            .context(WriteSnafu)?;

        debug!("Generated parquet file: {}", path);

        Ok(SinkMetadata::new(rows_writed, bytes_writed))
    }
}

pub struct ObjectStoreSinkProvider {
    location: Path,
    object_store: Arc<DynObjectStore>,
    serializer: Arc<DynRecordBatchSerializer>,
    file_extension: String,
}

impl ObjectStoreSinkProvider {
    pub fn new(
        location: Path,
        object_store: Arc<DynObjectStore>,
        serializer: Arc<DynRecordBatchSerializer>,
        file_extension: String,
    ) -> Self {
        Self {
            location,
            object_store,
            serializer,
            file_extension,
        }
    }
}

impl RecordBatchSinkProvider for ObjectStoreSinkProvider {
    fn create_batch_sink(
        &self,
        _metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Box<dyn RecordBatchSink> {
        let ctx = WriteContext::new(
            self.location.clone(),
            partition,
            self.file_extension.clone(),
        );

        Box::new(ObjectStoreSink {
            ctx,
            s: self.serializer.clone(),
            object_store: self.object_store.clone(),
        })
    }
}
