use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicI64;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use snafu::ResultExt;

use super::sink::{RecordBatchSink, RecordBatchSinkProvider};

use super::ParquetSnafu;
use super::Result;

pub struct ParquetRecordBatchSink {
    path: PathBuf,
    batch_num: AtomicI64,
}

#[async_trait]
impl RecordBatchSink for ParquetRecordBatchSink {
    async fn append(&self, record_batch: RecordBatch) -> Result<()> {
        let num = self
            .batch_num
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let filename = format!("part-{}.parquet", num);

        let path = self.path.join(&filename);
        let file = fs::File::create(&path)
            .unwrap_or_else(|_| panic!("create file error {:?}", path.as_os_str().to_str()));
        let mut writer =
            ArrowWriter::try_new(file, record_batch.schema(), None).context(ParquetSnafu)?;

        writer.write(&record_batch).context(ParquetSnafu)?;

        writer.close().context(ParquetSnafu)?;

        Ok(())
    }
}

pub struct ParquetRecordBatchSinkProvider {}

impl ParquetRecordBatchSinkProvider {
    pub fn new() -> Self {
        Self {}
    }
}

impl RecordBatchSinkProvider for ParquetRecordBatchSinkProvider {
    fn create_batch_sink(
        &self,
        _metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Box<dyn RecordBatchSink> {
        let path = "/tmp/iot/";
        let fs_path = std::path::Path::new(path).join(format!("{}", partition));

        fs::create_dir(&fs_path).unwrap();

        Box::new(ParquetRecordBatchSink {
            path: fs_path,
            batch_num: AtomicI64::new(0),
        })
    }
}
