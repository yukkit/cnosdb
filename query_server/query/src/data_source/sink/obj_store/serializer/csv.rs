use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::csv::WriterBuilder;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{pin_mut, TryStreamExt};
use spi::query::datasource::WriteContext;

use crate::data_source::{sink::RecordBatchSerializer, FetchBatchSnafu, Result, SerializeCsvSnafu};
use snafu::ResultExt;

pub struct CsvRecordBatchSerializer {
    with_header: bool,
    delimiter: u8,
}

impl CsvRecordBatchSerializer {
    pub fn new(with_header: bool, delimiter: u8) -> Self {
        Self {
            with_header,
            delimiter,
        }
    }
}

#[async_trait]
impl RecordBatchSerializer for CsvRecordBatchSerializer {
    async fn to_bytes(
        &self,
        _ctx: &WriteContext,
        stream: SendableRecordBatchStream,
    ) -> Result<(usize, Bytes)> {
        pin_mut!(stream);

        let mut num_rows = 0;
        let mut bytes = vec![];
        {
            let mut writer = WriterBuilder::new()
                .has_headers(self.with_header)
                .with_delimiter(self.delimiter)
                .build(&mut bytes);

            while let Some(batch) = stream.try_next().await.context(FetchBatchSnafu)? {
                num_rows += batch.num_rows();
                writer.write(&batch).context(SerializeCsvSnafu)?;
            }
        }

        Ok((num_rows, Bytes::from(bytes)))
    }
}
