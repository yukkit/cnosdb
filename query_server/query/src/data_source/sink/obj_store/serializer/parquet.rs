use std::{io::Write, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    parquet::{self, arrow::ArrowWriter},
    physical_plan::SendableRecordBatchStream,
};
use futures::pin_mut;
use futures::TryStreamExt;
use snafu::ResultExt;
use spi::query::datasource::WriteContext;

use crate::data_source::{
    sink::RecordBatchSerializer, BuildArrowWriterSnafu, DataSourceError, FetchBatchSnafu,
    SerializeSnafu,
};

pub struct ParquetRecordBatchSerializer {}

#[async_trait]
impl RecordBatchSerializer for ParquetRecordBatchSerializer {
    async fn to_bytes(
        &self,
        ctx: &WriteContext,
        stream: SendableRecordBatchStream,
    ) -> Result<(usize, Bytes), DataSourceError> {
        let (data, parquet_file_meta) = to_parquet_bytes(ctx, stream).await?;
        let num_rows = parquet_file_meta.num_rows as usize;
        Ok((num_rows, Bytes::from(data)))
    }
}

pub async fn to_parquet_bytes(
    ctx: &WriteContext,
    batches: SendableRecordBatchStream,
) -> Result<(Vec<u8>, parquet::format::FileMetaData), DataSourceError> {
    let mut bytes = vec![];
    let meta = to_parquet(ctx, batches, &mut bytes).await?;
    bytes.shrink_to_fit();

    Ok((bytes, meta))
}

pub async fn to_parquet<W>(
    _ctx: &WriteContext,
    batches: SendableRecordBatchStream,
    output: W,
) -> Result<parquet::format::FileMetaData, DataSourceError>
where
    W: Write + Send,
{
    let schema = batches.schema();

    let stream = batches;
    pin_mut!(stream);

    let mut writer =
        ArrowWriter::try_new(output, Arc::clone(&schema), None).context(BuildArrowWriterSnafu)?;

    while let Some(batch) = stream.try_next().await.context(FetchBatchSnafu)? {
        writer.write(&batch).context(SerializeSnafu)?;
    }

    let file_meta_data = writer
        .close()
        .map_err(|source| DataSourceError::CloseWriter { source })?;

    Ok(file_meta_data)
}
