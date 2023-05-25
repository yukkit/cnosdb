use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, Stream, StreamExt, TryFutureExt};
use metrics::count::U64Counter;
use models::arrow_array::build_arrow_array_builders;
use models::meta_data::VnodeId;
use models::SeriesKey;
use tskv::query_iterator::QueryOption;
use tskv::EngineRef;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::reader::MemoryRecordBatchStream;
use crate::{CoordinatorRecordBatchStream, SendableCoordinatorRecordBatchStream};

type Result<T, E = CoordinatorError> = std::result::Result<T, E>;

pub struct LocalTskvTagScanStream {
    state: StreamState,
    data_out: U64Counter,
}

impl LocalTskvTagScanStream {
    pub fn new(
        vnode_id: VnodeId,
        option: QueryOption,
        kv: EngineRef,
        data_out: U64Counter,
    ) -> Self {
        let futrue = async move {
            let (tenant, db, table) = (
                option.table_schema.tenant.as_str(),
                option.table_schema.db.as_str(),
                option.table_schema.name.as_str(),
            );

            let mut keys = Vec::new();

            for series_id in kv
                .get_series_id_by_filter(tenant, db, table, vnode_id, option.split.tags_filter())
                .await
                .map_err(CoordinatorError::from)?
                .into_iter()
            {
                if let Some(key) = kv.get_series_key(tenant, db, vnode_id, series_id).await? {
                    keys.push(key)
                }
            }

            let mut batches = vec![];
            for chunk in keys.chunks(option.batch_size) {
                let record_batch = series_keys_to_record_batch(option.df_schema.clone(), chunk)
                    .map_err(CoordinatorError::from)?;
                batches.push(record_batch)
            }

            Ok(Box::pin(MemoryRecordBatchStream::new(batches))
                as SendableCoordinatorRecordBatchStream)
        };

        let state = StreamState::Open(Box::pin(futrue));

        Self { state, data_out }
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Open(future) => match ready!(future.try_poll_unpin(cx)) {
                    Ok(stream) => {
                        self.state = StreamState::Scan(stream);
                    }
                    Err(err) => return Poll::Ready(Some(Err(err))),
                },
                StreamState::Scan(stream) => return stream.poll_next_unpin(cx),
            }
        }
    }
}

impl CoordinatorRecordBatchStream for LocalTskvTagScanStream {}

impl Stream for LocalTskvTagScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_inner(cx);
        if let Poll::Ready(Some(Ok(batch))) = &poll {
            self.data_out.inc(batch.get_array_memory_size() as u64);
        }
        poll
    }
}

pub type StreamFuture = BoxFuture<'static, CoordinatorResult<SendableCoordinatorRecordBatchStream>>;

enum StreamState {
    Open(StreamFuture),
    Scan(SendableCoordinatorRecordBatchStream),
}

fn series_keys_to_record_batch(
    schema: SchemaRef,
    series_keys: &[SeriesKey],
) -> Result<RecordBatch, ArrowError> {
    let tag_key_array = schema.fields.iter().map(|f| f.name()).collect::<Vec<_>>();
    let mut array_builders = build_arrow_array_builders(&schema, series_keys.len())?;
    for key in series_keys {
        for (k, array_builder) in tag_key_array.iter().zip(&mut array_builders) {
            let tag_value = key
                .tag_string_val(k)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            let builder = array_builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("Cast failed for List Builder<StringBuilder> during nested data parsing");
            builder.append_option(tag_value)
        }
    }
    let columns = array_builders
        .into_iter()
        .map(|mut b| b.finish())
        .collect::<Vec<_>>();
    let record_batch = RecordBatch::try_new(schema.clone(), columns)?;
    Ok(record_batch)
}
