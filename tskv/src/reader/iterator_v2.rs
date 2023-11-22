use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::PhysicalExpr;
use models::meta_data::VnodeId;
use tokio::runtime::Runtime;
use trace::{debug, SpanRecorder};

use super::merge::{DataMerger, ParallelMergeAdapter};
use super::series::SeriesReader;
use super::{
    EmptySchemableTskvRecordBatchStream, Projection, QueryOption, SendableTskvRecordBatchStream,
    SeriesGroupRowIteratorMetrics,
};
use crate::reader::chunk::ChunkReader;
use crate::reader::filter::DataFilter;
use crate::reader::schema_alignmenter::SchemaAlignmenter;
use crate::reader::utils::group_overlapping_segments;
use crate::reader::BatchReaderRef;
use crate::tseries_family::SuperVersion;
use crate::{EngineRef, Error, Result};

pub async fn execute(
    _runtime: Arc<Runtime>,
    engine: EngineRef,
    query_option: QueryOption,
    vnode_id: VnodeId,
    span_recorder: SpanRecorder,
) -> Result<SendableTskvRecordBatchStream> {
    // TODO refac: None 代表没有数据，后续不需要执行
    let super_version = {
        let mut span_recorder = span_recorder.child("get super version");
        engine
            .get_db_version(
                &query_option.table_schema.tenant,
                &query_option.table_schema.db,
                vnode_id,
            )
            .await
            .map_err(|err| {
                span_recorder.error(err.to_string());
                err
            })?
    };

    let series_ids = {
        let mut span_recorder = span_recorder.child("get series ids by filter");
        engine
            .get_series_id_by_filter(
                &query_option.table_schema.tenant,
                &query_option.table_schema.db,
                &query_option.table_schema.name,
                vnode_id,
                query_option.split.tags_filter(),
            )
            .await
            .map_err(|err| {
                span_recorder.error(err.to_string());
                err
            })?
    };

    if series_ids.is_empty() {
        return Ok(Box::pin(EmptySchemableTskvRecordBatchStream::new(
            query_option.df_schema.clone(),
        )));
    }

    debug!(
        "Iterating rows: vnode_id: {vnode_id}, serie_ids_count: {}",
        series_ids.len()
    );

    if query_option.aggregates.is_some() {
        // TODO: 查新实现聚合下推
        return Err(Error::CommonError {
            reason: "aggregates push down is not supported yet".to_string(),
        });
    }

    let factory = SeriesGroupBatchReaderFactory::new(
        engine,
        query_option,
        super_version,
        span_recorder,
        ExecutionPlanMetricsSet::new(),
    );

    if let Some(reader) = factory.create(&series_ids).await? {
        return Ok(reader.process()?);
    }

    Ok(Box::pin(EmptySchemableTskvRecordBatchStream::new(
        factory.schema(),
    )))
}

struct SeriesGroupBatchReader {
    engine: EngineRef,
    query_option: Arc<QueryOption>,
    vnode_id: u32,
    super_version: Option<Arc<SuperVersion>>,
    series_ids: Arc<[u32]>,
    batch_size: usize,

    #[allow(unused)]
    span_recorder: SpanRecorder,
    metrics: SeriesGroupRowIteratorMetrics,
}

pub struct SeriesGroupBatchReaderFactory {
    engine: EngineRef,
    query_option: QueryOption,
    super_version: Option<Arc<SuperVersion>>,

    #[allow(unused)]
    span_recorder: SpanRecorder,
    metrics: ExecutionPlanMetricsSet,
}

impl SeriesGroupBatchReaderFactory {
    pub fn new(
        engine: EngineRef,
        query_option: QueryOption,
        super_version: Option<Arc<SuperVersion>>,
        span_recorder: SpanRecorder,
        metrics: ExecutionPlanMetricsSet,
    ) -> Self {
        Self {
            engine,
            query_option,
            super_version,
            span_recorder,
            metrics,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.query_option.df_schema.clone()
    }

    pub async fn create(&self, series_ids: &[u32]) -> Result<Option<BatchReaderRef>> {
        // let metrics = SeriesGroupRowIteratorMetrics::new(&metrics_set, partition),
        if series_ids.is_empty() {
            return Ok(None);
        }

        let schema = &self.query_option.df_schema;
        let time_fields_schema = &self.query_option.df_schema;
        // TODO 使用query下推的物理表达式
        let predicate: Option<Arc<dyn PhysicalExpr>> = None;

        if let Some(super_version) = &self.super_version {
            let vnode_id = super_version.ts_family_id;
            let projection = Projection::from(schema.as_ref());
            let time_ranges = self.query_option.split.time_ranges();
            let column_files = super_version.column_files(time_ranges.as_ref());

            // 通过sid获取serieskey
            let mut sid_keys = vec![];
            for sid in series_ids.iter() {
                let series_key = self
                    .engine
                    .get_series_key(
                        &self.query_option.table_schema.tenant,
                        &self.query_option.table_schema.db,
                        vnode_id,
                        *sid,
                    )
                    .await?
                    .ok_or_else(|| Error::CommonError {
                        reason: "Thi maybe a bug".to_string(),
                    })?;
                sid_keys.push((sid, series_key));
            }

            // 获取所有的复合条件的chunk Vec<(sid, Vec<(chunk, reader)>)>
            let mut series_chunk_readers = vec![];
            for (sid, series_key) in sid_keys {
                // 选择含有series的所有文件
                let files = column_files
                    .iter()
                    .filter(|cf| cf.contains_series_id(*sid))
                    .collect::<Vec<_>>();
                // 选择含有series的所有chunk
                let mut chunks = Vec::with_capacity(files.len());
                for f in files {
                    let reader = super_version.version.get_tsm_reader2(f.file_path()).await?;
                    let chunk = reader.chunk().get(sid).ok_or_else(|| Error::CommonError {
                        reason: "Thi maybe a bug".to_string(),
                    })?;
                    chunks.push((chunk.clone(), reader));
                }
                series_chunk_readers.push((series_key, chunks));
            }

            let mut series_readers: Vec<BatchReaderRef> = vec![];
            for (series_key, mut chunks) in series_chunk_readers {
                if chunks.is_empty() {
                    continue;
                }
                // TODO 通过物理表达式根据page统计信息过滤chunk

                // 对 chunk 按照时间顺序排序
                // 使用 group_overlapping_segments 函数来对具有重叠关系的chunk进行分组。
                chunks.sort_unstable_by_key(|(e, _)| *e.time_range());
                let grouped_chunks = group_overlapping_segments(&chunks);

                debug!(
                    "series_key: {:?}, grouped_chunks num: {}",
                    series_key,
                    grouped_chunks.len()
                );

                let readers = grouped_chunks
                    .into_iter()
                    .map(|chunks| -> Result<BatchReaderRef> {
                        let chunk_readers = chunks
                            .into_iter()
                            .map(|(chunk, reader)| -> Result<BatchReaderRef> {
                                let chunk_reader = Arc::new(ChunkReader::try_new(
                                    reader,
                                    chunk,
                                    &projection,
                                    self.query_option.batch_size,
                                )?);

                                // 数据过滤
                                if let Some(predicate) = &predicate {
                                    return Ok(Arc::new(DataFilter::new(
                                        predicate.clone(),
                                        chunk_reader,
                                    )));
                                }

                                Ok(chunk_reader)
                            })
                            .collect::<Result<Vec<_>>>()?;

                        let merger = Arc::new(DataMerger::new(chunk_readers));
                        // 用 Null 值补齐缺失的 Field 列
                        let reader =
                            Arc::new(SchemaAlignmenter::new(merger, time_fields_schema.clone()));
                        Ok(reader)
                    })
                    .collect::<Result<Vec<_>>>()?;

                // 根据 series key 补齐对应的 tag 列
                let series_reader = Arc::new(SeriesReader::new(series_key, Arc::new(readers)));
                // 用 Null 值补齐缺失的 tag 列
                let reader = Arc::new(SchemaAlignmenter::new(series_reader, schema.clone()));

                series_readers.push(reader)
            }

            if series_readers.is_empty() {
                return Ok(None);
            }

            // 不同series reader并行执行
            let reader = Arc::new(ParallelMergeAdapter::try_new(
                schema.clone(),
                series_readers,
            )?);

            return Ok(Some(reader));
        }

        Ok(None)
    }
}
