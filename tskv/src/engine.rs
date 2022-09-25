use crate::error::Result;
use crate::index::IndexResult;
use crate::tsm::DataBlock;
use crate::{Options, TimeRange, TsKv};
use async_trait::async_trait;
use models::{FieldId, FieldInfo, SeriesId, SeriesKey, Tag, Timestamp, ValueType};
use protos::{
    kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest},
    models as fb_models,
};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use trace::debug;
use tracing::log::info;
pub type EngineRef = Arc<dyn Engine>;

#[async_trait]
pub trait Engine: Send + Sync + Debug {
    async fn write(&self, write_batch: WritePointsRpcRequest) -> Result<WritePointsRpcResponse>;

    async fn write_from_wal(
        &self,
        write_batch: WritePointsRpcRequest,
        seq: u64,
    ) -> Result<WritePointsRpcResponse>;

    fn read(
        &self,
        db: &str,
        sids: Vec<SeriesId>,
        time_range: &TimeRange,
        fields: Vec<u32>,
    ) -> HashMap<SeriesId, HashMap<u32, Vec<DataBlock>>>;

    fn drop_database(&self, database: &str) -> Result<()>;

    fn drop_table(&self, database: &str, table: &str) -> Result<()>;

    fn delete_series(
        &self,
        db: &str,
        sids: &[SeriesId],
        field_ids: &[FieldId],
        time_range: &TimeRange,
    ) -> Result<()>;

    fn get_table_schema(&self, db: &str, tab: &str) -> Result<Option<Vec<FieldInfo>>>;

    async fn get_series_id_list(&self, db: &str, tab: &str, tags: &[Tag]) -> IndexResult<Vec<u64>>;
    fn get_series_key(&self, db: &str, sid: u64) -> IndexResult<Option<SeriesKey>>;
}

#[derive(Debug, Default)]
pub struct MockEngine {}

#[async_trait]
impl Engine for MockEngine {
    async fn write(&self, write_batch: WritePointsRpcRequest) -> Result<WritePointsRpcResponse> {
        debug!("writing point");
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points).unwrap();

        debug!("writed point: {:?}", fb_points);

        Ok(WritePointsRpcResponse {
            version: write_batch.version,
            points: vec![],
        })
    }

    async fn write_from_wal(
        &self,
        write_batch: WritePointsRpcRequest,
        seq: u64,
    ) -> Result<WritePointsRpcResponse> {
        debug!("write point");
        Ok(WritePointsRpcResponse {
            version: write_batch.version,
            points: vec![],
        })
    }

    fn read(
        &self,
        db: &str,
        sids: Vec<SeriesId>,
        time_range: &TimeRange,
        fields: Vec<u32>,
    ) -> HashMap<SeriesId, HashMap<u32, Vec<DataBlock>>> {
        HashMap::new()
    }

    fn drop_database(&self, database: &str) -> Result<()> {
        println!("drop_database {:?}", database);
        Ok(())
    }

    fn drop_table(&self, database: &str, table: &str) -> Result<()> {
        println!("drop_table db:{:?}, table:{:?}", database, table);
        Ok(())
    }

    fn delete_series(
        &self,
        db: &str,
        sids: &[SeriesId],
        field_ids: &[FieldId],
        time_range: &TimeRange,
    ) -> Result<()> {
        todo!()
    }

    fn get_table_schema(&self, db: &str, tab: &str) -> Result<Option<Vec<FieldInfo>>> {
        debug!("get_table_schema db:{:?}, table:{:?}", db, tab);

        let types = vec![
            ValueType::Unknown,
            ValueType::Float,
            ValueType::Integer,
            ValueType::Unsigned,
            ValueType::Boolean,
            ValueType::String,
        ];

        let fields: Vec<FieldInfo> = types
            .iter()
            .enumerate()
            .map(|(i, e)| FieldInfo::new(i as u64, Vec::from(e.to_string()), e.to_owned(), 0_u8))
            .collect();

        Ok(Some(fields))
    }

    async fn get_series_id_list(&self, db: &str, tab: &str, tags: &[Tag]) -> IndexResult<Vec<u64>> {
        Ok(vec![])
    }

    fn get_series_key(&self, db: &str, sid: u64) -> IndexResult<Option<SeriesKey>> {
        Ok(None)
    }
}
