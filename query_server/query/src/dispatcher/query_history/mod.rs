mod buffer;
pub mod manager;
pub mod sender;
mod writer;

use std::time::Duration;

use derive_builder::Builder;
use line_protocol::{FieldValue, Line};

pub const QUERY_HISTORY_TABLE_NAME: &str = "query_history";

#[derive(Debug, Default, Builder)]
#[builder(setter(into, strip_option), default)]
pub struct QueryHistoryEntry {
    query_id: String,
    query_text: String,
    user_name: String,
    tenant_name: String,
    execution_status: String,
    error_message: Option<String>,
    start_time: i64,
    end_time: i64,
    // ms
    total_elapsed_time: i64,
    bytes_scanned_from_storage: Option<i64>,
    bytes_written_to_storage: Option<i64>,
    bytes_read_from_result: Option<i64>,
    rows_inserted: Option<i64>,
    rows_selected: Option<i64>,
}

impl QueryHistoryEntry {
    pub fn to_line_protocol<'a>(&'a self, measurement: &'a str) -> Line<'a> {
        let tags: Vec<(&str, &str)> = vec![
            ("user_name", &self.user_name),
            ("tenant_name", &self.tenant_name),
        ];

        let mut fields: Vec<(&str, FieldValue)> = vec![
            (
                "query_id",
                FieldValue::Str(self.query_id.as_bytes().to_vec()),
            ),
            (
                "query_text",
                FieldValue::Str(self.query_text.as_bytes().to_vec()),
            ),
            (
                "execution_status",
                FieldValue::Str(self.execution_status.as_bytes().to_vec()),
            ),
            ("end_time", FieldValue::I64(self.end_time)),
            (
                "total_elapsed_time",
                FieldValue::I64(self.total_elapsed_time),
            ),
        ];

        let timestamp = self.start_time;

        Line {
            hash_id: 0,
            measurement,
            tags,
            fields,
            timestamp,
        }
    }
}

#[derive(Debug, Builder)]
#[builder(setter(into, strip_option))]
pub struct HistoryManagerOptions {
    #[builder(default = "8192")]
    buffer_capacity: usize,
    #[builder(default = "Duration::from_secs(60)")]
    flush_interval: Duration,
}

impl HistoryManagerOptions {
    pub fn buffer_capacity(&self) -> usize {
        self.buffer_capacity
    }

    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }
}
