use coordinator::service::CoordinatorRef;
use line_protocol::Line;
use models::{consistency_level::ConsistencyLevel, line::parse_lines_to_points};
use protos::kv_service::WritePointsRpcRequest;
use trace::debug;

use super::QueryHistoryEntry;

pub struct HistoryWriter {
    tenant: String,
    database: String,
    table: String,

    coord: CoordinatorRef,
}

impl HistoryWriter {
    pub fn new(tenant: String, database: String, table: String, coord: CoordinatorRef) -> Self {
        Self {
            tenant,
            database,
            table,
            coord,
        }
    }

    pub async fn write_entries(&self, entries: &[QueryHistoryEntry]) {
        let measurement = &self.table;
        let lines = entries
            .iter()
            .map(|e| e.to_line_protocol(measurement))
            .collect::<Vec<_>>();

        self.write_lines(lines).await;
    }

    async fn write_lines<'a>(&self, mut lines: Vec<Line<'a>>) {
        debug!("write entries: {:?}", lines);
        let points = parse_lines_to_points(&self.database, &mut lines);
        let req = WritePointsRpcRequest { version: 1, points };
        let _ = self.coord.write_points(self.tenant.clone(), ConsistencyLevel::Any, req)
            .await
            .map_err(|e| {
                trace::error!("Failed to write sql history records, there may be a problem with the system. error: {}",
                    e
                );
            });
    }
}
