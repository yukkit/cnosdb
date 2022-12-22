use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;
use trace::{debug, warn};

use super::QueryHistoryEntry;

pub type HistorySenderRef = Arc<dyn HistorySender + Send + Sync>;

pub trait HistorySender {
    fn send_history(&self, entry: QueryHistoryEntry);

    fn is_closed(&self) -> bool;
}

pub struct MpscHistorySender {
    history_tx: UnboundedSender<QueryHistoryEntry>,
}

impl MpscHistorySender {
    pub fn new(statue_tx: UnboundedSender<QueryHistoryEntry>) -> Self {
        Self {
            history_tx: statue_tx,
        }
    }
}

impl HistorySender for MpscHistorySender {
    fn send_history(&self, entry: QueryHistoryEntry) {
        if self.history_tx.is_closed() {
            warn!("MpscHistorySender is closed");
            return;
        }
        
        debug!("Success to send a history entry: {}", &entry.query_id);
        let _ = self.history_tx.send(entry).unwrap();
    }

    fn is_closed(&self) -> bool {
        self.history_tx.is_closed()
    }
}
