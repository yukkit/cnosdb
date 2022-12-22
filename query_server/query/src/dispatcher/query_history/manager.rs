use std::sync::Arc;
use std::time::Duration;

use coordinator::service::CoordinatorRef;
use spi::query::{DEFAULT_CATALOG, DEFAULT_DATABASE};
use tokio::runtime::Runtime as TokioRuntime;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::time;
use trace::debug;

use super::buffer::Buffer;
use super::sender::{HistorySenderRef, MpscHistorySender};
use super::writer::HistoryWriter;
use super::{HistoryManagerOptions, QueryHistoryEntry, QUERY_HISTORY_TABLE_NAME};

pub struct QueryHistoryManager {
    options: HistoryManagerOptions,
    rt: Arc<TokioRuntime>,

    write_buffer: Arc<Buffer>,
}

impl QueryHistoryManager {
    pub fn new(
        options: HistoryManagerOptions,
        rt: Arc<TokioRuntime>,
        coord: CoordinatorRef,
    ) -> Self {
        let writer = HistoryWriter::new(
            DEFAULT_CATALOG.to_string(),
            DEFAULT_DATABASE.to_string(),
            QUERY_HISTORY_TABLE_NAME.to_string(),
            coord,
        );

        let write_buffer = Arc::new(Buffer::new(writer, options.buffer_capacity()));

        Self {
            options,
            rt,
            write_buffer,
        }
    }

    pub fn start(&self) -> HistorySenderRef {
        debug!("Begin start QueryHistoryManager");

        let (flush_tx, flush_rx) = mpsc::unbounded_channel::<()>();
        let write_buffer = self.write_buffer.clone();
        let interval = self.options.flush_interval();
        // start flush task
        self.rt
            .spawn(schedule_flush(interval, write_buffer, flush_rx));

        // start receive history task
        let (receive_entry_tx, receive_entry_rx) = mpsc::unbounded_channel::<QueryHistoryEntry>();
        let write_buffer = self.write_buffer.clone();
        self.rt.spawn(schedule_receive_entry(
            write_buffer,
            receive_entry_rx,
            flush_tx,
        ));

        Arc::new(MpscHistorySender::new(receive_entry_tx))
    }

    pub fn stop(&self) {
        // 1. 通过lineprotocol写入，不需要检查系统租户下是否含有history表
    }

    pub fn init(&self) {
        // 1. 通过lineprotocol写入，不需要检查系统租户下是否含有history表
    }
}

async fn schedule_flush(
    interval: Duration,
    write_buffer: Arc<Buffer>,
    mut flusher: UnboundedReceiver<()>,
) {
    debug!("Start schedule flush task");

    let mut interval = time::interval(interval);

    loop {
        tokio::select! {
            _instant = interval.tick() => {
                debug!("Refresh the cache periodically");
                // TODO
                write_buffer.switch_and_flush().await;
            }
            Some(_) = flusher.recv() => {
                debug!("Refresh the cache when the threshold is exceeded");
                // TODO
                write_buffer.switch_and_flush().await;
            }
        }
    }
}

async fn schedule_receive_entry(
    write_buffer: Arc<Buffer>,
    mut rx: UnboundedReceiver<QueryHistoryEntry>,
    flush: UnboundedSender<()>,
) {
    debug!("Start schedule receive entry task");

    while let Some(e) = rx.recv().await {
        debug!("Receive query history entry: {:?}", e);
        write_buffer.push(e);
        if write_buffer.should_flush() {
            let _ = flush.send(());
        }
    }
}
