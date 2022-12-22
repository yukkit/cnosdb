use parking_lot::Mutex;

use super::{writer::HistoryWriter, QueryHistoryEntry};

pub struct Buffer {
    writer: HistoryWriter,

    buffer: Mutex<Vec<QueryHistoryEntry>>,
}

impl Buffer {
    pub fn new(writer: HistoryWriter, buffer_capacity: usize) -> Self {
        let buffer = Mutex::new(Vec::with_capacity(buffer_capacity));

        Self { writer, buffer }
    }

    pub fn push(&self, entry: QueryHistoryEntry) {
        self.buffer.lock().push(entry);
    }

    pub fn len(&self) -> usize {
        self.buffer.lock().len()
    }

    pub fn capacity(&self) -> usize {
        self.buffer.lock().capacity()
    }

    pub fn should_flush(&self) -> bool {
        self.len() >= self.capacity() / 5 * 4
    }

    pub async fn switch_and_flush(&self) {
        // TODO
        // 切换写入buffer
        // 将老写入buffer写入目标库
        let mut data = self.buffer.lock();
        {
            self.writer.write_entries(data.as_slice()).await;
        }
        data.clear();
    }
}
