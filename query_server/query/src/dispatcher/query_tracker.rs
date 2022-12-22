use std::{collections::HashMap, ops::Deref, sync::Arc};

use parking_lot::RwLock;
use spi::{
    query::{execution::QueryExecution, QueryError},
    service::protocol::QueryId,
};
use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};
use trace::{debug, warn};

use crate::dispatcher::query_history::sender::HistorySender;

use super::query_history::{sender::HistorySenderRef, QueryHistoryEntry, QueryHistoryEntryBuilder};

pub struct QueryTracker {
    queries: RwLock<HashMap<QueryId, Arc<dyn QueryExecution>>>,
    query_limit_semaphore: Semaphore,
    history_sender: HistorySenderRef,
}

impl QueryTracker {
    pub fn new(query_limit: usize, history_sender: HistorySenderRef) -> Self {
        let query_limit_semaphore = Semaphore::new(query_limit);

        Self {
            queries: RwLock::new(HashMap::new()),
            query_limit_semaphore,
            history_sender,
        }
    }
}

impl QueryTracker {
    /// track a query
    ///
    /// [`QueryError::RequestLimit`]
    ///
    /// [`QueryError::Closed`] after call [`Self::close`]
    pub fn try_track_query(
        &self,
        query_id: QueryId,
        query: Arc<dyn QueryExecution>,
    ) -> std::result::Result<TrackedQuery, QueryError> {
        debug!(
            "total query count: {}, current query info {:?} status {:?}",
            self.queries.read().len(),
            query.info(),
            query.status(),
        );

        let _ = self.queries.write().insert(query_id, query.clone());

        let _permit = match self.query_limit_semaphore.try_acquire() {
            Ok(p) => p,
            Err(TryAcquireError::NoPermits) => {
                warn!("simultaneous request limit exceeded - dropping request");
                return Err(QueryError::RequestLimit);
            }
            Err(TryAcquireError::Closed) => {
                return Err(QueryError::Closed);
            }
        };

        Ok(TrackedQuery {
            _permit,
            tracker: self,
            query_id,
            query,
        })
    }

    pub fn query(&self, id: &QueryId) -> Option<Arc<dyn QueryExecution>> {
        self.queries.read().get(id).cloned()
    }

    pub fn _running_query_count(&self) -> usize {
        self.queries.read().len()
    }

    /// all running queries
    pub fn running_queries(&self) -> Vec<Arc<dyn QueryExecution>> {
        self.queries.read().values().cloned().collect()
    }

    /// Once closed, no new requests will be accepted
    ///
    /// After closing, tracking new requests through [`QueryTracker::try_track_query`] will return [`QueryError::Closed`]
    pub fn _close(&self) {
        self.query_limit_semaphore.close();
    }

    fn expire_query(&self, id: &QueryId) {
        self.queries.write().remove(id);
    }

    fn send_history(&self, entry: QueryHistoryEntry) {
        self.history_sender.send_history(entry);
    }
}

pub struct TrackedQuery<'a> {
    _permit: SemaphorePermit<'a>,
    tracker: &'a QueryTracker,
    query_id: QueryId,
    query: Arc<dyn QueryExecution>,
}

impl TrackedQuery<'_> {
    fn construct_and_send_history(&self) {
        let query_info = self.query.info();
        let query_status = self.query.status();

        let query_id = self.query_id.to_string();
        let query_text = query_info.query();
        let user_name = query_info.user_name();
        let tenant_name = query_info.tenant_name();
        let execution_status = query_status.query_state().as_ref();
        // TODO
        let error_message = "";
        let total_elapsed_time = query_status.duration().num_milliseconds();
        let start_time = query_status.start_time() * 1000 * 1000;
        let end_time = start_time + total_elapsed_time * 1000 * 1000;
        // TODO
        // let bytes_scanned_from_storage ;
        // let bytes_written_to_storage;
        // let bytes_read_from_result;
        // let rows_inserted;
        // let rows_selected;

        let entry = QueryHistoryEntryBuilder::default()
            .query_id(query_id)
            .query_text(query_text)
            .user_name(user_name)
            .tenant_name(tenant_name)
            .execution_status(execution_status)
            .error_message(error_message)
            .start_time(start_time)
            .end_time(end_time)
            .total_elapsed_time(total_elapsed_time)
            // .bytes_scanned_from_storage(bytes_scanned_from_storage)
            // .bytes_written_to_storage(bytes_written_to_storage)
            // .bytes_read_from_result(bytes_read_from_result)
            // .rows_inserted(rows_inserted)
            // .rows_selected(rows_selected)
            .build();

        match entry {
            Ok(e) => {
                trace::debug!("send query history entry: {:?}", e);
                self.tracker.send_history(e)
            }
            Err(err) => {
                trace::error!("construct {:?} history entry error: {}", self.query_id, err);
            }
        }
    }
}

impl Deref for TrackedQuery<'_> {
    type Target = dyn QueryExecution;

    fn deref(&self) -> &Self::Target {
        self.query.deref()
    }
}

impl Drop for TrackedQuery<'_> {
    fn drop(&mut self) {
        debug!("TrackedQuery drop: {:?}", &self.query_id);
        self.tracker.expire_query(&self.query_id);
        self.construct_and_send_history();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use spi::{query::execution::QueryExecution, service::protocol::QueryId};

    use crate::{dispatcher::query_history::sender::HistorySender, execution::QueryExecutionMock};

    use super::QueryTracker;

    struct HistorySenderMock;

    impl HistorySender for HistorySenderMock {
        fn send_history(&self, _entry: crate::dispatcher::query_history::QueryHistoryEntry) {}

        fn is_closed(&self) -> bool {
            false
        }
    }

    fn new_query_tracker(limit: usize) -> QueryTracker {
        todo!()
        // QueryTracker::new(limit, Arc::new(HistorySenderMock))
    }

    #[test]
    fn test_track_and_drop() {
        let query_id = QueryId::next_id();
        let query = Arc::new(QueryExecutionMock {});
        let tracker = new_query_tracker(10);

        let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();
        assert_eq!(tracker._running_query_count(), 1);

        let query_id = QueryId::next_id();
        let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();
        assert_eq!(tracker._running_query_count(), 2);

        {
            // 作用域结束时，结束对当前query的追踪
            let query_id = QueryId::next_id();
            let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();
            assert_eq!(tracker._running_query_count(), 3);
        }

        assert_eq!(tracker._running_query_count(), 2);

        let query_id = QueryId::next_id();
        let _tq = tracker.try_track_query(query_id, query).unwrap();
        assert_eq!(tracker._running_query_count(), 3);
    }

    #[test]
    fn test_exceed_query_limit() {
        let query_id = QueryId::next_id();
        let query = Arc::new(QueryExecutionMock {});
        let tracker = new_query_tracker(2);

        let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();
        assert_eq!(tracker._running_query_count(), 1);

        let query_id = QueryId::next_id();
        let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();
        assert_eq!(tracker._running_query_count(), 2);

        let query_id = QueryId::next_id();
        assert!(tracker.try_track_query(query_id, query).is_err())
    }

    #[test]
    fn test_get_query_info() {
        let query_id = QueryId::next_id();
        let query = Arc::new(QueryExecutionMock {});
        let tracker = new_query_tracker(2);

        let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();

        let exe = tracker.query(&query_id).unwrap();

        let info_actual = query.info();
        let info_found = exe.info();

        assert_eq!(info_actual, info_found);
    }
}
