pub mod query_executor;
pub mod replica_selection;
pub mod table_scan;
pub mod tag_scan;

use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream, StreamExt, TryFutureExt};
use models::meta_data::VnodeInfo;
pub use query_executor::*;
use tokio::macros::support::thread_rng_n;
use tskv::query_iterator::QueryOption;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::{CoordinatorRecordBatchStream, SendableCoordinatorRecordBatchStream};

/// A fallible future that reads to a stream of [`RecordBatch`]
pub type VnodeOpenFuture =
    BoxFuture<'static, CoordinatorResult<SendableCoordinatorRecordBatchStream>>;
/// A fallible future that checks the vnode query operation is available
pub type CheckFuture = BoxFuture<'static, CoordinatorResult<()>>;

/// Generic API for connect a vnode and reading to a stream of [`RecordBatch`]
pub trait VnodeOpener: Unpin {
    /// Asynchronously open the specified vnode and return a stream of [`RecordBatch`]
    fn open(&self, vnode: &VnodeInfo, option: &QueryOption) -> CoordinatorResult<VnodeOpenFuture>;
}

pub struct CheckedCoordinatorRecordBatchStream<O: VnodeOpener> {
    vnode: VnodeInfo,
    option: QueryOption,
    opener: O,
    state: StreamState,
}

impl<O: VnodeOpener> CheckedCoordinatorRecordBatchStream<O> {
    pub fn new(vnode: VnodeInfo, option: QueryOption, opener: O, checker: CheckFuture) -> Self {
        Self {
            vnode,
            option,
            opener,
            state: StreamState::Check(checker),
        }
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<CoordinatorResult<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Check(checker) => {
                    // TODO record time used
                    match ready!(checker.try_poll_unpin(cx)) {
                        Ok(_) => {
                            self.state = StreamState::Idle;
                        }
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    };
                }
                StreamState::Idle => {
                    // TODO record time used
                    let future = match self.opener.open(&self.vnode, &self.option) {
                        Ok(future) => future,
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    };
                    self.state = StreamState::Open(future);
                }
                StreamState::Open(future) => {
                    // TODO record time used
                    match ready!(future.poll_unpin(cx)) {
                        Ok(stream) => {
                            self.state = StreamState::Scan(stream);
                        }
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    };
                }
                StreamState::Scan(stream) => return stream.poll_next_unpin(cx),
            }
        }
    }
}

impl<O: VnodeOpener> Stream for CheckedCoordinatorRecordBatchStream<O> {
    type Item = Result<RecordBatch, CoordinatorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

impl<O: VnodeOpener> CoordinatorRecordBatchStream for CheckedCoordinatorRecordBatchStream<O> {}

enum StreamState {
    Check(CheckFuture),
    Idle,
    Open(VnodeOpenFuture),
    Scan(SendableCoordinatorRecordBatchStream),
}

pub struct CombinedRecordBatchStream {
    /// Stream entries
    entries: Vec<SendableCoordinatorRecordBatchStream>,
}

impl CombinedRecordBatchStream {
    /// Create an CombinedRecordBatchStream
    pub fn new(entries: Vec<SendableCoordinatorRecordBatchStream>) -> Self {
        Self { entries }
    }
}

impl Stream for CombinedRecordBatchStream {
    type Item = CoordinatorResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;

        let start = thread_rng_n(self.entries.len() as u32) as usize;
        let mut idx = start;

        for _ in 0..self.entries.len() {
            let stream = self.entries.get_mut(idx).unwrap();

            match Pin::new(stream).poll_next(cx) {
                Ready(Some(val)) => return Ready(Some(val)),
                Ready(None) => {
                    // Remove the entry
                    self.entries.swap_remove(idx);

                    // Check if this was the last entry, if so the cursor needs
                    // to wrap
                    if idx == self.entries.len() {
                        idx = 0;
                    } else if idx < start && start <= self.entries.len() {
                        // The stream being swapped into the current index has
                        // already been polled, so skip it.
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                }
                Pending => {
                    idx = idx.wrapping_add(1) % self.entries.len();
                }
            }
        }

        // If the map is empty, then the stream is complete.
        if self.entries.is_empty() {
            Ready(None)
        } else {
            Pending
        }
    }
}

impl CoordinatorRecordBatchStream for CombinedRecordBatchStream {}

/// Iterator over batches
pub struct MemoryRecordBatchStream {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Index into the data
    index: usize,
}

impl MemoryRecordBatchStream {
    /// Create an iterator for a vector of record batches
    pub fn new(data: Vec<RecordBatch>) -> Self {
        Self { data, index: 0 }
    }
}

impl Stream for MemoryRecordBatchStream {
    type Item = CoordinatorResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;
            let batch = &self.data[self.index - 1];
            Some(Ok(batch.to_owned()))
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl CoordinatorRecordBatchStream for MemoryRecordBatchStream {}
