//! Arrow batches in flight between DuckDB and Rust, bounded by a process-wide byte budget
//! (`engine.channel_bytes`, spec 01 §3/§5).

use std::ops::Deref;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use atlatl_core::error::Result;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

/// Bytes of Arrow data that may be in flight at once. A batch larger than the whole budget
/// reserves all of it, so it can still pass on its own.
#[derive(Clone, Debug)]
pub struct ByteBudget {
    sem: Arc<Semaphore>,
    total: usize,
}

impl ByteBudget {
    pub fn new(total: u64) -> Self {
        let total = usize::try_from(total)
            .unwrap_or(usize::MAX)
            .clamp(1, u32::MAX as usize);
        Self {
            sem: Arc::new(Semaphore::new(total)),
            total,
        }
    }

    pub fn total(&self) -> usize {
        self.total
    }

    pub fn in_use(&self) -> usize {
        self.total - self.sem.available_permits()
    }

    /// Waits until `batch` fits, then holds its bytes until the returned [`Batch`] drops.
    pub async fn reserve(&self, batch: RecordBatch) -> Batch {
        let n = batch.get_array_memory_size().clamp(1, self.total) as u32;
        let permit = self
            .sem
            .clone()
            .acquire_many_owned(n)
            .await
            .expect("budget semaphore is never closed");
        Batch {
            batch,
            _permit: Some(permit),
        }
    }
}

/// A `RecordBatch`, possibly holding part of a [`ByteBudget`] until dropped.
#[derive(Debug)]
pub struct Batch {
    batch: RecordBatch,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Batch {
    /// Outside any budget.
    pub fn unbudgeted(batch: RecordBatch) -> Self {
        Self {
            batch,
            _permit: None,
        }
    }

    /// Releases the budget.
    pub fn into_inner(self) -> RecordBatch {
        self.batch
    }
}

impl Deref for Batch {
    type Target = RecordBatch;
    fn deref(&self) -> &RecordBatch {
        &self.batch
    }
}

impl From<RecordBatch> for Batch {
    fn from(b: RecordBatch) -> Self {
        Self::unbudgeted(b)
    }
}

/// Query results from [`crate::Engine::stream`]. A failure mid-stream arrives as the last item;
/// the stream never ends early without one. Dropping it cancels the query.
#[derive(Debug)]
pub struct BatchStream {
    pub(crate) schema: SchemaRef,
    pub(crate) rx: mpsc::Receiver<Result<Batch>>,
}

impl BatchStream {
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub async fn next(&mut self) -> Option<Result<Batch>> {
        self.rx.recv().await
    }

    /// Every batch, or the first error.
    pub async fn collect(mut self) -> Result<Vec<RecordBatch>> {
        let mut out = vec![];
        while let Some(b) = self.next().await {
            out.push(b?.into_inner());
        }
        Ok(out)
    }
}
