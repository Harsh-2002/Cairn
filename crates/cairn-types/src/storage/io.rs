//! Backend I/O quiescence independent of async-future lifetime. No timer releases ownership.

use super::StorageToken;
use crate::BlobError;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, Waker};

struct State {
    cancelled: bool,
    active: usize,
    waiters: Vec<(Weak<()>, Waker)>,
}

struct Shared {
    attempt: StorageToken,
    generation: StorageToken,
    state: Mutex<State>,
    // In production this includes the node lock. Detached filesystem/kernel operations must
    // retain the lease through completion, so dropping the request cannot release exclusivity.
    _lifetime: Arc<dyn Send + Sync>,
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

/// Request/recovery observer. Cancellation closes lease admission atomically with its count.
#[derive(Clone)]
pub struct StorageIoWatch(Arc<Shared>);

impl std::fmt::Debug for StorageIoWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageIoWatch").finish_non_exhaustive()
    }
}

impl StorageIoWatch {
    /// Establish ownership before admission. The returned root lease moves into blob staging.
    #[must_use]
    pub fn new(
        attempt: StorageToken,
        generation: StorageToken,
        lifetime: Arc<dyn Send + Sync>,
    ) -> (Self, StorageIoLease) {
        let shared = Arc::new(Shared {
            attempt,
            generation,
            state: Mutex::new(State {
                cancelled: false,
                active: 1,
                waiters: Vec::new(),
            }),
            _lifetime: lifetime,
        });
        (Self(shared.clone()), StorageIoLease(shared))
    }

    /// Prevent any further backend operation from acquiring physical ownership.
    pub fn cancel(&self) {
        self.0.state().cancelled = true;
    }

    /// Stop admission and wait for actual completion of every retained backend operation.
    /// Dropping this future leaves ownership outstanding; it never manufactures a proof.
    pub async fn quiescent(&mut self) -> StorageQuiescence {
        self.cancel();
        Quiescent {
            shared: &self.0,
            identity: Arc::new(()),
        }
        .await
    }
}

/// Ownership held by an actual backend operation or handle. Deliberately not `Clone`: spawning
/// another operation requires `try_child`, which refuses after cancellation.
pub struct StorageIoLease(Arc<Shared>);

impl std::fmt::Debug for StorageIoLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageIoLease").finish_non_exhaustive()
    }
}

impl StorageIoLease {
    /// Obtain ownership before scheduling blocking work or submitting kernel I/O.
    pub fn try_child(&self) -> Result<Self, BlobError> {
        let mut state = self.0.state();
        if state.cancelled {
            return Err(BlobError::Io("storage write cancelled".to_owned()));
        }
        state.active = state
            .active
            .checked_add(1)
            .ok_or_else(|| BlobError::Io("storage I/O ownership overflow".to_owned()))?;
        Ok(Self(self.0.clone()))
    }

    /// Check cancellation before starting the next operation on an existing handle.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.state().cancelled
    }

    /// An unrelated attempt's lease cannot authorize this plan.
    #[must_use]
    pub fn owns(&self, attempt: &StorageToken, generation: &StorageToken) -> bool {
        &self.0.attempt == attempt && &self.0.generation == generation
    }
}

impl Drop for StorageIoLease {
    fn drop(&mut self) {
        let waiters = {
            let mut state = self.0.state();
            state.active -= 1;
            if state.active == 0 {
                std::mem::take(&mut state.waiters)
            } else {
                Vec::new()
            }
        };
        for (identity, waker) in waiters {
            if identity.upgrade().is_some() {
                waker.wake();
            }
        }
    }
}

/// Exact in-process proof; only the drained backend ownership set can construct it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageQuiescence {
    attempt: StorageToken,
    generation: StorageToken,
}

impl StorageQuiescence {
    #[must_use]
    pub fn attempt(&self) -> &StorageToken {
        &self.attempt
    }

    #[must_use]
    pub fn generation(&self) -> &StorageToken {
        &self.generation
    }
}

struct Quiescent<'a> {
    shared: &'a Arc<Shared>,
    identity: Arc<()>,
}

impl Future for Quiescent<'_> {
    type Output = StorageQuiescence;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.shared.state();
        if state.active == 0 {
            Poll::Ready(StorageQuiescence {
                attempt: self.shared.attempt.clone(),
                generation: self.shared.generation.clone(),
            })
        } else {
            let identity = Arc::downgrade(&self.identity);
            state.waiters.retain(|(slot, _)| slot.strong_count() != 0);
            if let Some((_, waker)) = state
                .waiters
                .iter_mut()
                .find(|(slot, _)| Weak::ptr_eq(slot, &identity))
            {
                waker.clone_from(cx.waker());
            } else {
                state.waiters.push((identity, cx.waker().clone()));
            }
            Poll::Pending
        }
    }
}

impl Drop for Quiescent<'_> {
    fn drop(&mut self) {
        let identity = Arc::downgrade(&self.identity);
        self.shared
            .state()
            .waiters
            .retain(|(slot, _)| !Weak::ptr_eq(slot, &identity));
    }
}

impl PartialEq for StorageIoWatch {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for StorageIoWatch {}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[test]
    fn cancellation_waits_for_all_actual_io_and_preserves_node_lifetime() {
        let lifetime = Arc::new(());
        let weak = Arc::downgrade(&lifetime);
        let attempt = StorageToken::generate();
        let generation = StorageToken::generate();
        let (mut watch, root) = StorageIoWatch::new(attempt.clone(), generation.clone(), lifetime);
        let queued = root.try_child().unwrap();
        let executing = root.try_child().unwrap();
        drop(root);
        assert!(watch.quiescent().now_or_never().is_none());
        assert!(queued.try_child().is_err());
        drop(queued);
        assert!(watch.quiescent().now_or_never().is_none());
        assert!(weak.upgrade().is_some());
        drop(executing);
        let proof = watch.quiescent().now_or_never().unwrap();
        assert_eq!(proof.attempt(), &attempt);
        assert_eq!(proof.generation(), &generation);
        drop(watch);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn dropping_observer_does_not_release_detached_io_lifetime() {
        let lifetime = Arc::new(());
        let weak = Arc::downgrade(&lifetime);
        let (watch, root) =
            StorageIoWatch::new(StorageToken::generate(), StorageToken::generate(), lifetime);
        let detached = root.try_child().unwrap();
        drop(root);
        drop(watch);
        assert!(weak.upgrade().is_some());
        drop(detached);
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn cloned_observers_wait_independently_and_cancelled_waiters_are_removed() {
        let (mut first, lease) = StorageIoWatch::new(
            StorageToken::generate(),
            StorageToken::generate(),
            Arc::new(()),
        );
        let mut second = first.clone();
        for _ in 0..64 {
            assert!(first.quiescent().now_or_never().is_none());
            assert!(first.0.state().waiters.is_empty());
        }
        let mut a = Box::pin(first.quiescent());
        let mut b = Box::pin(second.quiescent());
        assert!(a.as_mut().now_or_never().is_none());
        assert!(b.as_mut().now_or_never().is_none());
        drop(lease);
        assert!(a.now_or_never().is_some());
        assert!(b.now_or_never().is_some());
    }
}
