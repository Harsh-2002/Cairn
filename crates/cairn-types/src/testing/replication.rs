//! A fake replication sink that records intents and can simulate failures.

use crate::error::ReplicationError;
use crate::id::{ObjectKey, VersionId};
use crate::replication::ReplicatedObject;
use crate::traits::ReplicationSink;
use std::sync::Mutex;

/// What the fake sink should do on the next call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SinkBehavior {
    /// Succeed.
    #[default]
    Succeed,
    /// Fail retryably.
    Retryable,
    /// Fail terminally.
    Terminal,
}

/// A recorded replication intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedIntent {
    /// An object was put.
    Put {
        /// The key.
        key: ObjectKey,
        /// The version.
        version_id: VersionId,
        /// The logical size.
        size: u64,
    },
    /// A delete marker was propagated.
    DeleteMarker {
        /// The key.
        key: ObjectKey,
        /// The version.
        version_id: VersionId,
    },
}

/// A fake [`ReplicationSink`] capturing what would have been replicated.
#[derive(Debug, Default)]
pub struct FakeReplicationSink {
    behavior: Mutex<SinkBehavior>,
    intents: Mutex<Vec<RecordedIntent>>,
}

impl FakeReplicationSink {
    /// A sink that always succeeds.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the behavior for subsequent calls.
    pub fn set_behavior(&self, behavior: SinkBehavior) {
        *self.behavior.lock().unwrap() = behavior;
    }

    /// The recorded intents so far.
    #[must_use]
    pub fn intents(&self) -> Vec<RecordedIntent> {
        self.intents.lock().unwrap().clone()
    }

    fn check(&self) -> Result<(), ReplicationError> {
        match *self.behavior.lock().unwrap() {
            SinkBehavior::Succeed => Ok(()),
            SinkBehavior::Retryable => Err(ReplicationError::Retryable("simulated".to_owned())),
            SinkBehavior::Terminal => Err(ReplicationError::Terminal("simulated".to_owned())),
        }
    }
}

#[async_trait::async_trait]
impl ReplicationSink for FakeReplicationSink {
    async fn put_object(&self, object: ReplicatedObject) -> Result<(), ReplicationError> {
        self.check()?;
        self.intents.lock().unwrap().push(RecordedIntent::Put {
            key: object.key.clone(),
            version_id: object.version_id.clone(),
            size: object.size,
        });
        Ok(())
    }

    async fn delete_marker(
        &self,
        key: &ObjectKey,
        version: &VersionId,
    ) -> Result<(), ReplicationError> {
        self.check()?;
        self.intents
            .lock()
            .unwrap()
            .push(RecordedIntent::DeleteMarker {
                key: key.clone(),
                version_id: version.clone(),
            });
        Ok(())
    }
}
