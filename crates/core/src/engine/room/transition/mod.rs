//! room media transition choreography
//!
//! domain modules own facts, transition modules own time and effect modules own
//! side effects

mod publication;
mod subscription;

#[cfg(any(test, feature = "testing-transport"))]
pub(super) use publication::StagedPublish;
pub(super) use publication::StagedPublishRegistry;
