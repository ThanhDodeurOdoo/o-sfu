use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Clone)]
pub(crate) struct PreAuthWebSocketAdmission {
    global: Arc<Semaphore>,
    per_origin_capacity: usize,
    origins: Arc<Mutex<HashMap<Arc<str>, OriginAdmission>>>,
}

#[derive(Debug, Clone)]
struct OriginAdmission {
    semaphore: Arc<Semaphore>,
}

#[derive(Debug)]
pub(super) struct PreAuthWebSocketPermit {
    _global_permit: OwnedSemaphorePermit,
    origin_permit: Option<OwnedSemaphorePermit>,
    origin: Arc<str>,
    origins: Arc<Mutex<HashMap<Arc<str>, OriginAdmission>>>,
    per_origin_capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PreAuthWebSocketAdmissionRejection {
    Global,
    Origin,
}

impl PreAuthWebSocketAdmission {
    #[must_use]
    pub(crate) fn new(global_capacity: usize, per_origin_capacity: usize) -> Self {
        debug_assert!(global_capacity > 0);
        debug_assert!(per_origin_capacity > 0);
        Self {
            global: Arc::new(Semaphore::new(global_capacity)),
            per_origin_capacity,
            origins: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn try_acquire(
        &self,
        origin: &str,
    ) -> Result<PreAuthWebSocketPermit, PreAuthWebSocketAdmissionRejection> {
        let global_permit = Arc::clone(&self.global)
            .try_acquire_owned()
            .map_err(|_error| PreAuthWebSocketAdmissionRejection::Global)?;
        let origin = Arc::<str>::from(origin);
        let mut origins = lock_origins(&self.origins);
        let origin_admission =
            origins
                .entry(Arc::clone(&origin))
                .or_insert_with(|| OriginAdmission {
                    semaphore: Arc::new(Semaphore::new(self.per_origin_capacity)),
                });
        let origin_permit = Arc::clone(&origin_admission.semaphore)
            .try_acquire_owned()
            .map_err(|_error| PreAuthWebSocketAdmissionRejection::Origin)?;
        drop(origins);
        Ok(PreAuthWebSocketPermit {
            _global_permit: global_permit,
            origin_permit: Some(origin_permit),
            origin,
            origins: Arc::clone(&self.origins),
            per_origin_capacity: self.per_origin_capacity,
        })
    }

    #[cfg(test)]
    pub(super) fn available_global_permits(&self) -> usize {
        self.global.available_permits()
    }

    #[cfg(test)]
    pub(super) fn tracked_origin_count(&self) -> usize {
        lock_origins(&self.origins).len()
    }
}

impl Drop for PreAuthWebSocketPermit {
    fn drop(&mut self) {
        drop(self.origin_permit.take());
        let mut origins = lock_origins(&self.origins);
        let should_remove = origins.get(&self.origin).is_some_and(|admission| {
            admission.semaphore.available_permits() == self.per_origin_capacity
        });
        if should_remove {
            origins.remove(&self.origin);
        }
    }
}

fn lock_origins(
    origins: &Mutex<HashMap<Arc<str>, OriginAdmission>>,
) -> MutexGuard<'_, HashMap<Arc<str>, OriginAdmission>> {
    origins.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{PreAuthWebSocketAdmission, PreAuthWebSocketAdmissionRejection};

    #[test]
    fn pre_auth_admission_releases_dropped_permits() {
        let admission = PreAuthWebSocketAdmission::new(1, 1);
        let permit = admission.try_acquire("198.51.100.24");
        assert!(permit.is_ok());
        assert_eq!(admission.available_global_permits(), 0);
        assert!(matches!(
            admission.try_acquire("198.51.100.24"),
            Err(PreAuthWebSocketAdmissionRejection::Global)
        ));

        drop(permit);

        assert_eq!(admission.available_global_permits(), 1);
        assert_eq!(admission.tracked_origin_count(), 0);
        assert!(admission.try_acquire("198.51.100.24").is_ok());
    }

    #[test]
    fn pre_auth_admission_enforces_per_origin_capacity() {
        let admission = PreAuthWebSocketAdmission::new(2, 1);
        let first = match admission.try_acquire("198.51.100.24") {
            Ok(permit) => permit,
            Err(error) => {
                assert_eq!(Some(error), None, "first origin permit should be available");
                return;
            }
        };
        assert!(matches!(
            admission.try_acquire("198.51.100.24"),
            Err(PreAuthWebSocketAdmissionRejection::Origin)
        ));
        assert!(admission.try_acquire("203.0.113.4").is_ok());
        assert_eq!(first.origin.as_ref(), "198.51.100.24");
        drop(first);
    }
}
