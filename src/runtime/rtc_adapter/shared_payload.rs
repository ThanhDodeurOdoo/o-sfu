use std::{mem::take, sync::Arc};

#[derive(Debug)]
pub(crate) struct SharedPayload {
    storage: SharedPayloadStorage,
}

#[derive(Debug)]
enum SharedPayloadStorage {
    Owned(Vec<u8>),
    Shared(Arc<[u8]>),
}

impl SharedPayload {
    pub(super) fn from_vec(payload: Vec<u8>) -> Self {
        Self {
            storage: SharedPayloadStorage::Owned(payload),
        }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        match &self.storage {
            SharedPayloadStorage::Owned(payload) => payload.as_slice(),
            SharedPayloadStorage::Shared(payload) => payload.as_ref(),
        }
    }

    pub(super) fn share(&self) -> Self {
        Self {
            storage: match &self.storage {
                SharedPayloadStorage::Owned(payload) => {
                    SharedPayloadStorage::Shared(Arc::from(payload.as_slice()))
                }
                SharedPayloadStorage::Shared(payload) => {
                    SharedPayloadStorage::Shared(Arc::clone(payload))
                }
            },
        }
    }

    pub(super) fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub(super) fn take_write_payload(&mut self, is_last_destination: bool) -> Vec<u8> {
        match &mut self.storage {
            SharedPayloadStorage::Owned(payload) => {
                take_or_clone_owned_payload(payload, is_last_destination)
            }
            SharedPayloadStorage::Shared(payload) => payload.as_ref().to_vec(),
        }
    }
}

fn take_or_clone_owned_payload(data: &mut Vec<u8>, is_last_destination: bool) -> Vec<u8> {
    if is_last_destination {
        take(data)
    } else {
        data.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{SharedPayload, SharedPayloadStorage};

    #[test]
    fn shared_payload_exposes_bytes_and_len() {
        let payload = SharedPayload::from_vec(vec![1, 2, 3, 4]);

        assert_eq!(payload.as_slice(), [1, 2, 3, 4]);
        assert_eq!(payload.len(), 4);
    }

    #[test]
    fn shared_payload_clones_for_non_final_destination_when_owned() {
        let mut payload = SharedPayload::from_vec(vec![1, 2, 3, 4]);

        assert_eq!(payload.take_write_payload(false), vec![1, 2, 3, 4]);
        assert_eq!(payload.as_slice(), [1, 2, 3, 4]);
    }

    #[test]
    fn shared_payload_moves_for_final_destination_when_owned() {
        let mut payload = SharedPayload::from_vec(vec![5, 6, 7, 8]);

        assert_eq!(payload.take_write_payload(true), vec![5, 6, 7, 8]);
        assert!(payload.as_slice().is_empty());
    }

    #[test]
    fn shared_payload_clones_when_the_storage_is_already_shared() {
        let mut payload = SharedPayload {
            storage: SharedPayloadStorage::Shared(Arc::from([9, 10, 11].as_slice())),
        };

        assert_eq!(payload.take_write_payload(true), vec![9, 10, 11]);
        assert_eq!(payload.as_slice(), [9, 10, 11]);
    }

    #[test]
    fn shared_payload_promotes_owned_storage_into_shared_storage() {
        let payload = SharedPayload::from_vec(vec![12, 13, 14]);

        let shared = payload.share();

        assert_eq!(shared.as_slice(), [12, 13, 14]);
        assert!(matches!(shared.storage, SharedPayloadStorage::Shared(_)));
    }
}
