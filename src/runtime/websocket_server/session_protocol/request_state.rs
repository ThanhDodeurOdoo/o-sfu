use crate::signaling::protocol::RequestId;

#[derive(Debug, Default)]
pub(super) struct NativeRequestState {
    next_request_counter: u64,
    pending_ping_request_id: Option<RequestId>,
}

impl NativeRequestState {
    pub(super) fn awaiting_ping_response(&self) -> bool {
        self.pending_ping_request_id.is_some()
    }

    pub(super) fn start_ping(&mut self) -> Option<RequestId> {
        if self.pending_ping_request_id.is_some() {
            return None;
        }
        let request_id = self.next_request_id();
        self.pending_ping_request_id = Some(request_id.clone());
        Some(request_id)
    }

    pub(super) fn next_request_id(&mut self) -> RequestId {
        let request_id = RequestId::new(format!("server-{}", self.next_request_counter));
        self.next_request_counter = self.next_request_counter.saturating_add(1);
        request_id
    }

    pub(super) fn resolve_ping_response(&mut self, response_to: &RequestId) -> bool {
        if self
            .pending_ping_request_id
            .as_ref()
            .is_some_and(|request_id| request_id == response_to)
        {
            self.pending_ping_request_id = None;
            return true;
        }
        false
    }
}
