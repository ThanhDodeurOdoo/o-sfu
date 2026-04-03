use std::collections::BTreeMap;

use o_sfu_router::{
    Router, RouterError, RouterId, Session as RouterSession, SessionId as RouterSessionId,
};

use crate::signaling::shared::SessionId;

#[derive(Debug)]
pub(super) struct ChannelRouterState {
    router: Router,
    router_session_ids_by_session_id: BTreeMap<SessionId, RouterSessionId>,
}

impl ChannelRouterState {
    pub(super) fn new(router_id: RouterId) -> Self {
        Self {
            router: Router::new(router_id),
            router_session_ids_by_session_id: BTreeMap::new(),
        }
    }

    /// Ensure the pure router contains a session matching the signaling-layer session.
    ///
    /// The runtime still accepts integer and string signaling session IDs, so this
    /// channel-local map keeps that compatibility at the edge while the pure router
    /// continues to use compact numeric identifiers internally.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if joining the pure router fails.
    pub(super) fn ensure_session(
        &mut self,
        session_id: &SessionId,
        router_session_seed: u64,
    ) -> Result<(), RouterError> {
        if self
            .router_session_ids_by_session_id
            .contains_key(session_id)
        {
            return Ok(());
        }
        let router_session_id = RouterSessionId(router_session_seed);
        self.router
            .join_session(RouterSession::new(router_session_id))?;
        self.router_session_ids_by_session_id
            .insert(session_id.clone(), router_session_id);
        Ok(())
    }

    /// Remove the pure-router session for the signaling-layer session if one exists.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the runtime/session map and router
    /// state ever diverge.
    pub(super) fn remove_session(&mut self, session_id: &SessionId) -> Result<(), RouterError> {
        let Some(router_session_id) = self
            .router_session_ids_by_session_id
            .get(session_id)
            .copied()
        else {
            return Ok(());
        };
        self.router.remove_session(router_session_id)?;
        self.router_session_ids_by_session_id.remove(session_id);
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn session_count(&self) -> usize {
        self.router.session_count()
    }
}
