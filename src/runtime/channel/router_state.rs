use std::collections::BTreeMap;

use o_sfu_router::{
    Router, RouterError, RouterId, Session as RouterSession, SessionId as RouterSessionId,
    SessionInfo as RouterSessionInfo, SessionPermissions as RouterSessionPermissions,
};

use crate::signaling::shared::{
    SessionId, SessionInfo as SignalingSessionInfo,
    SessionPermissions as SignalingSessionPermissions,
};

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
        permissions: &SignalingSessionPermissions,
    ) -> Result<(), RouterError> {
        if self
            .router_session_ids_by_session_id
            .contains_key(session_id)
        {
            return self.update_session_permissions(session_id, permissions);
        }
        let router_session_id = RouterSessionId(router_session_seed);
        self.router.join_session(RouterSession::new(
            router_session_id,
            router_permissions(permissions),
        ))?;
        self.router_session_ids_by_session_id
            .insert(session_id.clone(), router_session_id);
        Ok(())
    }

    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the signaling/session map and router
    /// state ever diverge.
    pub(super) fn update_session_permissions(
        &mut self,
        session_id: &SessionId,
        permissions: &SignalingSessionPermissions,
    ) -> Result<(), RouterError> {
        let Some(router_session_id) = self
            .router_session_ids_by_session_id
            .get(session_id)
            .copied()
        else {
            return Ok(());
        };
        self.router
            .update_session_permissions(router_session_id, router_permissions(permissions))
    }

    /// # Errors
    ///
    /// Returns the underlying [`RouterError`] if the signaling/session map and router
    /// state ever diverge.
    pub(super) fn update_session_info(
        &mut self,
        session_id: &SessionId,
        info: &SignalingSessionInfo,
    ) -> Result<(), RouterError> {
        let Some(router_session_id) = self
            .router_session_ids_by_session_id
            .get(session_id)
            .copied()
        else {
            return Ok(());
        };
        self.router
            .update_session_info(router_session_id, router_info(info))
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

    pub(super) fn session_count(&self) -> u64 {
        u64::try_from(self.router.session_count()).unwrap_or(u64::MAX)
    }

    pub(super) fn camera_count(&self) -> u64 {
        let count = self
            .router
            .sessions()
            .filter(|session| session.info().is_camera_on() == Some(true))
            .count();
        u64::try_from(count).unwrap_or(u64::MAX)
    }

    pub(super) fn screen_count(&self) -> u64 {
        let count = self
            .router
            .sessions()
            .filter(|session| session.info().is_screen_sharing_on() == Some(true))
            .count();
        u64::try_from(count).unwrap_or(u64::MAX)
    }

    #[cfg(test)]
    pub(super) fn session_permissions(
        &self,
        session_id: &SessionId,
    ) -> Option<RouterSessionPermissions> {
        let router_session_id = self.router_session_ids_by_session_id.get(session_id)?;
        self.router
            .sessions()
            .find(|session| session.id() == *router_session_id)
            .map(o_sfu_router::Session::permissions)
    }
}

fn router_permissions(permissions: &SignalingSessionPermissions) -> RouterSessionPermissions {
    RouterSessionPermissions::new(
        permissions.transcription.unwrap_or(false),
        permissions.audio_recording.unwrap_or(false),
        permissions.video_recording.unwrap_or(false),
    )
}

fn router_info(info: &SignalingSessionInfo) -> RouterSessionInfo {
    RouterSessionInfo::new(
        info.is_talking,
        info.is_camera_on,
        info.is_screen_sharing_on,
        info.is_self_muted,
        info.is_deaf,
        info.is_raising_hand,
    )
}
