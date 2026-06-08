#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportEffectOutcome {
    Applied,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublishIntentOutcome {
    Noop,
    Queue,
    Activated,
    Staged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnpublishIntentOutcome {
    Noop,
    RolledBack,
    Unpublished,
}
