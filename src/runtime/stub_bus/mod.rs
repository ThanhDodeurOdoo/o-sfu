mod adapter;
mod bootstrap;

pub(crate) use adapter::StubWebRtcAdapter;
#[cfg(test)]
pub(crate) use adapter::StubWebRtcEvent;
