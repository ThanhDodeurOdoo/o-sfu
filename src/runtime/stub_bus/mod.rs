mod adapter;
#[cfg(test)]
mod bootstrap;

pub(crate) use adapter::StubWebRtcAdapter;
#[cfg(test)]
pub(crate) use adapter::StubWebRtcEvent;
