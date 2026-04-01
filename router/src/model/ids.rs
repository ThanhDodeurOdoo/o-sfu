#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct RouterId(pub u64);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct TransportId(pub u64);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ProducerId(pub u64);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ConsumerId(pub u64);
