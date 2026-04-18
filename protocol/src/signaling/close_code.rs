#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum WebSocketCloseCode {
    Clean = 1000,
    Leaving = 1001,
    ProtocolError = 1002,
    Error = 1011,
    AuthFailed = 4001,
    AuthTimeout = 4002,
    Kicked = 4003,
    ChannelFull = 4004,
}

impl WebSocketCloseCode {
    #[must_use]
    pub const fn from_u16(value: u16) -> Option<Self> {
        match value {
            1000 => Some(Self::Clean),
            1001 => Some(Self::Leaving),
            1002 => Some(Self::ProtocolError),
            1011 => Some(Self::Error),
            4001 => Some(Self::AuthFailed),
            4002 => Some(Self::AuthTimeout),
            4003 => Some(Self::Kicked),
            4004 => Some(Self::ChannelFull),
            _ => None,
        }
    }
}

impl From<WebSocketCloseCode> for u16 {
    fn from(value: WebSocketCloseCode) -> Self {
        match value {
            WebSocketCloseCode::Clean => 1000,
            WebSocketCloseCode::Leaving => 1001,
            WebSocketCloseCode::ProtocolError => 1002,
            WebSocketCloseCode::Error => 1011,
            WebSocketCloseCode::AuthFailed => 4001,
            WebSocketCloseCode::AuthTimeout => 4002,
            WebSocketCloseCode::Kicked => 4003,
            WebSocketCloseCode::ChannelFull => 4004,
        }
    }
}
