use std::{error::Error, fmt::Display};

use o_sfu_core::{prelude::SfuCoreError, server::transport::TransportAdapterError};
use o_sfu_router::{RouterError, RtpNegotiationError, SessionId};

fn assert_error<T: Error + Send + Sync + 'static>() {}

fn assert_display(error: impl Display) {
    assert!(!error.to_string().is_empty());
}

#[test]
fn public_error_types_implement_standard_error_traits() {
    assert_error::<RouterError>();
    assert_error::<RtpNegotiationError>();
    assert_error::<SfuCoreError>();
}

#[test]
fn public_error_display_messages_are_not_empty() {
    assert_display(RouterError::MissingSession(SessionId(1)));
    assert_display(RtpNegotiationError::NoCompatibleConsumerCodec);
    assert_display(SfuCoreError::Transport(TransportAdapterError::InvalidInput));
}
