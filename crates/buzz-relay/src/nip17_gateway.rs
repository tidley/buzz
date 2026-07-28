//! NIP-17 gateway cryptographic boundary for tunneled relay frames.
//!
//! A transport adapter supplies serialized kind `1059` events as text frames,
//! dispatches their plaintext through a [`crate::connection::VirtualConnection`],
//! then sends encrypted response frames through a public relay.

use nostr::{
    nips::nip59::{self, UnwrappedGift},
    Event, EventBuilder, Keys, Kind, PublicKey,
};
use thiserror::Error;

use crate::connection::VirtualConnection;
use crate::transport::RelayFrame;

/// Prefix for the per-client-launch NIP-17 session tag.
pub const SESSION_TAG_PREFIX: &str = "buzz-nip17-session:";

/// Decrypted request delivered by a NIP-17 gateway client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayRequest {
    /// Verified public key that authored the inner NIP-59 seal and rumor.
    pub sender: PublicKey,
    /// Per-client-launch identity used to isolate replayed public envelopes.
    pub session_id: String,
    /// The plaintext NIP-01 request frame for the relay dispatcher.
    pub frame: RelayFrame,
}

/// Errors returned while translating NIP-17 envelopes to relay frames.
#[derive(Debug, Error)]
pub enum GatewayError {
    /// The tunnel only carries text relay protocol frames.
    #[error("NIP-17 gateway requires a text relay frame")]
    NonTextFrame,
    /// The configured frame limit cannot represent a relay frame.
    #[error("NIP-17 gateway frame limit must be greater than zero")]
    InvalidFrameLimit,
    /// A serialized outer event was invalid.
    #[error("invalid NIP-17 envelope: {0}")]
    InvalidEnvelope(serde_json::Error),
    /// Gift-wrap encryption or decryption failed.
    #[error("NIP-59 gift-wrap error: {0}")]
    GiftWrap(String),
    /// The decrypted rumor was not a NIP-17 private direct message.
    #[error("NIP-17 gift-wrap rumor must be kind 14")]
    InvalidRumorKind,
    /// The NIP-17 envelope did not identify a client launch session.
    #[error("NIP-17 gift-wrap rumor is missing a session tag")]
    MissingSessionTag,
    /// A decrypted or outgoing plaintext frame exceeded the configured limit.
    #[error("NIP-17 gateway frame exceeds {max} bytes (got {got})")]
    FrameTooLarge {
        /// Configured maximum frame size in bytes.
        max: usize,
        /// Actual UTF-8 byte size of the frame.
        got: usize,
    },
    /// A request arrived from a different NIP-17 sender than the session owner.
    #[error("NIP-17 gateway sender does not match the authenticated session")]
    SenderMismatch,
}

/// Minimal NIP-17 request/response envelope translator.
#[derive(Debug, Clone)]
pub struct Nip17Gateway {
    keys: Keys,
    max_frame_bytes: usize,
}

impl Nip17Gateway {
    /// Creates a gateway for the recipient identity and plaintext frame limit.
    pub fn new(keys: Keys, max_frame_bytes: usize) -> Result<Self, GatewayError> {
        if max_frame_bytes == 0 {
            return Err(GatewayError::InvalidFrameLimit);
        }

        Ok(Self {
            keys,
            max_frame_bytes,
        })
    }

    /// Decrypts a serialized kind `1059` event into its inner relay request.
    ///
    /// The input is an event JSON text frame, not a NIP-01 `EVENT` array. The
    /// public-relay adapter owns the subscription and publish wire messages.
    pub async fn unwrap_request(
        &self,
        envelope: RelayFrame,
    ) -> Result<GatewayRequest, GatewayError> {
        let RelayFrame::Text(json) = envelope else {
            return Err(GatewayError::NonTextFrame);
        };
        let event: Event = serde_json::from_str(&json).map_err(GatewayError::InvalidEnvelope)?;
        let UnwrappedGift { sender, rumor } = nip59::extract_rumor(&self.keys, &event)
            .await
            .map_err(|error| GatewayError::GiftWrap(error.to_string()))?;
        if rumor.kind != Kind::PrivateDirectMessage {
            return Err(GatewayError::InvalidRumorKind);
        }
        self.check_frame_size(&rumor.content)?;
        let session_id = rumor
            .tags
            .iter()
            .find_map(|tag| {
                let values = tag.as_slice();
                (values.first().map(String::as_str) == Some("t"))
                    .then(|| values.get(1))
                    .flatten()
                    .and_then(|value| value.strip_prefix(SESSION_TAG_PREFIX))
                    .filter(|value| !value.is_empty())
            })
            .map(str::to_owned)
            .ok_or(GatewayError::MissingSessionTag)?;

        Ok(GatewayRequest {
            sender,
            session_id,
            frame: RelayFrame::Text(rumor.content),
        })
    }

    /// Encrypts a text relay response to the sender of `request`.
    ///
    /// The result is a serialized kind `1059` event. A public-relay adapter
    /// publishes it using its own NIP-01 `EVENT` command framing.
    pub async fn wrap_response(
        &self,
        request: &GatewayRequest,
        response: RelayFrame,
    ) -> Result<RelayFrame, GatewayError> {
        let RelayFrame::Text(content) = response else {
            return Err(GatewayError::NonTextFrame);
        };
        self.check_frame_size(&content)?;

        let recipient = request.sender.to_hex();
        let session = format!("{SESSION_TAG_PREFIX}{}", request.session_id);
        let rumor = EventBuilder::new(Kind::PrivateDirectMessage, content)
            .tags([
                nostr::Tag::parse(vec!["p", &recipient])
                    .map_err(|error| GatewayError::GiftWrap(error.to_string()))?,
                nostr::Tag::parse(vec!["t", &session])
                    .map_err(|error| GatewayError::GiftWrap(error.to_string()))?,
            ])
            .build(self.keys.public_key());
        let event = EventBuilder::gift_wrap(&self.keys, &request.sender, rumor, [])
            .await
            .map_err(|error| GatewayError::GiftWrap(error.to_string()))?;
        let json = serde_json::to_string(&event).map_err(GatewayError::InvalidEnvelope)?;

        Ok(RelayFrame::Text(json))
    }

    /// Decrypts an envelope and dispatches its plaintext through a virtual relay
    /// connection. Responses remain available from [`VirtualConnection::next_frame`]
    /// so adapters can preserve normal relay response ordering before wrapping
    /// each frame with [`Self::wrap_response`].
    pub async fn dispatch_envelope(
        &self,
        connection: &VirtualConnection,
        envelope: RelayFrame,
    ) -> Result<GatewayRequest, GatewayError> {
        let request = self.unwrap_request(envelope).await?;
        if !connection
            .is_authenticated_as(request.sender.to_bytes().as_slice())
            .await
        {
            // The first frame is normally AUTH, before the virtual session has
            // an owner. Once authenticated, each envelope must stay bound to
            // that NIP-17 seal author.
            if connection.is_authenticated().await {
                return Err(GatewayError::SenderMismatch);
            }
        }
        connection.receive_frame(request.frame.clone()).await;
        Ok(request)
    }

    fn check_frame_size(&self, frame: &str) -> Result<(), GatewayError> {
        if frame.len() > self.max_frame_bytes {
            return Err(GatewayError::FrameTooLarge {
                max: self.max_frame_bytes,
                got: frame.len(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nostr::{nips::nip59, EventBuilder, Keys, Kind, Tag};

    use super::{GatewayError, Nip17Gateway};
    use crate::transport::RelayFrame;

    const MAX_FRAME_BYTES: usize = 1024;

    #[tokio::test]
    async fn unwraps_a_real_nip17_gift_wrap_request() {
        let sender = Keys::generate();
        let recipient = Keys::generate();
        let request = r#"["REQ","inbox",{"kinds":[1]}]"#;
        let session_id = "test-session";
        let rumor = EventBuilder::new(Kind::PrivateDirectMessage, request)
            .tags([
                Tag::parse(vec!["p", &recipient.public_key().to_hex()]).expect("recipient tag"),
                Tag::parse(vec!["t", &format!("buzz-nip17-session:{session_id}")])
                    .expect("session tag"),
            ])
            .build(sender.public_key());
        let gift_wrap = EventBuilder::gift_wrap(&sender, &recipient.public_key(), rumor, [])
            .await
            .expect("build real NIP-59 gift wrap");
        let gateway = Nip17Gateway::new(recipient, MAX_FRAME_BYTES).expect("gateway");

        let decoded = gateway
            .unwrap_request(RelayFrame::Text(
                serde_json::to_string(&gift_wrap).expect("serialize gift wrap"),
            ))
            .await
            .expect("unwrap request");

        assert_eq!(decoded.sender, sender.public_key());
        assert_eq!(decoded.frame, RelayFrame::Text(request.to_string()));
        assert_eq!(decoded.session_id, session_id);
    }

    #[tokio::test]
    async fn wraps_a_real_nip17_gift_wrap_response() {
        let sender = Keys::generate();
        let gateway_keys = Keys::generate();
        let gateway = Nip17Gateway::new(gateway_keys.clone(), MAX_FRAME_BYTES).expect("gateway");
        let request = super::GatewayRequest {
            sender: sender.public_key(),
            session_id: "test-session".to_string(),
            frame: RelayFrame::Text(r#"["REQ","inbox",{}]"#.to_string()),
        };
        let response = r#"["EOSE","inbox"]"#;

        let wrapped = gateway
            .wrap_response(&request, RelayFrame::Text(response.to_string()))
            .await
            .expect("wrap response");
        let RelayFrame::Text(json) = wrapped else {
            panic!("response must be text");
        };
        let event = serde_json::from_str(&json).expect("deserialize gift wrap");
        let unwrapped = nip59::extract_rumor(&sender, &event)
            .await
            .expect("unwrap real NIP-59 response");

        assert_eq!(unwrapped.sender, gateway_keys.public_key());
        assert_eq!(unwrapped.rumor.kind, Kind::PrivateDirectMessage);
        assert_eq!(unwrapped.rumor.content, response);
    }

    #[tokio::test]
    async fn rejects_non_nip17_rumors_after_decryption() {
        let sender = Keys::generate();
        let recipient = Keys::generate();
        let rumor =
            EventBuilder::new(Kind::TextNote, "not a relay request").build(sender.public_key());
        let gift_wrap = EventBuilder::gift_wrap(&sender, &recipient.public_key(), rumor, [])
            .await
            .expect("build real NIP-59 gift wrap");
        let gateway = Nip17Gateway::new(recipient, MAX_FRAME_BYTES).expect("gateway");

        let error = gateway
            .unwrap_request(RelayFrame::Text(
                serde_json::to_string(&gift_wrap).expect("serialize gift wrap"),
            ))
            .await
            .expect_err("kind 1 rumor must be rejected");

        assert!(matches!(error, GatewayError::InvalidRumorKind));
    }

    #[tokio::test]
    async fn rejects_frames_that_exceed_the_configured_limit() {
        let sender = Keys::generate();
        let recipient = Keys::generate();
        let request = "x".repeat(MAX_FRAME_BYTES + 1);
        let rumor = EventBuilder::private_msg_rumor(recipient.public_key(), request)
            .build(sender.public_key());
        let gift_wrap = EventBuilder::gift_wrap(&sender, &recipient.public_key(), rumor, [])
            .await
            .expect("build real NIP-59 gift wrap");
        let gateway = Nip17Gateway::new(recipient, MAX_FRAME_BYTES).expect("gateway");

        let error = gateway
            .unwrap_request(RelayFrame::Text(
                serde_json::to_string(&gift_wrap).expect("serialize gift wrap"),
            ))
            .await
            .expect_err("oversize plaintext frame must be rejected");

        assert!(matches!(error, GatewayError::FrameTooLarge { .. }));
    }
}
