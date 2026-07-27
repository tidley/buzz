/// A transport-neutral client frame.
///
/// Relay protocol handling consumes text NIP-01 messages. Binary and control
/// frames remain explicit so WebSocket and tunneled transports share the same
/// lifecycle rules without forcing tunnel implementations to depend on Axum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayFrame {
    /// A UTF-8 NIP-01 protocol message.
    Text(String),
    /// A binary frame. WebSocket adapters may decode valid UTF-8 payloads.
    Binary(Vec<u8>),
    /// A transport keepalive request.
    Ping,
    /// A transport keepalive response.
    Pong,
    /// A transport close signal with its WebSocket-compatible status details.
    Close {
        /// WebSocket-compatible close status code.
        code: u16,
        /// Human-readable close status reason.
        reason: String,
    },
}

impl RelayFrame {
    /// Returns the frame payload when this is a text protocol message.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RelayFrame;

    #[test]
    fn text_frames_preserve_the_nostr_wire_payload() {
        let frame = RelayFrame::Text("[\"REQ\",\"sub\",{}]".to_string());

        assert_eq!(frame.as_text(), Some("[\"REQ\",\"sub\",{}]"));
    }

    #[test]
    fn binary_frames_are_not_text_frames() {
        assert_eq!(RelayFrame::Binary(vec![0xff]).as_text(), None);
    }

    #[test]
    fn close_frames_keep_restart_details_transport_neutral() {
        let frame = RelayFrame::Close {
            code: 1012,
            reason: "relay restarting".to_string(),
        };

        assert_eq!(
            frame,
            RelayFrame::Close {
                code: 1012,
                reason: "relay restarting".to_string(),
            }
        );
    }
}
