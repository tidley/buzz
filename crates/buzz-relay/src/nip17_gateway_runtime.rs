//! Opt-in public-relay runtime for the NIP-17 gateway transport.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use buzz_core::tenant::TenantContext;
use buzz_ws_client::{NostrWsConnection, RelayMessage};
use nostr::{Event, Keys, Kind, PublicKey};
use serde_json::json;
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::connection::VirtualConnection;
use crate::nip17_gateway::{GatewayRequest, Nip17Gateway};
use crate::state::AppState;
use crate::transport::RelayFrame;

const SESSION_DEDUP_CAPACITY: usize = 1_024;
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionKey {
    community: String,
    sender: String,
}

impl SessionKey {
    fn new(tenant: &TenantContext, sender: PublicKey) -> Self {
        Self {
            community: tenant.community().as_uuid().to_string(),
            sender: sender.to_hex(),
        }
    }
}

struct SessionInput {
    event_id: String,
    relay_url: String,
    request: GatewayRequest,
}

#[derive(Clone)]
struct EncryptedResponse {
    relay_url: String,
    event: Event,
}

/// Starts an independent public-relay loop for each configured NIP-17 relay.
///
/// The runtime binds all virtual sessions to the deployment's already-resolved
/// tenant. A session is keyed by that tenant and the verified NIP-59 seal
/// author, never by the unauthenticated outer gift-wrap author.
pub fn spawn(
    state: Arc<AppState>,
    tenant: TenantContext,
    private_key: &str,
    relays: Vec<String>,
) -> anyhow::Result<()> {
    let keys = Keys::parse(private_key).map_err(|error| {
        anyhow::anyhow!("invalid NIP-17 gateway key after config validation: {error}")
    })?;
    let gateway = Nip17Gateway::new(keys.clone(), state.config.max_frame_bytes)
        .map_err(|error| anyhow::anyhow!("invalid NIP-17 gateway: {error}"))?;
    let sessions = Arc::new(Mutex::new(HashMap::new()));
    let (responses, _) = broadcast::channel(1_024);

    for relay_url in relays {
        tokio::spawn(run_public_relay(
            relay_url,
            keys.clone(),
            gateway.clone(),
            Arc::clone(&state),
            tenant.clone(),
            Arc::clone(&sessions),
            responses.subscribe(),
            responses.clone(),
        ));
    }
    info!(pubkey = %keys.public_key(), "NIP-17 gateway runtime started");
    Ok(())
}

async fn run_public_relay(
    relay_url: String,
    keys: Keys,
    gateway: Nip17Gateway,
    state: Arc<AppState>,
    tenant: TenantContext,
    sessions: Arc<Mutex<HashMap<SessionKey, mpsc::Sender<SessionInput>>>>,
    mut responses: broadcast::Receiver<EncryptedResponse>,
    response_tx: broadcast::Sender<EncryptedResponse>,
) {
    loop {
        let mut connection = match NostrWsConnection::connect(&relay_url).await {
            Ok(connection) => connection,
            Err(error) => {
                warn!(%relay_url, %error, "NIP-17 gateway public relay connection failed");
                tokio::time::sleep(RECONNECT_DELAY).await;
                continue;
            }
        };
        let subscription_id = format!("buzz-nip17-{}", keys.public_key().to_hex());
        let request = json!([
            "REQ",
            subscription_id,
            { "kinds": [Kind::GiftWrap.as_u16()], "#p": [keys.public_key().to_hex()] }
        ]);
        if let Err(error) = connection.send_raw(&request).await {
            warn!(%relay_url, %error, "NIP-17 gateway subscription failed");
            tokio::time::sleep(RECONNECT_DELAY).await;
            continue;
        }
        info!(%relay_url, "NIP-17 gateway subscribed to gift wraps");

        loop {
            tokio::select! {
                response = responses.recv() => match response {
                    Ok(response) if response.relay_url == relay_url => {
                        if let Err(error) = connection.send_raw(&json!(["EVENT", response.event])).await {
                            warn!(%relay_url, %error, "NIP-17 gateway response publish failed");
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        warn!(%relay_url, count, "NIP-17 gateway response queue lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                },
                message = connection.next_event(Duration::from_secs(30)) => match message {
                    Ok(RelayMessage::Event { event, .. }) if event.kind == Kind::GiftWrap => {
                        route_envelope(
                            *event,
                            relay_url.clone(),
                            &gateway,
                            Arc::clone(&state),
                            tenant.clone(),
                            Arc::clone(&sessions),
                            response_tx.clone(),
                        ).await;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        debug!(%relay_url, %error, "NIP-17 gateway public relay disconnected");
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

async fn route_envelope(
    event: Event,
    relay_url: String,
    gateway: &Nip17Gateway,
    state: Arc<AppState>,
    tenant: TenantContext,
    sessions: Arc<Mutex<HashMap<SessionKey, mpsc::Sender<SessionInput>>>>,
    responses: broadcast::Sender<EncryptedResponse>,
) {
    let event_id = event.id.to_hex();
    let request = match gateway
        .unwrap_request(RelayFrame::Text(match serde_json::to_string(&event) {
            Ok(json) => json,
            Err(error) => {
                warn!(%error, "NIP-17 gateway could not serialize public event");
                return;
            }
        }))
        .await
    {
        Ok(request) => request,
        Err(error) => {
            debug!(%error, "NIP-17 gateway rejected public envelope");
            return;
        }
    };
    let key = SessionKey::new(&tenant, request.sender);
    let mut input = SessionInput {
        event_id,
        relay_url,
        request,
    };
    // A cancelled virtual connection can finish before its map entry is
    // observed. Remove that dead sender and create one replacement session.
    for _ in 0..2 {
        let sender = {
            let mut sessions = sessions.lock().await;
            if let Some(sender) = sessions.get(&key) {
                sender.clone()
            } else {
                let (sender, receiver) = mpsc::channel(128);
                sessions.insert(key.clone(), sender.clone());
                tokio::spawn(run_session(
                    receiver,
                    gateway.clone(),
                    Arc::clone(&state),
                    tenant.clone(),
                    responses.clone(),
                ));
                sender
            }
        };
        match sender.send(input).await {
            Ok(()) => return,
            Err(error) => {
                input = error.0;
                let mut sessions = sessions.lock().await;
                if sessions
                    .get(&key)
                    .is_some_and(|current| current.same_channel(&sender))
                {
                    sessions.remove(&key);
                }
            }
        }
    }
    warn!("NIP-17 gateway session stopped before receiving request");
}

async fn run_session(
    mut requests: mpsc::Receiver<SessionInput>,
    gateway: Nip17Gateway,
    state: Arc<AppState>,
    tenant: TenantContext,
    responses: broadcast::Sender<EncryptedResponse>,
) {
    let Some(first) = requests.recv().await else {
        return;
    };
    let sender = first.request.sender;
    let mut seen = HashSet::new();
    let mut seen_order = VecDeque::new();
    let mut relay_url = first.relay_url.clone();
    let mut connection =
        match VirtualConnection::open(state, SocketAddr::from(([0, 0, 0, 0], 0)), tenant).await {
            Ok(connection) => connection,
            Err(error) => {
                warn!(?error, "NIP-17 gateway could not open virtual session");
                return;
            }
        };

    dispatch_request(&mut connection, first, &mut seen, &mut seen_order).await;
    loop {
        tokio::select! {
            request = requests.recv() => match request {
                Some(request) => {
                    if request.request.sender != sender {
                        warn!("NIP-17 gateway session sender mismatch");
                        continue;
                    }
                    let request_relay = request.relay_url.clone();
                    if dispatch_request(&mut connection, request, &mut seen, &mut seen_order).await {
                        relay_url = request_relay;
                    }
                }
                None => break,
            },
            response = connection.next_frame() => match response {
                Some(response) => {
                    let request = GatewayRequest { sender, frame: RelayFrame::Text(String::new()) };
                    match gateway.wrap_response(&request, response).await {
                        Ok(RelayFrame::Text(json)) => match serde_json::from_str(&json) {
                            Ok(event) => { let _ = responses.send(EncryptedResponse { relay_url: relay_url.clone(), event }); }
                            Err(error) => warn!(%error, "NIP-17 gateway could not deserialize response envelope"),
                        },
                        Ok(_) => unreachable!("gateway only wraps text frames"),
                        Err(error) => warn!(%error, "NIP-17 gateway could not encrypt response"),
                    }
                }
                None => break,
            }
        }
    }
    connection.close().await;
}

async fn dispatch_request(
    connection: &mut VirtualConnection,
    request: SessionInput,
    seen: &mut HashSet<String>,
    seen_order: &mut VecDeque<String>,
) -> bool {
    if !seen.insert(request.event_id.clone()) {
        return false;
    }
    seen_order.push_back(request.event_id);
    if seen_order.len() > SESSION_DEDUP_CAPACITY {
        if let Some(oldest) = seen_order.pop_front() {
            seen.remove(&oldest);
        }
    }
    connection.receive_frame(request.request.frame).await;
    true
}

#[cfg(test)]
mod tests {
    use buzz_core::{tenant::TenantContext, CommunityId};
    use nostr::Keys;
    use uuid::Uuid;

    use super::SessionKey;

    #[test]
    fn sessions_are_scoped_by_verified_sender_and_tenant() {
        let sender = Keys::generate().public_key();
        let tenant_a =
            TenantContext::resolved(CommunityId::from_uuid(Uuid::from_u128(1)), "a.test");
        let tenant_b =
            TenantContext::resolved(CommunityId::from_uuid(Uuid::from_u128(2)), "b.test");
        assert_ne!(
            SessionKey::new(&tenant_a, sender),
            SessionKey::new(&tenant_b, sender)
        );
    }
}
