use std::{collections::HashMap, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use nostr::{
    nips::nip59::extract_rumor, Event, EventBuilder, JsonUtil, Keys, Kind, PublicKey, Tag,
};
use serde::{Deserialize, Serialize};
use tauri::{ipc::Channel, plugin::TauriPlugin, Manager, Runtime};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::{
    connect_async,
    tungstenite::protocol::{frame::coding::CloseCode, CloseFrame, Message},
};
use tokio_util::sync::CancellationToken;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(250);
const SEND_QUEUE_CAPACITY: usize = 64;
const NIP17_SESSION_TAG_PREFIX: &str = "buzz-nip17-session:";

pub(crate) fn install_crypto_provider() {
    // Dependencies enable both rustls providers; choose one before TLS setup.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

type Id = u32;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "data")]
enum WebSocketMessage {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(Option<CloseFramePayload>),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "transport", rename_all = "lowercase")]
enum SocketConfig {
    Direct,
    Nip17 {
        #[serde(rename = "gatewayPubkey")]
        gateway_pubkey: String,
        #[serde(rename = "publicRelayUrls")]
        public_relay_urls: Vec<String>,
    },
}

#[derive(Debug, Deserialize)]
struct CloseFramePayload {
    code: u16,
    reason: String,
}

impl From<WebSocketMessage> for Message {
    fn from(message: WebSocketMessage) -> Self {
        match message {
            WebSocketMessage::Text(value) => Message::Text(value.into()),
            WebSocketMessage::Binary(value) => Message::Binary(value.into()),
            WebSocketMessage::Ping(value) => Message::Ping(value.into()),
            WebSocketMessage::Pong(value) => Message::Pong(value.into()),
            WebSocketMessage::Close(frame) => Message::Close(frame.map(|frame| CloseFrame {
                code: CloseCode::from(frame.code),
                reason: frame.reason.into(),
            })),
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "type", content = "data")]
enum OutboundMessage {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(Option<CloseFramePayloadOut>),
    Error(String),
}

#[derive(Serialize)]
struct CloseFramePayloadOut {
    code: u16,
    reason: String,
}

struct SendRequest {
    message: Message,
    result: oneshot::Sender<Result<(), String>>,
}

struct ConnectionHandle {
    sender: mpsc::Sender<SendRequest>,
    cancel: CancellationToken,
    task: Mutex<Option<tauri::async_runtime::JoinHandle<()>>>,
}

#[derive(Clone)]
struct WebSocketManager {
    connections: Arc<Mutex<HashMap<Id, Arc<ConnectionHandle>>>>,
    connect_cancel: Arc<Mutex<CancellationToken>>,
}

impl Default for WebSocketManager {
    fn default() -> Self {
        Self {
            connections: Arc::default(),
            connect_cancel: Arc::new(Mutex::new(CancellationToken::new())),
        }
    }
}

impl WebSocketManager {
    async fn remove(&self, id: Id) -> Option<Arc<ConnectionHandle>> {
        self.connections.lock().await.remove(&id)
    }

    async fn disconnect_handle(handle: Arc<ConnectionHandle>) {
        handle.cancel.cancel();
        if let Some(mut task) = handle.task.lock().await.take() {
            if tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = task.await;
            }
        }
    }

    async fn disconnect(&self, id: Id) {
        if let Some(handle) = self.remove(id).await {
            Self::disconnect_handle(handle).await;
        }
    }
}

async fn open_connection(
    manager: &WebSocketManager,
    url: &str,
    on_message: Channel<serde_json::Value>,
) -> Result<Id, String> {
    let connect_cancel = manager.connect_cancel.lock().await.clone();
    let (socket, _) = tokio::select! {
        _ = connect_cancel.cancelled() => return Err("WebSocket connection cancelled".to_string()),
        result = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(url)) => result
            .map_err(|_| "WebSocket connection timed out".to_string())?
            .map_err(|error| error.to_string())?,
    };

    // Serialize registration with disconnect_all so a reload cannot miss a
    // connection that finished its handshake concurrently with teardown.
    let current_connect_cancel = manager.connect_cancel.lock().await;
    if connect_cancel.is_cancelled() {
        return Err("WebSocket connection cancelled".to_string());
    }

    let id = loop {
        let candidate = uuid::Uuid::new_v4().as_u128() as u32;
        if !manager.connections.lock().await.contains_key(&candidate) {
            break candidate;
        }
    };
    let (sender, receiver) = mpsc::channel(SEND_QUEUE_CAPACITY);
    let cancel = CancellationToken::new();
    let handle = Arc::new(ConnectionHandle {
        sender,
        cancel: cancel.clone(),
        task: Mutex::new(None),
    });
    let mut task_slot = handle.task.lock().await;
    manager.connections.lock().await.insert(id, handle.clone());

    let task_manager = manager.clone();
    let task = tauri::async_runtime::spawn(run_connection(
        id,
        socket,
        receiver,
        cancel,
        on_message,
        task_manager,
    ));
    *task_slot = Some(task);
    drop(task_slot);
    drop(current_connect_cancel);
    Ok(id)
}

#[tauri::command]
async fn connect(
    manager: tauri::State<'_, WebSocketManager>,
    state: tauri::State<'_, crate::app_state::AppState>,
    url: String,
    on_message: Channel<serde_json::Value>,
    config: Option<SocketConfig>,
) -> Result<Id, String> {
    match config.unwrap_or(SocketConfig::Direct) {
        SocketConfig::Direct => open_connection(manager.inner(), &url, on_message).await,
        SocketConfig::Nip17 {
            gateway_pubkey,
            public_relay_urls,
        } => {
            let keys = state
                .keys
                .lock()
                .map_err(|error| error.to_string())?
                .clone();
            open_nip17_connection(
                manager.inner(),
                keys,
                gateway_pubkey,
                public_relay_urls,
                on_message,
            )
            .await
        }
    }
}

async fn open_nip17_connection(
    manager: &WebSocketManager,
    keys: Keys,
    gateway_pubkey: String,
    public_relay_urls: Vec<String>,
    on_message: Channel<serde_json::Value>,
) -> Result<Id, String> {
    let gateway = PublicKey::parse(&gateway_pubkey)
        .map_err(|_| "NIP-17 gateway pubkey must be 64 hexadecimal characters".to_string())?;
    if public_relay_urls.is_empty() {
        return Err("NIP-17 requires at least one public relay".to_string());
    }
    if public_relay_urls.iter().any(|url| {
        !matches!(url::Url::parse(url), Ok(parsed) if matches!(parsed.scheme(), "ws" | "wss") && parsed.host().is_some())
    }) {
        return Err("NIP-17 public relays must use ws:// or wss:// URLs".to_string());
    }

    let connect_cancel = manager.connect_cancel.lock().await.clone();
    let mut sockets = Vec::new();
    for url in public_relay_urls {
        let result = tokio::select! {
            _ = connect_cancel.cancelled() => return Err("WebSocket connection cancelled".to_string()),
            result = tokio::time::timeout(CONNECT_TIMEOUT, connect_async(&url)) => result,
        };
        match result {
            Ok(Ok((socket, _))) => {
                eprintln!("buzz-desktop: NIP-17 connected to public relay {url}");
                sockets.push(socket);
            }
            Ok(Err(error)) => {
                eprintln!("buzz-desktop: NIP-17 public relay {url} rejected connection: {error}");
            }
            Err(_) => {
                eprintln!("buzz-desktop: NIP-17 public relay {url} connection timed out");
            }
        }
    }
    if sockets.is_empty() {
        return Err("Unable to connect to any configured NIP-17 public relay".to_string());
    }

    let current_connect_cancel = manager.connect_cancel.lock().await;
    if connect_cancel.is_cancelled() {
        return Err("WebSocket connection cancelled".to_string());
    }
    let id = loop {
        let candidate = uuid::Uuid::new_v4().as_u128() as u32;
        if !manager.connections.lock().await.contains_key(&candidate) {
            break candidate;
        }
    };
    let (sender, receiver) = mpsc::channel(SEND_QUEUE_CAPACITY);
    let cancel = CancellationToken::new();
    let handle = Arc::new(ConnectionHandle {
        sender,
        cancel: cancel.clone(),
        task: Mutex::new(None),
    });
    let mut task_slot = handle.task.lock().await;
    manager.connections.lock().await.insert(id, handle.clone());
    let task = tauri::async_runtime::spawn(run_nip17_connection(
        id,
        sockets,
        receiver,
        cancel,
        on_message,
        manager.clone(),
        keys,
        gateway,
    ));
    *task_slot = Some(task);
    drop(task_slot);
    drop(current_connect_cancel);
    Ok(id)
}

async fn run_nip17_connection(
    id: Id,
    sockets: Vec<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    mut receiver: mpsc::Receiver<SendRequest>,
    cancel: CancellationToken,
    on_message: Channel<serde_json::Value>,
    manager: WebSocketManager,
    keys: Keys,
    gateway: PublicKey,
) {
    let session_id = uuid::Uuid::new_v4().simple().to_string();
    let (incoming_tx, mut incoming_rx) = mpsc::channel(SEND_QUEUE_CAPACITY);
    let mut relay_senders = Vec::new();
    for socket in sockets {
        let (mut writer, mut reader) = socket.split();
        let (relay_tx, mut relay_rx) = mpsc::channel::<String>(SEND_QUEUE_CAPACITY);
        relay_senders.push(relay_tx);
        let inbound = incoming_tx.clone();
        let child_cancel = cancel.clone();
        tauri::async_runtime::spawn(async move {
            loop {
                tokio::select! {
                    _ = child_cancel.cancelled() => break,
                    message = relay_rx.recv() => match message {
                        Some(message) => if let Err(error) = writer.send(Message::Text(message.into())).await {
                            eprintln!("buzz-desktop: NIP-17 public relay write failed: {error}");
                            break;
                        },
                        None => break,
                    },
                    message = reader.next() => match message {
                        Some(Ok(Message::Text(message))) => { let _ = inbound.send(message.to_string()).await; }
                        Some(Ok(_)) => {}
                        _ => break,
                    },
                }
            }
        });
    }
    drop(incoming_tx);
    let subscription = format!("buzz-nip17-{id}");
    let request = serde_json::json!(["REQ", subscription, {"kinds": [1059], "#p": [keys.public_key().to_hex()]}]).to_string();
    for sender in &relay_senders {
        let _ = sender.send(request.clone()).await;
    }
    // A store-and-forward gateway has no connection until it receives a relay
    // frame. Send a harmless CLOSE to prompt its initial NIP-42 challenge.
    if let Ok(event) = wrap_nip17_frame(
        &keys,
        &gateway,
        &session_id,
        "[\"CLOSE\",\"__buzz_nip17_bootstrap\"]",
    )
    .await
    {
        if let Ok(event) = serde_json::from_str::<serde_json::Value>(&event) {
            let bootstrap = serde_json::json!(["EVENT", event]).to_string();
            for sender in &relay_senders {
                let _ = sender.send(bootstrap.clone()).await;
            }
            eprintln!("buzz-desktop: NIP-17 queued bootstrap envelope for public relays");
        } else {
            eprintln!("buzz-desktop: NIP-17 could not serialize bootstrap envelope");
        }
    } else {
        eprintln!("buzz-desktop: NIP-17 could not create bootstrap envelope");
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            request = receiver.recv() => {
                let Some(request) = request else { break };
                let result = match request.message {
                    Message::Text(frame) => wrap_nip17_frame(&keys, &gateway, &session_id, frame.as_str()).await,
                    _ => Err("NIP-17 relay transport only supports text frames".to_string()),
                };
                match result {
                    Ok(frame) => {
                        let result = serde_json::from_str::<serde_json::Value>(&frame)
                            .map_err(|error| error.to_string())
                            .and_then(|event| {
                                Ok(serde_json::json!(["EVENT", event]).to_string())
                            });
                        match result {
                            Ok(delivery) => {
                                let mut sent = false;
                                for sender in &relay_senders { if sender.send(delivery.clone()).await.is_ok() { sent = true; } }
                                let _ = request.result.send(if sent { Ok(()) } else { Err("NIP-17 public relays are disconnected".to_string()) });
                            }
                            Err(error) => { let _ = request.result.send(Err(error)); }
                        }
                    }
                    Err(error) => { let _ = request.result.send(Err(error)); }
                }
            }
            Some(frame) = incoming_rx.recv() => {
                if let Ok(message) = serde_json::from_str::<serde_json::Value>(&frame) {
                    if message.get(0).and_then(|value| value.as_str()) == Some("OK") {
                        let accepted = message.get(2).and_then(|value| value.as_bool()).unwrap_or(false);
                        let detail = message.get(3).and_then(|value| value.as_str()).unwrap_or("");
                        eprintln!("buzz-desktop: NIP-17 public relay publish accepted={accepted}: {detail}");
                    }
                }
                if let Some(frame) = unwrap_nip17_frame(&keys, &gateway, &session_id, &frame).await {
                    eprintln!("buzz-desktop: NIP-17 received gateway response");
                    if let Ok(value) = serde_json::to_value(OutboundMessage::Text(frame)) { let _ = on_message.send(value); }
                }
            }
        }
    }
    manager.remove(id).await;
}

async fn wrap_nip17_frame(
    keys: &Keys,
    gateway: &PublicKey,
    session_id: &str,
    frame: &str,
) -> Result<String, String> {
    let tag = Tag::parse(vec!["p", &gateway.to_hex()]).map_err(|error| error.to_string())?;
    let session = Tag::parse(vec![
        "t",
        &format!("{NIP17_SESSION_TAG_PREFIX}{session_id}"),
    ])
    .map_err(|error| error.to_string())?;
    let rumor = EventBuilder::new(Kind::from(14_u16), frame)
        .tags([tag, session])
        .build(keys.public_key());
    EventBuilder::gift_wrap(keys, gateway, rumor, [])
        .await
        .map_err(|error| error.to_string())
        .map(|event| event.as_json())
}

async fn unwrap_nip17_frame(
    keys: &Keys,
    gateway: &PublicKey,
    session_id: &str,
    frame: &str,
) -> Option<String> {
    let message: serde_json::Value = serde_json::from_str(frame).ok()?;
    let event: Event = serde_json::from_value(message.get(2)?.clone()).ok()?;
    let gift: serde_json::Value = serde_json::to_value(&event).ok()?;
    if event.kind != Kind::GiftWrap || !has_recipient(&gift, &keys.public_key().to_hex()) {
        return None;
    }
    let rumor = extract_rumor(keys, &event).await.ok()?;
    if rumor.sender != *gateway || rumor.rumor.kind != Kind::from(14_u16) {
        return None;
    }
    let rumor_value = serde_json::to_value(&rumor.rumor).ok()?;
    (has_recipient(&rumor_value, &keys.public_key().to_hex())
        && has_session(&rumor_value, session_id))
    .then_some(rumor.rumor.content)
}

fn has_session(event: &serde_json::Value, session_id: &str) -> bool {
    let expected = format!("{NIP17_SESSION_TAG_PREFIX}{session_id}");
    event["tags"].as_array().is_some_and(|tags| {
        tags.iter().any(|tag| {
            tag.as_array().is_some_and(|tag| {
                tag.len() >= 2 && tag[0] == "t" && tag[1].as_str() == Some(expected.as_str())
            })
        })
    })
}

fn has_recipient(event: &serde_json::Value, recipient: &str) -> bool {
    event["tags"].as_array().is_some_and(|tags| {
        tags.iter().any(|tag| {
            tag.as_array().is_some_and(|tag| {
                tag.len() >= 2
                    && tag[0] == "p"
                    && tag[1]
                        .as_str()
                        .is_some_and(|value| value.eq_ignore_ascii_case(recipient))
            })
        })
    })
}

async fn send_message(
    manager: &WebSocketManager,
    id: Id,
    message: WebSocketMessage,
) -> Result<(), String> {
    let handle = manager
        .connections
        .lock()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(|| format!("WebSocket connection {id} not found"))?;
    let (result_tx, result_rx) = oneshot::channel();
    tokio::time::timeout(
        WRITE_TIMEOUT,
        handle.sender.send(SendRequest {
            message: message.into(),
            result: result_tx,
        }),
    )
    .await
    .map_err(|_| "WebSocket send queue timed out".to_string())?
    .map_err(|_| "WebSocket connection closed".to_string())?;

    tokio::time::timeout(WRITE_TIMEOUT, result_rx)
        .await
        .map_err(|_| "WebSocket send timed out".to_string())?
        .map_err(|_| "WebSocket connection closed".to_string())?
}

#[tauri::command]
async fn send(
    manager: tauri::State<'_, WebSocketManager>,
    id: Id,
    message: WebSocketMessage,
) -> Result<(), String> {
    send_message(manager.inner(), id, message).await
}

#[tauri::command]
async fn disconnect(manager: tauri::State<'_, WebSocketManager>, id: Id) -> Result<(), String> {
    manager.disconnect(id).await;
    Ok(())
}

#[tauri::command]
async fn disconnect_all(manager: tauri::State<'_, WebSocketManager>) -> Result<(), String> {
    let mut connect_cancel = manager.connect_cancel.lock().await;
    connect_cancel.cancel();
    *connect_cancel = CancellationToken::new();
    let handles = {
        let mut connections = manager.connections.lock().await;
        connections
            .drain()
            .map(|(_, handle)| handle)
            .collect::<Vec<_>>()
    };
    futures_util::future::join_all(handles.into_iter().map(WebSocketManager::disconnect_handle))
        .await;
    Ok(())
}

async fn run_connection<S>(
    id: Id,
    mut socket: tokio_tungstenite::WebSocketStream<S>,
    mut receiver: mpsc::Receiver<SendRequest>,
    cancel: CancellationToken,
    on_message: Channel<serde_json::Value>,
    manager: WebSocketManager,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                let _ = tokio::time::timeout(
                    SHUTDOWN_TIMEOUT,
                    socket.send(Message::Close(Some(CloseFrame {
                        code: CloseCode::Normal,
                        reason: "disconnect".into(),
                    }))),
                ).await;
                break;
            }
            request = receiver.recv() => {
                let Some(request) = request else { break };
                let result = tokio::time::timeout(WRITE_TIMEOUT, socket.send(request.message))
                    .await
                    .map_err(|_| "WebSocket send timed out".to_string())
                    .and_then(|result| result.map_err(|error| error.to_string()));
                let failed = result.is_err();
                let _ = request.result.send(result);
                if failed { break; }
            }
            incoming = socket.next() => {
                let message = match incoming {
                    Some(Ok(message)) => outbound_message(message),
                    Some(Err(error)) => OutboundMessage::Error(error.to_string()),
                    None => OutboundMessage::Close(None),
                };
                let terminal = matches!(message, OutboundMessage::Close(_) | OutboundMessage::Error(_));
                if let Ok(value) = serde_json::to_value(message) {
                    let _ = on_message.send(value);
                }
                if terminal { break; }
            }
        }
    }
    manager.remove(id).await;
}

fn outbound_message(message: Message) -> OutboundMessage {
    match message {
        Message::Text(value) => OutboundMessage::Text(value.to_string()),
        Message::Binary(value) => OutboundMessage::Binary(value.to_vec()),
        Message::Ping(value) => OutboundMessage::Ping(value.to_vec()),
        Message::Pong(value) => OutboundMessage::Pong(value.to_vec()),
        Message::Close(frame) => OutboundMessage::Close(frame.map(|frame| CloseFramePayloadOut {
            code: frame.code.into(),
            reason: frame.reason.to_string(),
        })),
        Message::Frame(_) => OutboundMessage::Error("unexpected raw WebSocket frame".to_string()),
    }
}

pub fn init<R: Runtime>() -> TauriPlugin<R> {
    install_crypto_provider();
    tauri::plugin::Builder::new("websocket")
        .invoke_handler(tauri::generate_handler![
            connect,
            send,
            disconnect,
            disconnect_all
        ])
        .setup(|app, _api| {
            app.manage(WebSocketManager::default());
            Ok(())
        })
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use std::sync::atomic::{AtomicBool, Ordering};

    use tauri::ipc::InvokeResponseBody;
    use tokio::io::duplex;
    use tokio_tungstenite::{tungstenite::protocol::Role, WebSocketStream};

    fn silent_channel() -> Channel<serde_json::Value> {
        Channel::new(|_: InvokeResponseBody| Ok(()))
    }

    #[tokio::test]
    async fn nip17_only_accepts_frames_wrapped_by_the_configured_gateway() {
        let recipient = Keys::generate();
        let gateway = Keys::generate();
        let attacker = Keys::generate();
        let session_id = "current-session";
        let recipient_tag = Tag::parse(vec!["p", &recipient.public_key().to_hex()]).unwrap();
        let session_tag = Tag::parse(vec![
            "t",
            &format!("{NIP17_SESSION_TAG_PREFIX}{session_id}"),
        ])
        .unwrap();

        let response = EventBuilder::new(Kind::from(14_u16), "[\"NOTICE\",\"gateway\"]")
            .tags([recipient_tag.clone(), session_tag])
            .build(gateway.public_key());
        let gateway_wrap = EventBuilder::gift_wrap(&gateway, &recipient.public_key(), response, [])
            .await
            .unwrap();
        let attacker_response = EventBuilder::new(Kind::from(14_u16), "[\"NOTICE\",\"attacker\"]")
            .tags([recipient_tag])
            .build(attacker.public_key());
        let attacker_wrap =
            EventBuilder::gift_wrap(&attacker, &recipient.public_key(), attacker_response, [])
                .await
                .unwrap();

        let gateway_frame = serde_json::json!(["EVENT", "gift-wraps", gateway_wrap]).to_string();
        let attacker_frame = serde_json::json!(["EVENT", "gift-wraps", attacker_wrap]).to_string();
        assert_eq!(
            unwrap_nip17_frame(
                &recipient,
                &gateway.public_key(),
                session_id,
                &gateway_frame
            )
            .await,
            Some("[\"NOTICE\",\"gateway\"]".to_string())
        );
        assert_eq!(
            unwrap_nip17_frame(
                &recipient,
                &gateway.public_key(),
                session_id,
                &attacker_frame
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn nip17_wraps_outbound_relay_frames_for_the_gateway() {
        let client = Keys::generate();
        let gateway = Keys::generate();

        let wrapped = wrap_nip17_frame(
            &client,
            &gateway.public_key(),
            "test-session",
            "[\"REQ\",\"sub\",{\"kinds\":[9]}]",
        )
        .await
        .unwrap();
        let event = Event::from_json(wrapped).unwrap();
        let rumor = extract_rumor(&gateway, &event).await.unwrap();

        assert_eq!(event.kind, Kind::GiftWrap);
        assert_eq!(rumor.sender, client.public_key());
        assert_eq!(rumor.rumor.kind, Kind::from(14_u16));
        assert_eq!(rumor.rumor.content, "[\"REQ\",\"sub\",{\"kinds\":[9]}]");
    }

    #[tokio::test]
    async fn secure_websocket_reaches_tls_without_panicking() {
        install_crypto_provider();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let result = std::panic::AssertUnwindSafe(tokio_tungstenite::connect_async(format!(
            "wss://{address}"
        )))
        .catch_unwind()
        .await;

        assert!(result.is_ok(), "TLS setup must not panic");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn live_tcp_server_connect_send_and_disconnect() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (received_tx, received_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let message = socket.next().await.unwrap().unwrap();
            received_tx.send(message).unwrap();
            while let Some(message) = socket.next().await {
                if matches!(message, Ok(Message::Close(_))) {
                    break;
                }
            }
        });

        let manager = WebSocketManager::default();
        let id = open_connection(&manager, &format!("ws://{address}"), silent_channel())
            .await
            .unwrap();
        send_message(&manager, id, WebSocketMessage::Text("live-probe".into()))
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), received_rx)
                .await
                .unwrap()
                .unwrap(),
            Message::Text("live-probe".into())
        );

        manager.disconnect(id).await;
        assert!(!manager.connections.lock().await.contains_key(&id));
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("live server should observe native socket shutdown")
            .unwrap();
    }

    #[tokio::test]
    async fn eof_removes_connection() {
        let manager = WebSocketManager::default();
        let (client_io, server_io) = duplex(1024);
        let (client, server) = tokio::join!(
            WebSocketStream::from_raw_socket(client_io, Role::Client, None),
            WebSocketStream::from_raw_socket(server_io, Role::Server, None),
        );
        let (sender, receiver) = mpsc::channel(SEND_QUEUE_CAPACITY);
        let handle = Arc::new(ConnectionHandle {
            sender,
            cancel: CancellationToken::new(),
            task: Mutex::new(None),
        });
        manager.connections.lock().await.insert(1, handle.clone());
        let task = tauri::async_runtime::spawn(run_connection(
            1,
            client,
            receiver,
            handle.cancel.clone(),
            silent_channel(),
            manager.clone(),
        ));
        *handle.task.lock().await = Some(task);

        drop(server);
        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.connections.lock().await.contains_key(&1) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("EOF should clean up its native connection ID");
    }

    #[tokio::test]
    async fn disconnect_removes_and_drops_task_before_returning() {
        struct DropGuard(Arc<AtomicBool>);
        impl Drop for DropGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let manager = WebSocketManager::default();
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let (ready_tx, ready_rx) = oneshot::channel();
        let (sender, _receiver) = mpsc::channel(SEND_QUEUE_CAPACITY);
        let handle = Arc::new(ConnectionHandle {
            sender,
            cancel: CancellationToken::new(),
            task: Mutex::new(Some(tauri::async_runtime::spawn(async move {
                let _guard = DropGuard(task_dropped);
                ready_tx.send(()).unwrap();
                std::future::pending::<()>().await;
            }))),
        });
        manager.connections.lock().await.insert(7, handle);
        ready_rx.await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), manager.disconnect(7))
            .await
            .expect("disconnect should abort an unresponsive task");
        assert!(!manager.connections.lock().await.contains_key(&7));
        assert!(dropped.load(Ordering::SeqCst));

        // Repeated teardown is intentionally a no-op.
        manager.disconnect(7).await;
    }

    #[tokio::test]
    async fn teardown_gate_stays_closed_until_tasks_stop() {
        let manager = WebSocketManager::default();
        let gate = manager.connect_cancel.lock().await;
        let (sender, _receiver) = mpsc::channel(SEND_QUEUE_CAPACITY);
        let handle = Arc::new(ConnectionHandle {
            sender,
            cancel: CancellationToken::new(),
            task: Mutex::new(Some(tauri::async_runtime::spawn(async {
                std::future::pending::<()>().await;
            }))),
        });
        manager.connections.lock().await.insert(1, handle);
        gate.cancel();
        let handles = {
            let mut connections = manager.connections.lock().await;
            connections
                .drain()
                .map(|(_, handle)| handle)
                .collect::<Vec<_>>()
        };

        let shutdown = futures_util::future::join_all(
            handles.into_iter().map(WebSocketManager::disconnect_handle),
        );
        assert!(manager.connect_cancel.try_lock().is_err());
        shutdown.await;
        drop(gate);
        assert!(manager.connect_cancel.try_lock().is_ok());
    }

    #[tokio::test]
    async fn one_connection_does_not_block_another_send_queue() {
        let manager = WebSocketManager::default();
        let (blocked_sender, blocked_receiver) = mpsc::channel(1);
        blocked_sender
            .send(SendRequest {
                message: Message::Text("blocked".into()),
                result: oneshot::channel().0,
            })
            .await
            .unwrap();
        let blocked = Arc::new(ConnectionHandle {
            sender: blocked_sender,
            cancel: CancellationToken::new(),
            task: Mutex::new(None),
        });
        manager.connections.lock().await.insert(1, blocked);

        let (healthy_sender, mut healthy_receiver) = mpsc::channel(1);
        let healthy = Arc::new(ConnectionHandle {
            sender: healthy_sender.clone(),
            cancel: CancellationToken::new(),
            task: Mutex::new(None),
        });
        manager.connections.lock().await.insert(2, healthy);

        let (result, _) = oneshot::channel();
        tokio::time::timeout(
            Duration::from_millis(50),
            healthy_sender.send(SendRequest {
                message: Message::Text("healthy".into()),
                result,
            }),
        )
        .await
        .expect("a full queue on one connection must not block another")
        .unwrap();
        assert!(healthy_receiver.recv().await.is_some());
        drop(blocked_receiver);
    }
}
