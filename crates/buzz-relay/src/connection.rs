//! WebSocket connection lifecycle: semaphore → challenge → recv/send/heartbeat loops → cleanup.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message as WsMessage, WebSocket};
use futures_util::{Sink, SinkExt, StreamExt};
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use buzz_auth::{generate_challenge, AuthContext, LimitType};
use buzz_core::tenant::TenantContext;
use nostr::Filter;

use crate::handlers;
use crate::protocol::{ClientMessage, RelayMessage};
use crate::state::{run_registered_community_connection, AppState, CommunityConnectionGuard};
use crate::transport::RelayFrame;
use buzz_pubsub::EventTopic;

/// Maximum time a new socket may hold a connection slot without completing NIP-42 auth.
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared mutable subscription map for a single WebSocket connection.
pub(crate) type ConnectionSubscriptions = Arc<Mutex<HashMap<String, Vec<Filter>>>>;

/// Maximum outbound data frames buffered into the websocket sink before one flush.
const MAX_WS_SEND_BATCH: usize = 64;

/// NIP-42 authentication state for a single connection.
#[derive(Debug, Clone)]
pub enum AuthState {
    /// Challenge has been sent; awaiting a signed AUTH event from the client.
    Pending {
        /// The random challenge string sent to the client.
        challenge: String,
    },
    /// Client has successfully authenticated.
    Authenticated(AuthContext),
    /// Authentication attempt was rejected.
    Failed,
}

/// Per-connection state split by access pattern:
/// - `auth_state`: RwLock (read-heavy after initial auth)
/// - `subscriptions`: Mutex (write-heavy during REQ/CLOSE)
/// - `send_tx`, `ctrl_tx`, `cancel`: outside any lock (Clone+Send, no coordination needed)
pub struct ConnectionState {
    /// Unique identifier for this connection.
    pub conn_id: Uuid,
    /// The community this connection is bound to, resolved from the connection
    /// host at row zero (before any frame is read) and never overridable by
    /// client-supplied input. Every handler reads tenant scope from here.
    pub tenant: TenantContext,
    /// Remote socket address of the client.
    pub remote_addr: SocketAddr,
    /// Current NIP-42 authentication state.
    pub auth_state: RwLock<AuthState>,
    /// Active subscriptions keyed by subscription ID.
    pub subscriptions: ConnectionSubscriptions,
    /// Sender for outbound data messages (EVENT, NOTICE, OK, etc.).
    pub send_tx: mpsc::Sender<RelayFrame>,
    /// Sender for outbound control frames (Pong, Close).
    /// Separate channel with priority drain — if this channel fills too,
    /// the connection is closed (writer is completely stalled).
    pub ctrl_tx: mpsc::Sender<RelayFrame>,
    /// Token used to signal graceful shutdown of this connection's tasks.
    pub cancel: CancellationToken,
    /// Consecutive buffer-full events. Cancel only after `grace_limit`.
    /// Shared with `ConnectionManager::ConnEntry` so both direct sends and
    /// fan-out broadcasts track the same counter.
    pub backpressure_count: Arc<AtomicU8>,
    /// Configurable slow-client grace limit (from `Config::slow_client_grace_limit`).
    pub grace_limit: u8,
}

/// An in-process connection which uses the normal NIP-01 relay dispatcher.
///
/// Tunnel adapters supply and consume [`RelayFrame`] values without depending
/// on Axum. Call [`Self::close`] when the adapter session ends so subscriptions
/// and presence are released just as they are for a WebSocket disconnect.
pub struct VirtualConnection {
    conn: Arc<ConnectionState>,
    state: Arc<AppState>,
    data_rx: mpsc::Receiver<RelayFrame>,
    ctrl_rx: mpsc::Receiver<RelayFrame>,
    permit: OwnedSemaphorePermit,
    community_guard: CommunityConnectionGuard,
    auth_timeout_task: tokio::task::JoinHandle<()>,
}

/// Why a virtual connection could not be opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VirtualConnectionError {
    /// The relay has reached its configured connection limit.
    ConnectionLimitReached,
    /// The tenant was archived or could not be revalidated.
    CommunityUnavailable,
}

impl ConnectionState {
    /// Sends a data message to this connection's outbound channel.
    ///
    /// On a full buffer, increments the backpressure counter. The first
    /// `grace_limit` occurrences log a warning; sustained backpressure
    /// cancels the connection to prevent unbounded memory growth.
    pub fn send(&self, msg: String) -> bool {
        match self.send_tx.try_send(RelayFrame::Text(msg)) {
            Ok(_) => {
                // Successful send resets the grace counter.
                self.backpressure_count.store(0, Ordering::Relaxed);
                true
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                let count = self.backpressure_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.grace_limit {
                    warn!(conn_id = %self.conn_id, count, "sustained backpressure — closing slow client");
                    metrics::counter!("buzz_ws_backpressure_disconnects_total").increment(1);
                    self.cancel.cancel();
                } else {
                    warn!(conn_id = %self.conn_id, count, grace = self.grace_limit, "send buffer full — grace {count}/{}", self.grace_limit);
                }
                false
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                debug!(conn_id = %self.conn_id, "send channel closed");
                false
            }
        }
    }
}

impl VirtualConnection {
    /// Opens a transport-neutral relay session and queues its NIP-42 challenge.
    pub async fn open(
        state: Arc<AppState>,
        addr: SocketAddr,
        tenant: TenantContext,
    ) -> Result<Self, VirtualConnectionError> {
        let permit = state
            .conn_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| VirtualConnectionError::ConnectionLimitReached)?;
        let conn_id = Uuid::new_v4();
        let cancel = CancellationToken::new();
        let community_guard =
            state
                .community_connections
                .register(conn_id, tenant.community(), cancel.clone());
        if !matches!(
            state.db.is_community_active(tenant.community()).await,
            Ok(true)
        ) || cancel.is_cancelled()
        {
            cancel.cancel();
            return Err(VirtualConnectionError::CommunityUnavailable);
        }

        let challenge = generate_challenge();
        let (tx, data_rx) = mpsc::channel(state.config.send_buffer_size);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(8);
        let backpressure_count = Arc::new(AtomicU8::new(0));
        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let conn = Arc::new(ConnectionState {
            conn_id,
            tenant,
            remote_addr: addr,
            auth_state: RwLock::new(AuthState::Pending {
                challenge: challenge.clone(),
            }),
            subscriptions: Arc::clone(&subscriptions),
            send_tx: tx.clone(),
            ctrl_tx: ctrl_tx.clone(),
            cancel: cancel.clone(),
            backpressure_count: Arc::clone(&backpressure_count),
            grace_limit: state.config.slow_client_grace_limit,
        });
        state.conn_manager.register(
            conn_id,
            tx.clone(),
            ctrl_tx,
            cancel.clone(),
            conn.tenant.community(),
            backpressure_count,
            subscriptions,
            state.config.slow_client_grace_limit,
        );
        // The receiver is retained by this session, so this cannot fail here.
        let _ = tx
            .send(RelayFrame::Text(RelayMessage::auth_challenge(&challenge)))
            .await;

        let auth_timeout_conn = Arc::clone(&conn);
        let auth_timeout_task = tokio::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(AUTH_TIMEOUT) => {
                    if !matches!(*auth_timeout_conn.auth_state.read().await, AuthState::Authenticated(_)) {
                        auth_timeout_conn.cancel.cancel();
                    }
                }
                _ = cancel.cancelled() => {}
            }
        });

        Ok(Self {
            conn,
            state,
            data_rx,
            ctrl_rx,
            permit,
            community_guard,
            auth_timeout_task,
        })
    }

    /// Dispatches one incoming frame through the same auth and protocol handlers
    /// used by WebSocket connections.
    pub async fn receive_frame(&self, frame: RelayFrame) {
        if !handle_relay_frame(frame, Arc::clone(&self.conn), Arc::clone(&self.state), None).await {
            self.conn.cancel.cancel();
        }
    }

    /// Returns whether the session has authenticated as `pubkey`.
    pub async fn is_authenticated_as(&self, pubkey: &[u8]) -> bool {
        matches!(
            &*self.conn.auth_state.read().await,
            AuthState::Authenticated(context) if context.pubkey.to_bytes().as_slice() == pubkey
        )
    }

    /// Returns whether NIP-42 authentication has completed successfully.
    pub async fn is_authenticated(&self) -> bool {
        matches!(
            *self.conn.auth_state.read().await,
            AuthState::Authenticated(_)
        )
    }

    /// Receives the next relay response, prioritizing control frames.
    pub async fn next_frame(&mut self) -> Option<RelayFrame> {
        if let Ok(frame) = self.ctrl_rx.try_recv() {
            return Some(frame);
        }
        if let Ok(frame) = self.data_rx.try_recv() {
            return Some(frame);
        }
        tokio::select! {
            biased;
            Some(frame) = self.ctrl_rx.recv() => Some(frame),
            Some(frame) = self.data_rx.recv() => Some(frame),
            _ = self.conn.cancel.cancelled() => None,
            else => None,
        }
    }

    /// Closes the session and releases its subscriptions and presence state.
    pub async fn close(self) {
        self.conn.cancel.cancel();
        self.auth_timeout_task.abort();
        cleanup_connection(&self.conn, &self.state).await;
        drop(self.community_guard);
        drop(self.permit);
    }
}

/// Entry point for a new WebSocket connection.
///
/// Acquires a connection semaphore permit, sends the NIP-42 AUTH challenge,
/// then drives the send, heartbeat, and receive loops until the connection closes.
pub async fn handle_connection(
    socket: WebSocket,
    state: Arc<AppState>,
    addr: SocketAddr,
    tenant: TenantContext,
) {
    let conn_id = Uuid::new_v4();
    let cancel = CancellationToken::new();
    let community_id = tenant.community();
    let registry = Arc::clone(&state.community_connections);
    let check_state = Arc::clone(&state);
    let run_state = Arc::clone(&state);
    run_registered_community_connection(
        &registry,
        conn_id,
        community_id,
        cancel.clone(),
        move || async move { check_state.db.is_community_active(community_id).await },
        move || handle_active_connection(socket, run_state, addr, tenant, conn_id, cancel),
    )
    .await;
}

async fn handle_active_connection(
    socket: WebSocket,
    state: Arc<AppState>,
    addr: SocketAddr,
    tenant: TenantContext,
    conn_id: Uuid,
    cancel: CancellationToken,
) {
    let permit = match state.conn_semaphore.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            warn!("Connection limit reached, rejecting {addr}");
            return;
        }
    };

    let challenge = generate_challenge();

    let (tx, rx) = mpsc::channel::<RelayFrame>(state.config.send_buffer_size);
    // Control channel for Pong/Close — small capacity, guaranteed delivery
    // even when the data buffer is full.
    let (ctrl_tx, ctrl_rx) = mpsc::channel::<RelayFrame>(8);

    let backpressure_count = Arc::new(AtomicU8::new(0));
    let subscriptions = Arc::new(Mutex::new(HashMap::new()));

    let conn = Arc::new(ConnectionState {
        conn_id,
        tenant,
        remote_addr: addr,
        auth_state: RwLock::new(AuthState::Pending {
            challenge: challenge.clone(),
        }),
        subscriptions: Arc::clone(&subscriptions),
        send_tx: tx.clone(),
        ctrl_tx: ctrl_tx.clone(),
        cancel: cancel.clone(),
        backpressure_count: Arc::clone(&backpressure_count),
        grace_limit: state.config.slow_client_grace_limit,
    });

    info!(conn_id = %conn_id, addr = %addr, "WebSocket connection established");
    metrics::counter!(
        "buzz_ws_connections_total",
        "community" => conn.tenant.host().to_owned()
    )
    .increment(1);

    let challenge_msg = RelayMessage::auth_challenge(&challenge);
    if tx.send(RelayFrame::Text(challenge_msg)).await.is_err() {
        warn!(conn_id = %conn_id, "Failed to send AUTH challenge — client disconnected immediately");
        return;
    }

    // Gauge incremented AFTER challenge send succeeds — early disconnects
    // don't leak. Decremented in the cleanup path below.
    metrics::gauge!("buzz_ws_connections_active").increment(1.0);

    // Register after challenge succeeds — avoids leaked entries on early disconnect.
    state.conn_manager.register(
        conn_id,
        tx.clone(),
        ctrl_tx.clone(),
        cancel.clone(),
        conn.tenant.community(),
        Arc::clone(&backpressure_count),
        subscriptions,
        state.config.slow_client_grace_limit,
    );

    let (ws_send, ws_recv) = socket.split();

    let send_cancel = cancel.child_token();
    let send_task = tokio::spawn(send_loop(ws_send, rx, ctrl_rx, send_cancel));

    let missed_pongs = Arc::new(AtomicU8::new(0));
    let heartbeat_cancel = cancel.clone();
    let heartbeat_task = tokio::spawn(heartbeat_loop(
        ctrl_tx,
        Arc::clone(&missed_pongs),
        heartbeat_cancel,
    ));

    let auth_timeout_conn = Arc::clone(&conn);
    let auth_timeout_cancel = cancel.clone();
    let auth_timeout_task = tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep(AUTH_TIMEOUT) => {
                let authenticated = matches!(
                    *auth_timeout_conn.auth_state.read().await,
                    AuthState::Authenticated(_)
                );
                if !authenticated {
                    warn!(
                        conn_id = %auth_timeout_conn.conn_id,
                        timeout_secs = AUTH_TIMEOUT.as_secs(),
                        "NIP-42 auth timeout — closing connection"
                    );
                    metrics::counter!("buzz_ws_auth_timeouts_total").increment(1);
                    auth_timeout_cancel.cancel();
                }
            }
            _ = auth_timeout_cancel.cancelled() => {}
        }
    });

    recv_loop(
        ws_recv,
        Arc::clone(&conn),
        Arc::clone(&state),
        Arc::clone(&missed_pongs),
        cancel.clone(),
    )
    .await;

    cancel.cancel();
    let _ = send_task.await;
    let _ = heartbeat_task.await;
    let _ = auth_timeout_task.await;

    cleanup_connection(&conn, &state).await;
    metrics::gauge!("buzz_ws_connections_active").decrement(1.0);
    info!(conn_id = %conn_id, addr = %addr, "WebSocket connection closed");

    drop(permit);
}

/// Outbound send loop with control-frame priority.
///
/// Control frames (Pong, Close) are drained first on every iteration,
/// giving them priority over data frames. If the underlying socket writer
/// is stalled, control frames queue in the small ctrl_rx buffer; callers
/// treat a full control channel as terminal (Bug 7 fix).
async fn send_loop(
    ws_send: futures_util::stream::SplitSink<WebSocket, WsMessage>,
    data_rx: mpsc::Receiver<RelayFrame>,
    ctrl_rx: mpsc::Receiver<RelayFrame>,
    cancel: CancellationToken,
) {
    send_loop_inner(ws_send, data_rx, ctrl_rx, cancel).await;
}

async fn send_loop_inner<S>(
    mut ws_send: S,
    mut data_rx: mpsc::Receiver<RelayFrame>,
    mut ctrl_rx: mpsc::Receiver<RelayFrame>,
    cancel: CancellationToken,
) where
    S: Sink<WsMessage> + Unpin,
{
    loop {
        // Priority: drain all pending control frames before data.
        while let Ok(ctrl_msg) = ctrl_rx.try_recv() {
            if ws_send
                .send(relay_frame_to_ws_message(ctrl_msg))
                .await
                .is_err()
            {
                return;
            }
        }

        tokio::select! {
            // Biased: cancel > control > data. Cancel must win immediately
            // so backpressure-triggered shutdown isn't starved by queued data.
            biased;
            _ = cancel.cancelled() => {
                // Drain any queued control frames before closing. A ban
                // disconnect queues its `OK false "blocked: …"` reason frame on
                // ctrl and then cancels; without this drain the biased branch
                // would send Close first and the client would never learn why
                // (the top-of-loop drain does not run again after we break).
                // This makes "queue frame on ctrl, then cancel" a safe idiom.
                while let Ok(ctrl_msg) = ctrl_rx.try_recv() {
                    if ws_send.send(relay_frame_to_ws_message(ctrl_msg)).await.is_err() {
                        break;
                    }
                }
                let _ = ws_send.send(WsMessage::Close(None)).await;
                break;
            }
            Some(ctrl_msg) = ctrl_rx.recv() => {
                if ws_send.send(relay_frame_to_ws_message(ctrl_msg)).await.is_err() {
                    break;
                }
            }
            Some(msg) = data_rx.recv() => {
                let mut batched = 1usize;
                if ws_send.feed(relay_frame_to_ws_message(msg)).await.is_err() {
                    break;
                }

                while batched < MAX_WS_SEND_BATCH {
                    match data_rx.try_recv() {
                        Ok(next) => {
                            if ws_send.feed(relay_frame_to_ws_message(next)).await.is_err() {
                                return;
                            }
                            batched += 1;
                        }
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }

                if ws_send.flush().await.is_err() {
                    break;
                }
                metrics::histogram!("buzz_ws_send_batch_size").record(batched as f64);
            }
        }
    }
}

/// Converts an Axum WebSocket message as it enters the transport-neutral relay.
fn ws_message_to_relay_frame(message: WsMessage) -> RelayFrame {
    match message {
        WsMessage::Text(text) => RelayFrame::Text(text.to_string()),
        WsMessage::Binary(bytes) => RelayFrame::Binary(bytes.to_vec()),
        WsMessage::Ping(_) => RelayFrame::Ping,
        WsMessage::Pong(_) => RelayFrame::Pong,
        WsMessage::Close(close) => {
            let (code, reason) = close
                .map(|close| (close.code, close.reason.to_string()))
                .unwrap_or((1000, String::new()));
            RelayFrame::Close { code, reason }
        }
    }
}

/// Converts relay frames only when they leave through Axum's WebSocket sink.
fn relay_frame_to_ws_message(frame: RelayFrame) -> WsMessage {
    match frame {
        RelayFrame::Text(text) => WsMessage::Text(text.into()),
        RelayFrame::Binary(bytes) => WsMessage::Binary(bytes.into()),
        RelayFrame::Ping => WsMessage::Ping(axum::body::Bytes::new()),
        RelayFrame::Pong => WsMessage::Pong(axum::body::Bytes::new()),
        RelayFrame::Close { code, reason } => {
            WsMessage::Close(Some(axum::extract::ws::CloseFrame {
                code,
                reason: reason.into(),
            }))
        }
    }
}

/// 3 missed pongs → disconnect.
///
/// Sends Ping through the control channel so it isn't blocked by a full
/// data buffer. Uses `try_send` to keep the select loop responsive to
/// cancellation — a full control channel means the writer is stalled.
async fn heartbeat_loop(
    ctrl_tx: mpsc::Sender<RelayFrame>,
    missed_pongs: Arc<AtomicU8>,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // fetch_add returns the *previous* value before incrementing:
                //   prev=0 → now 1 (first miss)
                //   prev=1 → now 2 (second miss)
                //   prev=2 → now 3 (third miss → disconnect)
                let missed = missed_pongs.fetch_add(1, Ordering::Relaxed);
                if missed >= 2 {
                    warn!("3 missed pongs — closing connection");
                    cancel.cancel();
                    break;
                }
                if ctrl_tx.try_send(RelayFrame::Ping).is_err() {
                    warn!("control channel full — cannot send Ping, closing");
                    cancel.cancel();
                    break;
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

async fn recv_loop(
    mut ws_recv: futures_util::stream::SplitStream<WebSocket>,
    conn: Arc<ConnectionState>,
    state: Arc<AppState>,
    missed_pongs: Arc<AtomicU8>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            msg = ws_recv.next() => {
                match msg.map(|result| result.map(ws_message_to_relay_frame)) {
                    Some(Ok(frame @ (RelayFrame::Text(_) | RelayFrame::Binary(_)))) => {
                        if !handle_relay_frame(frame, Arc::clone(&conn), Arc::clone(&state), Some(&missed_pongs)).await {
                            break;
                        }
                    }
                    Some(Ok(RelayFrame::Pong)) => {
                        missed_pongs.store(0, Ordering::Relaxed);
                    }
                    Some(Ok(RelayFrame::Ping)) => {
                        // Send Pong through the control channel — priority
                        // delivery even when the data buffer is full (Bug 7 fix).
                        if conn.ctrl_tx.try_send(RelayFrame::Pong).is_err() {
                            // Control channel full means the socket writer is
                            // completely stalled — treat as terminal.
                            warn!(conn_id = %conn.conn_id, "control channel full — cannot send Pong, closing");
                            break;
                        }
                    }
                    Some(Ok(RelayFrame::Close { .. })) | None => {
                        debug!("WebSocket closed by client");
                        break;
                    }
                    Some(Err(e)) => {
                        debug!("WebSocket error: {e}");
                        break;
                    }
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

/// Applies transport-neutral frame validation and NIP-01 dispatch.
///
/// `missed_pongs` is supplied only by transports that emit keepalive frames.
async fn handle_relay_frame(
    frame: RelayFrame,
    conn: Arc<ConnectionState>,
    state: Arc<AppState>,
    missed_pongs: Option<&Arc<AtomicU8>>,
) -> bool {
    match frame {
        RelayFrame::Text(text) => {
            if text.len() > state.config.max_frame_bytes {
                conn.send(format!(
                    r#"["NOTICE","error: frame too large ({} bytes, limit {})"]"#,
                    text.len(),
                    state.config.max_frame_bytes
                ));
                return false;
            }
            trace!(len = text.len(), "frame received");
            handle_text_message(text, conn, state).await;
        }
        RelayFrame::Binary(bytes) => {
            if bytes.len() > state.config.max_frame_bytes {
                conn.send(format!(
                    r#"["NOTICE","error: binary frame too large ({} bytes, limit {})"]"#,
                    bytes.len(),
                    state.config.max_frame_bytes
                ));
                return false;
            }
            if let Ok(text) = String::from_utf8(bytes) {
                handle_text_message(text, conn, state).await;
            }
        }
        RelayFrame::Pong => {
            if let Some(missed_pongs) = missed_pongs {
                missed_pongs.store(0, Ordering::Relaxed);
            }
        }
        RelayFrame::Ping => {
            if conn.ctrl_tx.try_send(RelayFrame::Pong).is_err() {
                return false;
            }
        }
        RelayFrame::Close { .. } => return false,
    }
    true
}

async fn cleanup_connection(conn: &ConnectionState, state: &AppState) {
    for removed in state.sub_registry.remove_connection(conn.conn_id) {
        state
            .pubsub
            .release_topic(&conn.tenant, topic_for_subscription(removed.channel_id))
            .await;
    }
    state.conn_manager.deregister(conn.conn_id);
    if let AuthState::Authenticated(ref auth_ctx) = *conn.auth_state.read().await {
        let remaining = state.conn_manager.connection_ids_for_pubkey_in_community(
            conn.tenant.community(),
            auth_ctx.pubkey.to_bytes().as_slice(),
        );
        if remaining.is_empty() {
            let _ = state
                .pubsub
                .clear_presence(&conn.tenant, &auth_ctx.pubkey)
                .await;
        }
    }
}

async fn handle_text_message(text: String, conn: Arc<ConnectionState>, state: Arc<AppState>) {
    let msg = match ClientMessage::parse(&text) {
        Ok(m) => m,
        Err(e) => {
            conn.send(RelayMessage::notice(&format!("invalid message: {e}")));
            return;
        }
    };

    if !enforce_ws_admission(&msg, &conn, &state).await {
        return;
    }

    match msg {
        ClientMessage::Auth(event) => {
            // Auth is synchronous in the WS loop — no span context is lost.
            let span = tracing::info_span!("ws.auth", conn_id = %conn.conn_id);
            handlers::auth::handle_auth(event, Arc::clone(&conn), Arc::clone(&state))
                .instrument(span)
                .await;
        }
        ClientMessage::Event(event) => {
            let conn = Arc::clone(&conn);
            let state = Arc::clone(&state);
            let permit = match state.handler_semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    conn.send(RelayMessage::notice(
                        "rate-limited: too many concurrent requests",
                    ));
                    return;
                }
            };
            // Capture the parent span BEFORE the spawn so it is propagated into
            // the spawned future.  A bare `tokio::spawn` drops tracing context.
            let span = tracing::info_span!(
                "ws.event",
                conn_id = %conn.conn_id,
                event_id = tracing::field::Empty,
                kind = tracing::field::Empty,
            );
            tokio::spawn(
                async move {
                    handlers::event::handle_event(event, conn, state).await;
                    drop(permit);
                }
                .instrument(span),
            );
        }
        ClientMessage::Req { sub_id, filters } => {
            let conn = Arc::clone(&conn);
            let state = Arc::clone(&state);
            let permit = match state.handler_semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    conn.send(request_rejection_message(
                        Some(&sub_id),
                        "rate-limited: too many concurrent requests",
                    ));
                    return;
                }
            };
            let span = tracing::info_span!("ws.req", conn_id = %conn.conn_id, sub_id = %sub_id);
            tokio::spawn(
                async move {
                    handlers::req::handle_req(sub_id, filters, conn, state).await;
                    drop(permit);
                }
                .instrument(span),
            );
        }
        ClientMessage::Count { sub_id, filters } => {
            let conn = Arc::clone(&conn);
            let state = Arc::clone(&state);
            let permit = match state.handler_semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    conn.send(RelayMessage::notice(
                        "rate-limited: too many concurrent requests",
                    ));
                    return;
                }
            };
            let span = tracing::info_span!("ws.count", conn_id = %conn.conn_id, sub_id = %sub_id);
            tokio::spawn(
                async move {
                    handlers::count::handle_count(sub_id, filters, conn, state).await;
                    drop(permit);
                }
                .instrument(span),
            );
        }
        ClientMessage::Close(sub_id) => {
            handlers::close::handle_close(sub_id, Arc::clone(&conn), Arc::clone(&state)).await;
        }
    }
}

fn request_rejection_message(sub_id: Option<&str>, reason: &str) -> String {
    match sub_id {
        Some(sub_id) => RelayMessage::closed(sub_id, reason),
        None => RelayMessage::notice(reason),
    }
}

async fn enforce_ws_admission(
    msg: &ClientMessage,
    conn: &ConnectionState,
    state: &AppState,
) -> bool {
    let is_event = matches!(msg, ClientMessage::Event(_));
    if !is_event && !matches!(msg, ClientMessage::Req { .. } | ClientMessage::Count { .. }) {
        return true;
    }

    let (pubkey, is_agent) = {
        let auth = conn.auth_state.read().await;
        match &*auth {
            AuthState::Authenticated(ctx) => (ctx.pubkey, ctx.agent_owner_pubkey.is_some()),
            _ => return true,
        }
    };

    let limits = &state.auth.config().rate_limits;
    let (ws_window_secs, ws_limit) =
        crate::admission::ws_admission_budget(limits.human_ws_events_per_sec);
    let ws_result = crate::admission::check_principal(
        state.admission_rate_limiter.as_ref(),
        &conn.tenant,
        &pubkey,
        LimitType::WsEvents,
        ws_window_secs,
        ws_limit,
    )
    .await;
    let sub_id = match msg {
        ClientMessage::Req { sub_id, .. } => Some(sub_id.as_str()),
        _ => None,
    };
    if !send_admission_result(conn, ws_result, sub_id) {
        return false;
    }

    if is_event {
        let message_limit = if is_agent {
            limits.agent_standard_messages_per_min
        } else {
            limits.human_messages_per_min
        };
        let message_result = crate::admission::check_principal(
            state.admission_rate_limiter.as_ref(),
            &conn.tenant,
            &pubkey,
            LimitType::Messages,
            60,
            message_limit,
        )
        .await;
        if !send_admission_result(conn, message_result, None) {
            return false;
        }
    }

    true
}

fn send_admission_result(
    conn: &ConnectionState,
    result: Result<(), crate::admission::AdmissionError>,
    sub_id: Option<&str>,
) -> bool {
    match result {
        Ok(()) => true,
        Err(crate::admission::AdmissionError::Exceeded { reset_in_secs }) => {
            metrics::counter!("buzz_admission_rejections_total", "transport" => "websocket", "reason" => "quota").increment(1);
            conn.send(request_rejection_message(
                sub_id,
                &format!("rate-limited: quota exceeded; retry in {reset_in_secs}s"),
            ));
            false
        }
        Err(crate::admission::AdmissionError::Unavailable) => {
            metrics::counter!("buzz_admission_rejections_total", "transport" => "websocket", "reason" => "unavailable").increment(1);
            conn.send(request_rejection_message(
                sub_id,
                "rate-limited: shared admission unavailable",
            ));
            false
        }
    }
}

fn topic_for_subscription(channel_id: Option<Uuid>) -> EventTopic {
    match channel_id {
        Some(channel_id) => EventTopic::Channel(channel_id),
        None => EventTopic::Global,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default)]
    struct MockSinkState {
        messages: Vec<WsMessage>,
        flush_count: usize,
        fail_after_flushes: Option<usize>,
    }

    #[derive(Debug, Clone)]
    struct MockSink {
        state: Arc<Mutex<MockSinkState>>,
    }

    impl MockSink {
        fn new(fail_after_flushes: Option<usize>) -> (Self, Arc<Mutex<MockSinkState>>) {
            let state = Arc::new(Mutex::new(MockSinkState {
                fail_after_flushes,
                ..MockSinkState::default()
            }));
            (
                Self {
                    state: Arc::clone(&state),
                },
                state,
            )
        }
    }

    impl Sink<WsMessage> for MockSink {
        type Error = std::io::Error;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn start_send(self: std::pin::Pin<&mut Self>, item: WsMessage) -> Result<(), Self::Error> {
            self.state
                .lock()
                .expect("mock sink poisoned")
                .messages
                .push(item);
            Ok(())
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            let mut state = self.state.lock().expect("mock sink poisoned");
            state.flush_count += 1;
            if state
                .fail_after_flushes
                .is_some_and(|limit| state.flush_count >= limit)
            {
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "mock flush failure",
                )));
            }
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            self.poll_flush(cx)
        }
    }

    fn text_payloads(messages: &[WsMessage]) -> Vec<String> {
        messages
            .iter()
            .map(|msg| match msg {
                WsMessage::Text(text) => text.to_string(),
                other => panic!("unexpected websocket message in test: {other:?}"),
            })
            .collect()
    }

    fn test_connection(
        send_tx: mpsc::Sender<RelayFrame>,
        ctrl_tx: mpsc::Sender<RelayFrame>,
    ) -> Arc<ConnectionState> {
        Arc::new(ConnectionState {
            conn_id: Uuid::new_v4(),
            tenant: TenantContext::resolved(
                buzz_core::CommunityId::from_uuid(Uuid::nil()),
                "test.local",
            ),
            remote_addr: "127.0.0.1:1234".parse().expect("socket address"),
            auth_state: RwLock::new(AuthState::Pending {
                challenge: "test-challenge".to_string(),
            }),
            subscriptions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            send_tx,
            ctrl_tx,
            cancel: CancellationToken::new(),
            backpressure_count: Arc::new(AtomicU8::new(0)),
            grace_limit: 3,
        })
    }

    #[tokio::test]
    async fn transport_neutral_dispatch_returns_the_normal_parse_notice() {
        let state = crate::state::tests::test_state().await;
        let (send_tx, mut send_rx) = mpsc::channel(1);
        let (ctrl_tx, _ctrl_rx) = mpsc::channel(1);
        let conn = test_connection(send_tx, ctrl_tx);

        assert!(
            handle_relay_frame(
                RelayFrame::Text("not valid relay json".to_string()),
                conn,
                state,
                None,
            )
            .await
        );

        let RelayFrame::Text(response) = send_rx.recv().await.expect("parse notice") else {
            panic!("expected text protocol response");
        };
        assert!(response.starts_with("[\"NOTICE\",\"invalid message:"));
    }

    #[test]
    fn req_rejections_are_subscription_scoped() {
        let reason = "rate-limited: too many concurrent requests";
        let closed: serde_json::Value =
            serde_json::from_str(&request_rejection_message(Some("history-123"), reason))
                .expect("parse CLOSED");
        assert_eq!(closed, serde_json::json!(["CLOSED", "history-123", reason]));

        let notice: serde_json::Value =
            serde_json::from_str(&request_rejection_message(None, reason)).expect("parse NOTICE");
        assert_eq!(notice, serde_json::json!(["NOTICE", reason]));
    }

    #[test]
    fn restart_close_frame_keeps_its_code_and_reason_at_the_axum_boundary() {
        let message = relay_frame_to_ws_message(RelayFrame::Close {
            code: 1012,
            reason: "relay restarting".to_string(),
        });

        let WsMessage::Close(Some(close)) = message else {
            panic!("expected an Axum close message");
        };
        assert_eq!(close.code, 1012);
        assert_eq!(close.reason.as_str(), "relay restarting");
    }

    #[tokio::test]
    async fn send_loop_batches_queued_data_frames_into_one_flush() {
        let (data_tx, data_rx) = mpsc::channel(MAX_WS_SEND_BATCH);
        let (_ctrl_tx, ctrl_rx) = mpsc::channel(1);
        for i in 0..5 {
            data_tx
                .send(RelayFrame::Text(format!("data-{i}")))
                .await
                .expect("queue data frame");
        }

        let (sink, state) = MockSink::new(Some(1));
        send_loop_inner(sink, data_rx, ctrl_rx, CancellationToken::new()).await;

        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(state.flush_count, 1);
        assert_eq!(
            text_payloads(&state.messages),
            vec!["data-0", "data-1", "data-2", "data-3", "data-4"]
        );
    }

    #[tokio::test]
    async fn send_loop_batch_one_preserves_single_frame_flush_behavior() {
        let (data_tx, data_rx) = mpsc::channel(1);
        let (_ctrl_tx, ctrl_rx) = mpsc::channel(1);
        data_tx
            .send(RelayFrame::Text("single".into()))
            .await
            .expect("queue data frame");

        let (sink, state) = MockSink::new(Some(1));
        send_loop_inner(sink, data_rx, ctrl_rx, CancellationToken::new()).await;

        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(state.flush_count, 1);
        assert_eq!(text_payloads(&state.messages), vec!["single"]);
    }

    #[tokio::test]
    async fn send_loop_drains_control_before_batched_data_without_reordering() {
        let (data_tx, data_rx) = mpsc::channel(MAX_WS_SEND_BATCH);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
        data_tx
            .send(RelayFrame::Text("data-0".into()))
            .await
            .expect("queue data frame");
        data_tx
            .send(RelayFrame::Text("data-1".into()))
            .await
            .expect("queue data frame");
        ctrl_tx
            .send(RelayFrame::Text("control".into()))
            .await
            .expect("queue control frame");

        let (sink, state) = MockSink::new(Some(2));
        send_loop_inner(sink, data_rx, ctrl_rx, CancellationToken::new()).await;

        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(state.flush_count, 2);
        assert_eq!(
            text_payloads(&state.messages),
            vec!["control", "data-0", "data-1"]
        );
    }

    #[tokio::test]
    async fn send_loop_flushes_queued_control_before_close_on_cancel() {
        // A ban disconnect queues its `OK false "blocked: …"` reason frame on
        // the control channel and then cancels the token (B3). The biased
        // select polls the cancel branch first, so the reason frame would be
        // stranded unless the cancel branch drains ctrl before emitting Close.
        // This test exercises `send_loop_inner` end-to-end to prove the reason
        // frame reaches the client, in order, ahead of the Close.
        let (_data_tx, data_rx) = mpsc::channel(1);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(1);
        ctrl_tx
            .send(RelayFrame::Text("blocked: you are banned".into()))
            .await
            .expect("queue ban reason frame");

        let cancel = CancellationToken::new();
        cancel.cancel();

        let (sink, state) = MockSink::new(None);
        send_loop_inner(sink, data_rx, ctrl_rx, cancel).await;

        let state = state.lock().expect("mock sink poisoned");
        assert_eq!(
            state.messages.len(),
            2,
            "reason frame then Close, nothing else"
        );
        match &state.messages[0] {
            WsMessage::Text(text) => {
                assert_eq!(text.as_str(), "blocked: you are banned")
            }
            other => panic!("expected the ban reason frame first, got {other:?}"),
        }
        assert!(
            matches!(state.messages[1], WsMessage::Close(_)),
            "Close is sent only after the reason frame is flushed"
        );
    }
}
