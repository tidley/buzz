//! Android Flutter FFI boundary for FIPS.
//!
//! This crate owns one app-local `FipsMobileQuicSession`. Android invokes the
//! small C ABI to connect and exchange frames.

use std::sync::{LazyLock, Mutex};

use fips_mobile::{
    FipsMobileQuicSession, FipsMobileQuicSessionConfig, FipsMobileQuicSessionError,
    FipsMobileQuicSessionStatus, Identity,
};

/// The result of a bridge operation visible to Flutter.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BridgeStatus {
    /// The bridge has not been started or has been stopped.
    Stopped = 0,
    /// A FIPS session has started and is ready to connect.
    Running = 1,
    /// A persistent QUIC stream is ready for frame I/O.
    Connected = 2,
    /// A caller supplied a null, malformed, or invalid UTF-8 buffer.
    InvalidInput = 3,
    /// Frame I/O was requested before a connection was established.
    NotConnected = 4,
    /// The receive buffer cannot hold the pending frame.
    BufferTooSmall = 5,
    /// FIPS could not complete the requested operation.
    Failed = 6,
}

impl BridgeStatus {
    const fn code(self) -> u32 {
        self as u32
    }
}

struct Bridge {
    runtime: tokio::runtime::Runtime,
    session: FipsMobileQuicSession,
    pending_frame: Option<Vec<u8>>,
}

static BRIDGE: LazyLock<Mutex<Option<Bridge>>> = LazyLock::new(|| Mutex::new(None));

fn bridge_status(bridge: Option<&Bridge>) -> BridgeStatus {
    match bridge.map(|bridge| bridge.session.status()) {
        Some(FipsMobileQuicSessionStatus::Connected) => BridgeStatus::Connected,
        Some(FipsMobileQuicSessionStatus::Idle) => BridgeStatus::Running,
        Some(FipsMobileQuicSessionStatus::NotConnected | FipsMobileQuicSessionStatus::Stopped)
        | None => BridgeStatus::Stopped,
    }
}

fn session_error_status(error: FipsMobileQuicSessionError) -> BridgeStatus {
    match error {
        FipsMobileQuicSessionError::Status(FipsMobileQuicSessionStatus::NotConnected) => {
            BridgeStatus::NotConnected
        }
        FipsMobileQuicSessionError::Status(FipsMobileQuicSessionStatus::Connected) => {
            BridgeStatus::Connected
        }
        FipsMobileQuicSessionError::Status(FipsMobileQuicSessionStatus::Stopped) => {
            BridgeStatus::Stopped
        }
        _ => BridgeStatus::Failed,
    }
}

/// Starts an idle FIPS QUIC session. This operation is idempotent.
#[no_mangle]
pub extern "C" fn buzz_fips_mobile_start() -> u32 {
    let Ok(mut bridge) = BRIDGE.lock() else {
        return BridgeStatus::Failed.code();
    };
    if bridge.is_none() {
        let Ok(runtime) = tokio::runtime::Runtime::new() else {
            return BridgeStatus::Failed.code();
        };
        *bridge = Some(Bridge {
            runtime,
            session: FipsMobileQuicSession::new(
                Identity::generate(),
                FipsMobileQuicSessionConfig::default(),
            ),
            pending_frame: None,
        });
    }
    bridge_status(bridge.as_ref()).code()
}

/// Stops the FIPS session and releases its network resources. This operation is idempotent.
#[no_mangle]
pub extern "C" fn buzz_fips_mobile_stop() -> u32 {
    let Ok(mut bridge) = BRIDGE.lock() else {
        return BridgeStatus::Failed.code();
    };
    if let Some(mut bridge) = bridge.take() {
        if bridge.runtime.block_on(bridge.session.stop()).is_err() {
            return BridgeStatus::Failed.code();
        }
    }
    BridgeStatus::Stopped.code()
}

/// Gets the session lifecycle state without starting it.
#[no_mangle]
pub extern "C" fn buzz_fips_mobile_status() -> u32 {
    let Ok(bridge) = BRIDGE.lock() else {
        return BridgeStatus::Failed.code();
    };
    bridge_status(bridge.as_ref()).code()
}

/// Connects the started session to a peer identified by a UTF-8 Nostr `npub`.
///
/// # Safety
/// `peer` must point to `len` initialized bytes for the duration of this call,
/// unless `len` is zero.
#[no_mangle]
pub unsafe extern "C" fn buzz_fips_mobile_connect(peer: *const u8, len: usize) -> u32 {
    let peer = match input(peer, len).and_then(|bytes| std::str::from_utf8(bytes).ok()) {
        Some(peer) if !peer.is_empty() => peer.to_owned(),
        _ => return BridgeStatus::InvalidInput.code(),
    };
    let Ok(mut bridge) = BRIDGE.lock() else {
        return BridgeStatus::Failed.code();
    };
    let Some(bridge) = bridge.as_mut() else {
        return BridgeStatus::Stopped.code();
    };
    match bridge.runtime.block_on(bridge.session.connect(peer)) {
        Ok(()) => BridgeStatus::Connected.code(),
        Err(error) => session_error_status(error).code(),
    }
}

/// Sends one raw application frame through the connected FIPS QUIC session.
///
/// # Safety
/// `frame` must point to `len` initialized bytes for the duration of this call,
/// unless `len` is zero.
#[no_mangle]
pub unsafe extern "C" fn buzz_fips_mobile_send(frame: *const u8, len: usize) -> u32 {
    let Some(frame) = input(frame, len) else {
        return BridgeStatus::InvalidInput.code();
    };
    let Ok(mut bridge) = BRIDGE.lock() else {
        return BridgeStatus::Failed.code();
    };
    let Some(bridge) = bridge.as_mut() else {
        return BridgeStatus::Stopped.code();
    };
    match bridge.runtime.block_on(bridge.session.send(frame)) {
        Ok(()) => BridgeStatus::Connected.code(),
        Err(error) => session_error_status(error).code(),
    }
}

/// Receives one raw application frame into `frame` and writes its length to `out_len`.
/// A too-small buffer leaves the frame pending so the caller can retry.
///
/// # Safety
/// `out_len` must be a valid writable pointer. If `capacity` is nonzero,
/// `frame` must point to at least `capacity` writable bytes.
#[no_mangle]
pub unsafe extern "C" fn buzz_fips_mobile_receive(
    frame: *mut u8,
    capacity: usize,
    out_len: *mut usize,
) -> u32 {
    if out_len.is_null() || (capacity != 0 && frame.is_null()) {
        return BridgeStatus::InvalidInput.code();
    }
    let Ok(mut bridge) = BRIDGE.lock() else {
        return BridgeStatus::Failed.code();
    };
    let Some(bridge) = bridge.as_mut() else {
        return BridgeStatus::Stopped.code();
    };
    if bridge.pending_frame.is_none() {
        match bridge.runtime.block_on(bridge.session.receive()) {
            Ok(received) => bridge.pending_frame = Some(received),
            Err(error) => return session_error_status(error).code(),
        }
    }
    let Some(pending) = bridge.pending_frame.as_ref() else {
        return BridgeStatus::Failed.code();
    };
    // SAFETY: the caller guarantees `out_len` points to writable memory.
    unsafe { out_len.write(pending.len()) };
    if pending.len() > capacity {
        return BridgeStatus::BufferTooSmall.code();
    }
    if !pending.is_empty() {
        // SAFETY: the caller guarantees `frame` has `capacity` writable bytes.
        unsafe { std::ptr::copy_nonoverlapping(pending.as_ptr(), frame, pending.len()) };
    }
    bridge.pending_frame = None;
    BridgeStatus::Connected.code()
}

/// Backward-compatible alias for [`buzz_fips_mobile_send`].
///
/// # Safety
/// Same requirements as [`buzz_fips_mobile_send`].
#[no_mangle]
pub unsafe extern "C" fn buzz_fips_mobile_submit_frame(frame: *const u8, len: usize) -> u32 {
    // SAFETY: this ABI alias forwards the caller's documented pointer contract.
    unsafe { buzz_fips_mobile_send(frame, len) }
}

fn input<'a>(pointer: *const u8, len: usize) -> Option<&'a [u8]> {
    if pointer.is_null() && len != 0 {
        return None;
    }
    let pointer = if len == 0 {
        std::ptr::NonNull::dangling().as_ptr()
    } else {
        pointer
    };
    // SAFETY: a null pointer is only accepted for an empty slice; otherwise the
    // C caller contract requires `pointer` to reference `len` initialized bytes.
    Some(unsafe { std::slice::from_raw_parts(pointer, len) })
}

#[cfg(test)]
mod tests {
    use super::{
        buzz_fips_mobile_connect, buzz_fips_mobile_receive, buzz_fips_mobile_send,
        buzz_fips_mobile_start, buzz_fips_mobile_status, buzz_fips_mobile_stop,
        buzz_fips_mobile_submit_frame, BridgeStatus,
    };

    #[test]
    fn start_creates_an_idle_session() {
        assert_eq!(buzz_fips_mobile_stop(), BridgeStatus::Stopped as u32);
        assert_eq!(buzz_fips_mobile_start(), BridgeStatus::Running as u32);
        assert_eq!(buzz_fips_mobile_status(), BridgeStatus::Running as u32);
    }

    #[test]
    fn stop_is_idempotent() {
        assert_eq!(buzz_fips_mobile_stop(), BridgeStatus::Stopped as u32);
        assert_eq!(buzz_fips_mobile_stop(), BridgeStatus::Stopped as u32);
        assert_eq!(buzz_fips_mobile_status(), BridgeStatus::Stopped as u32);
    }

    #[test]
    fn frame_io_requires_a_connected_session() {
        assert_eq!(buzz_fips_mobile_start(), BridgeStatus::Running as u32);
        assert_eq!(
            unsafe { buzz_fips_mobile_send(std::ptr::null(), 0) },
            BridgeStatus::NotConnected as u32
        );
        let mut frame = [0_u8; 8];
        let mut length = 0;
        assert_eq!(
            unsafe { buzz_fips_mobile_receive(frame.as_mut_ptr(), frame.len(), &mut length) },
            BridgeStatus::NotConnected as u32
        );
        assert_eq!(
            unsafe { buzz_fips_mobile_submit_frame(std::ptr::null(), 1) },
            BridgeStatus::InvalidInput as u32
        );
        assert_eq!(buzz_fips_mobile_stop(), BridgeStatus::Stopped as u32);
    }

    #[test]
    fn connect_validates_the_peer_before_network_io() {
        assert_eq!(buzz_fips_mobile_stop(), BridgeStatus::Stopped as u32);
        assert_eq!(
            unsafe { buzz_fips_mobile_connect(std::ptr::null(), 1) },
            BridgeStatus::InvalidInput as u32
        );
    }
}
