//! Opt-in FIPS Nostr/STUN rendezvous responder for persistent NIP-01 streams.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use buzz_core::tenant::TenantContext;
use fips::{
    config::NostrDiscoveryConfig,
    discovery::nostr::{
        BootstrapEvent, NostrDiscovery, OverlayAdvert, OverlayEndpointAdvert, OverlayTransportKind,
        ADVERT_IDENTIFIER, ADVERT_VERSION,
    },
    quic::{accept_persistent, FipsQuicConnection, FipsQuicOptions, FipsQuicStream},
    Identity,
};
use tracing::{debug, info, warn};

use crate::{
    config::FipsConfig, connection::VirtualConnection, state::AppState, transport::RelayFrame,
};

/// Starts the FIPS advertiser and accepts every discovered peer's persistent QUIC stream.
pub fn spawn(
    state: Arc<AppState>,
    tenant: TenantContext,
    config: FipsConfig,
) -> anyhow::Result<()> {
    let identity = Identity::from_secret_str(&config.private_key)
        .map_err(|error| anyhow::anyhow!("invalid FIPS key after config validation: {error}"))?;
    tokio::spawn(run(state, tenant, identity, config));
    Ok(())
}

async fn run(state: Arc<AppState>, tenant: TenantContext, identity: Identity, config: FipsConfig) {
    let discovery_config = discovery_config(&config);
    let advert = OverlayAdvert {
        identifier: ADVERT_IDENTIFIER.to_string(),
        version: ADVERT_VERSION,
        endpoints: vec![OverlayEndpointAdvert {
            transport: OverlayTransportKind::Udp,
            addr: "nat".to_string(),
        }],
        signal_relays: Some(discovery_config.dm_relays.clone()),
        stun_servers: Some(discovery_config.stun_servers.clone()),
        stun_services: None,
    };
    let discovery = match NostrDiscovery::start(&identity, discovery_config).await {
        Ok(discovery) => discovery,
        Err(error) => {
            warn!(%error, "FIPS Nostr/STUN discovery failed to start");
            return;
        }
    };
    if let Err(error) = discovery.update_local_advert(Some(advert)).await {
        warn!(%error, "FIPS responder could not publish its NAT rendezvous advert");
        let _ = discovery.shutdown().await;
        return;
    }
    info!(npub = %identity.npub(), "FIPS responder started");
    loop {
        for event in discovery.drain_events().await {
            match event {
                BootstrapEvent::Established { traversal } => {
                    let state = Arc::clone(&state);
                    let tenant = tenant.clone();
                    let identity = match Identity::from_secret_str(&config.private_key) {
                        Ok(identity) => identity,
                        Err(error) => {
                            warn!(%error, "FIPS identity became invalid after startup validation");
                            return;
                        }
                    };
                    tokio::spawn(async move {
                        let options = FipsQuicOptions {
                            max_stream_bytes: state.config.max_frame_bytes,
                            ..FipsQuicOptions::default()
                        };
                        match accept_persistent(&identity, traversal, options).await {
                            Ok(connection) => serve_connection(state, tenant, connection).await,
                            Err(error) => {
                                debug!(%error, "FIPS QUIC connection was not established")
                            }
                        }
                    });
                }
                BootstrapEvent::Failed {
                    peer_config,
                    reason,
                } => {
                    debug!(peer = %peer_config.npub, %reason, "FIPS traversal failed");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn discovery_config(config: &FipsConfig) -> NostrDiscoveryConfig {
    NostrDiscoveryConfig {
        enabled: true,
        advertise: true,
        advert_relays: config.nostr_relays.clone(),
        dm_relays: config.nostr_relays.clone(),
        stun_servers: config.stun_servers.clone(),
        app: "buzz-relay".to_string(),
        ..NostrDiscoveryConfig::default()
    }
}

async fn serve_connection(
    state: Arc<AppState>,
    tenant: TenantContext,
    connection: FipsQuicConnection,
) {
    let addr = connection.remote_addr();
    loop {
        match connection.accept_bi().await {
            Ok(stream) => serve_stream(Arc::clone(&state), tenant.clone(), addr, stream).await,
            Err(error) => {
                debug!(peer = %connection.peer_npub(), %error, "FIPS peer closed its QUIC connection");
                return;
            }
        }
    }
}

async fn serve_stream(
    state: Arc<AppState>,
    tenant: TenantContext,
    addr: SocketAddr,
    mut stream: FipsQuicStream,
) {
    let mut connection = match VirtualConnection::open(state, addr, tenant).await {
        Ok(connection) => connection,
        Err(error) => {
            warn!(?error, "FIPS stream rejected before relay session startup");
            return;
        }
    };
    loop {
        tokio::select! {
            inbound = stream.recv_frame() => match inbound {
                Ok(frame) => match String::from_utf8(frame) {
                    Ok(frame) => connection.receive_frame(RelayFrame::Text(frame)).await,
                    Err(_) => break,
                },
                Err(_) => break,
            },
            outbound = connection.next_frame() => match outbound {
                Some(RelayFrame::Text(frame)) => if stream.send_frame(frame.as_bytes()).await.is_err() { break; },
                Some(RelayFrame::Binary(frame)) => if stream.send_frame(&frame).await.is_err() { break; },
                Some(RelayFrame::Ping | RelayFrame::Pong) => {},
                Some(RelayFrame::Close { .. }) | None => break,
            },
        }
    }
    connection.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_config_advertises_on_the_configured_public_relays() {
        let config = FipsConfig {
            private_key: "key".to_string(),
            nostr_relays: vec!["wss://relay.example/".to_string()],
            stun_servers: vec!["stun:stun.example:3478".to_string()],
        };
        let discovery = discovery_config(&config);

        assert!(discovery.enabled && discovery.advertise);
        assert_eq!(discovery.advert_relays, discovery.dm_relays);
        assert_eq!(discovery.stun_servers, ["stun:stun.example:3478"]);
    }
}
