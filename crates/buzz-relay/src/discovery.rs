//! Opt-in NIP-66 relay discovery publication.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use nostr::{EventBuilder, Keys, Kind, Tag, ToBech32};
use tracing::{info, warn};

use crate::config::DiscoveryConfig;
use crate::nip11::SUPPORTED_NIPS;

const NIP66_RELAY_DISCOVERY_KIND: u16 = 30_166;

/// Loads the persistent discovery identity, creating it at `path` if absent.
///
/// The key is deliberately separate from the relay signing key. Discovery is
/// public-facing and operators may rotate or revoke it independently.
pub fn load_or_create_identity(path: &Path) -> anyhow::Result<Keys> {
    match fs::read_to_string(path) {
        Ok(encoded) => {
            ensure_private_file(path)?;
            Keys::parse(encoded.trim()).map_err(|error| {
                anyhow::anyhow!("invalid discovery identity {}: {error}", path.display())
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let keys = Keys::generate();
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|error| {
                    anyhow::anyhow!(
                        "could not create discovery identity {}: {error}",
                        path.display()
                    )
                })?;
            set_private_permissions(&file)?;
            file.write_all(keys.secret_key().display_secret().to_string().as_bytes())?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            info!(path = %path.display(), npub = %keys.public_key().to_bech32()?, "Created discovery identity");
            Ok(keys)
        }
        Err(error) => Err(anyhow::anyhow!(
            "could not read discovery identity {}: {error}",
            path.display()
        )),
    }
}

/// Builds the signed NIP-66 parameterized relay discovery event.
pub fn announcement(keys: &Keys, relay_url: &str) -> anyhow::Result<nostr::Event> {
    let relay_url = normalized_relay_url(relay_url)?;
    let mut tags = vec![
        Tag::parse(["d", relay_url.as_str()])?,
        Tag::parse(["n", "clearnet"])?,
    ];
    for nip in SUPPORTED_NIPS {
        tags.push(Tag::parse(["N", &nip.to_string()])?);
    }
    EventBuilder::new(Kind::Custom(NIP66_RELAY_DISCOVERY_KIND), "")
        .tags(tags)
        .sign_with_keys(keys)
        .map_err(Into::into)
}

fn normalized_relay_url(raw: &str) -> anyhow::Result<String> {
    let url = url::Url::parse(raw.trim())?;
    if !matches!(url.scheme(), "ws" | "wss")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(anyhow::anyhow!(
            "RELAY_URL must be a ws:// or wss:// URL without credentials, query, or fragment"
        ));
    }
    Ok(url.to_string())
}

/// Publishes one signed discovery event to each configured public relay.
///
/// Publication is best-effort: discovery must not delay or prevent a relay
/// from serving its own clients.
pub async fn publish(config: DiscoveryConfig, keys: Keys, relay_url: String) {
    let event = match announcement(&keys, &relay_url) {
        Ok(event) => event,
        Err(error) => {
            warn!(%error, "Could not build discovery announcement");
            return;
        }
    };

    for relay in config.relays {
        match buzz_ws_client::NostrWsConnection::connect(&relay).await {
            Ok(mut connection) => match connection.send_event(event.clone()).await {
                Ok(response) if response.accepted => {
                    info!(relay = %relay, event_id = %event.id, "Published discovery announcement");
                }
                Ok(response) => {
                    warn!(relay = %relay, message = %response.message, "Discovery announcement rejected");
                }
                Err(error) => {
                    warn!(relay = %relay, %error, "Could not publish discovery announcement")
                }
            },
            Err(error) => warn!(relay = %relay, %error, "Could not connect to discovery relay"),
        }
    }
}

#[cfg(unix)]
fn ensure_private_file(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if fs::metadata(path)?.permissions().mode() & 0o077 != 0 {
        return Err(anyhow::anyhow!(
            "discovery identity {} must not be readable or writable by group or others",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_private_file(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_permissions(file: &std::fs::File) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_permissions(_file: &std::fs::File) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_persisted_and_reused() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("discovery.key");

        let created = load_or_create_identity(&path).expect("create identity");
        let loaded = load_or_create_identity(&path).expect("load identity");

        assert_eq!(created.public_key(), loaded.public_key());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn announcement_is_signed_nip66_event() {
        let keys = Keys::generate();
        let event = announcement(&keys, "wss://buzz.example/").expect("announcement");

        assert_eq!(event.kind, Kind::Custom(NIP66_RELAY_DISCOVERY_KIND));
        assert_eq!(event.pubkey, keys.public_key());
        assert!(event.verify().is_ok());
        assert!(event
            .tags
            .iter()
            .any(|tag| tag.as_slice() == ["d", "wss://buzz.example/"]));
    }

    #[test]
    fn announcement_normalizes_the_relay_url() {
        let event = announcement(&Keys::generate(), "wss://BUZZ.example").expect("announcement");

        assert!(event
            .tags
            .iter()
            .any(|tag| tag.as_slice() == ["d", "wss://buzz.example/"]));
    }
}
