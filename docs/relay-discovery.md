# Relay Discovery

`buzz-relay` can publish a signed NIP-66 kind `30166` relay discovery event at
startup. This is disabled by default.

Set these variables to enable it:

```sh
BUZZ_DISCOVERY_ENABLED=true
BUZZ_DISCOVERY_IDENTITY_PATH=/var/lib/buzz/discovery.key
```

`BUZZ_DISCOVERY_IDENTITY_PATH` is required. The relay creates a 0600 private
key file there on first startup and reuses it on later starts. Back up this
file. It is separate from `BUZZ_RELAY_PRIVATE_KEY`, so discovery has an
independent public Nostr identity.

On first startup, the relay logs this identity as an `npub`. Clients use that
identity to find its signed discovery event on the configured public relays.

`BUZZ_DISCOVERY_RELAYS` is an optional comma-separated list of `ws://` or
`wss://` relay URLs. Its default is:

```text
wss://relay.damus.io,wss://nos.lol,wss://nostr.mom
```

The event has the normalized `RELAY_URL` as its NIP-66 `d` tag, identifies the
relay as clearnet, and lists Buzz's supported NIPs. Each configured publisher
relay is attempted independently. Connection, timeout, or rejection failures
are logged and do not block relay startup.

This feature publishes discovery only. It does not proxy Buzz WebSocket traffic
through Nostr. A relay whose `RELAY_URL` is not publicly reachable still needs
the NIP-17 gateway transport before a mobile client can use it through public
relays.
