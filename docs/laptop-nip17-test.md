# Laptop NIP-17 Test

Use this procedure to run Buzz on this machine and connect Buzz Desktop from a
laptop without exposing the Buzz WebSocket port. The laptop connects only to
the configured public Nostr relays.

The FIPS responder is started too, but Buzz Desktop currently tests the
NIP-17 gateway path. A FIPS client session needs its own client integration.

## Server

1. Start the local dependencies and seed the development community:

```sh
. ./bin/activate-hermit
just setup
```

2. Generate two stable keys. Run this once for each identity and keep the
secret values private:

```sh
cargo run -p buzz-admin -- generate-key
```

Use the first key for `BUZZ_NIP17_GATEWAY_PRIVATE_KEY`. The public key printed
with it is the gateway public key used on the laptop. Use the second key for
`BUZZ_FIPS_PRIVATE_KEY`.

3. Add these values to `.env`. Keep the existing `RELAY_URL=ws://localhost:3000`.
It identifies the already-seeded local tenant; it does not need to be reachable
from the laptop when NIP-17 is enabled.

```sh
BUZZ_NIP17_GATEWAY_ENABLED=true
BUZZ_NIP17_GATEWAY_PRIVATE_KEY=<first generated secret key>
BUZZ_NIP17_GATEWAY_RELAYS=wss://relay.damus.io,wss://nos.lol,wss://nostr.mom

BUZZ_FIPS_ENABLED=true
BUZZ_FIPS_PRIVATE_KEY=<second generated secret key>
BUZZ_FIPS_NOSTR_RELAYS=wss://relay.damus.io,wss://nos.lol,wss://nostr.mom
BUZZ_FIPS_STUN_SERVERS=stun:stun.l.google.com:19302
```

4. Build and run the feature-enabled server:

```sh
just relay-fips-release
```

Verify its local health endpoint in another terminal:

```sh
curl -fsS http://127.0.0.1:3000/health
curl -fsS http://127.0.0.1:8080/_readiness
```

The startup log must include `FIPS responder started`. The NIP-17 runtime also
logs its public-relay connections. Outbound WebSocket access to the listed
public Nostr relays and outbound UDP access to the STUN server are required.

## Laptop Client

1. Build Buzz Desktop from the same commit:

```sh
. ./bin/activate-hermit
pnpm install
pnpm -C desktop tauri build
```

For development instead of a packaged app, use `pnpm -C desktop tauri dev`.

2. In the app, create or edit a community. Set its Relay URL to
`ws://localhost:3000`; this is an identity value for the local test tenant.

3. Enable **Private relay transport**, then enter:

```text
Gateway pubkey: <public key from the first generated key>
Public relays:
wss://relay.damus.io
wss://nos.lol
wss://nostr.mom
```

4. Save, join or create a channel, and send a message. Confirm the server log
receives NIP-17 gateway traffic and the message appears in the client.

The laptop does not need a route to this machine's `:3000` port for this test.
If the public relays block WebSockets on the laptop network, use reachable
public relays in both server and client configuration.

## Direct Fallback

For direct-WebSocket troubleshooting on the same LAN, use the server's LAN IP
from `hostname -I` as the community Relay URL, for example
`ws://<server-lan-ip>:3000`, and turn off Private relay transport. This is
separate from the NIP-17 test; restore the NIP-17 community settings before
resuming it.
