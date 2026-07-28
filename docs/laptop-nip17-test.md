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

2. On the first screen, select **Connect privately**. This does not need a
direct relay URL. Enter a community name, then enter:

```text
Gateway pubkey: <public key from the first generated key>
Public relays:
wss://relay.damus.io
wss://nos.lol
wss://nostr.mom
```

3. Select **Connect privately**, then join or create a channel and send a
message. Confirm the server log
receives NIP-17 gateway traffic and the message appears in the client.

The laptop never needs a route to this machine's `:3000` port. If the public
relays block WebSockets on the laptop network, use reachable public relays in
both server and client configuration.
