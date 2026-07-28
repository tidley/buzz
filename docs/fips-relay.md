# FIPS Relay Transport

The relay can accept NIP-01 sessions over FIPS QUIC. This is disabled unless
the relay is built with `--features fips` and `BUZZ_FIPS_ENABLED=true`.

Set a stable identity and public rendezvous services:

```sh
BUZZ_FIPS_ENABLED=true
BUZZ_FIPS_PRIVATE_KEY=<32-byte hex or nsec1 secret>
BUZZ_FIPS_NOSTR_RELAYS=wss://relay.example,wss://relay-backup.example
BUZZ_FIPS_STUN_SERVERS=stun:stun.example:3478
```

The runtime advertises its Nostr/STUN NAT endpoint, accepts identity-verified
FIPS QUIC streams, and exchanges length-delimited UTF-8 NIP-01 frames through
the relay's normal `VirtualConnection` dispatcher. Each FIPS connection is
bound once to the deployment community resolved from `RELAY_URL`; FIPS peers
cannot select a tenant through protocol frames.
