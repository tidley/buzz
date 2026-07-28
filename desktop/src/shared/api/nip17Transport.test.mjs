import assert from "node:assert/strict";
import test from "node:test";

import {
  normalizeNip17Config,
  relayTransportConfig,
} from "./nip17Transport.ts";

const GATEWAY = "a".repeat(64);

test("normalizes a valid opt-in NIP-17 gateway configuration", () => {
  assert.deepEqual(
    normalizeNip17Config({
      relayTransport: "nip17",
      nip17GatewayPubkey: GATEWAY.toUpperCase(),
      nip17PublicRelayUrls: ["wss://one.example", "ws://two.example"],
    }),
    {
      relayTransport: "nip17",
      nip17GatewayPubkey: GATEWAY,
      nip17PublicRelayUrls: ["wss://one.example", "ws://two.example"],
    },
  );
});

test("falls back to direct transport for incomplete persisted NIP-17 config", () => {
  assert.deepEqual(
    normalizeNip17Config({
      relayTransport: "nip17",
      nip17GatewayPubkey: "not-a-pubkey",
      nip17PublicRelayUrls: [],
    }),
    { relayTransport: "direct" },
  );
});

test("only sends NIP-17 socket configuration when explicitly selected", () => {
  assert.deepEqual(relayTransportConfig({ relayTransport: "direct" }), {
    transport: "direct",
  });
  assert.deepEqual(
    relayTransportConfig({
      relayTransport: "nip17",
      nip17GatewayPubkey: GATEWAY,
      nip17PublicRelayUrls: ["wss://relay.example"],
    }),
    {
      transport: "nip17",
      gatewayPubkey: GATEWAY,
      publicRelayUrls: ["wss://relay.example"],
    },
  );
});
