import type { Nip17CommunityConfig } from "@/shared/api/nip17Transport";

let currentConfig: Nip17CommunityConfig = { relayTransport: "direct" };

export function setRelayTransportConfig(config: Partial<Nip17CommunityConfig>) {
  currentConfig = config;
}

export function relayTransportConfigState() {
  return currentConfig;
}

export function usesNip17Transport() {
  return currentConfig.relayTransport === "nip17";
}
