import type { Community } from "@/features/communities/types";

export type Nip17CommunityConfig = Pick<
  Community,
  "relayTransport" | "nip17GatewayPubkey" | "nip17PublicRelayUrls"
>;

export function normalizeNip17Config(
  config: Partial<Nip17CommunityConfig>,
): Nip17CommunityConfig {
  const gatewayPubkey = config.nip17GatewayPubkey?.toLowerCase();
  const publicRelayUrls = config.nip17PublicRelayUrls;
  if (
    config.relayTransport !== "nip17" ||
    !gatewayPubkey ||
    !/^[0-9a-f]{64}$/.test(gatewayPubkey) ||
    !publicRelayUrls?.length ||
    !publicRelayUrls.every(isWebSocketUrl)
  ) {
    return { relayTransport: "direct" };
  }
  return {
    relayTransport: "nip17",
    nip17GatewayPubkey: gatewayPubkey,
    nip17PublicRelayUrls: publicRelayUrls,
  };
}

export function relayTransportConfig(config: Partial<Nip17CommunityConfig>) {
  const normalized = normalizeNip17Config(config);
  return normalized.relayTransport === "nip17"
    ? {
        transport: "nip17" as const,
        gatewayPubkey: normalized.nip17GatewayPubkey,
        publicRelayUrls: normalized.nip17PublicRelayUrls,
      }
    : { transport: "direct" as const };
}

function isWebSocketUrl(value: string) {
  try {
    const url = new URL(value);
    return (url.protocol === "ws:" || url.protocol === "wss:") && !!url.host;
  } catch {
    return false;
  }
}
