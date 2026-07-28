import { invokeTauri } from "@/shared/api/tauri";
import { relayClient } from "@/shared/api/relayClient";
import { usesNip17Transport } from "@/shared/api/relayTransportState";
import type { ChannelPageCursor, RelayEvent } from "@/shared/api/types";

const TIMELINE_KINDS = [
  9, 40002, 40008, 40099, 43001, 43002, 43003, 43004, 43005, 43006, 48100,
];

/** Fetch the flat Nostr event array for one server-assembled channel window. */
export async function getChannelWindowEvents(
  channelId: string,
  cursor: ChannelPageCursor | null = null,
  limitRows = 50,
): Promise<RelayEvent[]> {
  if (usesNip17Transport()) {
    return relayClient.fetchEvents({
      kinds: TIMELINE_KINDS,
      "#h": [channelId],
      limit: Math.min(limitRows, 200),
      ...(cursor ? { until: cursor.createdAt } : {}),
    });
  }
  return invokeTauri<RelayEvent[]>("get_channel_window", {
    channelId,
    limitRows,
    cursor: cursor
      ? { created_at: cursor.createdAt, event_id: cursor.eventId }
      : null,
  });
}
