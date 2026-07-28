import type {
  Channel,
  ChannelDetail,
  ChannelMember,
  ChannelMessagesPageResponse,
  ChannelPageCursor,
  ChannelType,
  CreateChannelInput,
  OpenDmInput,
  SetChannelPurposeInput,
  SetChannelTopicInput,
  UpdateChannelInput,
} from "@/shared/api/types";
import { invokeTauri } from "@/shared/api/tauri";
import { relayClient } from "@/shared/api/relayClient";
import { usesNip17Transport } from "@/shared/api/relayTransportState";
import { getIdentity } from "@/shared/api/tauriIdentity";

export type RawChannel = {
  id: string;
  name: string;
  channel_type: ChannelType;
  visibility: "open" | "private";
  description: string;
  topic: string | null;
  purpose: string | null;
  member_count: number;
  member_pubkeys: string[];
  last_message_at: string | null;
  archived_at: string | null;
  participants: string[];
  participant_pubkeys: string[];
  is_member?: boolean;
  ttl_seconds: number | null;
  ttl_deadline: string | null;
};

type RawChannelDetail = RawChannel & {
  created_by: string;
  created_at: string;
  updated_at: string;
  topic_set_by: string | null;
  topic_set_at: string | null;
  purpose_set_by: string | null;
  purpose_set_at: string | null;
  topic_required: boolean;
  max_members: number | null;
  nip29_group_id: string | null;
};

type RawChannelMember = {
  pubkey: string;
  role: ChannelMember["role"];
  is_agent?: boolean;
  joined_at: string;
  display_name: string | null;
};

type RawChannelMembersResponse = {
  members: RawChannelMember[];
  next_cursor: string | null;
};

export function fromRawChannel(channel: RawChannel): Channel {
  return {
    id: channel.id,
    name: channel.name,
    channelType: channel.channel_type,
    visibility: channel.visibility,
    description: channel.description,
    topic: channel.topic,
    purpose: channel.purpose,
    memberCount: channel.member_count,
    memberPubkeys: channel.member_pubkeys ?? [],
    lastMessageAt: channel.last_message_at,
    archivedAt: channel.archived_at,
    participants: channel.participants,
    participantPubkeys: channel.participant_pubkeys,
    isMember: channel.is_member ?? true,
    ttlSeconds: channel.ttl_seconds,
    ttlDeadline: channel.ttl_deadline,
  };
}

export function fromRawChannelDetail(channel: RawChannelDetail): ChannelDetail {
  return {
    ...fromRawChannel(channel),
    createdBy: channel.created_by,
    createdAt: channel.created_at,
    updatedAt: channel.updated_at,
    topicSetBy: channel.topic_set_by,
    topicSetAt: channel.topic_set_at,
    purposeSetBy: channel.purpose_set_by,
    purposeSetAt: channel.purpose_set_at,
    topicRequired: channel.topic_required,
    maxMembers: channel.max_members,
    nip29GroupId: channel.nip29_group_id,
  };
}

function fromRawChannelMember(member: RawChannelMember): ChannelMember {
  return {
    pubkey: member.pubkey,
    role: member.role,
    isAgent: member.is_agent ?? false,
    joinedAt: member.joined_at,
    displayName: member.display_name,
  };
}

export async function getChannels(): Promise<Channel[]> {
  if (usesNip17Transport()) {
    return getNip17Channels();
  }
  const channels = await invokeTauri<RawChannel[]>("get_channels");
  return channels.map(fromRawChannel);
}

async function getNip17Channels(): Promise<Channel[]> {
  const { pubkey } = await getIdentity();
  const memberEvents = await relayClient.fetchEvents({
    kinds: [39002],
    "#p": [pubkey],
    limit: 500,
  });
  const membersByChannel = new Map<string, string[]>();
  for (const event of memberEvents) {
    const channelId = tagValue(event.tags, "d");
    if (!channelId) continue;
    membersByChannel.set(
      channelId,
      event.tags
        .filter(([name, value]) => name === "p" && value)
        .map(([, value]) => value),
    );
  }
  const channelIds = [...membersByChannel.keys()];
  if (channelIds.length === 0) return [];

  const metadata = await relayClient.fetchEvents({
    kinds: [39000],
    "#d": channelIds,
    limit: channelIds.length,
  });
  const participantPubkeys = [
    ...new Set(
      metadata.flatMap((event) =>
        event.tags
          .filter(([tag, value]) => tag === "p" && value)
          .map(([, value]) => value),
      ),
    ),
  ];
  const profiles = await relayClient.fetchEvents({
    kinds: [0],
    authors: participantPubkeys,
    limit: participantPubkeys.length,
  });
  const profileNames = new Map(
    profiles.flatMap((event) => {
      try {
        const profile = JSON.parse(event.content) as {
          display_name?: string;
          name?: string;
        };
        const name = profile.display_name?.trim() || profile.name?.trim();
        return name ? [[event.pubkey.toLowerCase(), name] as const] : [];
      } catch {
        return [];
      }
    }),
  );
  return metadata.flatMap((event) => {
    const id = tagValue(event.tags, "d");
    const name = tagValue(event.tags, "name");
    if (!id || !name || !membersByChannel.has(id)) return [];

    const participantPubkeys = event.tags
      .filter(([tag, value]) => tag === "p" && value)
      .map(([, value]) => value);
    const participants = participantPubkeys.map(
      (participant) =>
        profileNames.get(participant.toLowerCase()) ?? participant,
    );
    const members = membersByChannel.get(id) ?? [];
    return [
      fromRawChannel({
        id,
        name,
        channel_type: (tagValue(event.tags, "t") ?? "stream") as ChannelType,
        visibility: hasTag(event.tags, "private") ? "private" : "open",
        description: tagValue(event.tags, "about") ?? "",
        topic: tagValue(event.tags, "topic"),
        purpose: tagValue(event.tags, "purpose"),
        member_count: members.length,
        member_pubkeys: members,
        last_message_at: null,
        archived_at: hasTag(event.tags, "archived")
          ? new Date(event.created_at * 1_000).toISOString()
          : null,
        participants,
        participant_pubkeys: participantPubkeys,
        is_member: true,
        ttl_seconds: null,
        ttl_deadline: null,
      }),
    ];
  });
}

function tagValue(tags: string[][], name: string): string | null {
  return tags.find(([tag, value]) => tag === name && value)?.[1] ?? null;
}

function hasTag(tags: string[][], name: string): boolean {
  return tags.some(([tag]) => tag === name);
}

export async function createChannel(
  input: CreateChannelInput,
): Promise<Channel> {
  return fromRawChannel(await invokeTauri<RawChannel>("create_channel", input));
}

export async function ensureStarterChannels(): Promise<Channel[]> {
  return (await invokeTauri<RawChannel[]>("ensure_starter_channels")).map(
    fromRawChannel,
  );
}

export async function openDm(input: OpenDmInput): Promise<Channel> {
  return fromRawChannel(await invokeTauri<RawChannel>("open_dm", input));
}

export async function hideDm(channelId: string): Promise<void> {
  await invokeTauri<void>("hide_dm", { channelId });
}

export async function getChannelDetails(
  channelId: string,
): Promise<ChannelDetail> {
  const detail = await invokeTauri<RawChannelDetail>("get_channel_details", {
    channelId,
  });
  return fromRawChannelDetail(detail);
}

export async function updateChannel(
  input: UpdateChannelInput,
): Promise<ChannelDetail> {
  const channel = await invokeTauri<RawChannelDetail>("update_channel", {
    input,
  });
  return fromRawChannelDetail(channel);
}

export async function setChannelTopic(
  input: SetChannelTopicInput,
): Promise<void> {
  await invokeTauri("set_channel_topic", input);
}

export async function setChannelPurpose(
  input: SetChannelPurposeInput,
): Promise<void> {
  await invokeTauri("set_channel_purpose", input);
}

export async function archiveChannel(channelId: string): Promise<void> {
  await invokeTauri("archive_channel", { channelId });
}

export async function unarchiveChannel(channelId: string): Promise<void> {
  await invokeTauri("unarchive_channel", { channelId });
}

export async function deleteChannel(channelId: string): Promise<void> {
  await invokeTauri("delete_channel", { channelId });
}

type RawChannelMessagesPageResponse = {
  events: ChannelMessagesPageResponse["events"];
  next_cursor: { created_at: number; event_id: string } | null;
};

/**
 * Fetch one keyset page of top-level channel history strictly older than a
 * cursor, via the bridge composite `(createdAt, eventId)` cursor.
 *
 * The desktop timeline pages history over WS `REQ` with a bare `until`
 * (`createdAt`) cursor, which cannot advance past a `createdAt` second denser
 * than one page. This is the escape hatch: `beforeId` is the id of the oldest
 * event already loaded at `before`, and the relay returns strictly-older rows
 * (`created_at < before OR (created_at = before AND id > beforeId)`). Pass the
 * returned `nextCursor` back to page further; `nextCursor` is null once a short
 * page proves history is exhausted.
 */
export async function getChannelMessagesBefore(
  channelId: string,
  cursor: ChannelPageCursor,
  limit?: number,
): Promise<ChannelMessagesPageResponse> {
  const response = await invokeTauri<RawChannelMessagesPageResponse>(
    "get_channel_messages_before",
    {
      channelId,
      before: cursor.createdAt,
      beforeId: cursor.eventId,
      limit: limit ?? null,
    },
  );

  return {
    events: response.events,
    nextCursor: response.next_cursor
      ? {
          createdAt: response.next_cursor.created_at,
          eventId: response.next_cursor.event_id,
        }
      : null,
  };
}

export async function getChannelMembers(
  channelId: string,
): Promise<ChannelMember[]> {
  const response = await invokeTauri<RawChannelMembersResponse>(
    "get_channel_members",
    { channelId },
  );
  return response.members.map(fromRawChannelMember);
}

export async function joinChannel(channelId: string): Promise<void> {
  await invokeTauri<void>("join_channel", { channelId });
}
