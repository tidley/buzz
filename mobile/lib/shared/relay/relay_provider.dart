import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:nostr/nostr.dart' as nostr;

import '../community/community.dart';
import '../community/community_provider.dart';
import 'relay_client.dart';

/// Relay connection configuration.
///
/// In the pure-nostr world the only secrets the app cares about are:
///   - `baseUrl` — where the relay lives (used for WS + media upload)
///   - `nsec`    — the user's signing key (drives NIP-42 AUTH and event sigs)
class RelayConfig {
  final String baseUrl;

  /// Nostr secret key (bech32 nsec) for signing events and NIP-42 AUTH.
  final String? nsec;
  final RelayTransportMode relayTransport;
  final String? nip17GatewayPubkey;
  final List<String> nip17PublicRelayUrls;

  /// Stored preference for FIPS-capable builds. It does not change crypto in
  /// the Dart client.
  final bool preferFips;

  const RelayConfig({
    required this.baseUrl,
    this.nsec,
    this.relayTransport = RelayTransportMode.direct,
    this.nip17GatewayPubkey,
    this.nip17PublicRelayUrls = const [],
    this.preferFips = false,
  });

  bool get usesNip17 => relayTransport == RelayTransportMode.nip17;

  /// Derive the websocket URL from the HTTP base URL.
  String get wsUrl {
    final uri = Uri.parse(baseUrl);
    final scheme = uri.scheme == 'https' ? 'wss' : 'ws';
    return uri.replace(scheme: scheme).toString();
  }
}

/// Compile-time environment config via --dart-define.
///
/// Run with:
///   flutter run --dart-define=BUZZ_RELAY_URL=http://localhost:3000
///
/// Or create a `.env.json` and use --dart-define-from-file=.env.json
class Env {
  static const relayUrl = String.fromEnvironment(
    'BUZZ_RELAY_URL',
    defaultValue: 'http://localhost:3000',
  );
}

class RelayConfigNotifier extends Notifier<RelayConfig> {
  @override
  RelayConfig build() {
    // Watch the active community so that when it changes (community switch),
    // the config rebuilds, triggering the full provider cascade.
    final activeAsync = ref.watch(activeCommunityProvider);
    final active = activeAsync.value;
    if (active != null) {
      return RelayConfig(
        baseUrl: active.relayUrl,
        nsec: active.nsec,
        relayTransport: active.relayTransport,
        nip17GatewayPubkey: active.nip17GatewayPubkey,
        nip17PublicRelayUrls: active.nip17PublicRelayUrls,
        preferFips: active.preferFips,
      );
    }

    // Fallback to compile-time env config (dev mode).
    return const RelayConfig(baseUrl: Env.relayUrl);
  }

  void update({
    required String baseUrl,
    String? nsec,
    RelayTransportMode relayTransport = RelayTransportMode.direct,
    String? nip17GatewayPubkey,
    List<String> nip17PublicRelayUrls = const [],
    bool preferFips = false,
  }) {
    state = RelayConfig(
      baseUrl: baseUrl,
      nsec: nsec,
      relayTransport: relayTransport,
      nip17GatewayPubkey: nip17GatewayPubkey,
      nip17PublicRelayUrls: nip17PublicRelayUrls,
      preferFips: preferFips,
    );
  }
}

final relayConfigProvider = NotifierProvider<RelayConfigNotifier, RelayConfig>(
  RelayConfigNotifier.new,
);

/// Derive the hex pubkey from a bech32 nsec, or null on any failure.
String? pubkeyFromNsec(String? nsec) {
  if (nsec == null || nsec.isEmpty) return null;
  try {
    final privkeyHex = nostr.Nip19.decode(payload: nsec).data;
    if (privkeyHex.isEmpty) return null;
    return nostr.Keys(privkeyHex).public;
  } catch (_) {
    return null;
  }
}

/// The current user's hex pubkey, derived from the active community nsec.
final myPubkeyProvider = Provider<String?>((ref) {
  final config = ref.watch(relayConfigProvider);
  return pubkeyFromNsec(config.nsec);
});

/// Provides a [RelayClient] that reacts to config changes.
///
/// Only used for the media upload HTTP endpoint now — all data flow goes
/// through the relay WebSocket session.
final relayClientProvider = Provider<RelayClient>((ref) {
  final config = ref.watch(relayConfigProvider);
  final client = RelayClient(baseUrl: config.baseUrl);
  ref.onDispose(client.dispose);
  return client;
});
