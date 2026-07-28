import 'dart:async';
import 'dart:convert';

import 'package:nostr/nostr.dart' as nostr;
import 'package:uuid/uuid.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// A bidirectional transport for relay protocol frames.
abstract class RelayTransport {
  /// Completes when the transport can send and receive frames.
  Future<void> get ready;

  /// Provides frames received from the relay.
  Stream<dynamic> get stream;

  /// Begins a relay session after the consumer has installed its frame listener.
  ///
  /// Direct WebSockets do not need a bootstrap frame. Store-and-forward
  /// transports use this hook to create the remote virtual connection without
  /// racing its initial NIP-42 challenge.
  Future<void> activate() async {}

  /// Sends a text frame to the relay.
  void send(String message);

  /// Closes the transport.
  Future<void> close();
}

/// Creates relay transports for a WebSocket URL.
typedef RelayTransportFactory = RelayTransport Function(Uri uri);

/// Transport factory used to connect to each public relay in a NIP-17 tunnel.
typedef PublicRelayTransportFactory = RelayTransport Function(Uri uri);

/// A [RelayTransport] that uses a [WebSocketChannel].
class WebSocketRelayTransport implements RelayTransport {
  final WebSocketChannel _channel;

  /// Connects a WebSocket channel to [uri].
  WebSocketRelayTransport.connect(Uri uri)
    : _channel = WebSocketChannel.connect(uri);

  @override
  Future<void> get ready => _channel.ready;

  @override
  Stream<dynamic> get stream => _channel.stream;

  @override
  Future<void> activate() async {}

  @override
  void send(String message) => _channel.sink.add(message);

  @override
  Future<void> close() => _channel.sink.close();
}

/// Carries relay protocol frames inside NIP-17 direct messages.
///
/// The gateway key is the expected author of the decrypted NIP-17 response.
/// Public relays only see signed kind:1059 gift wraps addressed to that key.
class Nip17RelayTransport implements RelayTransport {
  static const _sessionTagPrefix = 'buzz-nip17-session:';

  final String _privateKey;
  final String _publicKey;
  final String _gatewayPubkey;
  final List<RelayTransport> _publicRelays = [];
  final StreamController<dynamic> _frames = StreamController.broadcast();
  final List<StreamSubscription<dynamic>> _subscriptions = [];
  final String _subscriptionId;
  final String _sessionId;
  Future<void> _sendQueue = Future.value();

  @override
  late final Future<void> ready;

  /// Creates a tunnel over [publicRelayUrls] to [gatewayPubkey].
  ///
  /// [nsec] is the mobile identity that receives gateway responses. Supplying
  /// [publicRelayTransportFactory] makes the public relay layer testable.
  Nip17RelayTransport({
    required String nsec,
    required String gatewayPubkey,
    required List<Uri> publicRelayUrls,
    PublicRelayTransportFactory publicRelayTransportFactory =
        WebSocketRelayTransport.connect,
  }) : _privateKey = _decodePrivateKey(nsec),
       _gatewayPubkey = _validatePubkey(gatewayPubkey),
       _publicKey = nostr.Keys(_decodePrivateKey(nsec)).public,
       _subscriptionId = 'buzz-nip17-${DateTime.now().microsecondsSinceEpoch}',
       _sessionId = const Uuid().v4() {
    if (publicRelayUrls.isEmpty) {
      throw ArgumentError.value(
        publicRelayUrls,
        'publicRelayUrls',
        'At least one public relay is required',
      );
    }
    ready = _connect(publicRelayUrls, publicRelayTransportFactory);
  }

  /// Creates a [RelayTransportFactory] for a configured NIP-17 gateway.
  ///
  /// The URI supplied by [RelaySocket] is intentionally ignored: all traffic
  /// goes through [publicRelayUrls], not directly to the private relay.
  static RelayTransportFactory configured({
    required String nsec,
    required String gatewayPubkey,
    required List<Uri> publicRelayUrls,
    PublicRelayTransportFactory publicRelayTransportFactory =
        WebSocketRelayTransport.connect,
  }) {
    return (_) => Nip17RelayTransport(
      nsec: nsec,
      gatewayPubkey: gatewayPubkey,
      publicRelayUrls: publicRelayUrls,
      publicRelayTransportFactory: publicRelayTransportFactory,
    );
  }

  @override
  Stream<dynamic> get stream => _frames.stream;

  @override
  Future<void> activate() =>
      _wrapAndPublish('["CLOSE","__buzz_nip17_bootstrap"]');

  @override
  void send(String message) {
    // Relay protocol frames are ordered. Gift-wrap encryption is async, so
    // serialize it to preserve the ordering RelaySocket expects.
    _sendQueue = _sendQueue.then((_) => _wrapAndPublish(message));
  }

  @override
  Future<void> close() async {
    for (final subscription in _subscriptions) {
      await subscription.cancel();
    }
    _subscriptions.clear();
    for (final relay in _publicRelays) {
      await relay.close();
    }
    _publicRelays.clear();
    await _frames.close();
  }

  Future<void> _connect(
    List<Uri> publicRelayUrls,
    PublicRelayTransportFactory transportFactory,
  ) async {
    final connections = await Future.wait<RelayTransport?>(
      publicRelayUrls.map((uri) async {
        try {
          final relay = transportFactory(uri);
          await relay.ready;
          return relay;
        } catch (_) {
          return null;
        }
      }),
    );
    _publicRelays.addAll(connections.whereType<RelayTransport>());
    if (_publicRelays.isEmpty) {
      throw StateError('Unable to connect to any configured public relay');
    }

    for (final relay in _publicRelays) {
      _subscriptions.add(relay.stream.listen(_handlePublicRelayFrame));
      relay.send(
        jsonEncode([
          'REQ',
          _subscriptionId,
          {
            'kinds': [nostr.GiftWrap.kindGiftWrap],
            '#p': [_publicKey],
          },
        ]),
      );
    }
  }

  Future<void> _wrapAndPublish(String frame) async {
    try {
      final rumor = nostr.Event.unsigned(
        pubkey: _publicKey,
        kind: nostr.DirectMessage.kindDirectMessage,
        content: frame,
        tags: [
          ['p', _gatewayPubkey],
          ['t', '$_sessionTagPrefix$_sessionId'],
        ],
      );
      final giftWrap = await nostr.GiftWrap.wrap(
        rumor: rumor,
        authorSecretKey: _privateKey,
        recipientPubkey: _gatewayPubkey,
      );
      final message = jsonEncode(['EVENT', giftWrap.toMap()]);
      for (final relay in _publicRelays) {
        relay.send(message);
      }
    } catch (error, stackTrace) {
      _frames.addError(error, stackTrace);
    }
  }

  void _handlePublicRelayFrame(dynamic raw) {
    if (raw is! String) return;
    try {
      final message = jsonDecode(raw);
      if (message is! List ||
          message.length != 3 ||
          message[0] != 'EVENT' ||
          message[2] is! Map<String, dynamic>) {
        return;
      }
      final event = nostr.Event.fromMap(message[2] as Map<String, dynamic>);
      unawaited(_unwrapGatewayResponse(event));
    } catch (_) {
      // Public relay input is untrusted. Invalid envelopes are ignored.
    }
  }

  Future<void> _unwrapGatewayResponse(nostr.Event giftWrap) async {
    try {
      if (giftWrap.kind != nostr.GiftWrap.kindGiftWrap ||
          !_hasRecipient(giftWrap.tags, _publicKey)) {
        return;
      }
      final rumor = await nostr.DirectMessage.parse(
        giftWrap: giftWrap,
        recipientSecretKey: _privateKey,
      );

      // NIP-59 authenticates the seal signer as the rumor author. Requiring
      // the configured gateway here prevents a public-relay participant from
      // injecting otherwise valid encrypted relay frames.
      if (rumor.pubkey.toLowerCase() != _gatewayPubkey ||
          rumor.kind != nostr.DirectMessage.kindDirectMessage ||
          !_hasRecipient(rumor.tags, _publicKey) ||
          !_hasSessionTag(rumor.tags, _sessionId)) {
        return;
      }
      _frames.add(rumor.content);
    } catch (_) {
      // Decryption and signature failures are untrusted public-relay input.
    }
  }

  static bool _hasRecipient(List<List<String>> tags, String pubkey) => tags.any(
    (tag) => tag.length >= 2 && tag[0] == 'p' && tag[1].toLowerCase() == pubkey,
  );

  static bool _hasSessionTag(List<List<String>> tags, String sessionId) =>
      tags.any(
        (tag) =>
            tag.length >= 2 &&
            tag[0] == 't' &&
            tag[1] == '$_sessionTagPrefix$sessionId',
      );

  static String _decodePrivateKey(String nsec) {
    final decoded = nostr.Nip19.decode(payload: nsec);
    if (decoded.prefix != nostr.Nip19Prefix.nsec || decoded.data.length != 64) {
      throw ArgumentError.value(nsec, 'nsec', 'Invalid Nostr secret key');
    }
    return decoded.data;
  }

  static String _validatePubkey(String pubkey) {
    final normalized = pubkey.toLowerCase();
    if (!RegExp(r'^[0-9a-f]{64}$').hasMatch(normalized)) {
      throw ArgumentError.value(
        pubkey,
        'gatewayPubkey',
        'Expected a 32-byte hexadecimal public key',
      );
    }
    return normalized;
  }
}
