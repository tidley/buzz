import 'dart:async';
import 'dart:convert';

import 'package:buzz/shared/relay/relay_transport.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:nostr/nostr.dart' as nostr;

void main() {
  test(
    'publishes relay frames as NIP-17 gift wraps to every public relay',
    () async {
      final user = nostr.Keys.generate();
      final gateway = nostr.Keys.generate();
      final firstRelay = _ControlledPublicRelay();
      final secondRelay = _ControlledPublicRelay();
      final transport = Nip17RelayTransport(
        nsec: user.nsec,
        gatewayPubkey: gateway.public,
        publicRelayUrls: [
          Uri.parse('wss://first.example'),
          Uri.parse('wss://second.example'),
        ],
        publicRelayTransportFactory: (uri) =>
            uri.host == 'first.example' ? firstRelay : secondRelay,
      );
      addTearDown(transport.close);
      await transport.ready;

      final firstEvent = firstRelay.nextEvent();
      final secondEvent = secondRelay.nextEvent();
      transport.send('["REQ","inbox",{"kinds":[1]}]');

      for (final event in [await firstEvent, await secondEvent]) {
        expect(event.kind, nostr.GiftWrap.kindGiftWrap);
        expect(_hasRecipient(event.tags, gateway.public), isTrue);
        final rumor = await nostr.DirectMessage.parse(
          giftWrap: event,
          recipientSecretKey: gateway.secret,
        );
        expect(rumor.kind, nostr.DirectMessage.kindDirectMessage);
        expect(rumor.pubkey, user.public);
        expect(_hasRecipient(rumor.tags, gateway.public), isTrue);
        expect(rumor.content, '["REQ","inbox",{"kinds":[1]}]');
      }
    },
  );

  test('delivers only gateway-authenticated NIP-17 response frames', () async {
    final user = nostr.Keys.generate();
    final gateway = nostr.Keys.generate();
    final attacker = nostr.Keys.generate();
    final relay = _ControlledPublicRelay();
    final transport = Nip17RelayTransport(
      nsec: user.nsec,
      gatewayPubkey: gateway.public,
      publicRelayUrls: [Uri.parse('wss://relay.example')],
      publicRelayTransportFactory: (_) => relay,
    );
    addTearDown(transport.close);
    await transport.ready;

    final received = <dynamic>[];
    final subscription = transport.stream.listen(received.add);
    addTearDown(subscription.cancel);

    relay.addEvent(
      await _gatewayResponse(
        gateway: attacker,
        recipient: user,
        content: '["NOTICE","forged"]',
      ),
    );
    await Future<void>.delayed(Duration.zero);

    relay.addEvent(
      await _gatewayResponse(
        gateway: gateway,
        recipient: user,
        content: '["NOTICE","from gateway"]',
      ),
    );
    await Future<void>.delayed(Duration.zero);

    expect(received, ['["NOTICE","from gateway"]']);
  });
}

Future<nostr.Event> _gatewayResponse({
  required nostr.Keys gateway,
  required nostr.Keys recipient,
  required String content,
}) {
  return nostr.DirectMessage.create(
    message: content,
    authorSecretKey: gateway.secret,
    recipientPubkey: recipient.public,
  );
}

bool _hasRecipient(List<List<String>> tags, String pubkey) =>
    tags.any((tag) => tag.length >= 2 && tag[0] == 'p' && tag[1] == pubkey);

class _ControlledPublicRelay implements RelayTransport {
  final StreamController<dynamic> _incoming = StreamController.broadcast();
  final StreamController<String> _outgoing = StreamController.broadcast();

  @override
  Future<void> get ready => Future.value();

  @override
  Stream<dynamic> get stream => _incoming.stream;

  @override
  Future<void> activate() async {}

  void addEvent(nostr.Event event) {
    _incoming.add(jsonEncode(['EVENT', 'gift-wraps', event.toMap()]));
  }

  Future<nostr.Event> nextEvent() async {
    await for (final raw in _outgoing.stream) {
      final message = jsonDecode(raw) as List<dynamic>;
      if (message.first == 'EVENT') {
        return nostr.Event.fromMap(message[1] as Map<String, dynamic>);
      }
    }
    throw StateError('Public relay closed before a gift wrap was published');
  }

  @override
  void send(String message) => _outgoing.add(message);

  @override
  Future<void> close() async {
    await _incoming.close();
    await _outgoing.close();
  }
}
