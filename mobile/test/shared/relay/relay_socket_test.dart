import 'dart:async';
import 'dart:convert';

import 'package:buzz/shared/relay/relay_socket.dart';
import 'package:buzz/shared/relay/relay_transport.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:nostr/nostr.dart' as nostr;

void main() {
  test('uses the transport to authenticate, send, and close', () async {
    final transport = _ControlledRelayTransport();
    final received = <List<dynamic>>[];
    var connected = false;
    final socket = RelaySocket(
      wsUrl: 'wss://relay.example',
      nsec: nostr.Keys.generate().nsec,
      onMessage: received.add,
      onConnected: () => connected = true,
      onDisconnected: (_) {},
      transportFactory: (_) => transport,
    );
    addTearDown(socket.dispose);

    final connecting = socket.connect();
    transport.add(['AUTH', 'challenge']);
    final auth = jsonDecode(await transport.firstSent) as List<dynamic>;
    final event = auth[1] as Map<String, dynamic>;
    transport.add(['OK', event['id'], true]);
    await connecting;

    socket.send(['REQ', 'subscription']);
    transport.add(['NOTICE', 'hello']);
    await Future<void>.delayed(Duration.zero);
    await socket.disconnect();

    expect(connected, isTrue);
    expect(jsonDecode(transport.sent.last), ['REQ', 'subscription']);
    expect(received, [
      ['NOTICE', 'hello'],
    ]);
    expect(transport.closed, isTrue);
  });
}

class _ControlledRelayTransport implements RelayTransport {
  final StreamController<dynamic> _messages = StreamController();
  final Completer<String> _firstSent = Completer();
  final List<String> sent = [];
  var closed = false;

  Future<String> get firstSent => _firstSent.future;

  void add(List<dynamic> message) => _messages.add(jsonEncode(message));

  @override
  Future<void> get ready => Future.value();

  @override
  Stream<dynamic> get stream => _messages.stream;

  @override
  Future<void> activate() async {}

  @override
  void send(String message) {
    sent.add(message);
    if (!_firstSent.isCompleted) _firstSent.complete(message);
  }

  @override
  Future<void> close() async {
    closed = true;
    await _messages.close();
  }
}
