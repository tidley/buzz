import 'dart:typed_data';

import 'package:buzz/shared/relay/fips_relay_transport.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  const peer = 'npub1relaypeer';

  test('starts, connects, sends, and polls received frames', () async {
    final bridge = _FakeFipsBridge()
      ..receiveResults.add(
        FipsReceiveResult.frame(Uint8List.fromList([104, 105])),
      );
    final transport = FipsRelayTransport(
      Uri.parse('wss://relay.example?fipsPeer=$peer'),
      bridge: bridge,
      isAndroid: () => true,
      pollInterval: Duration.zero,
    );
    addTearDown(transport.close);

    await transport.ready;
    expect(bridge.started, 1);
    expect(bridge.connectedPeers, [peer]);

    transport.send('hello');
    expect(bridge.sent, [
      Uint8List.fromList([104, 101, 108, 108, 111]),
    ]);
    expect(await transport.stream.first, 'hi');
  });

  test('retries receive with the native requested capacity', () async {
    final bridge = _FakeFipsBridge()
      ..receiveResults.add(FipsReceiveResult.bufferTooSmall(3))
      ..receiveResults.add(
        FipsReceiveResult.frame(Uint8List.fromList([98, 121, 101])),
      );
    final transport = FipsRelayTransport(
      Uri.parse('wss://relay.example?fipsPeer=$peer'),
      bridge: bridge,
      isAndroid: () => true,
      pollInterval: Duration.zero,
    );
    addTearDown(transport.close);

    await transport.ready;
    expect(await transport.stream.first, 'bye');
    await transport.close();
    expect(bridge.receiveCapacities.take(2), [65536, 3]);
  });

  test('maps bridge failures to stream errors and closes', () async {
    final bridge = _FakeFipsBridge()
      ..receiveResults.add(
        const FipsReceiveResult.status(FipsBridgeStatus.notConnected),
      );
    final transport = FipsRelayTransport(
      Uri.parse('wss://relay.example?fipsPeer=$peer'),
      bridge: bridge,
      isAndroid: () => true,
      pollInterval: Duration.zero,
    );
    addTearDown(transport.close);

    await transport.ready;
    await expectLater(transport.stream, emitsError(isA<FipsBridgeException>()));
    expect(bridge.stopped, 1);
  });

  test('rejects non-Android use and invalid FIPS peer URLs', () async {
    final bridge = _FakeFipsBridge();
    final nonAndroid = FipsRelayTransport(
      Uri.parse('wss://relay.example?fipsPeer=$peer'),
      bridge: bridge,
      isAndroid: () => false,
    );
    final invalidPeer = FipsRelayTransport(
      Uri.parse('wss://relay.example'),
      bridge: bridge,
      isAndroid: () => true,
    );
    addTearDown(nonAndroid.close);
    addTearDown(invalidPeer.close);

    await expectLater(nonAndroid.ready, throwsA(isA<UnsupportedError>()));
    await expectLater(invalidPeer.ready, throwsArgumentError);
  });
}

class _FakeFipsBridge implements FipsBridge {
  int started = 0;
  int stopped = 0;
  final List<String> connectedPeers = [];
  final List<Uint8List> sent = [];
  final List<int> receiveCapacities = [];
  final List<FipsReceiveResult> receiveResults = [];

  @override
  FipsBridgeStatus start() {
    started++;
    return FipsBridgeStatus.running;
  }

  @override
  FipsBridgeStatus connect(String peer) {
    connectedPeers.add(peer);
    return FipsBridgeStatus.connected;
  }

  @override
  FipsBridgeStatus send(Uint8List frame) {
    sent.add(frame);
    return FipsBridgeStatus.connected;
  }

  @override
  Future<FipsReceiveResult> receive(int capacity) async {
    receiveCapacities.add(capacity);
    if (receiveResults.isEmpty) {
      return const FipsReceiveResult.status(FipsBridgeStatus.failed);
    }
    return receiveResults.removeAt(0);
  }

  @override
  FipsBridgeStatus stop() {
    stopped++;
    return FipsBridgeStatus.stopped;
  }
}
