import 'dart:convert';

import 'package:flutter_secure_storage/flutter_secure_storage.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:buzz/shared/community/community.dart';
import 'package:buzz/shared/community/community_storage.dart';

/// In-memory fake that extends Fake to satisfy all FlutterSecureStorage
/// interface methods, but implements the core read/write/delete with real
/// in-memory logic.
class FakeSecureStorage extends Fake implements FlutterSecureStorage {
  final Map<String, String> _data = {};

  @override
  Future<String?> read({
    required String key,
    AppleOptions? iOptions,
    AndroidOptions? aOptions,
    LinuxOptions? lOptions,
    WebOptions? webOptions,
    AppleOptions? mOptions,
    WindowsOptions? wOptions,
  }) async => _data[key];

  @override
  Future<void> write({
    required String key,
    required String? value,
    AppleOptions? iOptions,
    AndroidOptions? aOptions,
    LinuxOptions? lOptions,
    WebOptions? webOptions,
    AppleOptions? mOptions,
    WindowsOptions? wOptions,
  }) async {
    if (value != null) {
      _data[key] = value;
    } else {
      _data.remove(key);
    }
  }

  @override
  Future<void> delete({
    required String key,
    AppleOptions? iOptions,
    AndroidOptions? aOptions,
    LinuxOptions? lOptions,
    WebOptions? webOptions,
    AppleOptions? mOptions,
    WindowsOptions? wOptions,
  }) async => _data.remove(key);

  @override
  Future<Map<String, String>> readAll({
    AppleOptions? iOptions,
    AndroidOptions? aOptions,
    LinuxOptions? lOptions,
    WebOptions? webOptions,
    AppleOptions? mOptions,
    WindowsOptions? wOptions,
  }) async => Map.from(_data);

  @override
  Future<void> deleteAll({
    AppleOptions? iOptions,
    AndroidOptions? aOptions,
    LinuxOptions? lOptions,
    WebOptions? webOptions,
    AppleOptions? mOptions,
    WindowsOptions? wOptions,
  }) async => _data.clear();

  @override
  Future<bool> containsKey({
    required String key,
    AppleOptions? iOptions,
    AndroidOptions? aOptions,
    LinuxOptions? lOptions,
    WebOptions? webOptions,
    AppleOptions? mOptions,
    WindowsOptions? wOptions,
  }) async => _data.containsKey(key);

  // Convenience for setting up test data.
  String? operator [](String key) => _data[key];
  void operator []=(String key, String value) => _data[key] = value;
}

void main() {
  late FakeSecureStorage fakeSecure;
  late CommunityStorage storage;

  setUp(() {
    fakeSecure = FakeSecureStorage();
    storage = CommunityStorage(secure: fakeSecure);
  });

  group('CommunityStorage', () {
    test('loadAll returns empty list when no data', () async {
      final result = await storage.loadAll();
      expect(result, isEmpty);
    });

    test('save and loadAll round-trips a community', () async {
      final ws = Community.create(
        name: 'Test',
        relayUrl: 'https://relay.example.com',
        pubkey: 'abc123',
      );

      await storage.save(ws);
      final loaded = await storage.loadAll();

      expect(loaded, hasLength(1));
      expect(loaded.first.id, ws.id);
      expect(loaded.first.name, 'Test');
      expect(loaded.first.relayUrl, 'https://relay.example.com');
      expect(loaded.first.pubkey, 'abc123');
    });

    test('persists validated NIP-17 and FIPS preferences', () async {
      const gatewayPubkey =
          '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef';
      final community = Community.create(
        name: 'Private',
        relayUrl: 'https://relay.example.com',
        relayTransport: RelayTransportMode.nip17,
        nip17GatewayPubkey: gatewayPubkey,
        nip17PublicRelayUrls: const [
          'wss://relay-one.example',
          'wss://relay-two.example',
        ],
        preferFips: true,
      );

      await storage.save(community);
      final loaded = (await storage.loadAll()).single;

      expect(loaded.relayTransport, RelayTransportMode.nip17);
      expect(loaded.nip17GatewayPubkey, gatewayPubkey);
      expect(loaded.nip17PublicRelayUrls, community.nip17PublicRelayUrls);
      expect(loaded.preferFips, isTrue);
    });

    test(
      'falls back to direct transport for malformed stored NIP-17 config',
      () async {
        fakeSecure['buzz_communities'] = jsonEncode([
          {
            'id': 'community-1',
            'name': 'Malformed',
            'relayUrl': 'https://relay.example.com',
            'relayTransport': 'nip17',
            'nip17GatewayPubkey': 'not-a-pubkey',
            'nip17PublicRelayUrls': ['https://not-a-websocket.example'],
            'preferFips': true,
            'addedAt': DateTime.utc(2026).toIso8601String(),
          },
        ]);

        final loaded = (await storage.loadAll()).single;

        expect(loaded.relayTransport, RelayTransportMode.direct);
        expect(loaded.nip17GatewayPubkey, isNull);
        expect(loaded.nip17PublicRelayUrls, isEmpty);
        expect(loaded.preferFips, isTrue);
      },
    );

    test('rejects incomplete programmatic NIP-17 configuration', () {
      expect(
        () => Community.create(
          name: 'Invalid',
          relayUrl: 'https://relay.example.com',
          relayTransport: RelayTransportMode.nip17,
        ),
        throwsArgumentError,
      );
    });

    test('save updates existing community with same id', () async {
      final ws = Community.create(
        name: 'Original',
        relayUrl: 'https://relay.example.com',
      );

      await storage.save(ws);
      await storage.save(ws.copyWith(name: 'Updated'));

      final loaded = await storage.loadAll();
      expect(loaded, hasLength(1));
      expect(loaded.first.name, 'Updated');
    });

    test('remove deletes a community', () async {
      final ws1 = Community.create(
        name: 'One',
        relayUrl: 'https://one.example.com',
      );
      final ws2 = Community.create(
        name: 'Two',
        relayUrl: 'https://two.example.com',
      );

      await storage.save(ws1);
      await storage.save(ws2);
      await storage.remove(ws1.id);

      final loaded = await storage.loadAll();
      expect(loaded, hasLength(1));
      expect(loaded.first.id, ws2.id);
    });

    test('active community ID persists', () async {
      await storage.saveActiveId('ws-123');
      final id = await storage.loadActiveId();
      expect(id, 'ws-123');
    });

    test('clearActiveId removes active ID', () async {
      await storage.saveActiveId('ws-123');
      await storage.clearActiveId();
      final id = await storage.loadActiveId();
      expect(id, isNull);
    });

    group('migration', () {
      test('migrates saved workspaces to communities', () async {
        final legacy = Community.create(
          name: 'Legacy',
          relayUrl: 'https://legacy.example.com',
        );
        fakeSecure['buzz_workspaces'] = jsonEncode([legacy.toJson()]);
        fakeSecure['buzz_active_workspace_id'] = legacy.id;

        final loaded = await storage.loadAll();

        expect(loaded.single.id, legacy.id);
        expect(await storage.loadActiveId(), legacy.id);
        expect(fakeSecure['buzz_communities'], isNotNull);
        expect(fakeSecure['buzz_workspaces'], isNull);
        expect(fakeSecure['buzz_active_workspace_id'], isNull);
      });

      test('migrates legacy keys to community on first load', () async {
        fakeSecure['buzz_relay_url'] = 'https://legacy.example.com';
        fakeSecure['buzz_token'] = 'legacy_token';
        fakeSecure['buzz_pubkey'] = 'legacy_pub';
        fakeSecure['buzz_nsec'] = 'legacy_nsec';

        final loaded = await storage.loadAll();

        expect(loaded, hasLength(1));
        expect(loaded.first.relayUrl, 'https://legacy.example.com');
        expect(loaded.first.pubkey, 'legacy_pub');
        expect(loaded.first.nsec, 'legacy_nsec');
        expect(loaded.first.name, isNotEmpty);

        // Legacy keys should be deleted.
        expect(fakeSecure['buzz_relay_url'], isNull);
        expect(fakeSecure['buzz_token'], isNull);
        expect(fakeSecure['buzz_pubkey'], isNull);
        expect(fakeSecure['buzz_nsec'], isNull);

        // Active ID should be set.
        final activeId = await storage.loadActiveId();
        expect(activeId, loaded.first.id);
      });

      test('does not migrate when no legacy keys exist', () async {
        final loaded = await storage.loadAll();
        expect(loaded, isEmpty);
      });

      test('does not re-migrate after first load', () async {
        fakeSecure['buzz_relay_url'] = 'https://legacy.example.com';
        fakeSecure['buzz_token'] = 'legacy_token';

        final first = await storage.loadAll();
        expect(first, hasLength(1));

        final second = await storage.loadAll();
        expect(second, hasLength(1));
        expect(second.first.id, first.first.id);
      });

      test('migration generates name from localhost URL', () async {
        fakeSecure['buzz_relay_url'] = 'http://localhost:3000';
        fakeSecure['buzz_token'] = 'tok';

        final loaded = await storage.loadAll();
        expect(loaded.first.name, 'Local Dev');
      });
    });
  });
}
