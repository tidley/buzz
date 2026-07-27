import 'package:uuid/uuid.dart';

const _uuid = Uuid();
const _sentinel = Object();

enum RelayTransportMode {
  direct('direct'),
  nip17('nip17');

  const RelayTransportMode(this.storageValue);

  final String storageValue;

  static RelayTransportMode? fromStorageValue(Object? value) {
    for (final mode in values) {
      if (value == mode.storageValue) return mode;
    }
    return null;
  }
}

class Community {
  final String id;
  final String name;
  final String relayUrl;
  final String? pubkey;
  final String? nsec;
  final RelayTransportMode relayTransport;
  final String? nip17GatewayPubkey;
  final List<String> nip17PublicRelayUrls;
  final bool preferFips;
  final DateTime addedAt;

  Community({
    required this.id,
    required this.name,
    required this.relayUrl,
    this.pubkey,
    this.nsec,
    this.relayTransport = RelayTransportMode.direct,
    this.nip17GatewayPubkey,
    List<String> nip17PublicRelayUrls = const [],
    this.preferFips = false,
    required this.addedAt,
  }) : nip17PublicRelayUrls = List.unmodifiable(nip17PublicRelayUrls) {
    _validateTransportConfiguration(
      relayTransport: relayTransport,
      nip17GatewayPubkey: nip17GatewayPubkey,
      nip17PublicRelayUrls: nip17PublicRelayUrls,
    );
  }

  factory Community.create({
    required String name,
    required String relayUrl,
    String? pubkey,
    String? nsec,
    RelayTransportMode relayTransport = RelayTransportMode.direct,
    String? nip17GatewayPubkey,
    List<String> nip17PublicRelayUrls = const [],
    bool preferFips = false,
  }) {
    return Community(
      id: _uuid.v4(),
      name: name,
      relayUrl: relayUrl,
      pubkey: pubkey,
      nsec: nsec,
      relayTransport: relayTransport,
      nip17GatewayPubkey: nip17GatewayPubkey,
      nip17PublicRelayUrls: nip17PublicRelayUrls,
      preferFips: preferFips,
      addedAt: DateTime.now(),
    );
  }

  Community copyWith({
    String? name,
    String? relayUrl,
    Object? pubkey = _sentinel,
    Object? nsec = _sentinel,
    RelayTransportMode? relayTransport,
    Object? nip17GatewayPubkey = _sentinel,
    List<String>? nip17PublicRelayUrls,
    bool? preferFips,
  }) {
    return Community(
      id: id,
      name: name ?? this.name,
      relayUrl: relayUrl ?? this.relayUrl,
      pubkey: pubkey == _sentinel ? this.pubkey : pubkey as String?,
      nsec: nsec == _sentinel ? this.nsec : nsec as String?,
      relayTransport: relayTransport ?? this.relayTransport,
      nip17GatewayPubkey: nip17GatewayPubkey == _sentinel
          ? this.nip17GatewayPubkey
          : nip17GatewayPubkey as String?,
      nip17PublicRelayUrls: nip17PublicRelayUrls ?? this.nip17PublicRelayUrls,
      preferFips: preferFips ?? this.preferFips,
      addedAt: addedAt,
    );
  }

  Map<String, dynamic> toJson() => {
    'id': id,
    'name': name,
    'relayUrl': relayUrl,
    if (pubkey != null) 'pubkey': pubkey,
    if (nsec != null) 'nsec': nsec,
    'relayTransport': relayTransport.storageValue,
    if (nip17GatewayPubkey != null) 'nip17GatewayPubkey': nip17GatewayPubkey,
    if (nip17PublicRelayUrls.isNotEmpty)
      'nip17PublicRelayUrls': nip17PublicRelayUrls,
    if (preferFips) 'preferFips': true,
    'addedAt': addedAt.toIso8601String(),
  };

  factory Community.fromJson(Map<String, dynamic> json) {
    final transport = RelayTransportMode.fromStorageValue(
      json['relayTransport'],
    );
    final gatewayPubkey = json['nip17GatewayPubkey'];
    final publicRelayUrls = json['nip17PublicRelayUrls'];
    final validNip17 =
        transport == RelayTransportMode.nip17 &&
        gatewayPubkey is String &&
        publicRelayUrls is List &&
        publicRelayUrls.every((url) => url is String) &&
        _isValidNip17Configuration(
          gatewayPubkey,
          publicRelayUrls.cast<String>(),
        );

    // Existing installations and malformed persisted values use the safe,
    // direct path instead of making the community impossible to load.
    return Community(
      id: json['id'] as String,
      name: json['name'] as String,
      relayUrl: json['relayUrl'] as String,
      pubkey: json['pubkey'] as String?,
      nsec: json['nsec'] as String?,
      relayTransport: validNip17
          ? RelayTransportMode.nip17
          : RelayTransportMode.direct,
      nip17GatewayPubkey: validNip17 ? gatewayPubkey : null,
      nip17PublicRelayUrls: validNip17 ? publicRelayUrls.cast<String>() : [],
      preferFips: json['preferFips'] == true,
      addedAt: DateTime.parse(json['addedAt'] as String),
    );
  }

  static void _validateTransportConfiguration({
    required RelayTransportMode relayTransport,
    required String? nip17GatewayPubkey,
    required List<String> nip17PublicRelayUrls,
  }) {
    if (relayTransport == RelayTransportMode.direct) return;
    if (!_isValidNip17Configuration(nip17GatewayPubkey, nip17PublicRelayUrls)) {
      throw ArgumentError(
        'NIP-17 requires a 32-byte hexadecimal gateway pubkey and at least '
        'one ws:// or wss:// public relay URL.',
      );
    }
  }

  static bool _isValidNip17Configuration(
    String? gatewayPubkey,
    List<String> publicRelayUrls,
  ) {
    if (gatewayPubkey == null ||
        !RegExp(r'^[0-9a-fA-F]{64}$').hasMatch(gatewayPubkey) ||
        publicRelayUrls.isEmpty) {
      return false;
    }
    return publicRelayUrls.every((url) {
      final uri = Uri.tryParse(url);
      return uri != null &&
          uri.host.isNotEmpty &&
          (uri.scheme == 'ws' || uri.scheme == 'wss');
    });
  }

  /// Derive a human-friendly community name from a relay URL.
  static String nameFromUrl(String url) {
    try {
      final host = Uri.parse(url).host;
      if (host.contains('localhost') || host == '127.0.0.1') return 'Local Dev';
      final parts = host.split('.');
      if (parts.length > 2) return parts.first;
      return host;
    } catch (_) {
      return 'Community';
    }
  }
}
