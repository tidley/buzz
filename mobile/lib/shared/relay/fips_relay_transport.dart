import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'relay_transport.dart';

/// Return codes exposed by the optional `buzz-fips-mobile` C ABI.
enum FipsBridgeStatus {
  stopped(0),
  running(1),
  connected(2),
  invalidInput(3),
  notConnected(4),
  bufferTooSmall(5),
  failed(6);

  const FipsBridgeStatus(this.code);

  final int code;

  static FipsBridgeStatus fromCode(int code) =>
      values.firstWhere((status) => status.code == code, orElse: () => failed);
}

/// A frame or status returned by [FipsBridge.receive].
class FipsReceiveResult {
  const FipsReceiveResult.frame(this.frame)
    : status = FipsBridgeStatus.connected,
      requiredCapacity = null;

  const FipsReceiveResult.bufferTooSmall(int length)
    : frame = null,
      status = FipsBridgeStatus.bufferTooSmall,
      requiredCapacity = length;

  const FipsReceiveResult.status(this.status)
    : frame = null,
      requiredCapacity = null;

  final Uint8List? frame;
  final FipsBridgeStatus status;
  final int? requiredCapacity;
}

/// Dart-facing operations provided by the optional FIPS native bridge.
abstract interface class FipsBridge {
  /// Starts the app-local FIPS session.
  FipsBridgeStatus start();

  /// Connects to a peer identified by its Nostr `npub`.
  FipsBridgeStatus connect(String peer);

  /// Sends one UTF-8 relay frame.
  FipsBridgeStatus send(Uint8List frame);

  /// Waits for one frame without blocking Flutter's UI isolate.
  Future<FipsReceiveResult> receive(int capacity);

  /// Stops the app-local FIPS session.
  FipsBridgeStatus stop();
}

/// Creates an FIPS bridge. Exposed to make factory selection testable.
typedef FipsBridgeFactory = FipsBridge Function();

/// Indicates that the optional Android native library is not packaged.
class FipsBridgeUnavailableException implements Exception {
  const FipsBridgeUnavailableException(this.cause);

  final Object cause;

  @override
  String toString() => 'FIPS bridge is unavailable: $cause';
}

/// Describes a failed FIPS bridge operation.
class FipsBridgeException implements Exception {
  const FipsBridgeException(this.operation, this.status);

  final String operation;
  final FipsBridgeStatus status;

  @override
  String toString() => 'FIPS $operation failed: ${status.name}';
}

/// FFI implementation of [FipsBridge].
class FfiFipsBridge implements FipsBridge {
  FfiFipsBridge._(DynamicLibrary library)
    : _start = library.lookupFunction<_StatusNative, _StatusDart>(
        'buzz_fips_mobile_start',
      ),
      _connect = library.lookupFunction<_BytesNative, _BytesDart>(
        'buzz_fips_mobile_connect',
      ),
      _send = library.lookupFunction<_BytesNative, _BytesDart>(
        'buzz_fips_mobile_send',
      ),
      _stop = library.lookupFunction<_StatusNative, _StatusDart>(
        'buzz_fips_mobile_stop',
      );

  final _StatusDart _start;
  final _BytesDart _connect;
  final _BytesDart _send;
  final _StatusDart _stop;

  /// Loads the optional `libbuzz_fips_mobile.so` Android library.
  factory FfiFipsBridge.load() {
    try {
      return FfiFipsBridge._(DynamicLibrary.open('libbuzz_fips_mobile.so'));
    } on Object catch (error) {
      throw FipsBridgeUnavailableException(error);
    }
  }

  @override
  FipsBridgeStatus start() => FipsBridgeStatus.fromCode(_start());

  @override
  FipsBridgeStatus connect(String peer) => _withUtf8(
    peer,
    (bytes, length) => FipsBridgeStatus.fromCode(_connect(bytes, length)),
  );

  @override
  FipsBridgeStatus send(Uint8List frame) => _withBytes(
    frame,
    (bytes, length) => FipsBridgeStatus.fromCode(_send(bytes, length)),
  );

  @override
  Future<FipsReceiveResult> receive(int capacity) =>
      Isolate.run(() => _receiveFromNative(capacity));

  @override
  FipsBridgeStatus stop() => FipsBridgeStatus.fromCode(_stop());
}

/// A relay transport over the optional Android FIPS QUIC bridge.
///
/// The relay URI must include a `fipsPeer=<peer-npub>` query parameter. The
/// FIPS native library is loaded only on Android and is optional in Android
/// packages.
class FipsRelayTransport implements RelayTransport {
  FipsRelayTransport(
    this._uri, {
    required FipsBridge bridge,
    bool Function()? isAndroid,
    this.pollInterval = const Duration(milliseconds: 10),
  }) : _bridge = bridge,
       _isAndroid = isAndroid ?? _platformIsAndroid {
    ready = _connect();
  }

  static const _initialReceiveCapacity = 64 * 1024;

  final Uri _uri;
  final FipsBridge _bridge;
  final bool Function() _isAndroid;
  final Duration pollInterval;
  final StreamController<dynamic> _frames = StreamController.broadcast();
  bool _closed = false;
  bool _polling = false;

  @override
  late final Future<void> ready;

  /// Returns a factory when FIPS is usable; otherwise returns `null` for the
  /// caller to fall back to its normal WebSocket transport.
  static RelayTransportFactory? configuredIfAvailable({
    FipsBridgeFactory bridgeFactory = FfiFipsBridge.load,
    bool Function()? isAndroid,
  }) {
    final platformIsAndroid = isAndroid ?? _platformIsAndroid;
    if (!platformIsAndroid()) return null;
    try {
      final bridge = bridgeFactory();
      return (uri) =>
          FipsRelayTransport(uri, bridge: bridge, isAndroid: platformIsAndroid);
    } on FipsBridgeUnavailableException {
      return null;
    }
  }

  @override
  Stream<dynamic> get stream {
    _frames.onListen = _startPolling;
    return _frames.stream;
  }

  @override
  Future<void> activate() async {}

  @override
  void send(String message) {
    _requireConnected(
      'send',
      _bridge.send(Uint8List.fromList(utf8.encode(message))),
    );
  }

  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    _bridge.stop();
    await _frames.close();
  }

  Future<void> _connect() async {
    if (!_isAndroid()) {
      throw UnsupportedError(
        'FIPS relay transport is available only on Android',
      );
    }
    final peer = _uri.queryParameters['fipsPeer'];
    if (peer == null || peer.isEmpty) {
      throw ArgumentError.value(
        _uri,
        'uri',
        'FIPS relay URLs must include fipsPeer=<peer-npub>',
      );
    }
    final start = _bridge.start();
    if (start != FipsBridgeStatus.running &&
        start != FipsBridgeStatus.connected) {
      throw FipsBridgeException('start', start);
    }
    _requireConnected('connect', _bridge.connect(peer));
  }

  void _startPolling() {
    if (_polling || _closed) return;
    _polling = true;
    unawaited(_poll());
  }

  Future<void> _poll() async {
    while (!_closed) {
      try {
        var capacity = _initialReceiveCapacity;
        FipsReceiveResult result;
        do {
          result = await _bridge.receive(capacity);
          capacity = result.requiredCapacity ?? capacity;
        } while (result.status == FipsBridgeStatus.bufferTooSmall && !_closed);
        if (_closed) return;
        if (result.status != FipsBridgeStatus.connected ||
            result.frame == null) {
          throw FipsBridgeException('receive', result.status);
        }
        _frames.add(utf8.decode(result.frame!));
        if (pollInterval > Duration.zero) {
          await Future<void>.delayed(pollInterval);
        }
      } on Object catch (error, stackTrace) {
        if (!_closed) _frames.addError(error, stackTrace);
        await close();
      }
    }
  }

  void _requireConnected(String operation, FipsBridgeStatus status) {
    if (status != FipsBridgeStatus.connected) {
      throw FipsBridgeException(operation, status);
    }
  }
}

bool _platformIsAndroid() => Platform.isAndroid;

typedef _StatusNative = Uint32 Function();
typedef _StatusDart = int Function();
typedef _BytesNative = Uint32 Function(Pointer<Uint8>, IntPtr);
typedef _BytesDart = int Function(Pointer<Uint8>, int);
typedef _ReceiveNative =
    Uint32 Function(Pointer<Uint8>, IntPtr, Pointer<IntPtr>);
typedef _ReceiveDart = int Function(Pointer<Uint8>, int, Pointer<IntPtr>);

FipsBridgeStatus _withUtf8(
  String value,
  FipsBridgeStatus Function(Pointer<Uint8> bytes, int length) action,
) => _withBytes(Uint8List.fromList(utf8.encode(value)), action);

FipsBridgeStatus _withBytes(
  Uint8List value,
  FipsBridgeStatus Function(Pointer<Uint8> bytes, int length) action,
) {
  final bytes = calloc<Uint8>(value.length);
  try {
    bytes.asTypedList(value.length).setAll(0, value);
    return action(bytes, value.length);
  } finally {
    calloc.free(bytes);
  }
}

FipsReceiveResult _receiveFromNative(int capacity) {
  final library = DynamicLibrary.open('libbuzz_fips_mobile.so');
  final receive = library.lookupFunction<_ReceiveNative, _ReceiveDart>(
    'buzz_fips_mobile_receive',
  );
  final frame = calloc<Uint8>(capacity);
  final outLength = calloc<IntPtr>();
  try {
    final status = FipsBridgeStatus.fromCode(
      receive(frame, capacity, outLength),
    );
    final length = outLength.value;
    if (status == FipsBridgeStatus.bufferTooSmall) {
      return FipsReceiveResult.bufferTooSmall(length);
    }
    if (status != FipsBridgeStatus.connected) {
      return FipsReceiveResult.status(status);
    }
    return FipsReceiveResult.frame(
      Uint8List.fromList(frame.asTypedList(length)),
    );
  } finally {
    calloc.free(outLength);
    calloc.free(frame);
  }
}
