import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:flutter_printer_plus/src/io/net_conn.dart';

class _SocketProbe implements Socket {
  final input = StreamController<Uint8List>();
  final bytes = <int>[];
  bool failWrite = false;
  bool destroyed = false;
  int flushCount = 0;

  @override
  int get port => 12345;

  @override
  StreamSubscription<Uint8List> listen(
    void Function(Uint8List)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) =>
      input.stream.listen(onData,
          onError: onError, onDone: onDone, cancelOnError: cancelOnError);

  @override
  void add(List<int> data) {
    if (failWrite) throw StateError('Injected write failure');
    bytes.addAll(data);
  }

  @override
  Future<void> flush() async {
    flushCount++;
  }

  @override
  void destroy() {
    destroyed = true;
    input.close();
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

void main() {
  test('write failure destroys old socket before a subsequent connection',
      () async {
    final first = _SocketProbe()..failWrite = true;
    final second = _SocketProbe();
    var attempts = 0;
    await IOOverrides.runZoned(() async {
      final conn = NetConn('printer');
      await conn.connect(throwError: true);
      await expectLater(
        conn.writeMultiBytes([
          [1, 2]
        ], isDisconnect: false),
        throwsException,
      );
      expect(conn.connected, isFalse);
      expect(first.destroyed, isTrue);
      final count = await conn.writeMultiBytes([
        [3, 4],
        [5]
      ], isDisconnect: false);
      expect(count, 3);
      expect(second.bytes, [3, 4, 5]);
      expect(second.flushCount, 1);
      expect(attempts, 2);
      second.destroy();
    }, socketConnect: (host, port,
        {sourceAddress, sourcePort = 0, timeout}) async {
      return attempts++ == 0 ? first : second;
    });
  });
}
