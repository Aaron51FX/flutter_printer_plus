import 'dart:async';
import 'dart:collection';

import '../io/net_conn.dart';
import '../tools/log_tool.dart';

/// Coordinates print jobs for multiple network printers identified by IP.
///
/// The controller lazily creates a dedicated worker (and socket connection)
/// per printer address. Each worker serializes its own queue, ensuring only
/// one socket connection exists for that printer at a time. Connections are
/// closed automatically once the printer queue becomes idle.
class PrinterJobController {
  PrinterJobController({
    this.maxRetriesPerJob = 3,
    this.retryDelay = const Duration(seconds: 1),
    this.idleDisconnectDelay = const Duration(seconds: 5),
  }) : assert(maxRetriesPerJob > 0, 'maxRetriesPerJob must be > 0');

  final int maxRetriesPerJob;
  final Duration retryDelay;
  final Duration idleDisconnectDelay;

  final Map<String, _PrinterWorker> _workers = <String, _PrinterWorker>{};
  bool _disposed = false;

  /// Enqueue a print job for the printer at [address].
  ///
  /// Returns the total bytes written once the job finishes.
  Future<int> enqueue(
    String address,
    List<List<int>> payload, {
    Duration? timeout,
  }) {
    if (_disposed) {
      return Future.error(
        StateError('PrinterJobController has been disposed'),
      );
    }
    final worker = _workers.putIfAbsent(
      address,
      () => _PrinterWorker(
        address: address,
        maxRetriesPerJob: maxRetriesPerJob,
        retryDelay: retryDelay,
        idleDisconnectDelay: idleDisconnectDelay,
        onFullyIdle: () => _cleanupWorker(address),
      ),
    );
    return worker.enqueue(payload, timeout: timeout);
  }

  /// Dispose all workers and release resources.
  Future<void> dispose() async {
    if (_disposed) {
      return;
    }
    _disposed = true;
    final disposes = _workers.values.map((worker) => worker.dispose());
    await Future.wait(disposes, eagerError: false);
    _workers.clear();
  }

  /// Dispose a specific printer worker when it is no longer needed.
  Future<void> disposePrinter(String address) async {
    final worker = _workers.remove(address);
    if (worker != null) {
      await worker.dispose();
    }
  }

  void _cleanupWorker(String address) {
    final worker = _workers[address];
    if (worker == null) {
      return;
    }
    if (worker.isCompletelyIdle) {
      _workers.remove(address);
    }
  }
}

class _PrinterWorker {
  _PrinterWorker({
    required this.address,
    required this.maxRetriesPerJob,
    required this.retryDelay,
    required this.idleDisconnectDelay,
    required this.onFullyIdle,
  })  : connection = NetConn(address),
        assert(maxRetriesPerJob > 0);

  final String address;
  final int maxRetriesPerJob;
  final Duration retryDelay;
  final Duration idleDisconnectDelay;
  final VoidCallback onFullyIdle;
  final NetConn connection;

  final Queue<_QueuedJob> _jobQueue = Queue<_QueuedJob>();
  bool _processing = false;
  bool _disposed = false;
  Timer? _idleTimer;
  Future<void>? _disconnecting;

  bool get isCompletelyIdle =>
      !_processing && _jobQueue.isEmpty && !_disposed && _idleTimer == null;

  Future<int> enqueue(List<List<int>> payload, {Duration? timeout}) {
    if (_disposed) {
      return Future.error(
        StateError('Worker for $address has been disposed'),
      );
    }
    final job = _QueuedJob(payload, Completer<int>(), timeout);
    _jobQueue.add(job);
    _startProcessing();
    return job.completer.future;
  }

  Future<void> dispose() async {
    if (_disposed) {
      return;
    }
    _disposed = true;
    _cancelIdleTimer();
    while (_jobQueue.isNotEmpty) {
      final job = _jobQueue.removeFirst();
      if (!job.completer.isCompleted) {
        job.completer.completeError(
          StateError('Worker disposed for $address'),
        );
      }
    }
    await _safeDisconnect();
  }

  void _startProcessing() {
    if (_processing || _disposed) {
      return;
    }
    _cancelIdleTimer();
    _processing = true;
    Future.microtask(() async {
      try {
        await _processQueue();
      } finally {
        _processing = false;
      }
      if (_disposed) {
        return;
      }
      if (_jobQueue.isNotEmpty) {
        _startProcessing();
      } else {
        _scheduleIdleDisconnect();
        onFullyIdle();
      }
    });
  }

  Future<void> _processQueue() async {
    while (!_disposed && _jobQueue.isNotEmpty) {
      final job = _jobQueue.removeFirst();
      try {
        await _ensureConnected();
        final sendFuture = _sendWithRetry(job.payload);
        final written = job.timeout == null
            ? await sendFuture
            : await sendFuture.timeout(
                job.timeout!,
                onTimeout: () => throw TimeoutException(
                  'Print job timed out after ${job.timeout!.inMilliseconds}ms',
                ),
              );
        job.completer.complete(written);
      } catch (e, st) {
        job.completer.completeError(e, st);
        await _safeDisconnect();
      }
    }
  }

  Future<void> _ensureConnected() async {
    final disconnecting = _disconnecting;
    if (disconnecting != null) {
      await disconnecting;
    }
    if (connection.connected) {
      return;
    }
    await connection.connect(throwError: true);
  }

  Future<int> _sendWithRetry(List<List<int>> data) async {
    int attempt = 0;
    while (true) {
      attempt++;
      try {
        return await connection.writeMultiBytes(
          data,
          isDisconnect: false,
        );
      } catch (e) {
        LogTool.log(
          'Printer $address send attempt $attempt failed: ${e.toString()}',
        );
        if (attempt >= maxRetriesPerJob) {
          rethrow;
        }
        await _safeDisconnect();
        await Future.delayed(_delayForAttempt(attempt));
        await _ensureConnected();
      }
    }
  }

  Duration _delayForAttempt(int attempt) {
    if (retryDelay.isNegative || retryDelay == Duration.zero) {
      return Duration.zero;
    }
    return Duration(milliseconds: retryDelay.inMilliseconds * attempt);
  }

  void _scheduleIdleDisconnect() {
    if (idleDisconnectDelay <= Duration.zero) {
      _disconnectLater();
      return;
    }
    _idleTimer = Timer(idleDisconnectDelay, _disconnectLater);
  }

  void _cancelIdleTimer() {
    _idleTimer?.cancel();
    _idleTimer = null;
  }

  void _disconnectLater() {
    _cancelIdleTimer();
    Future.microtask(() async {
      // Another job may arrive around the same time; the processing path will
      // await this future before reconnecting.
      _disconnecting ??= _safeDisconnect();
      try {
        await _disconnecting;
      } finally {
        _disconnecting = null;
      }
      if (_jobQueue.isEmpty && !_disposed) {
        onFullyIdle();
      }
    });
  }

  Future<void> _safeDisconnect() async {
    if (!connection.connected) {
      return;
    }
    try {
      await connection.disconnect();
    } catch (e) {
      LogTool.log('Printer $address disconnect error: ${e.toString()}');
    }
  }
}

typedef VoidCallback = void Function();

class _QueuedJob {
  _QueuedJob(this.payload, this.completer, this.timeout);

  final List<List<int>> payload;
  final Completer<int> completer;
  final Duration? timeout;
}
