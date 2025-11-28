import 'dart:async';
import 'dart:collection';

import '../io/net_conn.dart';
import '../tools/log_tool.dart';

/// A single-printer job controller that serializes print requests, ensures
/// only one socket session is active at a time, and disconnects once the
/// queue drains.
class PrinterJobController {
  PrinterJobController({
    required this.connection,
    this.maxRetriesPerJob = 3,
    this.retryDelay = const Duration(seconds: 1),
    this.idleDisconnectDelay = const Duration(seconds: 5),
  }) : assert(maxRetriesPerJob > 0, 'maxRetriesPerJob must be > 0');

  final NetConn connection;
  final int maxRetriesPerJob;
  final Duration retryDelay;
  final Duration idleDisconnectDelay;

  final Queue<_QueuedJob> _jobQueue = Queue<_QueuedJob>();
  bool _processing = false;
  bool _disposed = false;
  Timer? _idleTimer;

  /// Push a print job into the queue. The returned [Future] completes with
  /// the total byte length written to the socket.
  Future<int> enqueue(List<List<int>> payload, {Duration? timeout}) {
    if (_disposed) {
      return Future.error(
        StateError('PrinterJobController has been disposed'),
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
          StateError('PrinterJobController disposed'),
        );
      }
    }
    if (connection.connected) {
      await _safeDisconnect();
    }
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
        if (_disposed) {
          return;
        }
        if (_jobQueue.isNotEmpty) {
          _startProcessing();
        } else {
          _scheduleIdleDisconnect();
        }
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
          'PrinterJobController send attempt $attempt failed: ${e.toString()}',
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
      await _safeDisconnect();
    });
  }

  Future<void> _safeDisconnect() async {
    if (!connection.connected) {
      return;
    }
    try {
      await connection.disconnect();
    } catch (e) {
      LogTool.log('PrinterJobController disconnect error: ${e.toString()}');
    }
  }
}

class _QueuedJob {
  _QueuedJob(this.payload, this.completer, this.timeout);

  final List<List<int>> payload;
  final Completer<int> completer;
  final Duration? timeout;
}
