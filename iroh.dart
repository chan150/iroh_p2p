import 'dart:async';
import 'dart:io';
import 'dart:math';

/// Iroh P2P 연결 상태 및 이벤트 모델
sealed class IrohEvent {}

/// 원격 피어와 연결 수립됨
class IrohConnectedEvent extends IrohEvent {
  final String remoteNodeId;
  final String pathType; // e.g., "Direct P2P" or "Relay"
  final String rawDetails;

  IrohConnectedEvent({
    required this.remoteNodeId,
    required this.pathType,
    required this.rawDetails,
  });

  bool get isDirect => pathType.contains('Direct') || rawDetails.contains('ip:');
  bool get isRelay => pathType.contains('Relay') || rawDetails.contains('relay:');

  @override
  String toString() =>
      'IrohConnectedEvent(remoteNodeId: $remoteNodeId, path: $pathType)';
}

/// 원격 피어로부터 텍스트 메시지 수신
class IrohMessageReceivedEvent extends IrohEvent {
  final String message;
  final DateTime timestamp;

  IrohMessageReceivedEvent({
    required this.message,
    DateTime? timestamp,
  }) : timestamp = timestamp ?? DateTime.now();

  @override
  String toString() => 'IrohMessageReceivedEvent(message: $message)';
}

/// Ping RTT(지연시간) 단일 측정 결과
class IrohPingResultEvent extends IrohEvent {
  final int seq;
  final int total;
  final int rttMs;

  IrohPingResultEvent({
    required this.seq,
    required this.total,
    required this.rttMs,
  });

  @override
  String toString() => 'IrohPingResultEvent(seq: $seq/$total, rtt: ${rttMs}ms)';
}

/// 히스토그램 구간 데이터
class IrohLatencyBucket {
  final int startMs;
  final int endMs;
  final int count;
  final double percentage;

  IrohLatencyBucket({
    required this.startMs,
    required this.endMs,
    required this.count,
    required this.percentage,
  });
}

/// Ping 다중 측정 완료 레이턴시 분포(Distribution) 리포트
class IrohPingDistributionEvent extends IrohEvent {
  final int sent;
  final int received;
  final double lossRate;
  final int minMs;
  final int maxMs;
  final double avgMs;
  final double stdDevMs;
  final double jitterMs;
  final int p50Ms; // 중앙값
  final int p90Ms;
  final int p95Ms;
  final int p99Ms;
  final List<IrohLatencyBucket> buckets;

  IrohPingDistributionEvent({
    required this.sent,
    required this.received,
    required this.lossRate,
    required this.minMs,
    required this.maxMs,
    required this.avgMs,
    required this.stdDevMs,
    required this.jitterMs,
    required this.p50Ms,
    required this.p90Ms,
    required this.p95Ms,
    required this.p99Ms,
    required this.buckets,
  });

  @override
  String toString() =>
      'IrohPingDistributionEvent(total: $sent, min: ${minMs}ms, max: ${maxMs}ms, avg: ${avgMs.toStringAsFixed(1)}ms, p50: ${p50Ms}ms, p95: ${p95Ms}ms, jitter: ${jitterMs.toStringAsFixed(1)}ms)';
}

/// 대역폭(Bandwidth) 벤치마크 결과
class IrohBenchReportEvent extends IrohEvent {
  final double megabytes;
  final double seconds;
  final double speedMbs;
  final double speedMbps;
  final bool isSender; // true: 송신 측, false: 수신 측

  IrohBenchReportEvent({
    required this.megabytes,
    required this.seconds,
    required this.speedMbs,
    required this.isSender,
  }) : speedMbps = speedMbs * 8.0;

  @override
  String toString() =>
      'IrohBenchReportEvent(${isSender ? "송신" : "수신"}: ${megabytes.toStringAsFixed(2)}MB in ${seconds.toStringAsFixed(2)}s -> ${speedMbs.toStringAsFixed(2)} MB/s (${speedMbps.toStringAsFixed(2)} Mbps))';
}

/// 파일 전송 진행률 이벤트
class IrohFileProgressEvent extends IrohEvent {
  final String fileName;
  final int currentBytes;
  final int totalBytes;
  final double percentage;
  final double speedMbs;
  final bool isSending; // true: 내가 전송 중, false: 상대방 파일 수신 중

  IrohFileProgressEvent({
    required this.fileName,
    required this.currentBytes,
    required this.totalBytes,
    required this.speedMbs,
    required this.isSending,
  }) : percentage = totalBytes > 0 ? (currentBytes / totalBytes) * 100.0 : 0.0;

  @override
  String toString() =>
      'IrohFileProgressEvent(${isSending ? "송신" : "수신"}: $fileName, ${(currentBytes / 1048576).toStringAsFixed(2)}MB / ${(totalBytes / 1048576).toStringAsFixed(2)}MB (${percentage.toStringAsFixed(1)}%) - ${speedMbs.toStringAsFixed(2)} MB/s)';
}

/// 파일 전송 완료 이벤트
class IrohFileSentEvent extends IrohEvent {
  final String fileName;
  final int totalBytes;
  final double seconds;
  final double speedMbs;

  IrohFileSentEvent({
    required this.fileName,
    required this.totalBytes,
    required this.seconds,
    required this.speedMbs,
  });

  @override
  String toString() =>
      'IrohFileSentEvent($fileName, ${(totalBytes / 1048576).toStringAsFixed(2)}MB in ${seconds.toStringAsFixed(2)}s @ ${speedMbs.toStringAsFixed(2)} MB/s)';
}

/// 파일 수신 완료 이벤트
class IrohFileReceivedEvent extends IrohEvent {
  final String fileName;
  final String savedPath;
  final int totalBytes;
  final double seconds;
  final double speedMbs;

  IrohFileReceivedEvent({
    required this.fileName,
    required this.savedPath,
    required this.totalBytes,
    required this.seconds,
    required this.speedMbs,
  });

  @override
  String toString() =>
      'IrohFileReceivedEvent(saved: $savedPath, ${(totalBytes / 1048576).toStringAsFixed(2)}MB in ${seconds.toStringAsFixed(2)}s @ ${speedMbs.toStringAsFixed(2)} MB/s)';
}

/// 연결 종료 이벤트
class IrohDisconnectedEvent extends IrohEvent {
  final String reason;
  IrohDisconnectedEvent({this.reason = '상대방과의 연결이 종료되었습니다.'});

  @override
  String toString() => 'IrohDisconnectedEvent(reason: $reason)';
}

/// 실시간 원격 화면 프레임 이벤트 (JPEG 이미지 바이트)
class IrohScreenFrameEvent extends IrohEvent {
  final int frameSeq;
  final int width;
  final int height;
  final List<int> jpegBytes;
  final DateTime timestamp;

  IrohScreenFrameEvent({
    required this.frameSeq,
    required this.width,
    required this.height,
    required this.jpegBytes,
    DateTime? timestamp,
  }) : timestamp = timestamp ?? DateTime.now();

  @override
  String toString() =>
      'IrohScreenFrameEvent(frame: #$frameSeq, resolution: ${width}x$height, size: ${jpegBytes.length} bytes)';
}

/// 에러 이벤트
class IrohErrorEvent extends IrohEvent {
  final String error;
  IrohErrorEvent(this.error);

  @override
  String toString() => 'IrohErrorEvent(error: $error)';
}

/// Flutter / Dart 애플리케이션을 위한 Iroh P2P 클라이언트 인터페이스
abstract class IrohP2PClient {
  Stream<IrohEvent> get events;
  bool get isConnected;
  String? get remoteNodeId;

  Future<String> startHost({int channel = 0});
  Future<void> connect({int? channel, String? ticket});
  Future<void> sendMessage(String message);
  Future<void> sendFile(String filePath);
  Future<void> startScreenShare({int fps = 30, int quality = 75});
  Future<void> sendMouseMove(double normalizedX, double normalizedY);
  Future<void> sendMouseClick({bool isRight = false});
  Future<void> sendMouseWheel(int delta);
  Future<void> sendKey(int keyCode, bool isDown);
  Future<void> sendText(String text);
  Future<void> ping({int count = 20});
  Future<void> bench({int megabytes = 10});
  Future<void> disconnect();
}

/// flutter_rust_bridge 또는 백그라운드 프로세스/스트림과 연동되는 기본 구현체
class IrohP2PController implements IrohP2PClient {
  final StreamController<IrohEvent> _eventController =
      StreamController<IrohEvent>.broadcast();

  final List<int> _pingSamples = [];
  DateTime? _benchStartTime;
  int _benchReceivedBytes = 0;

  bool _isConnected = false;
  String? _remoteNodeId;
  String? _currentPathType;

  final Future<void> Function(String rawMessage)? _rawSender;

  IrohP2PController({Future<void> Function(String rawMessage)? rawSender})
      : _rawSender = rawSender;

  @override
  Stream<IrohEvent> get events => _eventController.stream;

  @override
  bool get isConnected => _isConnected;

  @override
  String? get remoteNodeId => _remoteNodeId;

  String? get currentPathType => _currentPathType;

  @override
  Future<String> startHost({int channel = 0}) async {
    _eventController.add(IrohMessageReceivedEvent(
      message: '[호스트 대기 시작] 채널 #$channel 번호로 연결을 대기합니다.',
    ));
    return 'channel:$channel';
  }

  @override
  Future<void> connect({int? channel, String? ticket}) async {
    final target = channel != null ? '$channel' : (ticket ?? '0');
    _eventController.add(IrohMessageReceivedEvent(
      message: '[연결 시도] $target 에 연결을 시도합니다...',
    ));
  }

  @override
  Future<void> sendMessage(String message) async {
    final trimmed = message.trim();
    if (trimmed.isEmpty) return;
    await _sendRaw(trimmed);
  }

  @override
  Future<void> sendFile(String filePath) async {
    final file = File(filePath);
    if (!await file.exists()) {
      _eventController.add(IrohErrorEvent('전송할 파일이 존재하지 않습니다: $filePath'));
      return;
    }
    // 콘솔/브릿지 명령 전송
    await _sendRaw('/send $filePath');
  }

  @override
  Future<void> ping({int count = 20}) async {
    final clampedCount = count.clamp(1, 500);
    _pingSamples.clear();
    final now = DateTime.now().millisecondsSinceEpoch;
    await _sendRaw('__PING__:1:$now:$clampedCount');
  }

  @override
  Future<void> startScreenShare({int fps = 30, int quality = 75}) async {
    final clampedFps = fps.clamp(5, 60);
    final clampedQuality = quality.clamp(30, 95);
    await _sendRaw('/share $clampedFps $clampedQuality');
  }

  @override
  Future<void> sendMouseMove(double normalizedX, double normalizedY) async {
    final x = normalizedX.clamp(0.0, 1.0);
    final y = normalizedY.clamp(0.0, 1.0);
    await _sendRaw('/mouse ${x.toStringAsFixed(4)} ${y.toStringAsFixed(4)}');
  }

  @override
  Future<void> sendMouseClick({bool isRight = false}) async {
    await _sendRaw('/click ${isRight ? "R" : "L"}');
  }

  @override
  Future<void> sendMouseWheel(int delta) async {
    await _sendRaw('__CTRL__:MW:$delta');
  }

  @override
  Future<void> sendKey(int keyCode, bool isDown) async {
    await _sendRaw('__CTRL__:${isDown ? "KD" : "KU"}:$keyCode');
  }

  @override
  Future<void> sendText(String text) async {
    await _sendRaw('__CTRL__:TX:$text');
  }

  @override
  Future<void> bench({int megabytes = 10}) async {
    final clampedMb = megabytes.clamp(1, 200);
    await _sendRaw('/bench $clampedMb');
  }

  @override
  Future<void> disconnect() async {
    if (_isConnected) {
      await _sendRaw('/quit');
      _handleDisconnected('로컬에서 연결을 종료했습니다.');
    }
  }

  /// Rust 코어 / 수신 스트림으로부터 들어오는 원시 메시지를 파싱하여 이벤트로 변환
  void handleIncomingRaw(String raw) {
    if (raw.startsWith('__PING__:')) {
      final pong = raw.replaceFirst('__PING__:', '__PONG__:');
      _sendRaw(pong);
    } else if (raw.startsWith('__PONG__:')) {
      final parts = raw.split(':');
      if (parts.length >= 4) {
        final seq = int.tryParse(parts[1]) ?? 1;
        final ts = int.tryParse(parts[2]) ?? 0;
        final total = int.tryParse(parts[3]) ?? 20;

        final now = DateTime.now().millisecondsSinceEpoch;
        final rtt = (now - ts).clamp(0, 999999);
        _pingSamples.add(rtt);

        _eventController.add(IrohPingResultEvent(
          seq: seq,
          total: total,
          rttMs: rtt,
        ));

        if (seq < total) {
          Future.delayed(const Duration(milliseconds: 50), () {
            final nextNow = DateTime.now().millisecondsSinceEpoch;
            _sendRaw('__PING__:${seq + 1}:$nextNow:$total');
          });
        } else {
          final report = _calculateDistributionReport(total);
          if (report != null) {
            _eventController.add(report);
          }
        }
      }
    } else if (raw.startsWith('__BENCH_START__:')) {
      _benchStartTime = DateTime.now();
      _benchReceivedBytes = 0;
    } else if (raw.startsWith('__BENCH_CHUNK__:')) {
      _benchReceivedBytes += raw.length;
    } else if (raw.startsWith('__BENCH_END__:')) {
      if (_benchStartTime != null) {
        final elapsedSec = DateTime.now()
                .difference(_benchStartTime!)
                .inMicroseconds /
            1000000.0;
        final mbytes = _benchReceivedBytes / (1024.0 * 1024.0);
        final speedMbs = mbytes / (elapsedSec > 0 ? elapsedSec : 0.001);

        final report = IrohBenchReportEvent(
          megabytes: mbytes,
          seconds: elapsedSec,
          speedMbs: speedMbs,
          isSender: false,
        );
        _eventController.add(report);
        _sendRaw(
            '__BENCH_REPORT__:${mbytes.toStringAsFixed(2)}:${elapsedSec.toStringAsFixed(2)}:${speedMbs.toStringAsFixed(2)}');
        _benchStartTime = null;
      }
    } else if (raw.startsWith('__BENCH_REPORT__:')) {
      final parts = raw.split(':');
      if (parts.length >= 4) {
        final mb = double.tryParse(parts[1]) ?? 0;
        final sec = double.tryParse(parts[2]) ?? 0;
        final speed = double.tryParse(parts[3]) ?? 0;
        _eventController.add(IrohBenchReportEvent(
          megabytes: mb,
          seconds: sec,
          speedMbs: speed,
          isSender: false,
        ));
      }
    } else if (raw.startsWith('__FILE_PROGRESS__:')) {
      // __FILE_PROGRESS__:<filename>:<current>:<total>:<speed_mbs>:<is_sending>
      final parts = raw.split(':');
      if (parts.length >= 6) {
        final name = parts[1];
        final current = int.tryParse(parts[2]) ?? 0;
        final total = int.tryParse(parts[3]) ?? 0;
        final speed = double.tryParse(parts[4]) ?? 0.0;
        final isSending = parts[5] == '1' || parts[5] == 'true';
        _eventController.add(IrohFileProgressEvent(
          fileName: name,
          currentBytes: current,
          totalBytes: total,
          speedMbs: speed,
          isSending: isSending,
        ));
      }
    } else if (raw.startsWith('__FILE_RECEIVED__:')) {
      // __FILE_RECEIVED__:<filename>:<saved_path>:<bytes>:<sec>:<speed_mbs>
      final parts = raw.split(':');
      if (parts.length >= 6) {
        _eventController.add(IrohFileReceivedEvent(
          fileName: parts[1],
          savedPath: parts[2],
          totalBytes: int.tryParse(parts[3]) ?? 0,
          seconds: double.tryParse(parts[4]) ?? 0.0,
          speedMbs: double.tryParse(parts[5]) ?? 0.0,
        ));
      }
    } else if (raw.startsWith('__FILE_SENT__:')) {
      // __FILE_SENT__:<filename>:<bytes>:<sec>:<speed_mbs>
      final parts = raw.split(':');
      if (parts.length >= 5) {
        _eventController.add(IrohFileSentEvent(
          fileName: parts[1],
          totalBytes: int.tryParse(parts[2]) ?? 0,
          seconds: double.tryParse(parts[3]) ?? 0.0,
          speedMbs: double.tryParse(parts[4]) ?? 0.0,
        ));
      }
    } else if (raw.startsWith('__CONNECTED__:')) {
      final parts = raw.split(':');
      _isConnected = true;
      _remoteNodeId = parts.length > 1 ? parts[1] : 'Unknown';
      _currentPathType = parts.length > 2 ? parts[2] : 'Direct P2P';
      _eventController.add(IrohConnectedEvent(
        remoteNodeId: _remoteNodeId!,
        pathType: _currentPathType!,
        rawDetails: raw,
      ));
    } else if (raw == '[상대방이 대화를 종료했습니다]' || raw.startsWith('__DISCONNECTED__')) {
      _handleDisconnected('상대방이 연결을 종료했습니다.');
    } else {
      _eventController.add(IrohMessageReceivedEvent(message: raw));
    }
  }

  IrohPingDistributionEvent? _calculateDistributionReport(int totalSent) {
    if (_pingSamples.isEmpty) return null;
    final sorted = List<int>.from(_pingSamples)..sort();
    final n = sorted.length;
    final minMs = sorted.first;
    final maxMs = sorted.last;
    final sum = sorted.reduce((a, b) => a + b);
    final avgMs = sum / n;

    final variance =
        sorted.map((x) => pow(x - avgMs, 2)).reduce((a, b) => a + b) / n;
    final stdDevMs = sqrt(variance);

    double jitterMs = 0.0;
    if (n > 1) {
      int diffSum = 0;
      for (int i = 0; i < n - 1; i++) {
        diffSum += (sorted[i + 1] - sorted[i]).abs();
      }
      jitterMs = diffSum / (n - 1);
    }

    final p50Ms = sorted[(n * 0.50).clamp(0, n - 1).toInt()];
    final p90Ms = sorted[(n * 0.90).clamp(0, n - 1).toInt()];
    final p95Ms = sorted[(n * 0.95).clamp(0, n - 1).toInt()];
    final p99Ms = sorted[(n * 0.99).clamp(0, n - 1).toInt()];

    final lossRate =
        totalSent > 0 ? ((totalSent - n) / totalSent) * 100.0 : 0.0;

    final numBuckets = min(5, max(1, maxMs - minMs + 1));
    final step = max(1, ((maxMs - minMs) / numBuckets).ceil());
    final List<IrohLatencyBucket> buckets = [];

    for (int i = 0; i < numBuckets; i++) {
      final bStart = minMs + (i * step);
      final bEnd = i == numBuckets - 1 ? maxMs : bStart + step - 1;
      final count = sorted.where((r) => r >= bStart && r <= bEnd).length;
      final pct = (count / n) * 100.0;
      buckets.add(IrohLatencyBucket(
        startMs: bStart,
        endMs: bEnd,
        count: count,
        percentage: pct,
      ));
    }

    return IrohPingDistributionEvent(
      sent: totalSent,
      received: n,
      lossRate: lossRate,
      minMs: minMs,
      maxMs: maxMs,
      avgMs: avgMs,
      stdDevMs: stdDevMs,
      jitterMs: jitterMs,
      p50Ms: p50Ms,
      p90Ms: p90Ms,
      p95Ms: p95Ms,
      p99Ms: p99Ms,
      buckets: buckets,
    );
  }

  void _handleDisconnected(String reason) {
    _isConnected = false;
    _remoteNodeId = null;
    _currentPathType = null;
    _eventController.add(IrohDisconnectedEvent(reason: reason));
  }

  Future<void> _sendRaw(String raw) async {
    if (_rawSender != null) {
      await _rawSender(raw);
    }
  }

  void dispose() {
    _eventController.close();
  }
}
