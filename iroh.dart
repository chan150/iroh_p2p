import 'dart:async';

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

/// Ping 5회 측정 완료 통계 요약
class IrohPingSummaryEvent extends IrohEvent {
  final int minMs;
  final int maxMs;
  final double avgMs;

  IrohPingSummaryEvent({
    required this.minMs,
    required this.maxMs,
    required this.avgMs,
  });

  @override
  String toString() =>
      'IrohPingSummaryEvent(min: ${minMs}ms, max: ${maxMs}ms, avg: ${avgMs.toStringAsFixed(1)}ms)';
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

/// 연결 종료 이벤트
class IrohDisconnectedEvent extends IrohEvent {
  final String reason;
  IrohDisconnectedEvent({this.reason = '상대방과의 연결이 종료되었습니다.'});

  @override
  String toString() => 'IrohDisconnectedEvent(reason: $reason)';
}

/// 에러 이벤트
class IrohErrorEvent extends IrohEvent {
  final String error;
  IrohErrorEvent(this.error);

  @override
  String toString() => 'IrohErrorEvent(error: $error)';
}

/// Flutter / Dart 애플리케이션을 위한 Iroh P2P 클라이언트 인터페이스
/// 
/// - 채널 번호(0, 1, 2...) 기반 Zero-Config 자동 연결
/// - 수동 티켓(Ticket) 기반 연결
/// - 실시간 메시지 송수신
/// - /ping 지연시간 및 /bench 대역폭 측정 기능 지원
abstract class IrohP2PClient {
  /// 현재 P2P 이벤트 스트림 (UI에 실시간 연결)
  Stream<IrohEvent> get events;

  /// 현재 연결 여부
  bool get isConnected;

  /// 현재 연결된 원격 Node ID
  String? get remoteNodeId;

  /// [Host] 특정 채널 번호(기본값: 0)로 수신 대기 시작
  Future<String> startHost({int channel = 0});

  /// [Client] 채널 번호(예: 0, 1, 2) 또는 티켓 문자열로 원격 피어에 접속
  Future<void> connect({int? channel, String? ticket});

  /// 일반 텍스트 메시지 전송
  Future<void> sendMessage(String message);

  /// 실시간 지연시간(RTT) 측정 요청 (5회 핑-퐁)
  Future<void> ping();

  /// 대역폭(Throughput) 벤치마크 실행 (기본 5MB 전송)
  Future<void> bench({int megabytes = 5});

  /// 연결 종료
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

  // 저수준 송신 브릿지 함수 (FRB / StreamSink)
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
  Future<void> ping() async {
    _pingSamples.clear();
    final now = DateTime.now().millisecondsSinceEpoch;
    await _sendRaw('__PING__:1:$now:5');
  }

  @override
  Future<void> bench({int megabytes = 5}) async {
    final clampedMb = megabytes.clamp(1, 50);
    final totalBytes = clampedMb * 1024 * 1024;
    final chunkSize = 64 * 1024; // 64KB
    final numChunks = totalBytes ~/ chunkSize;
    final chunkPayload = 'X' * chunkSize;

    await _sendRaw('__BENCH_START__:$totalBytes');
    final startTime = DateTime.now();

    for (int i = 0; i < numChunks; i++) {
      await _sendRaw('__BENCH_CHUNK__:$chunkPayload');
    }

    await _sendRaw('__BENCH_END__:done');
    final elapsedSec =
        DateTime.now().difference(startTime).inMicroseconds / 1000000.0;
    final speedMbs = clampedMb / elapsedSec;

    final report = IrohBenchReportEvent(
      megabytes: clampedMb.toDouble(),
      seconds: elapsedSec,
      speedMbs: speedMbs,
      isSender: true,
    );
    _eventController.add(report);
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
      // PING 수신 -> PONG 자동 응답
      final pong = raw.replaceFirst('__PING__:', '__PONG__:');
      _sendRaw(pong);
    } else if (raw.startsWith('__PONG__:')) {
      // PONG 수신 -> RTT 계산
      final parts = raw.split(':');
      if (parts.length >= 4) {
        final seq = int.tryParse(parts[1]) ?? 1;
        final ts = int.tryParse(parts[2]) ?? 0;
        final total = int.tryParse(parts[3]) ?? 5;

        final now = DateTime.now().millisecondsSinceEpoch;
        final rtt = (now - ts).clamp(0, 999999);
        _pingSamples.add(rtt);

        _eventController.add(IrohPingResultEvent(
          seq: seq,
          total: total,
          rttMs: rtt,
        ));

        if (seq < total) {
          Future.delayed(const Duration(milliseconds: 150), () {
            final nextNow = DateTime.now().millisecondsSinceEpoch;
            _sendRaw('__PING__:${seq + 1}:$nextNow:$total');
          });
        } else {
          final min = _pingSamples.reduce((a, b) => a < b ? a : b);
          final max = _pingSamples.reduce((a, b) => a > b ? a : b);
          final avg = _pingSamples.reduce((a, b) => a + b) / _pingSamples.length;

          _eventController.add(IrohPingSummaryEvent(
            minMs: min,
            maxMs: max,
            avgMs: avg,
          ));
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
      // 상대방의 수신 레포트
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
    } else if (raw.startsWith('__CONNECTED__:')) {
      // 내부 핸드셰이크 연결 알림
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
      // 일반 대화 메시지
      _eventController.add(IrohMessageReceivedEvent(message: raw));
    }
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
