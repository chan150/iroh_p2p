# Iroh P2P 통신 예제 (공유기 / 방화벽 / NAT 환경 대응)

이 프로젝트는 **[Iroh](https://iroh.computer)** 라이브러리를 사용하여, **공유기(NAT), 방화벽, 사설 인트라넷 등으로 인해 직접적인 공인 IP 접속이 불가능한 두 컴퓨터/소프트웨어 간에 안전한 P2P 통신 채널을 수립하고 실시간 데이터를 교환하는 예제**입니다.

---

## 💡 Iroh가 NAT / 방화벽 환경을 극복하는 원리

전통적인 소켓 통신(TCP/UDP)에서는 양쪽 모두 공유기 뒤에 있거나 방화벽이 있으면 직접 연결할 수 없습니다. Iroh는 이를 다음과 같은 다계층 전략으로 자동 해결합니다:

```
[ 피어 A (공유기/방화벽 뒤) ]                           [ 피어 B (공유기/방화벽 뒤) ]
       │                                                      │
       │─── 1. Relay(DERP) 접속 & 노드 주소(티켓) 생성 ─────▶│
       │                                                      │
       │◀── 2. STUN / UPnP / Hole Punching 직접 연결 시도 ──▶│ (직접 P2P 성공 시)
       │                                                      │
       │═══════════ (직접 연결 불가 시 자동 폴백) ═══════════│
       │                                                      │
       └───▶ [ Iroh 글로벌 Relay (DERP) 서버 ] ◀──────────────┘
                    (E2E 종단간 암호화 터널)
```

1. **글로벌 릴레이(DERP) & STUN 등록 (`presets::N0`)**:
   - 노드가 시작되면 Iroh의 글로벌 릴레이 네트워크에 연결하고, STUN/UPnP를 통해 자신의 공인 IP와 로컬 IP를 파악합니다.
2. **연결 정보 티켓(EndpointAddr)**:
   - 노드의 고유 암호화 키(Node ID)와 릴레이 정보, 감지된 네트워크 주소를 포함하는 티켓 문자열을 생성합니다.
3. **홀 펀칭(Hole Punching)을 통한 Direct P2P 우선 시도**:
   - 두 노드가 티켓을 교환하면, 릴레이를 통해 시그널링을 주고받은 뒤 서로에게 직접 UDP 패킷을 쏴서 공유기의 NAT 매핑 테이블을 뚫는 홀 펀칭을 시도합니다.
4. **DERP Relay 폴백 (100% 연결 보장)**:
   - 인트라넷 보안 정책이나 대칭형 NAT(Symmetric NAT) 등으로 홀 펀칭이 실패하더라도, **Iroh Relay 서버를 통해 종단간 암호화(QUIC TLS 1.3)된 상태로 패킷을 중계**하여 끊김 없이 통신을 보장합니다. (중계 서버조차 패킷 내용을 복호화할 수 없음)

---

## 🚀 초간단 실행 방법 (티켓 필요 없는 채널 모드)

긴 티켓을 복사/붙여넣기할 필요 없이, **채널 번호(`0`, `1`, `2`, `3`...)** 만으로 즉시 연결할 수 있습니다!

### 1. 호스트(수신 대기) 실행
```bash
# 기본 채널 0번으로 대기
cargo run -- listen

# 또는 특정 채널 번호(예: 1번)로 대기
cargo run -- listen 1
```

### 2. 클라이언트(접속) 실행
```bash
# 기본 채널 0번으로 즉시 자동 접속 (티켓 필요 없음!)
cargo run -- connect

# 또는 특정 채널 번호(예: 1번)로 자동 접속
cargo run -- connect 1
```
*(기존처럼 `connect <티켓문자열>` 형태로 수동 티켓 접속도 100% 호환됩니다.)*

---

## ⚡ 실시간 파일 전송 및 성능 진단 명령어

통신 중 터미널에 아래 명령어를 입력하여 파일 전송 및 네트워크 품질을 즉시 제어할 수 있습니다:

* **`/send <파일경로>`** (또는 `/file <파일경로>`): 대화 중 로컬 파일을 상대방에게 초고속 스트리밍 전송 (실시간 프로그레스 바 `[████░░░░] 45.2% (24.5 MB/s)` 표시, 수신 측 `received/` 폴더에 자동 저장)
* **`/ping [횟수]`**: 왕복 지연시간(RTT) 측정 및 **레이턴시 정밀 분포도(p50, p90, p95, p99 백분위수, 지터, 아스키 히스토그램)** 출력 (예: `/ping 50`)
* **`/bench [MB]`**: 지정한 크기(예: `/bench 10`, `/bench 50`)의 데이터를 고속 전송하여 **실제 전송 대역폭(`MB/s`, `Mbps`)** 실측
* **`/stats`**: 현재 QUIC 연결 상태, 직접 P2P 여부, 송수신 바이트/패킷 수, 패킷 손실 수 출력
* **`/help`**: 명령어 목록 안내
* **`/quit`**: 연결 종료

---

## 📁 단독 파일 전송 모드 (One-Click Send/Recv)

대화 세션 없이 파일 전송만 빠르게 수행할 수도 있습니다:

```bash
# PC 1 (수신 대기):
cargo run -- recv 0

---

## 🖥️ 실시간 화면 공유 & 원격 제어 (Remote Desktop)

두 PC 간에 별도의 복잡한 원격 제어 소프트웨어(팀뷰어, AnyDesk 등) 설치 없이, Iroh P2P로 직접 저지연 화면을 스트리밍하고 원격 마우스/키보드를 조작할 수 있습니다:

### 1. 화면 공유 호스트 실행 (내 화면을 상대방에게 스트리밍)
```bash
# 채널 0번으로 화면 공유 시작 (기본: 30 FPS, JPEG 품질 75%)
cargo run -- share 0

# FPS 및 화질 지정 (예: 60 FPS, 품질 85%)
cargo run -- share 0 --fps 60 --quality 85
```

### 2. 원격 뷰어 실행 (상대방 화면 수신 및 모니터링)
```bash
# 채널 0번 호스트 화면 수신
cargo run -- view 0
```

### 3. 대화 세션 내에서 화면 공유 & 원격 제어
```
/share 30 75    : 대화 도중 내 화면을 상대방에게 실시간 공유 시작
/mouse 0.5 0.5  : 원격 마우스 커서를 화면 중앙으로 이동 (0.0~1.0 비율 좌표)
/click L        : 원격 마우스 좌클릭 (R: 우클릭)
```

---

## 🧪 자동화 통합 테스트 실행

```bash
cargo test -- --nocapture
```
* `test_p2p_screen_frame_streaming`: 실시간 화면 프레임 스트리밍 및 파싱 검증
* `test_remote_control_event_serialization`: 원격 마우스/키보드 제어 이벤트 직렬화/역직렬화 검증
* `test_p2p_file_streaming_transfer`: P2P 파일 스트리밍 전송 및 100% 무결성 검증
* `test_channel_zero_config_connection`: 티켓 없는 채널 기반 자동 P2P 연결 검증
* `test_p2p_direct_or_relay_communication`: E2E P2P 데이터 통신 검증
* `test_p2p_reconnection_loop`: 다중 세션 및 연속 재접속 검증
* `test_ticket_multiline_and_legacy_decoding`: 멀티라인 티켓 정제 및 디코딩 검증

---

## 📂 프로젝트 구조

- `src/remote.rs`: 고속 모니터 화면 캡처, JPEG 인코딩 스트리머 (`ScreenStreamer`), Windows 네이티브 입력 시뮬레이터 (`WindowsInputSimulator`), 원격 제어 프로토콜 (`RemoteControlEvent`)
- `src/lib.rs`: 다중 스트림 자동 디스패처 (`dispatch_incoming_bi_stream`), 파일 스트리밍 (`send_file_stream`, `receive_file_stream`), 레이턴시 분포 계산 (`analyze_ping_distribution`), 결정론적 채널 키 생성 (`derive_channel_keys`)
- `src/main.rs`: 파일 전송/수신, 실시간 화면 공유(`share`), 원격 뷰어(`view`) 및 대화형 CLI
- `iroh.dart`: Flutter / Dart 애플리케이션용 화면 프레임 이벤트(`IrohScreenFrameEvent`), 마우스/키보드 원격 제어 API, P2P 파일 전송, 실시간 메시징 클라이언트
- `tests/p2p_test.rs`: 7개 시나리오 무인 자동화 E2E 통합 테스트

