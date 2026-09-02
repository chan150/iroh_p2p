pub mod remote;

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use iroh::{Endpoint, endpoint::presets, EndpointAddr, RelayMode};

/// ALPN (Application-Layer Protocol Negotiation)
/// 서로 다른 프로토콜 간의 충돌을 방지하기 위한 식별자입니다.
pub const CHAT_ALPN: &[u8] = b"iroh-p2p-chat/1.0";

/// Iroh `EndpointAddr`를 압축된 바이너리(Postcard) + URL-Safe Base64 티켓 문자열로 직렬화합니다.
pub fn encode_ticket(addr: &EndpointAddr) -> Result<String> {
    let bytes = postcard::to_allocvec(addr).context("EndpointAddr 바이너리 직렬화 실패")?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

/// 티켓 문자열(또는 파일 경로)을 파싱하여 `EndpointAddr`로 복원합니다.
/// 터미널 줄바꿈(\r, \n), 공백 등이 포함되어 있어도 자동으로 정제하여 파싱합니다.
pub fn decode_ticket(input: &str) -> Result<EndpointAddr> {
    // 1. 만약 입력값이 존재하는 파일 경로라면 파일 내용 읽기
    let raw_str = if std::path::Path::new(input.trim()).is_file() {
        std::fs::read_to_string(input.trim()).context("티켓 파일 읽기 실패")?
    } else {
        input.to_string()
    };

    // 2. 터미널 줄바꿈, 공백 등 모든 공백 문자 완벽 제거
    let cleaned: String = raw_str.chars().filter(|c| !c.is_whitespace()).collect();

    // 3. Base64 디코딩
    let bytes = URL_SAFE_NO_PAD
        .decode(&cleaned)
        .context("티켓 Base64 디코딩 실패 (티켓 문자열을 올바르게 복사했는지 확인하세요)")?;

    // 4. Postcard 바이너리 역직렬화 시도 -> 실패 시 JSON 역직렬화 시도 (하위 호환)
    if let Ok(addr) = postcard::from_bytes::<EndpointAddr>(&bytes) {
        return Ok(addr);
    }
    if let Ok(addr) = serde_json::from_slice::<EndpointAddr>(&bytes) {
        return Ok(addr);
    }

    anyhow::bail!("지원하지 않거나 손상된 티켓 형식입니다.")
}

/// 채널 번호(0, 1, 2, 3...)로부터 호스트의 고정 SecretKey와 접속용 EndpointAddr를 결정론적으로 생성합니다.
pub fn derive_channel_keys(channel: u32) -> (iroh::SecretKey, EndpointAddr) {
    let salt = b"iroh-private-p2p-channel-salt-2026-auth";
    let mut seed = [0u8; 32];
    for (i, byte) in salt.iter().enumerate() {
        seed[i % 32] ^= byte;
    }
    let ch_bytes = channel.to_le_bytes();
    for (i, byte) in ch_bytes.iter().enumerate() {
        seed[i] ^= byte;
    }
    for i in 0..32 {
        seed[i] = seed[i].wrapping_add((i as u8).wrapping_mul(7));
    }

    let secret_key = iroh::SecretKey::from_bytes(&seed);
    let public_key = secret_key.public();
    let relay_url: iroh::RelayUrl = "https://aps1-1.relay.n0.iroh.link./".parse().unwrap();
    let target_addr = EndpointAddr::new(public_key).with_relay_url(relay_url);

    (secret_key, target_addr)
}

/// Iroh Endpoint를 생성합니다.
/// secret_key를 지정하면 고정 Node ID를 사용하며, None이면 임의의 새 키를 생성합니다.
pub async fn create_endpoint_with_secret_key(
    secret_key: Option<iroh::SecretKey>,
    alpns: Vec<Vec<u8>>,
) -> Result<Endpoint> {
    let dns_resolver = iroh::dns::DnsResolver::with_nameserver("1.1.1.1:53".parse().unwrap());

    let mut builder = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Default)
        .dns_resolver(dns_resolver)
        .alpns(alpns);

    if let Some(sk) = secret_key {
        builder = builder.secret_key(sk);
    }

    let endpoint = builder.bind().await.context("Iroh Endpoint 바인딩 실패")?;

    // 릴레이 서버 핸드셰이크 및 공인/사설 네트워크 주소 준비 완료 대기 (최대 5초)
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), endpoint.online()).await;

    Ok(endpoint)
}

/// Iroh Endpoint를 임의의 키로 생성합니다.
pub async fn create_endpoint(alpns: Vec<Vec<u8>>) -> Result<Endpoint> {
    create_endpoint_with_secret_key(None, alpns).await
}

/// 현재 활성화된 네트워크 경로(Direct P2P vs Relay)를 사람이 읽기 쉬운 문자열로 변환합니다.
pub fn format_path_info(conn: &iroh::endpoint::Connection) -> String {
    let paths_debug = format!("{:?}", conn.paths());
    if paths_debug.contains("ip:") {
        format!("✅ [Direct P2P (직접 연결)] (상세: {})", paths_debug)
    } else if paths_debug.contains("relay:") {
        format!("🌐 [Relay (릴레이 경유)] (상세: {})", paths_debug)
    } else {
        format!("ℹ️ [경로 탐색 중] (상세: {})", paths_debug)
    }
}

/// QUIC 연결 통계(RTT, 송수신 바이트 수, 손실 패킷 등)를 반환합니다.
pub fn format_stats_info(conn: &iroh::endpoint::Connection) -> String {
    let stats = conn.stats();
    let rtt_info = if let Some(path) = conn.paths().iter().next() {
        conn.rtt(path.id())
            .map(|rtt| format!("{:.2?}", rtt))
            .unwrap_or_else(|| "측정 중".to_string())
    } else {
        "경로 없음".to_string()
    };

    format!(
        "RTT(지연시간): {} | 송신: {:.2} KB ({} pkts) | 수신: {:.2} KB ({} pkts) | 손실: {} pkts",
        rtt_info,
        stats.udp_tx.bytes as f64 / 1024.0,
        stats.udp_tx.datagrams,
        stats.udp_rx.bytes as f64 / 1024.0,
        stats.udp_rx.datagrams,
        stats.lost_packets,
    )
}

/// Ping 레이턴시 정밀 분포 통계 데이터
#[derive(Debug, Clone)]
pub struct PingDistributionStats {
    pub sent: usize,
    pub received: usize,
    pub loss_rate: f64,
    pub min_ms: u128,
    pub max_ms: u128,
    pub avg_ms: f64,
    pub std_dev_ms: f64,
    pub jitter_ms: f64,
    pub p50_ms: u128,
    pub p90_ms: u128,
    pub p95_ms: u128,
    pub p99_ms: u128,
    pub buckets: Vec<(u128, u128, usize, f64)>, // (구간 시작, 구간 끝, 개수, 비율%)
}

/// 수집된 RTT 샘플들로부터 레이턴시 분포, 백분위수, 지터, 표준편차 및 히스토그램을 계산합니다.
pub fn analyze_ping_distribution(mut rtts: Vec<u128>, total_sent: usize) -> Option<PingDistributionStats> {
    if rtts.is_empty() {
        return None;
    }
    rtts.sort_unstable();
    let n = rtts.len();
    let min_ms = rtts[0];
    let max_ms = rtts[n - 1];
    let sum: u128 = rtts.iter().sum();
    let avg_ms = sum as f64 / n as f64;

    // 분산 및 표준편차 계산
    let variance = rtts
        .iter()
        .map(|&x| {
            let diff = x as f64 - avg_ms;
            diff * diff
        })
        .sum::<f64>()
        / n as f64;
    let std_dev_ms = variance.sqrt();

    // 지터 (RFC 3550: 인접 RTT 차이의 평균)
    let jitter_ms = if n > 1 {
        let diff_sum: u128 = rtts.windows(2).map(|w| w[1].abs_diff(w[0])).sum();
        diff_sum as f64 / (n - 1) as f64
    } else {
        0.0
    };

    // 백분위수 (Percentiles)
    let p50_ms = rtts[(n as f64 * 0.50).min((n - 1) as f64) as usize];
    let p90_ms = rtts[(n as f64 * 0.90).min((n - 1) as f64) as usize];
    let p95_ms = rtts[(n as f64 * 0.95).min((n - 1) as f64) as usize];
    let p99_ms = rtts[(n as f64 * 0.99).min((n - 1) as f64) as usize];

    let loss_rate = if total_sent > 0 {
        ((total_sent.saturating_sub(n)) as f64 / total_sent as f64) * 100.0
    } else {
        0.0
    };

    // 히스토그램 버킷 구성 (4~6개 구간)
    let num_buckets = 5.min((max_ms.saturating_sub(min_ms) + 1) as usize).max(1);
    let step = (((max_ms - min_ms) as f64 / num_buckets as f64).ceil() as u128).max(1);
    let mut buckets = Vec::new();

    for i in 0..num_buckets {
        let b_start = min_ms + (i as u128 * step);
        let b_end = if i == num_buckets - 1 {
            max_ms
        } else {
            b_start + step - 1
        };
        let count = rtts.iter().filter(|&&r| r >= b_start && r <= b_end).count();
        let pct = (count as f64 / n as f64) * 100.0;
        buckets.push((b_start, b_end, count, pct));
    }

    Some(PingDistributionStats {
        sent: total_sent,
        received: n,
        loss_rate,
        min_ms,
        max_ms,
        avg_ms,
        std_dev_ms,
        jitter_ms,
        p50_ms,
        p90_ms,
        p95_ms,
        p99_ms,
        buckets,
    })
}

/// 레이턴시 분포 통계를 터미널에 시각적으로 출력하기 위한 포맷 함수
pub fn format_ping_distribution_report(stats: &PingDistributionStats) -> String {
    let mut out = String::new();
    out.push_str("============================================================\n");
    out.push_str(&format!(
        " 📊 [PING 레이턴시 정밀 분포 분석 리포트 (총 {}회 시도)]\n",
        stats.sent
    ));
    out.push_str("============================================================\n");
    out.push_str(&format!(
        " • 패킷 전송/수신 : {} / {} pkts (손실률: {:.1}%)\n",
        stats.sent, stats.received, stats.loss_rate
    ));
    out.push_str(&format!(
        " • 최소 / 최대    : {} ms / {} ms\n",
        stats.min_ms, stats.max_ms
    ));
    out.push_str(&format!(
        " • 평균 ± 표준편차: {:.2} ms ± {:.2} ms (지터: {:.2} ms)\n",
        stats.avg_ms, stats.std_dev_ms, stats.jitter_ms
    ));
    out.push_str(&format!(
        " • 백분위수(p-tile): p50(중앙값)={}ms | p90={}ms | p95={}ms | p99={}ms\n",
        stats.p50_ms, stats.p90_ms, stats.p95_ms, stats.p99_ms
    ));
    out.push_str("------------------------------------------------------------\n");
    out.push_str(" [지연시간 구간별 빈도 분포도 (Histogram)]\n");

    for (start, end, count, pct) in &stats.buckets {
        let bar_len = (pct / 3.0).round() as usize;
        let bar = "█".repeat(bar_len);
        out.push_str(&format!(
            "  {:>4} ~ {:<4} ms : {:>3}개 ({:>5.1}%)  {}\n",
            start, end, count, pct, bar
        ));
    }
    out.push_str("============================================================");
    out
}

pub async fn read_exact_stream(recv: &mut iroh::endpoint::RecvStream, buf: &mut [u8]) -> Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        match recv.read(&mut buf[filled..]).await.context("스트림 읽기 실패")? {
            Some(0) | None => anyhow::bail!("스트림이 예기치 않게 종료되었습니다."),
            Some(n) => filled += n,
        }
    }
    Ok(())
}

/// 스트림을 통해 지정된 로컬 파일을 원격 피어에게 초고속 스트리밍 전송합니다.
pub async fn send_file_stream<F>(
    mut send_stream: iroh::endpoint::SendStream,
    mut recv_stream: iroh::endpoint::RecvStream,
    file_path: &std::path::Path,
    mut progress_callback: F,
) -> Result<(String, u64, std::time::Duration)>
where
    F: FnMut(u64, u64, f64), // (전송된 바이트, 총 바이트, 속도 MB/s)
{
    use tokio::io::AsyncReadExt;

    let file_name = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .context("올바른 파일 이름을 찾을 수 없습니다.")?;
    let file = tokio::fs::File::open(file_path)
        .await
        .context("파일 열기 실패")?;
    let metadata = file.metadata().await.context("파일 메타데이터 조회 실패")?;
    let file_size = metadata.len();

    // 1. 헤더 전송: [매직 4바이트 ("FILE")] [파일명길이 2바이트 (u16)] [파일명 바이트] [파일크기 8바이트 (u64)]
    let name_bytes = file_name.as_bytes();
    let name_len = name_bytes.len() as u16;

    let mut header = Vec::with_capacity(4 + 2 + name_bytes.len() + 8);
    header.extend_from_slice(b"FILE");
    header.extend_from_slice(&name_len.to_le_bytes());
    header.extend_from_slice(name_bytes);
    header.extend_from_slice(&file_size.to_le_bytes());

    send_stream.write_all(&header).await.context("파일 헤더 전송 실패")?;

    // 2. 파일 데이터 청크 스트리밍 (512KB BufReader + 256KB 청크 + 100ms 쓰로틀링)
    let mut reader = tokio::io::BufReader::with_capacity(512 * 1024, file);
    let start_time = std::time::Instant::now();
    let mut last_progress = std::time::Instant::now();
    let mut buffer = vec![0u8; 256 * 1024];
    let mut sent_bytes = 0u64;

    while sent_bytes < file_size {
        let n = reader.read(&mut buffer).await.context("파일 읽기 실패")?;
        if n == 0 {
            break;
        }
        send_stream.write_all(&buffer[..n]).await.context("파일 청크 전송 실패")?;
        sent_bytes += n as u64;

        let now = std::time::Instant::now();
        if now.duration_since(last_progress).as_millis() >= 100 || sent_bytes == file_size {
            let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
            let speed_mbs = (sent_bytes as f64 / (1024.0 * 1024.0)) / elapsed;
            progress_callback(sent_bytes, file_size, speed_mbs);
            last_progress = now;
        }
    }

    send_stream.finish().context("스트림 종료 알림 실패")?;

    // 3. 상대방의 수신 완료 ACK 확인 (2바이트 "OK")
    let mut ack = [0u8; 2];
    read_exact_stream(&mut recv_stream, &mut ack).await.context("수신 확인 ACK 대기 실패")?;
    if &ack != b"OK" {
        anyhow::bail!("상대방이 비정상 응답을 반환했습니다.");
    }

    Ok((file_name.to_string(), file_size, start_time.elapsed()))
}

/// 수신 스트림으로부터 파일을 받아 `save_dir` 디렉터리에 저장합니다.
pub async fn receive_file_stream<F>(
    send_stream: iroh::endpoint::SendStream,
    mut recv_stream: iroh::endpoint::RecvStream,
    save_dir: &std::path::Path,
    progress_callback: F,
) -> Result<(std::path::PathBuf, u64, std::time::Duration)>
where
    F: FnMut(u64, u64, f64),
{
    let mut magic = [0u8; 4];
    read_exact_stream(&mut recv_stream, &mut magic).await.context("매직 바이트 읽기 실패")?;
    if &magic != b"FILE" {
        anyhow::bail!("파일 전송 프로토콜 형식이 아닙니다.");
    }
    receive_file_body(send_stream, recv_stream, save_dir, progress_callback).await
}

async fn receive_file_body<F>(
    mut send_stream: iroh::endpoint::SendStream,
    mut recv_stream: iroh::endpoint::RecvStream,
    save_dir: &std::path::Path,
    mut progress_callback: F,
) -> Result<(std::path::PathBuf, u64, std::time::Duration)>
where
    F: FnMut(u64, u64, f64),
{
    use tokio::io::AsyncWriteExt;

    let mut name_len_buf = [0u8; 2];
    read_exact_stream(&mut recv_stream, &mut name_len_buf).await.context("파일명 길이 읽기 실패")?;
    let name_len = u16::from_le_bytes(name_len_buf) as usize;

    let mut name_buf = vec![0u8; name_len];
    read_exact_stream(&mut recv_stream, &mut name_buf).await.context("파일명 읽기 실패")?;
    let raw_name = String::from_utf8(name_buf).context("파일명 UTF-8 디코딩 실패")?;
    let file_name = std::path::Path::new(&raw_name)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("received_file.bin");

    let mut size_buf = [0u8; 8];
    read_exact_stream(&mut recv_stream, &mut size_buf).await.context("파일 크기 읽기 실패")?;
    let file_size = u64::from_le_bytes(size_buf);

    tokio::fs::create_dir_all(save_dir).await.context("저장 디렉터리 생성 실패")?;
    let mut save_path = save_dir.join(file_name);
    let mut counter = 1;
    while save_path.exists() {
        let stem = std::path::Path::new(file_name)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("file");
        let ext = std::path::Path::new(file_name)
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| format!(".{}", e))
            .unwrap_or_default();
        save_path = save_dir.join(format!("{}({}){}", stem, counter, ext));
        counter += 1;
    }

    let out_file = tokio::fs::File::create(&save_path).await.context("저장용 파일 생성 실패")?;
    let mut writer = tokio::io::BufWriter::with_capacity(512 * 1024, out_file);

    let start_time = std::time::Instant::now();
    let mut last_progress = std::time::Instant::now();
    let mut buffer = vec![0u8; 256 * 1024];
    let mut received_bytes = 0u64;

    while received_bytes < file_size {
        let remaining = (file_size - received_bytes) as usize;
        let to_read = buffer.len().min(remaining);
        let n = match recv_stream.read(&mut buffer[..to_read]).await.context("파일 데이터 수신 실패")? {
            Some(0) | None => break,
            Some(n) => n,
        };
        writer.write_all(&buffer[..n]).await.context("로컬 파일 쓰기 실패")?;
        received_bytes += n as u64;

        let now = std::time::Instant::now();
        if now.duration_since(last_progress).as_millis() >= 100 || received_bytes == file_size {
            let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
            let speed_mbs = (received_bytes as f64 / (1024.0 * 1024.0)) / elapsed;
            progress_callback(received_bytes, file_size, speed_mbs);
            last_progress = now;
        }
    }

    writer.flush().await.context("파일 플러시 실패")?;

    send_stream.write_all(b"OK").await.context("ACK 전송 실패")?;
    send_stream.finish().context("ACK 스트림 종료 실패")?;

    Ok((save_path, received_bytes, start_time.elapsed()))
}

/// QUIC 바이너리 스트림을 통해 지정된 크기(MB)의 더미 데이터를 고속 전송하여 실제 대역폭을 측정합니다.
pub async fn send_benchmark_stream(
    mut send_stream: iroh::endpoint::SendStream,
    mut recv_stream: iroh::endpoint::RecvStream,
    megabytes: usize,
) -> Result<(usize, std::time::Duration, f64)> {
    let total_bytes = megabytes * 1024 * 1024;
    let mut header = Vec::with_capacity(12);
    header.extend_from_slice(b"BNCH");
    header.extend_from_slice(&(total_bytes as u64).to_le_bytes());
    send_stream.write_all(&header).await.context("벤치마크 헤더 전송 실패")?;

    let chunk_size = 256 * 1024;
    let chunk = vec![0xEEu8; chunk_size];
    let start_time = std::time::Instant::now();
    let mut sent = 0usize;

    while sent < total_bytes {
        let to_send = chunk_size.min(total_bytes - sent);
        send_stream.write_all(&chunk[..to_send]).await.context("벤치마크 데이터 전송 실패")?;
        sent += to_send;
    }

    send_stream.finish().context("벤치마크 스트림 종료 실패")?;

    let mut ack = [0u8; 2];
    read_exact_stream(&mut recv_stream, &mut ack).await.context("벤치마크 ACK 대기 실패")?;
    let elapsed = start_time.elapsed();
    let speed_mbs = (megabytes as f64) / elapsed.as_secs_f64().max(0.001);

    Ok((megabytes, elapsed, speed_mbs))
}

/// QUIC 바이너리 스트림을 통해 벤치마크 데이터를 수신하고 수신 대역폭을 계산합니다.
pub async fn receive_benchmark_stream(
    send_stream: iroh::endpoint::SendStream,
    mut recv_stream: iroh::endpoint::RecvStream,
) -> Result<(f64, std::time::Duration, f64)> {
    let mut magic = [0u8; 4];
    read_exact_stream(&mut recv_stream, &mut magic).await.context("매직 바이트 읽기 실패")?;
    if &magic != b"BNCH" {
        anyhow::bail!("벤치마크 프로토콜 형식이 아닙니다.");
    }
    receive_benchmark_body(send_stream, recv_stream).await
}

async fn receive_benchmark_body(
    mut send_stream: iroh::endpoint::SendStream,
    mut recv_stream: iroh::endpoint::RecvStream,
) -> Result<(f64, std::time::Duration, f64)> {
    let mut size_buf = [0u8; 8];
    read_exact_stream(&mut recv_stream, &mut size_buf).await.context("벤치마크 크기 수신 실패")?;
    let total_bytes = u64::from_le_bytes(size_buf);

    let start_time = std::time::Instant::now();
    let mut buffer = vec![0u8; 256 * 1024];
    let mut received = 0u64;

    while received < total_bytes {
        let remaining = (total_bytes - received) as usize;
        let to_read = buffer.len().min(remaining);
        let n = match recv_stream.read(&mut buffer[..to_read]).await.context("벤치마크 데이터 수신 실패")? {
            Some(0) | None => break,
            Some(n) => n,
        };
        received += n as u64;
    }

    send_stream.write_all(b"OK").await.context("벤치마크 ACK 전송 실패")?;
    send_stream.finish().context("벤치마크 스트림 종료 실패")?;

    let elapsed = start_time.elapsed();
    let mb = received as f64 / (1024.0 * 1024.0);
    let speed_mbs = mb / elapsed.as_secs_f64().max(0.001);

    Ok((mb, elapsed, speed_mbs))
}

/// 백그라운드로 유입되는 새로운 양방향 스트림의 헤더 매직을 검사하여 파일 수신, 벤치마크, 화면 공유, 제어 스트림을 자동 디스패치합니다.
pub enum IncomingStreamResult {
    File { path: std::path::PathBuf, size: u64, duration: std::time::Duration },
    Benchmark { megabytes: f64, duration: std::time::Duration, speed_mbs: f64 },
    ScreenStream { send_stream: iroh::endpoint::SendStream, recv_stream: iroh::endpoint::RecvStream },
    ControlStream { send_stream: iroh::endpoint::SendStream, recv_stream: iroh::endpoint::RecvStream },
}

pub async fn dispatch_incoming_bi_stream<F>(
    send_stream: iroh::endpoint::SendStream,
    mut recv_stream: iroh::endpoint::RecvStream,
    save_dir: &std::path::Path,
    progress_callback: F,
) -> Result<IncomingStreamResult>
where
    F: FnMut(u64, u64, f64),
{
    let mut magic = [0u8; 4];
    read_exact_stream(&mut recv_stream, &mut magic).await.context("스트림 매직 읽기 실패")?;

    match &magic {
        b"FILE" => {
            let (path, size, duration) = receive_file_body(send_stream, recv_stream, save_dir, progress_callback).await?;
            Ok(IncomingStreamResult::File { path, size, duration })
        }
        b"BNCH" => {
            let (megabytes, duration, speed_mbs) = receive_benchmark_body(send_stream, recv_stream).await?;
            Ok(IncomingStreamResult::Benchmark { megabytes, duration, speed_mbs })
        }
        b"SCRN" => {
            Ok(IncomingStreamResult::ScreenStream { send_stream, recv_stream })
        }
        b"CTRL" => {
            Ok(IncomingStreamResult::ControlStream { send_stream, recv_stream })
        }
        _ => anyhow::bail!("알 수 없는 스트림 매직: {:?}", magic),
    }
}

