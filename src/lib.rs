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
