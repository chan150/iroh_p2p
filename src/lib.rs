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

/// Iroh Endpoint를 생성합니다.
/// n0 글로벌 Relay 서버(DERP)를 기본 활성화하되,
/// 불필요한 백그라운드 Pkarr/DNS 게시 시도(dns.iroh.link)를 방지하기 위해 presets::Minimal + RelayMode::Default를 사용합니다.
pub async fn create_endpoint(alpns: Vec<Vec<u8>>) -> Result<Endpoint> {
    // 로컬 공유기/사설 DNS 서버의 쿼리 거부(Query refused) 문제를 방지하기 위해
    // 신뢰할 수 있는 공용 DNS(1.1.1.1:53)를 기본 네임서버로 설정
    let dns_resolver = iroh::dns::DnsResolver::with_nameserver("1.1.1.1:53".parse().unwrap());

    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Default)
        .dns_resolver(dns_resolver)
        .alpns(alpns)
        .bind()
        .await
        .context("Iroh Endpoint 바인딩 실패")?;

    // 릴레이 서버 핸드셰이크 및 공인/사설 네트워크 주소 준비 완료 대기 (최대 5초)
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), endpoint.online()).await;

    Ok(endpoint)
}
