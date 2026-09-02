use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use iroh_p2p_example::{
    analyze_ping_distribution, create_endpoint, create_endpoint_with_secret_key, decode_ticket,
    derive_channel_keys, encode_ticket, format_path_info, format_ping_distribution_report,
    format_stats_info, receive_file_stream, send_file_stream, CHAT_ALPN,
};
use std::io::Write;
use tokio::io::AsyncBufReadExt;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

#[derive(Parser, Debug)]
#[command(name = "iroh-p2p-example")]
#[command(about = "공유기/방화벽 뒤에서도 동작하는 초고속 Iroh P2P 통신 및 파일 전송 소프트웨어", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// P2P 연결 수신 대기 (Host 모드, 채널 번호 기본값: 0)
    Listen {
        /// 고정 채널 번호 (예: 0, 1, 2, 3...)
        #[arg(default_value = "0")]
        channel: u32,
    },
    /// 원격 피어에 연결 (Client 모드, 채널 번호 또는 티켓 입력)
    Connect {
        /// 접속할 채널 번호(예: 0, 1, 2) 또는 상대방의 연결 티켓 (생략 시 기본 채널 0)
        #[arg(default_value = "0")]
        target: String,
    },
    /// 단일 파일 전송 (원클릭 파일 전송 모드)
    Send {
        /// 대상 채널 번호(예: 0, 1, 2) 또는 상대방의 티켓
        #[arg(default_value = "0")]
        target: String,
        /// 전송할 로컬 파일 경로
        file: std::path::PathBuf,
    },
    /// 파일 수신 대기 전용 모드
    Recv {
        /// 수신 대기할 채널 번호 (기본값: 0)
        #[arg(default_value = "0")]
        channel: u32,
        /// 저장할 폴더 경로 (기본값: ./received)
        #[arg(short, long)]
        save_dir: Option<std::path::PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Listen { channel } => run_listener(channel).await?,
        Commands::Connect { target } => run_connector(target).await?,
        Commands::Send { target, file } => run_file_sender(target, file).await?,
        Commands::Recv { channel, save_dir } => run_file_receiver(channel, save_dir).await?,
    }

    Ok(())
}

fn print_session_guide() {
    println!("------------------------------------------------------------");
    println!(" 🚀 실시간 P2P 통신 및 파일 전송 준비 완료!");
    println!("  • 일반 텍스트 입력 후 Enter: 메시지 전송");
    println!("  • /send <파일경로>  : 로컬 파일을 상대방에게 초고속 스트리밍 전송");
    println!("  • /ping [횟수]      : 왕복 지연시간(RTT) 및 레이턴시 분포도 분석 (기본: 20회)");
    println!("  • /bench [MB]       : 대역폭(Throughput) 속도 측정 (기본: 5MB)");
    println!("  • /stats            : QUIC 연결 상태 및 패킷 손실 통계");
    println!("  • /help             : 명령어 안내 | /quit : 대화 종료");
    println!("------------------------------------------------------------\n");
}

/// [수신 대기 모드]
/// 채널 번호(0, 1, 2...) 기반 고정 키로 Endpoint를 생성하여 수신 대기
async fn run_listener(channel: u32) -> Result<()> {
    let (secret_key, _) = derive_channel_keys(channel);

    println!("============================================================");
    println!(" [Iroh P2P Host / Listener] - 채널 #{}", channel);
    println!(" Endpoint를 초기화하고 Relay 서버 및 NAT 주소를 탐색 중입니다...");
    println!("============================================================");

    let endpoint = create_endpoint_with_secret_key(Some(secret_key), vec![CHAT_ALPN.to_vec()]).await?;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let my_addr = endpoint.addr();
    let ticket = encode_ticket(&my_addr)?;

    // 편의를 위해 ticket.txt 파일로도 저장
    let _ = std::fs::write("ticket.txt", &ticket);

    println!("\n 나의 Endpoint ID : {}", endpoint.id());
    println!(" 탐지된 IP 목록   : {:?}", my_addr.addrs);
    println!("------------------------------------------------------------");
    println!(" 🔒 [채널 모드 활성화]");
    println!(" 상대방은 티켓 입력 없이 아래 명령어만 치면 즉시 연결됩니다:");
    println!(" 👉 .\\iroh-p2p-example.exe connect {}", channel);
    println!("------------------------------------------------------------");
    println!(" (참고용 전체 티켓은 ticket.txt 파일에 저장되었습니다)");
    println!("\n 상대방의 연결을 대기하고 있습니다... (Ctrl+C 로 취소)");

    // 2. 상대방의 연결을 계속 수신 대기하는 루프
    while let Some(incoming) = endpoint.accept().await {
        let conn = match incoming.await {
            Ok(c) => c,
            Err(e) => {
                eprintln!(" [오류] QUIC 핸드셰이크 실패: {:?}", e);
                continue;
            }
        };

        println!("\n============================================================");
        println!(" [연결 성공!] 원격 피어와 연결되었습니다.");
        println!(" 원격 Node ID: {}", conn.remote_id());
        println!(" 연결 경로 유형: {}", format_path_info(&conn));
        println!("============================================================");

        // 3. 스트림 수락 및 대화 시작
        let (send_stream, recv_stream) = match conn.accept_bi().await {
            Ok(streams) => streams,
            Err(e) => {
                eprintln!(" [오류] 양방향 스트림 수락 실패: {:?}", e);
                continue;
            }
        };

        print_session_guide();

        if let Err(e) = handle_chat_session(&conn, send_stream, recv_stream).await {
            eprintln!(" [오류] 채팅 세션 중 에러 발생: {:?}", e);
        }

        println!("\n============================================================");
        println!(" [대기 중] 클라이언트 세션이 종료되었습니다.");
        println!(" 채널 #{} 번으로 새로운 연결을 계속 대기합니다... (Ctrl+C 로 종료)", channel);
        println!("============================================================\n");
    }

    endpoint.close().await;
    Ok(())
}

/// [연결 모드]
/// 채널 번호(예: 0, 1, 2) 또는 티켓 문자열을 통해 대상 피어에 연결
async fn run_connector(target_input: String) -> Result<()> {
    let trimmed = target_input.trim();

    // 1. 숫자인 경우 (채널 번호 모드 -> 티켓 입력 필요 없음!)
    let (remote_addr, mode_label) = if let Ok(channel) = trimmed.parse::<u32>() {
        let (_, target_addr) = derive_channel_keys(channel);
        (target_addr, format!("채널 #{} 자동 접속 모드", channel))
    } else {
        // 2. 문자열/티켓 파일 경로인 경우 (티켓 디코딩 모드)
        let target_addr = decode_ticket(trimmed)?;
        (target_addr, "티켓 수동 접속 모드".to_string())
    };

    println!("============================================================");
    println!(" [Iroh P2P Connector] - {}", mode_label);
    println!(" 대상 Node ID: {}", remote_addr.id);
    println!(" Endpoint를 초기화하고 P2P / Relay 연결을 시도합니다...");
    println!("============================================================");

    let endpoint = create_endpoint(vec![]).await?;

    let conn = endpoint
        .connect(remote_addr, CHAT_ALPN)
        .await
        .context("원격 피어 연결 실패")?;

    println!("\n============================================================");
    println!(" [연결 성공!] 원격 피어와 연결되었습니다.");
    println!(" 원격 Node ID: {}", conn.remote_id());
    println!(" 연결 경로 유형: {}", format_path_info(&conn));
    println!("============================================================");

    let (send_stream, recv_stream) = conn.open_bi().await.context("양방향 스트림 열기 실패")?;

    print_session_guide();

    handle_chat_session(&conn, send_stream, recv_stream).await?;

    endpoint.close().await;
    Ok(())
}

/// [단일 파일 전송 모드]
/// 대상 피어(채널 또는 티켓)로 연결하여 지정된 로컬 파일을 전송하고 종료
async fn run_file_sender(target_input: String, file_path: std::path::PathBuf) -> Result<()> {
    if !file_path.exists() {
        anyhow::bail!("전송할 파일이 존재하지 않습니다: {:?}", file_path);
    }

    let trimmed = target_input.trim();
    let (remote_addr, mode_label) = if let Ok(channel) = trimmed.parse::<u32>() {
        let (_, target_addr) = derive_channel_keys(channel);
        (target_addr, format!("채널 #{} 파일 전송", channel))
    } else {
        let target_addr = decode_ticket(trimmed)?;
        (target_addr, "티켓 수동 파일 전송".to_string())
    };

    println!("============================================================");
    println!(" 🚀 [Iroh P2P 파일 전송기] - {}", mode_label);
    println!(" 대상 Node ID: {}", remote_addr.id);
    println!(" 전송 파일   : {:?}", file_path);
    println!("============================================================");

    let endpoint = create_endpoint(vec![]).await?;
    let conn = endpoint.connect(remote_addr, CHAT_ALPN).await.context("원격 피어 연결 실패")?;

    println!(" 🔗 [연결 완료] 파일 스트리밍 전송을 시작합니다...");
    let (send_stream, recv_stream) = conn.open_bi().await.context("파일 전송 스트림 생성 실패")?;

    let (file_name, size, elapsed) = send_file_stream(
        send_stream,
        recv_stream,
        &file_path,
        |current, total, speed_mbs| {
            render_progress("전송 중", current, total, speed_mbs);
        },
    )
    .await?;

    println!("\n------------------------------------------------------------");
    let speed_mbs = (size as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(0.001);
    println!(" ✅ [파일 전송 완료!]");
    println!("  • 파일명    : {}", file_name);
    println!("  • 크기      : {:.2} MB ({} bytes)", size as f64 / (1024.0 * 1024.0), size);
    println!("  • 소요 시간 : {:.2}초", elapsed.as_secs_f64());
    println!("  • 평균 속도 : {:.2} MB/s ({:.2} Mbps)", speed_mbs, speed_mbs * 8.0);
    println!("============================================================");

    endpoint.close().await;
    Ok(())
}

/// [파일 수신 전용 모드]
/// 채널 번호로 수신 대기하며, 전달받는 모든 파일을 지정 폴더에 자동 저장
async fn run_file_receiver(channel: u32, save_dir_opt: Option<std::path::PathBuf>) -> Result<()> {
    let save_dir = save_dir_opt.unwrap_or_else(|| std::path::PathBuf::from("received"));
    let (secret_key, _) = derive_channel_keys(channel);

    println!("============================================================");
    println!(" 📥 [Iroh P2P 파일 수신기] - 채널 #{}", channel);
    println!(" 저장 폴더   : {:?}", save_dir);
    println!(" 파일 수신을 대기하고 있습니다... (Ctrl+C 로 종료)");
    println!("============================================================");

    let endpoint = create_endpoint_with_secret_key(Some(secret_key), vec![CHAT_ALPN.to_vec()]).await?;

    while let Some(incoming) = endpoint.accept().await {
        let conn = match incoming.await {
            Ok(c) => c,
            Err(e) => {
                eprintln!(" [오류] 연결 수락 실패: {:?}", e);
                continue;
            }
        };

        println!("\n 🔗 원격 피어 접속 감지: {}", conn.remote_id());

        // 스트림 수신 루프
        while let Ok((send_stream, recv_stream)) = conn.accept_bi().await {
            let save_dir_clone = save_dir.clone();
            tokio::spawn(async move {
                match receive_file_stream(
                    send_stream,
                    recv_stream,
                    &save_dir_clone,
                    |current, total, speed_mbs| {
                        render_progress("수신 중", current, total, speed_mbs);
                    },
                )
                .await
                {
                    Ok((path, size, elapsed)) => {
                        println!("\n------------------------------------------------------------");
                        let speed_mbs = (size as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(0.001);
                        println!(" ✅ [파일 수신 완료!]");
                        println!("  • 저장 경로 : {:?}", path);
                        println!("  • 크기      : {:.2} MB", size as f64 / (1024.0 * 1024.0));
                        println!("  • 소요 시간 : {:.2}초 ({:.2} MB/s)", elapsed.as_secs_f64(), speed_mbs);
                        println!("------------------------------------------------------------\n");
                    }
                    Err(e) => {
                        eprintln!("\n [오류] 파일 수신 실패: {:?}", e);
                    }
                }
            });
        }
    }

    endpoint.close().await;
    Ok(())
}

fn render_progress(action: &str, current: u64, total: u64, speed_mbs: f64) {
    let pct = if total > 0 {
        (current as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    let cur_mb = current as f64 / (1024.0 * 1024.0);
    let tot_mb = total as f64 / (1024.0 * 1024.0);
    let bar_filled = (pct / 5.0).clamp(0.0, 20.0) as usize;
    let bar_empty = 20 - bar_filled;
    let bar = format!("[{}{}]", "█".repeat(bar_filled), "░".repeat(bar_empty));
    print!(
        "\r {} {:.2}MB / {:.2}MB {} {:>5.1}% ({:.2} MB/s)  ",
        action, cur_mb, tot_mb, bar, pct, speed_mbs
    );
    let _ = std::io::stdout().flush();
}

/// 양방향 스트림을 통한 실시간 텍스트 채팅 및 대화 중 파일 전송 처리
async fn handle_chat_session(
    conn: &iroh::endpoint::Connection,
    send_stream: iroh::endpoint::SendStream,
    recv_stream: iroh::endpoint::RecvStream,
) -> Result<()> {
    let mut framed_send = FramedWrite::new(send_stream, LinesCodec::new());
    let mut framed_recv = FramedRead::new(recv_stream, LinesCodec::new());

    let mut stdin_lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut bench_receive_start: Option<(std::time::Instant, usize)> = None;
    let mut ping_stats: Vec<u128> = Vec::new();

    // 백그라운드 파일 수신 태스크: 채팅 중에 상대방이 /send 로 파일을 전송하면 자동으로 수신
    let conn_clone = conn.clone();
    tokio::spawn(async move {
        while let Ok((send, recv)) = conn_clone.accept_bi().await {
            tokio::spawn(async move {
                println!("\n 📥 [알림] 상대방으로부터 파일 수신을 시작합니다...");
                match receive_file_stream(
                    send,
                    recv,
                    std::path::Path::new("received"),
                    |current, total, speed_mbs| {
                        render_progress("수신 중", current, total, speed_mbs);
                    },
                )
                .await
                {
                    Ok((path, size, elapsed)) => {
                        println!("\n------------------------------------------------------------");
                        let speed_mbs = (size as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(0.001);
                        println!(" ✅ [파일 수신 완료!]");
                        println!("  • 저장 위치 : {:?}", path);
                        println!("  • 파일 크기 : {:.2} MB", size as f64 / (1024.0 * 1024.0));
                        println!("  • 소요 시간 : {:.2}초 ({:.2} MB/s)", elapsed.as_secs_f64(), speed_mbs);
                        println!("------------------------------------------------------------\n");
                    }
                    Err(e) => {
                        eprintln!("\n [오류] 파일 수신 중 오류 발생: {:?}", e);
                    }
                }
            });
        }
    });

    loop {
        tokio::select! {
            // 콘솔 사용자 입력 처리
            line = stdin_lines.next_line() => {
                match line {
                    Ok(Some(msg)) => {
                        let trimmed = msg.trim();
                        if trimmed == "/quit" {
                            println!(" [알림] 대화를 종료합니다.");
                            let _ = framed_send.send("[상대방이 대화를 종료했습니다]".to_string()).await;
                            break;
                        } else if trimmed == "/help" {
                            print_session_guide();
                        } else if trimmed == "/stats" {
                            println!("------------------------------------------------------------");
                            println!(" 📊 [QUIC 연결 상태 및 통계]");
                            println!(" 경로 상태 : {}", format_path_info(conn));
                            println!(" 통계 요약 : {}", format_stats_info(conn));
                            println!("------------------------------------------------------------");
                        } else if trimmed.starts_with("/send") || trimmed.starts_with("/file") {
                            // 대화 중 파일 전송 명령 (/send <경로>)
                            let parts: Vec<&str> = trimmed.splitn(2, ' ').collect();
                            if parts.len() < 2 {
                                println!(" [사용법] /send <전송할_파일_경로> (예: /send D:\\photo.png)");
                                continue;
                            }
                            let raw_path = parts[1].trim().trim_matches('"');
                            let path = std::path::PathBuf::from(raw_path);

                            if !path.exists() {
                                println!(" [오류] 지정한 파일이 존재하지 않습니다: {:?}", path);
                                continue;
                            }

                            println!(" 🚀 [파일 전송 시작] {:?}", path);
                            match conn.open_bi().await {
                                Ok((send, recv)) => {
                                    match send_file_stream(send, recv, &path, |current, total, speed_mbs| {
                                        render_progress("전송 중", current, total, speed_mbs);
                                    }).await {
                                        Ok((name, size, elapsed)) => {
                                            println!("\n------------------------------------------------------------");
                                            let speed_mbs = (size as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(0.001);
                                            println!(" ✅ [전송 완료] '{}' ({:.2} MB) 전송 완료 (속도: {:.2} MB/s)", name, size as f64 / (1024.0 * 1024.0), speed_mbs);
                                            println!("------------------------------------------------------------\n");
                                        }
                                        Err(e) => {
                                            eprintln!("\n [오류] 파일 전송 실패: {:?}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!(" [오류] 파일 전송용 스트림 생성 실패: {:?}", e);
                                }
                            }
                        } else if trimmed.starts_with("/ping") {
                            let parts: Vec<&str> = trimmed.split_whitespace().collect();
                            let count: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(20).clamp(1, 500);

                            ping_stats.clear();
                            println!(" ⏱️ [PING] {}회 왕복 지연시간 측정 및 분포 분석을 시작합니다...", count);
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis();
                            let _ = framed_send.send(format!("__PING__:1:{}:{}", now, count)).await;
                        } else if trimmed.starts_with("/bench") {
                            let parts: Vec<&str> = trimmed.split_whitespace().collect();
                            let mb: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(5).clamp(1, 50);

                            println!(" 🚀 [대역폭 측정] {} MB 데이터 전송 속도를 측정합니다...", mb);
                            let chunk_size = 64 * 1024; // 64KB 청크
                            let chunk = "X".repeat(chunk_size);
                            let chunk_msg = format!("__BENCH_CHUNK__:{}", chunk);
                            let total_bytes = mb * 1024 * 1024;
                            let num_chunks = total_bytes / chunk_size;

                            let _ = framed_send.send(format!("__BENCH_START__:{}", total_bytes)).await;
                            let start = std::time::Instant::now();
                            for _ in 0..num_chunks {
                                if let Err(e) = framed_send.send(chunk_msg.clone()).await {
                                    eprintln!(" [오류] 벤치마크 전송 중 에러: {:?}", e);
                                    break;
                                }
                            }
                            let _ = framed_send.send("__BENCH_END__:done".to_string()).await;
                            let elapsed = start.elapsed().as_secs_f64();
                            let speed_mbs = (mb as f64) / elapsed;
                            let speed_mbps = speed_mbs * 8.0;

                            println!("------------------------------------------------------------");
                            println!(" ✅ [송신 완료] 총 전송량: {} MB | 소요 시간: {:.2}초", mb, elapsed);
                            println!(" ⚡ [송신 대역폭] 👉 {:.2} MB/s ({:.2} Mbps)", speed_mbs, speed_mbps);
                            println!("------------------------------------------------------------");
                        } else if !trimmed.is_empty() {
                            if let Err(e) = framed_send.send(trimmed.to_string()).await {
                                eprintln!(" [오류] 메시지 전송 실패: {:?}", e);
                                break;
                            }
                        }
                    }
                    Ok(None) => break, // EOF (Ctrl+D / Ctrl+Z)
                    Err(e) => {
                        eprintln!(" [오류] 입력 읽기 실패: {:?}", e);
                        break;
                    }
                }
            }
            // 원격 피어로부터 메시지/명령 수신 처리
            incoming_msg = framed_recv.next() => {
                match incoming_msg {
                    Some(Ok(msg)) => {
                        if msg.starts_with("__PING__:") {
                            let pong = msg.replace("__PING__:", "__PONG__:");
                            let _ = framed_send.send(pong).await;
                        } else if msg.starts_with("__PONG__:") {
                            let parts: Vec<&str> = msg.split(':').collect();
                            if parts.len() >= 4 {
                                let seq: usize = parts[1].parse().unwrap_or(1);
                                let ts: u128 = parts[2].parse().unwrap_or(0);
                                let total: usize = parts[3].parse().unwrap_or(20);

                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();
                                let rtt = now.saturating_sub(ts);
                                ping_stats.push(rtt);
                                println!(" 🎯 [Ping #{}/{}] RTT: {} ms", seq, total, rtt);

                                if seq < total {
                                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                                    let next_now = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis();
                                    let _ = framed_send.send(format!("__PING__:{}:{}:{}", seq + 1, next_now, total)).await;
                                } else {
                                    if let Some(report) = analyze_ping_distribution(ping_stats.clone(), total) {
                                        println!("\n{}", format_ping_distribution_report(&report));
                                    }
                                }
                            }
                        } else if msg.starts_with("__BENCH_START__:") {
                            bench_receive_start = Some((std::time::Instant::now(), 0));
                            println!(" 📥 [성능 측정] 상대방이 보낸 대역폭 벤치마크 데이터를 수신 중입니다...");
                        } else if msg.starts_with("__BENCH_CHUNK__:") {
                            if let Some((_, total)) = &mut bench_receive_start {
                                *total += msg.len();
                            }
                        } else if msg.starts_with("__BENCH_END__:") {
                            if let Some((start, total)) = bench_receive_start.take() {
                                let elapsed = start.elapsed().as_secs_f64();
                                let mbytes = total as f64 / (1024.0 * 1024.0);
                                let speed_mbs = mbytes / elapsed;
                                let speed_mbps = speed_mbs * 8.0;

                                println!("------------------------------------------------------------");
                                println!(" ✅ [수신 완료] 총 수신량: {:.2} MB | 소요 시간: {:.2}초", mbytes, elapsed);
                                println!(" ⚡ [수신 대역폭] 👉 {:.2} MB/s ({:.2} Mbps)", speed_mbs, speed_mbps);
                                println!("------------------------------------------------------------");
                                let _ = framed_send.send(format!("__BENCH_REPORT__:{:.2}:{:.2}:{:.2}", mbytes, elapsed, speed_mbs)).await;
                            }
                        } else if msg.starts_with("__BENCH_REPORT__:") {
                            let parts: Vec<&str> = msg.split(':').collect();
                            if parts.len() >= 4 {
                                println!(" ℹ️ [상대방 측정 결과] 수신량: {} MB | 시간: {}s | 대역폭: {} MB/s", parts[1], parts[2], parts[3]);
                            }
                        } else {
                            println!(" [상대방] {}", msg);
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!(" [오류] 메시지 수신 오류: {:?}", e);
                        break;
                    }
                    None => {
                        println!("\n [알림] 상대방이 연결을 종료했습니다.");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}



