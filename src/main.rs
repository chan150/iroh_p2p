use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use iroh_p2p_example::{
    analyze_ping_distribution, create_endpoint, create_endpoint_with_secret_key, decode_ticket,
    derive_channel_keys, dispatch_incoming_bi_stream, encode_ticket, format_path_info,
    format_ping_distribution_report, format_stats_info, read_exact_stream,
    remote::{receive_screen_frame, RemoteControlEvent, ScreenStreamer, WindowsInputSimulator},
    send_benchmark_stream, send_file_stream, IncomingStreamResult, CHAT_ALPN,
};
use std::io::Write;
use tokio::io::AsyncBufReadExt;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

#[derive(Parser, Debug)]
#[command(name = "iroh-p2p-example")]
#[command(about = "공유기/방화벽 뒤에서도 동작하는 초고속 Iroh P2P 통신, 파일 전송, 실시간 화면 공유 및 원격 제어", long_about = None)]
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
    /// 실시간 화면 공유 서버 모드 (내 화면을 피어에게 스트리밍)
    Share {
        /// 수신 대기할 채널 번호 (기본값: 0)
        #[arg(default_value = "0")]
        channel: u32,
        /// 초당 프레임 수 (기본값: 30)
        #[arg(long, default_value = "30")]
        fps: u32,
        /// JPEG 압축 품질 (30 ~ 95, 기본값: 75)
        #[arg(long, default_value = "75")]
        quality: u8,
    },
    /// 원격 화면 수신 및 제어 뷰어 모드
    View {
        /// 접속할 채널 번호 또는 티켓 (기본값: 0)
        #[arg(default_value = "0")]
        target: String,
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
        Commands::Share { channel, fps, quality } => run_screen_sharer(channel, fps, quality).await?,
        Commands::View { target } => run_screen_viewer(target).await?,
    }

    Ok(())
}

fn print_session_guide() {
    println!("------------------------------------------------------------");
    println!(" 🚀 실시간 P2P 통신, 파일 전송, 화면 공유 및 원격 제어!");
    println!("  • 일반 텍스트 입력 후 Enter: 메시지 전송");
    println!("  • /send <파일경로>  : 로컬 파일을 상대방에게 초고속 스트리밍 전송");
    println!("  • /share [FPS] [품질] : 내 화면 실시간 공유 시작 (기본: 30 FPS, 75%)");
    println!("  • /mouse <x> <y>    : 원격 마우스 커서 이동 테스트 (비율 0.0 ~ 1.0)");
    println!("  • /click <L|R>      : 원격 마우스 클릭 테스트 (L: 좌클릭, R: 우클릭)");
    println!("  • /ping [횟수]      : 왕복 지연시간(RTT) 및 레이턴시 분포도 분석 (기본: 20회)");
    println!("  • /bench [MB]       : 대역폭(Throughput) 속도 측정 (기본: 10MB)");
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
                match dispatch_incoming_bi_stream(
                    send_stream,
                    recv_stream,
                    &save_dir_clone,
                    |current, total, speed_mbs| {
                        render_progress("수신 중", current, total, speed_mbs);
                    },
                )
                .await
                {
                    Ok(IncomingStreamResult::File { path, size, duration }) => {
                        println!("\n------------------------------------------------------------");
                        let speed_mbs = (size as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64().max(0.001);
                        println!(" ✅ [파일 수신 완료!]");
                        println!("  • 저장 경로 : {:?}", path);
                        println!("  • 크기      : {:.2} MB", size as f64 / (1024.0 * 1024.0));
                        println!("  • 소요 시간 : {:.2}초 ({:.2} MB/s)", duration.as_secs_f64(), speed_mbs);
                        println!("------------------------------------------------------------\n");
                    }
                    Ok(IncomingStreamResult::Benchmark { megabytes, duration, speed_mbs }) => {
                        println!("\n------------------------------------------------------------");
                        println!(" ✅ [바이너리 벤치마크 수신 완료]");
                        println!("  • 전송량    : {:.2} MB | 소요 시간: {:.2}초", megabytes, duration.as_secs_f64());
                        println!("  • ⚡ 대역폭 : {:.2} MB/s ({:.2} Mbps)", speed_mbs, speed_mbs * 8.0);
                        println!("------------------------------------------------------------\n");
                    }
                    Ok(IncomingStreamResult::ScreenStream { .. }) | Ok(IncomingStreamResult::ControlStream { .. }) => {
                        // 파일 전용 수신기에서는 화면/제어 스트림 무시
                    }
                    Err(e) => {
                        eprintln!("\n [오류] 스트림 처리 실패: {:?}", e);
                    }
                }
            });
        }
    }

    endpoint.close().await;
    Ok(())
}

/// [실시간 화면 공유 호스트 모드]
async fn run_screen_sharer(channel: u32, fps: u32, quality: u8) -> Result<()> {
    let (secret_key, _) = derive_channel_keys(channel);

    println!("============================================================");
    println!(" 🖥️ [Iroh P2P 실시간 화면 공유 호스트] - 채널 #{}", channel);
    println!(" 설정: FPS {}, JPEG 압축 품질 {}%", fps, quality);
    println!(" 상대방 뷰어 접속을 대기하고 있습니다...");
    println!(" 👉 .\\iroh-p2p-example.exe view {}", channel);
    println!("============================================================");

    let endpoint = create_endpoint_with_secret_key(Some(secret_key), vec![CHAT_ALPN.to_vec()]).await?;
    let input_sim = std::sync::Arc::new(WindowsInputSimulator::new());

    while let Some(incoming) = endpoint.accept().await {
        let conn = match incoming.await {
            Ok(c) => c,
            Err(e) => {
                eprintln!(" [오류] 연결 실패: {:?}", e);
                continue;
            }
        };

        println!("\n 🔗 [뷰어 접속 완료!] 화면 스트리밍을 시작합니다.");

        // 1. 화면 전송 스트림 시작
        let (send_screen, _recv) = conn.open_bi().await.context("화면 스트림 생성 실패")?;
        let streamer = ScreenStreamer::new(0, fps, quality)?;
        tokio::spawn(async move {
            if let Err(e) = streamer.start_stream(send_screen).await {
                eprintln!(" [화면 스트리밍 종료/오류]: {:?}", e);
            }
        });

        // 2. 원격 마우스/키보드 제어 이벤트 수신 루프
        let sim_clone = input_sim.clone();
        let conn_clone = conn.clone();
        tokio::spawn(async move {
            while let Ok((_, recv_ctrl)) = conn_clone.accept_bi().await {
                let sim = sim_clone.clone();
                tokio::spawn(async move {
                    let mut reader = tokio::io::BufReader::new(recv_ctrl);
                    let mut line_buf = String::new();
                    while let Ok(n) = reader.read_line(&mut line_buf).await {
                        if n == 0 {
                            break;
                        }
                        if let Some(event) = RemoteControlEvent::deserialize(&line_buf) {
                            sim.execute(event);
                        }
                        line_buf.clear();
                    }
                });
            }
        });
    }

    endpoint.close().await;
    Ok(())
}

/// [원격 화면 수신 뷰어 모드]
async fn run_screen_viewer(target_input: String) -> Result<()> {
    let trimmed = target_input.trim();
    let (remote_addr, mode_label) = if let Ok(channel) = trimmed.parse::<u32>() {
        let (_, target_addr) = derive_channel_keys(channel);
        (target_addr, format!("채널 #{} 화면 수신 뷰어", channel))
    } else {
        let target_addr = decode_ticket(trimmed)?;
        (target_addr, "티켓 화면 수신 뷰어".to_string())
    };

    println!("============================================================");
    println!(" 📺 [Iroh P2P 원격 뷰어] - {}", mode_label);
    println!(" 원격 화면 호스트에 연결 중...");
    println!("============================================================");

    let endpoint = create_endpoint(vec![]).await?;
    let conn = endpoint.connect(remote_addr, CHAT_ALPN).await.context("화면 호스트 연결 실패")?;

    println!(" 🔗 [연결 성공!] 실시간 프레임 수신을 시작합니다...\n");

    // 스트림 수신 대기
    while let Ok((_, mut recv)) = conn.accept_bi().await {
        let mut magic = [0u8; 4];
        if read_exact_stream(&mut recv, &mut magic).await.is_err() {
            continue;
        }

        if &magic == b"SCRN" {
            let mut frame_count = 0u64;
            let mut total_bytes = 0u64;
            let start_time = std::time::Instant::now();
            let mut last_log = std::time::Instant::now();

            loop {
                match receive_screen_frame(&mut recv).await {
                    Ok(frame) => {
                        frame_count += 1;
                        total_bytes += frame.jpeg_data.len() as u64;

                        let now = std::time::Instant::now();
                        if now.duration_since(last_log).as_millis() >= 500 {
                            let elapsed = start_time.elapsed().as_secs_f64().max(0.001);
                            let current_fps = frame_count as f64 / elapsed;
                            let bitrate_mbps = (total_bytes as f64 * 8.0 / (1024.0 * 1024.0)) / elapsed;
                            print!(
                                "\r 🖥️ [화면 수신 중] 해상도: {}x{} | 프레임 #{:<6} | 실측 FPS: {:>4.1} | 비트레이트: {:>5.2} Mbps  ",
                                frame.width, frame.height, frame.frame_seq, current_fps, bitrate_mbps
                            );
                            let _ = std::io::stdout().flush();
                            last_log = now;
                        }
                    }
                    Err(e) => {
                        println!("\n [알림] 화면 스트림이 종료되었습니다: {:?}", e);
                        break;
                    }
                }
            }
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

/// 양방향 스트림을 통한 실시간 텍스트 채팅, 파일 전송, 화면 공유 및 원격 제어 처리
async fn handle_chat_session(
    conn: &iroh::endpoint::Connection,
    send_stream: iroh::endpoint::SendStream,
    recv_stream: iroh::endpoint::RecvStream,
) -> Result<()> {
    let mut framed_send = FramedWrite::new(send_stream, LinesCodec::new());
    let mut framed_recv = FramedRead::new(recv_stream, LinesCodec::new());

    let mut stdin_lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut ping_stats: Vec<u128> = Vec::new();
    let input_sim = std::sync::Arc::new(WindowsInputSimulator::new());

    // 백그라운드 스트림 수신 태스크: 파일 전송, 고속 벤치마크, 화면 수신, 원격 제어 디스패치
    let conn_clone = conn.clone();
    let sim_clone = input_sim.clone();
    tokio::spawn(async move {
        while let Ok((send, recv)) = conn_clone.accept_bi().await {
            let sim = sim_clone.clone();
            tokio::spawn(async move {
                match dispatch_incoming_bi_stream(
                    send,
                    recv,
                    std::path::Path::new("received"),
                    |current, total, speed_mbs| {
                        render_progress("수신 중", current, total, speed_mbs);
                    },
                )
                .await
                {
                    Ok(IncomingStreamResult::File { path, size, duration }) => {
                        println!("\n------------------------------------------------------------");
                        let speed_mbs = (size as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64().max(0.001);
                        println!(" ✅ [파일 수신 완료!]");
                        println!("  • 저장 위치 : {:?}", path);
                        println!("  • 파일 크기 : {:.2} MB", size as f64 / (1024.0 * 1024.0));
                        println!("  • 소요 시간 : {:.2}초 ({:.2} MB/s)", duration.as_secs_f64(), speed_mbs);
                        println!("------------------------------------------------------------\n");
                    }
                    Ok(IncomingStreamResult::Benchmark { megabytes, duration, speed_mbs }) => {
                        println!("\n------------------------------------------------------------");
                        println!(" ✅ [바이너리 벤치마크 수신 완료]");
                        println!("  • 데이터 수신량 : {:.2} MB", megabytes);
                        println!("  • 소요 시간     : {:.2}초", duration.as_secs_f64());
                        println!("  • ⚡ [수신 대역폭] 👉 {:.2} MB/s ({:.2} Mbps)", speed_mbs, speed_mbs * 8.0);
                        println!("------------------------------------------------------------\n");
                    }
                    Ok(IncomingStreamResult::ScreenStream { mut recv_stream, .. }) => {
                        println!("\n 🖥️ [알림] 상대방이 화면 공유 스트리밍을 시작했습니다...");
                        tokio::spawn(async move {
                            let mut count = 0u64;
                            while let Ok(_frame) = receive_screen_frame(&mut recv_stream).await {
                                count += 1;
                                if count % 30 == 0 {
                                    print!("\r 🖥️ [화면 수신 중] 총 {} 프레임 수신됨...  ", count);
                                    let _ = std::io::stdout().flush();
                                }
                            }
                        });
                    }
                    Ok(IncomingStreamResult::ControlStream { recv_stream, .. }) => {
                        tokio::spawn(async move {
                            let mut reader = tokio::io::BufReader::new(recv_stream);
                            let mut line_buf = String::new();
                            while let Ok(n) = reader.read_line(&mut line_buf).await {
                                if n == 0 { break; }
                                if let Some(event) = RemoteControlEvent::deserialize(&line_buf) {
                                    sim.execute(event);
                                }
                                line_buf.clear();
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("\n [오류] 스트림 수신 처리 중 오류: {:?}", e);
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
                        } else if trimmed.starts_with("/share") {
                            let parts: Vec<&str> = trimmed.split_whitespace().collect();
                            let fps: u32 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(30).clamp(5, 60);
                            let quality: u8 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(75).clamp(30, 95);

                            match ScreenStreamer::new(0, fps, quality) {
                                Ok(streamer) => {
                                    match conn.open_bi().await {
                                        Ok((send, _recv)) => {
                                            tokio::spawn(async move {
                                                if let Err(e) = streamer.start_stream(send).await {
                                                    eprintln!(" [화면 공유 에러]: {:?}", e);
                                                }
                                            });
                                            println!(" 🖥️ [화면 공유 시작] 상대방에게 내 화면 실시간 스트리밍을 시작했습니다! (FPS: {}, 품질: {}%)", fps, quality);
                                        }
                                        Err(e) => eprintln!(" [오류] 화면 공유 스트림 열기 실패: {:?}", e),
                                    }
                                }
                                Err(e) => eprintln!(" [오류] 화면 캡처 초기화 실패: {:?}", e),
                            }
                        } else if trimmed.starts_with("/mouse") {
                            let parts: Vec<&str> = trimmed.split_whitespace().collect();
                            if parts.len() >= 3 {
                                let x: f32 = parts[1].parse().unwrap_or(0.5);
                                let y: f32 = parts[2].parse().unwrap_or(0.5);
                                let event = RemoteControlEvent::MouseMove { x, y };
                                match conn.open_bi().await {
                                    Ok((mut send, _)) => {
                                        let mut buf = Vec::new();
                                        buf.extend_from_slice(b"CTRL");
                                        let _ = send.write_all(&buf).await;
                                        let _ = send.write_all(format!("{}\n", event.serialize()).as_bytes()).await;
                                        let _ = send.finish();
                                        println!(" 🖱️ [원격 마우스 이동 전송] x: {:.2}, y: {:.2}", x, y);
                                    }
                                    Err(e) => eprintln!(" [오류] 제어 스트림 열기 실패: {:?}", e),
                                }
                            } else {
                                println!(" [사용법] /mouse <x비율 0.0~1.0> <y비율 0.0~1.0> (예: /mouse 0.5 0.5)");
                            }
                        } else if trimmed.starts_with("/click") {
                            let parts: Vec<&str> = trimmed.split_whitespace().collect();
                            let btn_str = parts.get(1).copied().unwrap_or("L");
                            let button = if btn_str.eq_ignore_ascii_case("R") {
                                iroh_p2p_example::remote::MouseButton::Right
                            } else {
                                iroh_p2p_example::remote::MouseButton::Left
                            };
                            match conn.open_bi().await {
                                Ok((mut send, _)) => {
                                    let mut buf = Vec::new();
                                    buf.extend_from_slice(b"CTRL");
                                    let _ = send.write_all(&buf).await;
                                    let event_down = RemoteControlEvent::MouseDown { button };
                                    let event_up = RemoteControlEvent::MouseUp { button };
                                    let _ = send.write_all(format!("{}\n{}\n", event_down.serialize(), event_up.serialize()).as_bytes()).await;
                                    let _ = send.finish();
                                    println!(" 🖱️ [원격 마우스 클릭 전송] {:?}", button);
                                }
                                Err(e) => eprintln!(" [오류] 제어 스트림 열기 실패: {:?}", e),
                            }
                        } else if trimmed.starts_with("/bench") {
                            let parts: Vec<&str> = trimmed.split_whitespace().collect();
                            let mb: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(10).clamp(1, 200);

                            println!(" 🚀 [고속 대역폭 측정] {} MB 바이너리 스트리밍 전송을 시작합니다...", mb);
                            match conn.open_bi().await {
                                Ok((send, recv)) => {
                                    match send_benchmark_stream(send, recv, mb).await {
                                        Ok((megabytes, elapsed, speed_mbs)) => {
                                            println!("------------------------------------------------------------");
                                            println!(" ✅ [송신 완료] 총 전송량: {} MB | 소요 시간: {:.2}초", megabytes, elapsed.as_secs_f64());
                                            println!(" ⚡ [송신 대역폭] 👉 {:.2} MB/s ({:.2} Mbps)", speed_mbs, speed_mbs * 8.0);
                                            println!("------------------------------------------------------------\n");
                                        }
                                        Err(e) => {
                                            eprintln!(" [오류] 벤치마크 전송 실패: {:?}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!(" [오류] 벤치마크 스트림 생성 실패: {:?}", e);
                                }
                            }
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



