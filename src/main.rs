use std::io::{self, Write};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use iroh_p2p_example::{create_endpoint, decode_ticket, encode_ticket, format_path_info, CHAT_ALPN};
use tokio::io::AsyncBufReadExt;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

#[derive(Parser, Debug)]
#[command(name = "iroh-p2p-example")]
#[command(about = "공유기/방화벽 뒤에서도 동작하는 Iroh P2P 통신 예제", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// P2P 연결 수신 대기 (Host 모드)
    Listen,
    /// 티켓을 사용해 원격 피어에 연결 (Client 모드)
    Connect {
        /// 상대방이 발급한 연결 티켓 (또는 생략 시 콘솔에서 입력)
        ticket: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // 로깅 초기화 (필요시 RUST_LOG 환경변수로 제어 가능)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Listen => run_listener().await?,
        Commands::Connect { ticket } => run_connector(ticket).await?,
    }

    Ok(())
}

/// [수신 대기 모드]
/// Iroh Endpoint를 생성하고, Relay 및 NAT 주소를 등록한 후 수신 대기
async fn run_listener() -> Result<()> {
    println!("============================================================");
    println!(" [Iroh P2P Host / Listener]");
    println!(" Endpoint를 초기화하고 Relay 서버 및 NAT 주소를 탐색 중입니다...");
    println!("============================================================");

    // 1. presets::N0를 사용하여 Endpoint 생성
    // presets::N0는 n0 글로벌 DERP Relay 서버와 Pkarr/DNS 주소 탐색 기능을 자동으로 활성화합니다.
    let endpoint = create_endpoint(vec![CHAT_ALPN.to_vec()]).await?;

    // Relay 서버와의 연결 및 로컬/공인 주소 탐색이 완료될 때까지 잠시 대기
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let my_addr = endpoint.addr();
    let ticket = encode_ticket(&my_addr)?;

    // 편의를 위해 ticket.txt 파일로도 저장
    let _ = std::fs::write("ticket.txt", &ticket);

    println!("\n 나의 Endpoint ID: {}", endpoint.id());
    println!(" 탐지된 로컬/공인 IP 목록: {:?}", my_addr.addrs);
    println!("\n------------------------------------------------------------");
    println!(" 아래의 [연결 티켓]을 복사해서 상대방에게 전달하세요:");
    println!(" (현재 디렉터리의 ticket.txt 파일에도 저장되었습니다)");
    println!("\n{}", ticket);
    println!("------------------------------------------------------------");
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

        println!("\n [연결 성공!] 원격 피어와 연결되었습니다.");
        println!(" 원격 Node ID: {}", conn.remote_id());
        println!(" 연결 경로 유형: {}", format_path_info(&conn));

        // 3. 스트림 수락 및 대화 시작
        let (send_stream, recv_stream) = match conn.accept_bi().await {
            Ok(streams) => streams,
            Err(e) => {
                eprintln!(" [오류] 양방향 스트림 수락 실패: {:?}", e);
                continue;
            }
        };

        println!("------------------------------------------------------------");
        println!(" 실시간 대화가 시작되었습니다. 메시지를 입력 후 Enter를 누르세요.");
        println!(" 대화를 종료하려면 '/quit'을 입력하거나 Ctrl+C를 누르세요.");
        println!("------------------------------------------------------------\n");

        if let Err(e) = handle_chat_session(send_stream, recv_stream).await {
            eprintln!(" [오류] 채팅 세션 중 에러 발생: {:?}", e);
        }

        println!("\n============================================================");
        println!(" [대기 중] 클라이언트 세션이 종료되었습니다.");
        println!(" 동일한 티켓으로 새로운 연결을 계속 대기합니다... (Ctrl+C 로 종료)");
        println!("============================================================\n");
    }

    endpoint.close().await;
    Ok(())
}

/// [연결 모드]
/// 상대방의 티켓을 디코딩하여 Direct P2P / Relay를 통해 연결
async fn run_connector(ticket_arg: Option<String>) -> Result<()> {
    let ticket_str = match ticket_arg {
        Some(t) => t,
        None => {
            print!("상대방의 연결 티켓을 입력하세요: ");
            io::stdout().flush()?;
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            input.trim().to_string()
        }
    };

    let remote_addr = decode_ticket(&ticket_str)?;

    println!("============================================================");
    println!(" [Iroh P2P Connector]");
    println!(" 대상 Node ID: {}", remote_addr.id);
    println!(" Endpoint를 초기화하고 P2P / Relay 연결을 시도합니다...");
    println!("============================================================");

    // 1. Endpoint 생성
    let endpoint = create_endpoint(vec![]).await?;

    // 2. 원격 주소로 QUIC 연결 수립
    // Iroh는 우선 Direct UDP(Hole Punching / STUN / UPnP)를 시도하고,
    // 공유기/방화벽으로 인해 직접 연결이 안 되면 자동으로 Relay(DERP)를 통해 종단간 암호화 터널을 엽니다.
    let conn = endpoint
        .connect(remote_addr, CHAT_ALPN)
        .await
        .context("원격 피어 연결 실패")?;

    println!("\n [연결 성공!] 원격 피어와 연결되었습니다.");
    println!(" 원격 Node ID: {}", conn.remote_id());
    println!(" 연결 경로 유형: {}", format_path_info(&conn));

    // 3. 양방향 스트림 열기
    let (send_stream, recv_stream) = conn.open_bi().await.context("양방향 스트림 열기 실패")?;

    println!("------------------------------------------------------------");
    println!(" 실시간 대화가 시작되었습니다. 메시지를 입력 후 Enter를 누르세요.");
    println!(" 대화를 종료하려면 '/quit'을 입력하거나 Ctrl+C를 누르세요.");
    println!("------------------------------------------------------------\n");

    handle_chat_session(send_stream, recv_stream).await?;

    endpoint.close().await;
    Ok(())
}

/// 양방향 스트림을 통한 실시간 텍스트 채팅 세션 처리
async fn handle_chat_session(
    send_stream: iroh::endpoint::SendStream,
    recv_stream: iroh::endpoint::RecvStream,
) -> Result<()> {
    let mut framed_send = FramedWrite::new(send_stream, LinesCodec::new());
    let mut framed_recv = FramedRead::new(recv_stream, LinesCodec::new());

    let mut stdin_lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();

    loop {
        tokio::select! {
            // 콘솔 입력 읽기 및 상대방에게 전송
            line = stdin_lines.next_line() => {
                match line {
                    Ok(Some(msg)) => {
                        let trimmed = msg.trim();
                        if trimmed == "/quit" {
                            println!(" [알림] 대화를 종료합니다.");
                            let _ = framed_send.send("[상대방이 대화를 종료했습니다]".to_string()).await;
                            break;
                        }
                        if !trimmed.is_empty() {
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
            // 원격 피어로부터 메시지 수신 및 화면 출력
            incoming_msg = framed_recv.next() => {
                match incoming_msg {
                    Some(Ok(msg)) => {
                        println!(" [상대방] {}", msg);
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


