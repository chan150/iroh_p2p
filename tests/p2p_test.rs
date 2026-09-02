use anyhow::Result;
use iroh_p2p_example::{create_endpoint, decode_ticket, encode_ticket, CHAT_ALPN};

#[tokio::test]
async fn test_p2p_direct_or_relay_communication() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("iroh=debug,warn")
        .try_init();

    // 1. Peer A (Listener) 설정
    let endpoint_a = create_endpoint(vec![CHAT_ALPN.to_vec()]).await?;
    let addr_a = endpoint_a.addr();
    let ticket_a = encode_ticket(&addr_a)?;
    println!("Peer A Addr: {:?}", addr_a);

    // 티켓 디코딩 검증
    let decoded_addr_a = decode_ticket(&ticket_a)?;
    assert_eq!(addr_a.id, decoded_addr_a.id);

    // 동기화를 위한 채널
    let (tx_done, rx_done) = tokio::sync::oneshot::channel::<()>();

    // Peer A 수신 대기 비동기 태스크
    let listener_handle = tokio::spawn(async move {
        println!("Peer A waiting for accept...");
        let incoming = endpoint_a.accept().await.expect("Accept failed");
        let conn = incoming.await.expect("Handshake failed");
        println!("Peer A accepted conn from {:?}", conn.remote_id());
        println!("Peer A connection paths: {:?}", conn.paths());
        let (mut send, mut recv) = conn.accept_bi().await.expect("Accept stream failed");

        // B로부터 메시지 수신
        let mut buf = vec![0u8; 1024];
        let n = recv.read(&mut buf).await.expect("Read failed").unwrap_or(0);
        let msg = String::from_utf8_lossy(&buf[..n]);
        println!("Peer A received: {}", msg);
        assert_eq!(msg, "Hello from Peer B");

        // A가 B로 응답 전송
        send.write_all(b"Hello back from Peer A").await.expect("Write failed");
        send.finish().expect("Finish stream failed");
        println!("Peer A sent reply and finished stream");

        // B가 응답을 다 받을 때까지 연결(conn)을 유지
        let _ = rx_done.await;
        println!("Peer A closing connection");
        endpoint_a.close().await;
    });

    // 2. Peer B (Connector) 설정 및 연결
    let endpoint_b = create_endpoint(vec![]).await?;
    let conn_b = endpoint_b
        .connect(decoded_addr_a, CHAT_ALPN)
        .await
        .expect("Connect failed");

    let (mut send_b, mut recv_b) = conn_b.open_bi().await.expect("Open stream failed");

    // B가 A로 메시지 전송
    send_b.write_all(b"Hello from Peer B").await.expect("Send failed");
    send_b.finish().expect("Finish send failed");

    // B가 A로부터 응답 수신
    let mut resp_buf = vec![0u8; 1024];
    let n = recv_b.read(&mut resp_buf).await.expect("Read response failed").unwrap_or(0);
    let resp = String::from_utf8_lossy(&resp_buf[..n]);
    assert_eq!(resp, "Hello back from Peer A");

    // Peer A에게 완료 신호 전송
    let _ = tx_done.send(());

    // Listener 태스크 종료 대기
    listener_handle.await.expect("Listener task panicked");

    // graceful close
    endpoint_b.close().await;

    println!("P2P Communication test passed successfully!");
    Ok(())
}

#[test]
fn test_ticket_multiline_and_legacy_decoding() -> Result<()> {
    // 사용자가 입력한 여러 줄로 줄바꿈된 JSON 티켓
    let multiline_ticket = "
    eyJpZCI6IjRiNDdlODQyODI0MmVhZTJiODAxZTU4NzBiZWQxZGRkNGQ2NzNmNDJjOGRlNThmOGIwYTU3NjVmYjYzMjdhOGUiLCJhZGRycyI6W
    3siUmVsYXkiOiJodHRwczovL2FwczEtMS5yZWxheS5uMC5pcm9oLmxpbmsuLyJ9LHsiSXAiOiIxMDYuMjUxLjg4LjE0MDo0MjY2NCJ9LHsiSX
    AiOiIxMDYuMjUxLjg4LjE0MDo2NTIyMyJ9LHsiSXAiOiIxNzIuMjQuMTYwLjE6NjUyMjMifSx7IklwIjoiMTkyLjE2OC4wLjEwMjo2NTIyMyJ9
    XX0
    ";

    let decoded = decode_ticket(multiline_ticket)?;
    println!("Decoded multiline ticket successfully: {:?}", decoded.id);
    assert_eq!(
        decoded.id.to_string(),
        "4b47e8428242eae2b801e5870bed1ddd4d673f42c8de58f8b0a5765fb6327a8e"
    );

    Ok(())
}

#[tokio::test]
async fn test_p2p_reconnection_loop() -> Result<()> {
    // 1. Peer A (Listener) 설정 - 2회 연속 연결을 받는 루프
    let endpoint_a = create_endpoint(vec![CHAT_ALPN.to_vec()]).await?;
    let addr_a = endpoint_a.addr();
    let ticket_a = encode_ticket(&addr_a)?;

    let listener_handle = tokio::spawn(async move {
        for round in 1..=2 {
            let incoming = endpoint_a.accept().await.expect("Accept failed");
            let conn = incoming.await.expect("Handshake failed");
            
            // 각 클라이언트 연결을 독립된 비동기 태스크로 처리
            tokio::spawn(async move {
                let (mut send, mut recv) = conn.accept_bi().await.expect("Accept stream failed");
                let mut buf = vec![0u8; 1024];
                let n = recv.read(&mut buf).await.expect("Read failed").unwrap_or(0);
                let msg = String::from_utf8_lossy(&buf[..n]);
                assert_eq!(msg, format!("Hello Round {}", round));

                send.write_all(format!("Ack Round {}", round).as_bytes())
                    .await
                    .expect("Write failed");
                send.finish().expect("Finish stream failed");

                // 클라이언트가 데이터를 수신하고 연결을 닫을 때까지 연결 객체(conn) 유지
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                let _ = conn;
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        endpoint_a.close().await;
    });

    // 2. Client 1 접속 & 종료
    let target_addr = decode_ticket(&ticket_a)?;
    let endpoint_c1 = create_endpoint(vec![]).await?;
    let conn_c1 = endpoint_c1
        .connect(target_addr.clone(), CHAT_ALPN)
        .await
        .expect("Client 1 connect failed");
    let (mut send1, mut recv1) = conn_c1.open_bi().await.expect("Open bi failed");
    send1.write_all(b"Hello Round 1").await.expect("Send failed");
    send1.finish().expect("Finish send failed");

    let mut buf1 = vec![0u8; 1024];
    let n1 = recv1.read(&mut buf1).await.expect("Read failed").unwrap_or(0);
    assert_eq!(String::from_utf8_lossy(&buf1[..n1]), "Ack Round 1");
    endpoint_c1.close().await; // Client 1 종료!

    // 3. Client 2가 동일한 티켓으로 다시 접속 & 종료
    let endpoint_c2 = create_endpoint(vec![]).await?;
    let conn_c2 = endpoint_c2
        .connect(target_addr, CHAT_ALPN)
        .await
        .expect("Client 2 connect failed");
    let (mut send2, mut recv2) = conn_c2.open_bi().await.expect("Open bi failed");
    send2.write_all(b"Hello Round 2").await.expect("Send failed");
    send2.finish().expect("Finish send failed");

    let mut buf2 = vec![0u8; 1024];
    let n2 = recv2.read(&mut buf2).await.expect("Read failed").unwrap_or(0);
    assert_eq!(String::from_utf8_lossy(&buf2[..n2]), "Ack Round 2");
    listener_handle.await.expect("Listener panicked");
    println!("Consecutive reconnections test passed successfully!");
    Ok(())
}

#[tokio::test]
async fn test_channel_zero_config_connection() -> Result<()> {
    use iroh_p2p_example::{create_endpoint_with_secret_key, derive_channel_keys};

    let channel_num = 7u32;
    let (secret_key, target_addr) = derive_channel_keys(channel_num);

    // 1. Host: 채널 7번의 고정 키로 Endpoint 생성
    let host_endpoint = create_endpoint_with_secret_key(Some(secret_key), vec![CHAT_ALPN.to_vec()]).await?;
    let host_id = host_endpoint.id();

    let host_handle = tokio::spawn(async move {
        let incoming = host_endpoint.accept().await.expect("Host accept failed");
        let conn = incoming.await.expect("Host handshake failed");
        let (mut send, mut recv) = conn.accept_bi().await.expect("Host bi stream failed");

        let mut buf = vec![0u8; 1024];
        let n = recv.read(&mut buf).await.expect("Host read failed").unwrap_or(0);
        assert_eq!(String::from_utf8_lossy(&buf[..n]), "Ping Channel 7");

        send.write_all(b"Pong Channel 7").await.expect("Host write failed");
        send.finish().expect("Host finish failed");

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        host_endpoint.close().await;
    });

    // 2. Client: 티켓 없이 오직 channel 7의 target_addr로 바로 연결
    let client_endpoint = create_endpoint(vec![]).await?;
    let conn = client_endpoint
        .connect(target_addr, CHAT_ALPN)
        .await
        .expect("Client channel connect failed");

    assert_eq!(conn.remote_id(), host_id);

    let (mut send, mut recv) = conn.open_bi().await.expect("Client bi stream failed");
    send.write_all(b"Ping Channel 7").await.expect("Client send failed");
    send.finish().expect("Client finish failed");

    let mut buf = vec![0u8; 1024];
    let n = recv.read(&mut buf).await.expect("Client recv failed").unwrap_or(0);
    assert_eq!(String::from_utf8_lossy(&buf[..n]), "Pong Channel 7");

    client_endpoint.close().await;
    host_handle.await.expect("Host panicked");

    println!("Zero-config channel #7 test passed successfully!");
    Ok(())
}

#[tokio::test]
async fn test_p2p_file_streaming_transfer() -> Result<()> {
    use iroh_p2p_example::{create_endpoint_with_secret_key, derive_channel_keys, receive_file_stream, send_file_stream};

    let test_dir = std::env::temp_dir().join("iroh_file_test_dir");
    let save_dir = test_dir.join("received");
    tokio::fs::create_dir_all(&test_dir).await?;

    // 1. 2MB 테스트 파일 생성
    let source_file_path = test_dir.join("sample_test_doc.bin");
    let test_payload = vec![0xABu8; 2 * 1024 * 1024]; // 2MB
    tokio::fs::write(&source_file_path, &test_payload).await?;

    let channel_num = 12u32;
    let (secret_key, target_addr) = derive_channel_keys(channel_num);

    let host_endpoint = create_endpoint_with_secret_key(Some(secret_key), vec![CHAT_ALPN.to_vec()]).await?;
    let save_dir_clone = save_dir.clone();

    // Host: 파일 수신 대기
    let host_handle = tokio::spawn(async move {
        let incoming = host_endpoint.accept().await.expect("Host accept failed");
        let conn = incoming.await.expect("Host handshake failed");
        let (send, recv) = conn.accept_bi().await.expect("Host accept bi stream failed");

        let (received_path, received_bytes, duration) = receive_file_stream(
            send,
            recv,
            &save_dir_clone,
            |_cur, _tot, _spd| {},
        )
        .await
        .expect("Host receive_file_stream failed");

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        host_endpoint.close().await;

        (received_path, received_bytes, duration)
    });

    // Client: 파일 전송
    let client_endpoint = create_endpoint(vec![]).await?;
    let conn = client_endpoint.connect(target_addr, CHAT_ALPN).await.expect("Client connect failed");
    let (send, recv) = conn.open_bi().await.expect("Client open bi stream failed");

    let (sent_name, sent_bytes, sent_dur) = send_file_stream(
        send,
        recv,
        &source_file_path,
        |_cur, _tot, _spd| {},
    )
    .await
    .expect("Client send_file_stream failed");

    assert_eq!(sent_name, "sample_test_doc.bin");
    assert_eq!(sent_bytes, 2 * 1024 * 1024);

    let (saved_path, recv_bytes, _) = host_handle.await.expect("Host panic");
    assert_eq!(recv_bytes, 2 * 1024 * 1024);

    // 수신 파일 내용 검증
    let received_content = tokio::fs::read(&saved_path).await?;
    assert_eq!(received_content, test_payload);

    client_endpoint.close().await;

    // 임시 파일 정리
    let _ = tokio::fs::remove_dir_all(&test_dir).await;

    println!(
        "P2P File Transfer Test (2MB in {:.2}s) passed with 100% integrity!",
        sent_dur.as_secs_f64()
    );
    Ok(())
}

#[test]
fn test_remote_control_event_serialization() {
    use iroh_p2p_example::remote::{MouseButton, RemoteControlEvent};

    // 마우스 이동
    let ev1 = RemoteControlEvent::MouseMove { x: 0.25, y: 0.75 };
    let ser1 = ev1.serialize();
    assert_eq!(ser1, "MM:0.2500:0.7500");
    let de1 = RemoteControlEvent::deserialize(&ser1).expect("Failed to deserialize MM");
    assert_eq!(de1, ev1);

    // 마우스 다운/업
    let ev2 = RemoteControlEvent::MouseDown { button: MouseButton::Right };
    let ser2 = ev2.serialize();
    assert_eq!(ser2, "MD:R");
    let de2 = RemoteControlEvent::deserialize(&ser2).expect("Failed to deserialize MD");
    assert_eq!(de2, ev2);

    // 마우스 휠
    let ev3 = RemoteControlEvent::MouseWheel { delta: -120 };
    let ser3 = ev3.serialize();
    assert_eq!(ser3, "MW:-120");
    let de3 = RemoteControlEvent::deserialize(&ser3).expect("Failed to deserialize MW");
    assert_eq!(de3, ev3);

    // 키보드 키 다운/업
    let ev4 = RemoteControlEvent::KeyDown { key_code: 65 };
    let ser4 = ev4.serialize();
    assert_eq!(ser4, "KD:65");
    let de4 = RemoteControlEvent::deserialize(&ser4).expect("Failed to deserialize KD");
    assert_eq!(de4, ev4);

    // 텍스트 입력
    let ev5 = RemoteControlEvent::TextInput { text: "Hello P2P!".to_string() };
    let ser5 = ev5.serialize();
    assert_eq!(ser5, "TX:Hello P2P!");
    let de5 = RemoteControlEvent::deserialize(&ser5).expect("Failed to deserialize TX");
    assert_eq!(de5, ev5);
}

#[tokio::test]
async fn test_p2p_screen_frame_streaming() -> Result<()> {
    use iroh_p2p_example::remote::receive_screen_frame;

    let host_endpoint = create_endpoint(vec![CHAT_ALPN.to_vec()]).await?;
    let target_addr = host_endpoint.addr();

    // Host: 화면 프레임 스트림 수신
    let host_handle = tokio::spawn(async move {
        let incoming = host_endpoint.accept().await.expect("Host accept failed");
        let conn = incoming.await.expect("Host handshake failed");
        let (_send, mut recv) = conn.accept_bi().await.expect("Host accept bi failed");

        let mut magic = [0u8; 4];
        iroh_p2p_example::read_exact_stream(&mut recv, &mut magic).await.expect("Read magic failed");
        assert_eq!(&magic, b"SCRN");

        let frame = receive_screen_frame(&mut recv).await.expect("Receive frame failed");
        assert_eq!(frame.frame_seq, 42);
        assert_eq!(frame.width, 1920);
        assert_eq!(frame.height, 1080);
        assert_eq!(frame.jpeg_data, vec![0xFF, 0xD8, 0xFF, 0xE0, 0x01, 0x02, 0x03]);

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        host_endpoint.close().await;
    });

    // Client: 더미 화면 프레임 송신
    let client_endpoint = create_endpoint(vec![]).await?;
    let conn = client_endpoint.connect(target_addr, CHAT_ALPN).await.expect("Client connect failed");
    let (mut send, _recv) = conn.open_bi().await.expect("Client open bi failed");

    // 1. 매직 헤더
    send.write_all(b"SCRN").await?;

    // 2. 프레임 헤더: seq 42, width 1920, height 1080, len 7
    let dummy_jpeg = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x01, 0x02, 0x03];
    let mut frame_hdr = [0u8; 12];
    frame_hdr[0..4].copy_from_slice(&42u32.to_le_bytes());
    frame_hdr[4..6].copy_from_slice(&1920u16.to_le_bytes());
    frame_hdr[6..8].copy_from_slice(&1080u16.to_le_bytes());
    frame_hdr[8..12].copy_from_slice(&(dummy_jpeg.len() as u32).to_le_bytes());

    send.write_all(&frame_hdr).await?;
    send.write_all(&dummy_jpeg).await?;
    send.finish()?;

    host_handle.await.expect("Host failed");
    client_endpoint.close().await;

    println!("P2P Screen Frame Streaming Test Passed!");
    Ok(())
}
