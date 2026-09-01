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
    endpoint_c2.close().await; // Client 2 종료!

    listener_handle.await.expect("Listener panicked");
    println!("Consecutive reconnections test passed successfully!");
    Ok(())
}
