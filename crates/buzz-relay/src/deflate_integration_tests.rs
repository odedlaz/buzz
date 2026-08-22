//! Buzz-specific wire and corpus checks for axum's `permessage-deflate` route.
//!
//! Negotiation grammar belongs to tungstenite and route-policy behavior belongs
//! to axum. These rows retain the properties specific to Buzz: the production
//! builder shape, an on-wire compression/control pair, and opt-in corpus sweeps.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::extract::ws::{Message as WsMessage, WebSocket};
use axum::extract::{PerMessageDeflate, WebSocketUpgrade};
use axum::http::header;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// A relay server reached through a byte-counting TCP proxy.
struct Harness {
    proxy_addr: std::net::SocketAddr,
    server_to_client_wire_bytes: Arc<AtomicUsize>,
}

async fn send_payloads(mut socket: WebSocket, payloads: Vec<String>) {
    for payload in payloads {
        socket
            .send(WsMessage::Text(payload.into()))
            .await
            .expect("send the payload");
    }
    socket.flush().await.expect("flush payloads");
    // Hold the socket open until the client has read them.
    let _ = socket.recv().await;
}

async fn spawn_harness(payload: String, policy: Option<PerMessageDeflate>) -> Harness {
    spawn_harness_many(vec![payload], policy).await
}

/// Several payloads over one connection, so context takeover is observable.
async fn spawn_harness_many(payloads: Vec<String>, policy: Option<PerMessageDeflate>) -> Harness {
    let app = Router::new().route(
        "/",
        get(move |ws: WebSocketUpgrade| {
            let payloads = payloads.clone();
            async move {
                // This is the production composition point: Buzz's parser
                // limits and compression policy live on the same axum builder.
                let ws = ws.max_message_size(1 << 20).max_frame_size(1 << 20);
                match policy {
                    Some(policy) => ws
                        .compression(policy)
                        .on_upgrade(move |socket| send_payloads(socket, payloads)),
                    None => ws.on_upgrade(move |socket| send_payloads(socket, payloads)),
                }
            }
        }),
    );

    serve_through_counting_proxy(app).await
}

async fn serve_through_counting_proxy(app: Router) -> Harness {
    let server_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let server_addr = server_listener.local_addr().expect("server addr");
    tokio::spawn(async move {
        let _ = axum::serve(server_listener, app).await;
    });

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
    let server_to_client_wire_bytes = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&server_to_client_wire_bytes);
    tokio::spawn(async move {
        let (mut client_side, _) = proxy_listener.accept().await.expect("proxy accept");
        let mut server_side = tokio::net::TcpStream::connect(server_addr)
            .await
            .expect("proxy dial");
        let (mut client_read, mut client_write) = client_side.split();
        let (mut server_read, mut server_write) = server_side.split();
        let client_to_server = async {
            let mut buf = [0u8; 8192];
            loop {
                match client_read.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if server_write.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
        };
        let server_to_client = async {
            let mut buf = [0u8; 8192];
            loop {
                match server_read.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        counter.fetch_add(n, Ordering::Relaxed);
                        if client_write.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
        };
        tokio::join!(client_to_server, server_to_client);
    });

    Harness {
        proxy_addr,
        server_to_client_wire_bytes,
    }
}

fn client_config() -> tungstenite_pmd::protocol::WebSocketConfig {
    tungstenite_pmd::protocol::WebSocketConfig::default().enable_deflate()
}

async fn response_for_offer(offer: Option<&[u8]>) -> Option<String> {
    match offer {
        Some(offer) => response_for_offers(&[offer]).await,
        None => response_for_offers(&[]).await,
    }
}

async fn response_for_offers(offers: &[&[u8]]) -> Option<String> {
    let harness = spawn_harness("policy probe".to_owned(), Some(PerMessageDeflate::new())).await;
    let mut stream = tokio::net::TcpStream::connect(harness.proxy_addr)
        .await
        .expect("connect raw client");
    let mut request = format!(
        "GET / HTTP/1.1\r\nHost: {}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
        harness.proxy_addr
    )
    .into_bytes();
    for offer in offers {
        request.extend_from_slice(b"Sec-WebSocket-Extensions: ");
        request.extend_from_slice(offer);
        request.extend_from_slice(b"\r\n");
    }
    request.extend_from_slice(b"\r\n");
    stream
        .write_all(&request)
        .await
        .expect("write raw handshake");

    let mut response = Vec::new();
    while !response.windows(4).any(|window| window == b"\r\n\r\n") {
        let mut buf = [0; 1024];
        let read = stream.read(&mut buf).await.expect("read raw handshake");
        assert!(read > 0, "server closed before completing the handshake");
        response.extend_from_slice(&buf[..read]);
    }
    let head_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("complete response head")
        + 4;
    let response = std::str::from_utf8(&response[..head_end]).expect("ASCII response headers");
    assert!(response.starts_with("HTTP/1.1 101 "), "{response}");
    response.split("\r\n").find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("sec-websocket-extensions")
            .then(|| value.trim().to_owned())
    })
}

#[tokio::test]
async fn the_axum_route_preserves_the_relay_negotiation_matrix() {
    type NegotiationCase<'a> = (&'a str, Option<&'a [u8]>, Option<&'a str>);
    let cases: &[NegotiationCase<'_>] = &[
        ("no header", None, None),
        (
            "bare offer",
            Some(b"permessage-deflate"),
            Some("permessage-deflate"),
        ),
        (
            "browser offer",
            Some(b"permessage-deflate; client_max_window_bits"),
            Some("permessage-deflate"),
        ),
        (
            "valued client window",
            Some(b"permessage-deflate; client_max_window_bits=10"),
            Some("permessage-deflate; client_max_window_bits=10"),
        ),
        (
            "both takeover flags",
            Some(b"permessage-deflate; server_no_context_takeover; client_no_context_takeover"),
            Some("permessage-deflate; server_no_context_takeover; client_no_context_takeover"),
        ),
        (
            "one takeover flag",
            Some(b"permessage-deflate; server_no_context_takeover"),
            Some("permessage-deflate; server_no_context_takeover"),
        ),
        (
            "supported server window",
            Some(b"permessage-deflate; server_max_window_bits=10"),
            Some("permessage-deflate; server_max_window_bits=10"),
        ),
        (
            "server window below backend support",
            Some(b"permessage-deflate; server_max_window_bits=8"),
            None,
        ),
        (
            "server window above RFC range",
            Some(b"permessage-deflate; server_max_window_bits=16"),
            None,
        ),
        (
            "non-numeric server window",
            Some(b"permessage-deflate; server_max_window_bits=abc"),
            None,
        ),
        (
            "valueless server window",
            Some(b"permessage-deflate; server_max_window_bits"),
            None,
        ),
        (
            "unknown parameter",
            Some(b"permessage-deflate; x-not-a-parameter"),
            None,
        ),
        (
            "valued takeover flag",
            Some(b"permessage-deflate; server_no_context_takeover=1"),
            None,
        ),
        (
            "unrelated extension first",
            Some(b"x-other-extension, permessage-deflate"),
            Some("permessage-deflate"),
        ),
        (
            "unrelated obs-text",
            Some(b"x-other-extension; value=\x80, permessage-deflate"),
            Some("permessage-deflate"),
        ),
        (
            "unacceptable offer before acceptable",
            Some(b"permessage-deflate; server_max_window_bits=8, permessage-deflate"),
            Some("permessage-deflate"),
        ),
        ("no deflate offer", Some(b"x-other-extension"), None),
        (
            "case-insensitive names",
            Some(b"PerMessage-Deflate; Server_No_Context_Takeover"),
            Some("permessage-deflate; server_no_context_takeover"),
        ),
        (
            "quoted parameter value",
            Some(b"permessage-deflate; client_max_window_bits=\"10\""),
            Some("permessage-deflate; client_max_window_bits=10"),
        ),
    ];

    for &(case, offer, expected) in cases {
        assert_eq!(
            response_for_offer(offer).await.as_deref(),
            expected,
            "{case}"
        );
    }
}

#[tokio::test]
async fn the_axum_route_honours_every_supported_server_window() {
    for bits in 9..=15 {
        let offer = format!("permessage-deflate; server_max_window_bits={bits}");
        let expected = format!("permessage-deflate; server_max_window_bits={bits}");
        let response = response_for_offer(Some(offer.as_bytes())).await;
        assert_eq!(response.as_deref(), Some(expected.as_str()));
    }
}

#[tokio::test]
async fn unrelated_obs_text_does_not_hide_a_separate_offer() {
    let response =
        response_for_offers(&[b"x-other-extension; value=\x80", b"permessage-deflate"]).await;
    assert_eq!(response.as_deref(), Some("permessage-deflate"));
}

#[tokio::test]
async fn negotiated_compression_preserves_the_relay_message_limit() {
    let max = 1024;
    let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new().route(
        "/",
        get(move |ws: WebSocketUpgrade| {
            let result_tx = result_tx.clone();
            async move {
                ws.max_message_size(max)
                    .max_frame_size(max)
                    .compression(PerMessageDeflate::new())
                    .on_upgrade(move |mut socket| async move {
                        let rejected = matches!(socket.recv().await, Some(Err(_)));
                        let _ = result_tx.send(rejected);
                    })
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let addr = listener.local_addr().expect("server addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let (mut client, response) = tokio_tungstenite_pmd::connect_async_with_config(
        format!("ws://{addr}/"),
        Some(client_config()),
        false,
    )
    .await
    .expect("client connects");
    assert_eq!(
        response.headers()[header::SEC_WEBSOCKET_EXTENSIONS],
        "permessage-deflate"
    );
    client
        .send(tungstenite_pmd::protocol::Message::Text(
            "compresses tiny but inflates past the limit "
                .repeat(128)
                .into(),
        ))
        .await
        .expect("send oversized compressed message");
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(10), result_rx.recv())
            .await
            .expect("server observes the message")
            .expect("server reports the result"),
        "the negotiated socket must retain Buzz's message-size limit"
    );
}

#[tokio::test]
async fn the_axum_route_negotiates_and_compresses_on_the_wire() {
    // Deliberately extreme: this is a correctness discriminator, never a
    // production-ratio measurement.
    let payload = "buzz relay event payload ".repeat(10_000);
    let payload_len = payload.len();
    let harness = spawn_harness(payload.clone(), Some(PerMessageDeflate::new())).await;

    let (mut client, response) = tokio_tungstenite_pmd::connect_async_with_config(
        format!("ws://{}/", harness.proxy_addr),
        Some(client_config()),
        false,
    )
    .await
    .expect("client connects");

    assert_eq!(
        response.headers()[header::SEC_WEBSOCKET_EXTENSIONS],
        "permessage-deflate"
    );
    let received = tokio::time::timeout(std::time::Duration::from_secs(10), client.next())
        .await
        .expect("payload arrives")
        .expect("stream is open")
        .expect("no protocol error");
    match received {
        tungstenite_pmd::protocol::Message::Text(text) => assert_eq!(text.as_str(), payload),
        other => panic!("expected text, got {other:?}"),
    }

    let wire = harness.server_to_client_wire_bytes.load(Ordering::Relaxed);
    assert!(
        wire < payload_len / 10,
        "expected under a tenth of {payload_len} bytes, saw {wire}"
    );
    println!(
        "PROOF payload={payload_len} wire={wire} ratio={:.1}x",
        payload_len as f64 / wire as f64
    );
}

#[tokio::test]
async fn without_an_offer_the_same_axum_route_stays_plain() {
    let payload = "buzz relay event payload ".repeat(10_000);
    let payload_len = payload.len();
    let harness = spawn_harness(payload.clone(), Some(PerMessageDeflate::new())).await;

    let (mut client, response) =
        tokio_tungstenite_pmd::connect_async(format!("ws://{}/", harness.proxy_addr))
            .await
            .expect("client connects");
    assert!(!response
        .headers()
        .contains_key(header::SEC_WEBSOCKET_EXTENSIONS));

    let received = tokio::time::timeout(std::time::Duration::from_secs(10), client.next())
        .await
        .expect("payload arrives")
        .expect("stream is open")
        .expect("no protocol error");
    match received {
        tungstenite_pmd::protocol::Message::Text(text) => assert_eq!(text.as_str(), payload),
        other => panic!("expected text, got {other:?}"),
    }

    let wire = harness.server_to_client_wire_bytes.load(Ordering::Relaxed);
    assert!(
        wire >= payload_len,
        "plain wire cost must cover {payload_len} payload bytes, saw {wire}"
    );
    println!("CONTROL payload={payload_len} wire={wire} (uncompressed)");
}

fn captured_frames() -> (Vec<String>, usize) {
    let path = std::env::var("BUZZ_DEFLATE_FRAMES")
        .expect("set BUZZ_DEFLATE_FRAMES to one WebSocket text frame per line");
    let frames: Vec<_> = std::fs::read_to_string(path)
        .expect("read captured frames")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect();
    assert!(!frames.is_empty(), "captured frame file is empty");
    let raw = frames.iter().map(String::len).sum();
    (frames, raw)
}

async fn read_all(
    mut client: tokio_tungstenite_pmd::WebSocketStream<
        tokio_tungstenite_pmd::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    frames: &[String],
) {
    for (index, expected) in frames.iter().enumerate() {
        let message = tokio::time::timeout(std::time::Duration::from_secs(30), client.next())
            .await
            .expect("frame arrives")
            .expect("stream stays open")
            .expect("no protocol error");
        match message {
            tungstenite_pmd::protocol::Message::Text(text) => {
                assert_eq!(text.as_str(), expected, "frame {index} must round-trip")
            }
            other => panic!("expected text frame {index}, got {other:?}"),
        }
    }
}

/// Informational measurement against captured relay traffic.
#[tokio::test]
#[ignore = "needs BUZZ_DEFLATE_FRAMES; deterministic informational measurement"]
async fn real_relay_traffic() {
    let (frames, raw) = captured_frames();
    for policy in [Some(PerMessageDeflate::new()), None] {
        let harness = spawn_harness_many(frames.clone(), policy).await;
        let (client, _) = if policy.is_some() {
            tokio_tungstenite_pmd::connect_async_with_config(
                format!("ws://{}/", harness.proxy_addr),
                Some(client_config()),
                false,
            )
            .await
            .expect("client connects")
        } else {
            tokio_tungstenite_pmd::connect_async(format!("ws://{}/", harness.proxy_addr))
                .await
                .expect("client connects")
        };
        read_all(client, &frames).await;
        let wire = harness.server_to_client_wire_bytes.load(Ordering::Relaxed);
        let label = if policy.is_some() { "deflate" } else { "plain" };
        println!(
            "REAL {label} frames={} raw={raw} wire={wire} ratio={:.3}x",
            frames.len(),
            raw as f64 / wire as f64
        );
    }
}

/// Informational compression-level sweep on the captured corpus.
#[tokio::test]
#[ignore = "needs BUZZ_DEFLATE_FRAMES; deterministic informational measurement"]
async fn level_sweep() {
    let (frames, raw) = captured_frames();
    for level in 1..=9 {
        let harness =
            spawn_harness_many(frames.clone(), Some(PerMessageDeflate::new().level(level))).await;
        let (client, _) = tokio_tungstenite_pmd::connect_async_with_config(
            format!("ws://{}/", harness.proxy_addr),
            Some(client_config()),
            false,
        )
        .await
        .expect("client connects");
        read_all(client, &frames).await;
        let wire = harness.server_to_client_wire_bytes.load(Ordering::Relaxed);
        println!(
            "SWEEP level={level} raw={raw} wire={wire} ratio={:.3}x",
            raw as f64 / wire as f64
        );
    }
}

/// Informational compression-window sweep on the captured corpus.
#[tokio::test]
#[ignore = "needs BUZZ_DEFLATE_FRAMES; deterministic informational measurement"]
async fn window_sweep() {
    let (frames, raw) = captured_frames();
    for window_bits in 9..=15 {
        let harness = spawn_harness_many(
            frames.clone(),
            Some(PerMessageDeflate::new().max_window_bits(window_bits)),
        )
        .await;
        let (client, _) = tokio_tungstenite_pmd::connect_async_with_config(
            format!("ws://{}/", harness.proxy_addr),
            Some(client_config()),
            false,
        )
        .await
        .expect("client connects");
        read_all(client, &frames).await;
        let wire = harness.server_to_client_wire_bytes.load(Ordering::Relaxed);
        println!(
            "WINDOW wb={window_bits} raw={raw} wire={wire} ratio={:.3}x",
            raw as f64 / wire as f64
        );
    }
}
