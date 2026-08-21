//! Wire-cost measurement for `permessage-deflate`, on captured relay traffic.
//!
//! This file is the *instrument*: corpus, counting proxy, client, and the ratio
//! arithmetic. It is deliberately ignorant of how the server negotiates the
//! extension — that is [`super::deflate_adapter`], and it is the only part that
//! changes when the negotiation moves from a bespoke module into axum.
//!
//! The split exists so both sides of that migration are measured by the same
//! bytes of code. A harness rebuilt halfway through, then compared against the
//! number it exists to validate, is unconvincing whichever way it comes out: a
//! matching ratio reads as coincidence and a different one is unattributable.
//!
//! Every test here needs a corpus and is therefore `#[ignore]`d:
//!
//! ```sh
//! BUZZ_DEFLATE_FRAMES=/path/to/events.jsonl \
//!   cargo test -p buzz-relay --lib real_relay_traffic -- --ignored --nocapture
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use futures_util::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tungstenite_pmd::extensions::compression::deflate::DeflateConfig;
use tungstenite_pmd::protocol::{Message as TMessage, WebSocketConfig};

use super::deflate_adapter as adapter;

/// One frame per line. All frames go over a single connection, because that is
/// the only way context takeover shows up — each message reuses the previous
/// one's sliding window, and that is where most of the ratio comes from.
fn corpus() -> Vec<String> {
    let file = std::env::var("BUZZ_DEFLATE_FRAMES")
        .expect("set BUZZ_DEFLATE_FRAMES to a file of one frame per line");
    let frames: Vec<String> = std::fs::read_to_string(&file)
        .expect("read the frame file")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect();
    assert!(!frames.is_empty(), "the frame file is empty");
    frames
}

fn client_config() -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.extensions.permessage_deflate = Some(DeflateConfig::default());
    config
}

/// Serves `app` behind a byte-counting TCP proxy, reads every frame back, and
/// returns what the server actually put on the wire.
///
/// `offer` decides whether the *client* asks for compression; the server's half
/// of that decision belongs to the adapter. A run with `None` is the negative
/// control: if it also reported a small wire cost, the counter would be
/// measuring nothing.
async fn wire_cost(app: Router, frames: &[String], offer: Option<WebSocketConfig>) -> usize {
    let server = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let server_addr = server.local_addr().expect("server addr");
    tokio::spawn(async move {
        let _ = axum::serve(server, app).await;
    });

    let proxy = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let proxy_addr = proxy.local_addr().expect("proxy addr");
    let wire_bytes = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&wire_bytes);
    tokio::spawn(async move {
        let (mut client_side, _) = proxy.accept().await.expect("proxy accept");
        let mut server_side = tokio::net::TcpStream::connect(server_addr)
            .await
            .expect("proxy dial");
        let (mut client_read, mut client_write) = client_side.split();
        let (mut server_read, mut server_write) = server_side.split();
        // Only the server-to-client direction is counted: that is the direction
        // the relay pays for, and the request side is a fixed handshake.
        let up = pump(&mut client_read, &mut server_write, None);
        let down = pump(&mut server_read, &mut client_write, Some(&counter));
        tokio::join!(up, down);
    });

    let url = format!("ws://{proxy_addr}/");
    let mut client = match offer {
        Some(config) => {
            tokio_tungstenite_pmd::connect_async_with_config(url, Some(config), false)
                .await
                .expect("client connects")
                .0
        }
        None => tokio_tungstenite_pmd::connect_async(url)
            .await
            .expect("client connects")
            .0,
    };

    let mut seen = 0usize;
    while seen < frames.len() {
        let msg = tokio::time::timeout(Duration::from_secs(30), client.next())
            .await
            .expect("frames arrive")
            .expect("stream stays open")
            .expect("no protocol error");
        if let TMessage::Text(text) = msg {
            assert_eq!(text.as_str(), frames[seen], "frame {seen} must round-trip");
            seen += 1;
        }
    }

    wire_bytes.load(Ordering::Relaxed)
}

async fn pump<R, W>(from: &mut R, to: &mut W, count: Option<&AtomicUsize>)
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut buf = [0u8; 8192];
    loop {
        match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if let Some(counter) = count {
                    counter.fetch_add(n, Ordering::Relaxed);
                }
                if to.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        }
    }
}

fn raw_bytes(frames: &[String]) -> usize {
    frames.iter().map(String::len).sum()
}

#[tokio::test]
#[ignore = "needs a captured frame file; see the module docs"]
async fn real_relay_traffic() {
    let frames = corpus();
    let raw = raw_bytes(&frames);

    for deflate in [true, false] {
        let app = if deflate {
            adapter::echo_app(frames.clone())
        } else {
            adapter::echo_app_without_compression(frames.clone())
        };
        let offer = deflate.then(client_config);
        let wire = wire_cost(app, &frames, offer).await;
        let label = if deflate { "deflate" } else { "plain  " };
        println!(
            "REAL {label} frames={} raw={raw} wire={wire} ratio={:.2}x",
            frames.len(),
            raw as f64 / wire as f64
        );
    }
}

/// Ratio against the compression level, 1..=9. Deterministic — same input, same
/// settings, same output bytes — so this needs no repetition and no control.
#[tokio::test]
#[ignore = "needs a captured frame file; see the module docs"]
async fn level_sweep() {
    let frames = corpus();
    let raw = raw_bytes(&frames);

    for level in 1..=9u8 {
        let app = adapter::echo_app_at_level(frames.clone(), level);
        let wire = wire_cost(app, &frames, Some(client_config())).await;
        println!(
            "SWEEP level={level} raw={raw} wire={wire} ratio={:.3}x",
            raw as f64 / wire as f64
        );
    }
}

/// Ratio against the server's compression window, 9..=15.
///
/// The server can set this unilaterally: an inflater with a wider window reads a
/// narrower stream, so we compress at N and put nothing in the response. That is
/// what makes it the one memory knob usable without client cooperation.
#[tokio::test]
#[ignore = "needs a captured frame file; see the module docs"]
async fn window_sweep() {
    let frames = corpus();
    let raw = raw_bytes(&frames);

    for bits in 9..=15u8 {
        let app = adapter::echo_app_at_window(frames.clone(), bits);
        let wire = wire_cost(app, &frames, Some(client_config())).await;
        println!(
            "WINDOW wb={bits} raw={raw} wire={wire} ratio={:.3}x",
            raw as f64 / wire as f64
        );
    }
}
