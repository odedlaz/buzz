//! `permessage-deflate` (RFC 7692) negotiation for the relay's client socket.
//!
//! axum owns the HTTP upgrade and emits only `Connection`, `Upgrade`,
//! `Sec-WebSocket-Accept` and optionally `Sec-WebSocket-Protocol`
//! (`axum::extract::ws`), so it can never answer an extension offer. The relay
//! therefore runs this half of the handshake itself: read the offer here,
//! decide the response, and hand `tungstenite` a [`WebSocketConfig`] carrying
//! the parameters we committed to. `WebSocketContext::new` builds the
//! compression context from that config, which is the only public seam — the
//! negotiated-`Extensions` constructors are `pub(crate)` upstream.
//!
//! The response must describe what we will actually do. Anything we cannot
//! configure is declined rather than silently ignored, because a client that
//! sees a parameter accepted and a stream that does not honour it fails at the
//! first compressed frame instead of at the handshake.

use tungstenite_pmd::extensions::compression::deflate::DeflateConfig;
use tungstenite_pmd::protocol::{Role, WebSocketConfig};

/// The outcome of accepting an offer: what to echo, and what we will do.
pub(crate) struct Negotiated {
    /// Value for the `Sec-WebSocket-Extensions` response header.
    pub(crate) response_value: String,
    /// Parameters the response commits us to.
    pub(crate) deflate: DeflateConfig,
}

/// One extension in an offer: a name plus its parameters.
struct Offer<'a> {
    name: &'a str,
    params: Vec<(&'a str, Option<&'a str>)>,
}

/// Parse a `Sec-WebSocket-Extensions` request header.
///
/// Deliberately lenient about what it keeps and strict about what it claims:
/// an unparsable parameter is retained verbatim so [`negotiate`] can decline on
/// it, rather than being dropped into an offer that then looks acceptable.
fn parse_offers(header: &str) -> Vec<Offer<'_>> {
    header
        .split(',')
        .filter_map(|ext| {
            let mut parts = ext.split(';').map(str::trim);
            let name = parts.next().filter(|n| !n.is_empty())?;
            let params = parts
                .filter(|p| !p.is_empty())
                .map(|p| match p.split_once('=') {
                    Some((k, v)) => (k.trim(), Some(v.trim().trim_matches('"'))),
                    None => (p, None),
                })
                .collect();
            Some(Offer { name, params })
        })
        .collect()
}

/// Decide the response to an extension offer.
///
/// Returns `None` to decline every offer, which is always valid and leaves the
/// connection uncompressed.
pub(crate) fn negotiate(header: Option<&str>) -> Option<Negotiated> {
    let offers = parse_offers(header?);
    // First acceptable `permessage-deflate` wins, per RFC 7692 section 5.1: the
    // server picks one offer and ignores the rest.
    offers
        .iter()
        .filter(|o| o.name.eq_ignore_ascii_case("permessage-deflate"))
        .find_map(accept_offer)
}

fn accept_offer(offer: &Offer<'_>) -> Option<Negotiated> {
    let mut deflate = DeflateConfig::default();
    let mut echo = Vec::new();

    for (key, value) in &offer.params {
        match (key.to_ascii_lowercase().as_str(), value) {
            // Both takeover flags are settable, so honour and echo them.
            ("server_no_context_takeover", None) => {
                deflate = deflate.set_no_context_takeover(Role::Server, true);
                echo.push("server_no_context_takeover".to_owned());
            }
            ("client_no_context_takeover", None) => {
                deflate = deflate.set_no_context_takeover(Role::Client, true);
                echo.push("client_no_context_takeover".to_owned());
            }
            // The client advertising a window it can compress with. Omitting it
            // from the response tells the client to use 15 (RFC 7692 section
            // 7.1.2.2), which matches the default we configure, so there is
            // nothing to honour and nothing to echo.
            ("client_max_window_bits", _) => {}
            // A limit on *our* window, and the client asks because its inflater
            // cannot go wider — so omitting it from the response would have us
            // compress into a window it cannot read. Honour it where we can.
            // `set_max_window_bits` rejects anything outside the supported range,
            // which is the only authority on what this build can deflate with:
            // RFC 7692 allows 8 but flate2 cannot compress that narrow.
            ("server_max_window_bits", Some(bits)) => {
                let requested = bits.parse::<u8>().ok()?;
                deflate = deflate.set_max_window_bits(Role::Server, requested).ok()?;
                echo.push(format!("server_max_window_bits={requested}"));
            }
            // RFC 7692 section 7.1.2.1 requires a value for this parameter, so a
            // bare token is malformed rather than a request for our default.
            ("server_max_window_bits", None) => return None,
            // An unknown or malformed parameter makes the offer one we cannot
            // claim to satisfy.
            _ => return None,
        }
    }

    let response_value = std::iter::once("permessage-deflate".to_owned())
        .chain(echo)
        .collect::<Vec<_>>()
        .join("; ");
    Some(Negotiated {
        response_value,
        deflate,
    })
}

/// Build the socket config for a negotiated connection.
pub(crate) fn socket_config(max_frame_bytes: usize, deflate: DeflateConfig) -> WebSocketConfig {
    let mut config = WebSocketConfig::default()
        .max_message_size(Some(max_frame_bytes))
        .max_frame_size(Some(max_frame_bytes));
    config.extensions.permessage_deflate = Some(deflate);
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accepted(header: &str) -> String {
        negotiate(Some(header))
            .expect("offer should be accepted")
            .response_value
    }

    #[test]
    fn no_header_declines() {
        assert!(negotiate(None).is_none());
    }

    #[test]
    fn bare_offer_is_accepted_bare() {
        assert_eq!(accepted("permessage-deflate"), "permessage-deflate");
    }

    #[test]
    fn what_browsers_send_is_accepted() {
        // Chrome and Firefox both offer the valueless form.
        assert_eq!(
            accepted("permessage-deflate; client_max_window_bits"),
            "permessage-deflate"
        );
    }

    #[test]
    fn client_max_window_bits_is_not_echoed_because_it_is_not_honoured() {
        // Echoing a value we do not configure is the failure this guards.
        assert_eq!(
            accepted("permessage-deflate; client_max_window_bits=10"),
            "permessage-deflate"
        );
    }

    #[test]
    fn takeover_flags_are_honoured_and_echoed_together() {
        let n = negotiate(Some(
            "permessage-deflate; server_no_context_takeover; client_no_context_takeover",
        ))
        .expect("both flags are settable");
        assert!(n.deflate.server_no_context_takeover);
        assert!(n.deflate.client_no_context_takeover);
        assert_eq!(
            n.response_value,
            "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
        );
    }

    #[test]
    fn each_takeover_flag_is_echoed_only_when_offered() {
        let n =
            negotiate(Some("permessage-deflate; server_no_context_takeover")).expect("settable");
        assert!(n.deflate.server_no_context_takeover);
        assert!(!n.deflate.client_no_context_takeover);
        assert_eq!(
            n.response_value,
            "permessage-deflate; server_no_context_takeover"
        );
    }

    #[test]
    fn a_supported_server_window_is_honoured_and_echoed() {
        // The clients that send this are the memory-constrained ones the feature
        // exists for; declining would leave them uncompressed.
        let n = negotiate(Some("permessage-deflate; server_max_window_bits=10"))
            .expect("10 is inside the supported range");
        assert_eq!(
            n.response_value,
            "permessage-deflate; server_max_window_bits=10"
        );
        assert_eq!(n.deflate.server_max_window_bits().get(), 10);
    }

    #[test]
    fn every_supported_server_window_is_honoured() {
        // 9..=15 is what this build can deflate with; the setter is the authority.
        for bits in 9..=15u8 {
            let offer = format!("permessage-deflate; server_max_window_bits={bits}");
            let n = negotiate(Some(&offer)).unwrap_or_else(|| panic!("{bits} should be honoured"));
            assert_eq!(n.deflate.server_max_window_bits().get(), bits);
        }
    }

    #[test]
    fn a_window_we_cannot_compress_with_declines() {
        // RFC 7692 allows 8; flate2 cannot deflate that narrow, so answering
        // without the parameter would compress into a window it cannot read.
        assert!(negotiate(Some("permessage-deflate; server_max_window_bits=8")).is_none());
        assert!(negotiate(Some("permessage-deflate; server_max_window_bits=16")).is_none());
        assert!(negotiate(Some("permessage-deflate; server_max_window_bits=abc")).is_none());
    }

    #[test]
    fn a_valueless_server_window_declines() {
        // RFC 7692 section 7.1.2.1 requires a value for this parameter.
        assert!(negotiate(Some("permessage-deflate; server_max_window_bits")).is_none());
    }

    #[test]
    fn unknown_parameter_declines() {
        assert!(negotiate(Some("permessage-deflate; x-not-a-parameter")).is_none());
    }

    #[test]
    fn a_takeover_flag_with_a_value_declines() {
        // RFC 7692 gives these no value; one that has a value is malformed.
        assert!(negotiate(Some("permessage-deflate; server_no_context_takeover=1")).is_none());
    }

    #[test]
    fn other_extensions_are_skipped_not_fatal() {
        assert_eq!(
            accepted("x-other-extension, permessage-deflate"),
            "permessage-deflate"
        );
    }

    #[test]
    fn an_unacceptable_deflate_offer_does_not_shadow_an_acceptable_one() {
        // Chrome sends one offer, but the RFC allows several and the first may
        // be the one we decline.
        assert_eq!(
            accepted("permessage-deflate; server_max_window_bits=8, permessage-deflate"),
            "permessage-deflate"
        );
    }

    #[test]
    fn no_deflate_offer_declines() {
        assert!(negotiate(Some("x-other-extension")).is_none());
    }

    #[test]
    fn name_and_parameters_are_case_insensitive() {
        let n = negotiate(Some("PerMessage-Deflate; Server_No_Context_Takeover"))
            .expect("names are ASCII case-insensitive");
        assert!(n.deflate.server_no_context_takeover);
    }

    #[test]
    fn quoted_parameter_values_are_unwrapped() {
        // A legal quoted form must not read as an unknown parameter.
        assert_eq!(
            accepted("permessage-deflate; client_max_window_bits=\"10\""),
            "permessage-deflate"
        );
    }

    #[test]
    fn socket_config_carries_the_negotiated_deflate() {
        let n = negotiate(Some("permessage-deflate; client_no_context_takeover")).unwrap();
        let config = socket_config(64 * 1024, n.deflate);
        let deflate = config
            .extensions
            .permessage_deflate
            .expect("deflate is configured");
        assert!(deflate.client_no_context_takeover);
        assert_eq!(config.max_frame_size, Some(64 * 1024));
    }
}

// ---------------------------------------------------------------------------
// Driving the upgrade ourselves
// ---------------------------------------------------------------------------

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::ws::Message as WsMessage;
use axum::http::{header, HeaderValue, Request, Response, StatusCode};
use futures_util::{Sink, Stream};
use hyper_util::rt::TokioIo;
use tokio_tungstenite_pmd::WebSocketStream;
use tungstenite_pmd::handshake::derive_accept_key;
use tungstenite_pmd::protocol::frame::coding::CloseCode;
use tungstenite_pmd::protocol::{CloseFrame, Message as TMessage};

/// A negotiated `permessage-deflate` connection, presented in axum's message and
/// error types so [`crate::connection`] cannot tell which socket it holds.
pub(crate) struct DeflateSocket(WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>);

impl Stream for DeflateSocket {
    type Item = Result<WsMessage, axum::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return match Pin::new(&mut self.0).poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => match into_axum(msg) {
                    // `Frame` is only produced by the low-level API, which we do
                    // not use. Skip rather than invent a message for it.
                    None => continue,
                    Some(msg) => Poll::Ready(Some(Ok(msg))),
                },
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(axum::Error::new(e)))),
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

impl Sink<WsMessage> for DeflateSocket {
    type Error = axum::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0)
            .poll_ready(cx)
            .map_err(axum::Error::new)
    }

    fn start_send(mut self: Pin<&mut Self>, item: WsMessage) -> Result<(), Self::Error> {
        Pin::new(&mut self.0)
            .start_send(from_axum(item))
            .map_err(axum::Error::new)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0)
            .poll_flush(cx)
            .map_err(axum::Error::new)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.0)
            .poll_close(cx)
            .map_err(axum::Error::new)
    }
}

/// `None` for `Frame`, which the high-level read path never yields.
fn into_axum(msg: TMessage) -> Option<WsMessage> {
    Some(match msg {
        // Both `Utf8Bytes` are validated wrappers over `bytes::Bytes`, so this
        // moves the buffer and re-validates rather than copying.
        TMessage::Text(t) => WsMessage::Text(bytes::Bytes::from(t).try_into().ok()?),
        TMessage::Binary(b) => WsMessage::Binary(b),
        TMessage::Ping(b) => WsMessage::Ping(b),
        TMessage::Pong(b) => WsMessage::Pong(b),
        TMessage::Close(f) => WsMessage::Close(f.map(|f| axum::extract::ws::CloseFrame {
            code: f.code.into(),
            reason: bytes::Bytes::from(f.reason).try_into().unwrap_or_default(),
        })),
        TMessage::Frame(_) => return None,
    })
}

fn from_axum(msg: WsMessage) -> TMessage {
    match msg {
        WsMessage::Text(t) => TMessage::Text(
            bytes::Bytes::from(t)
                .try_into()
                .unwrap_or_else(|_| String::new().into()),
        ),
        WsMessage::Binary(b) => TMessage::Binary(b),
        WsMessage::Ping(b) => TMessage::Ping(b),
        WsMessage::Pong(b) => TMessage::Pong(b),
        WsMessage::Close(f) => TMessage::Close(f.map(|f| CloseFrame {
            code: CloseCode::from(f.code),
            reason: bytes::Bytes::from(f.reason).try_into().unwrap_or_default(),
        })),
    }
}

/// Answer the upgrade ourselves so the 101 can carry the extension header.
///
/// Returns `None` when this is not a WebSocket upgrade we can complete, leaving
/// the caller to fall through to axum's own path — which produces the correct
/// rejection for a malformed request, so we deliberately do not duplicate that.
pub(crate) fn try_upgrade<F, Fut>(
    req: &mut Request<Body>,
    negotiated: Negotiated,
    max_frame_bytes: usize,
    on_socket: F,
) -> Option<Response<Body>>
where
    F: FnOnce(DeflateSocket) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    if !is_websocket_upgrade(req) {
        return None;
    }
    let accept = derive_accept_key(req.headers().get(header::SEC_WEBSOCKET_KEY)?.as_bytes());
    let extensions = HeaderValue::from_str(&negotiated.response_value).ok()?;
    // Build the response before taking anything out of the request: removing
    // `OnUpgrade` is what commits us, because axum's extractor would then find it
    // missing. Every fallible step therefore happens while `None` is still safe.
    let response = Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::CONNECTION, HeaderValue::from_static("upgrade"))
        .header(header::UPGRADE, HeaderValue::from_static("websocket"))
        .header(header::SEC_WEBSOCKET_ACCEPT, accept)
        .header(header::SEC_WEBSOCKET_EXTENSIONS, extensions)
        .body(Body::empty())
        .ok()?;

    let on_upgrade = req.extensions_mut().remove::<hyper::upgrade::OnUpgrade>()?;
    let config = socket_config(max_frame_bytes, negotiated.deflate);

    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => {
                let stream = WebSocketStream::from_raw_socket(
                    TokioIo::new(upgraded),
                    Role::Server,
                    Some(config),
                )
                .await;
                on_socket(DeflateSocket(stream)).await;
            }
            // The client vanished between our 101 and the upgrade completing.
            Err(e) => tracing::debug!("deflate upgrade never completed: {e}"),
        }
    });

    Some(response)
}

/// The RFC 6455 section 4.2.1 preconditions axum checks before upgrading.
fn is_websocket_upgrade(req: &Request<Body>) -> bool {
    fn header_contains(req: &Request<Body>, name: header::HeaderName, needle: &str) -> bool {
        req.headers()
            .get(&name)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.to_ascii_lowercase().contains(needle))
    }

    req.method() == axum::http::Method::GET
        && header_contains(req, header::CONNECTION, "upgrade")
        && header_contains(req, header::UPGRADE, "websocket")
        && req
            .headers()
            .get(header::SEC_WEBSOCKET_VERSION)
            .is_some_and(|v| v == "13")
        && req.headers().contains_key(header::SEC_WEBSOCKET_KEY)
}

#[cfg(test)]
mod upgrade_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use axum::routing::get;
    use axum::Router;
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tungstenite_pmd::extensions::compression::deflate::DeflateConfig;

    use super::*;

    /// A relay server whose only job is to echo one message back, reached through
    /// a byte-counting TCP proxy so the wire cost is measured rather than assumed.
    struct Harness {
        proxy_addr: std::net::SocketAddr,
        wire_bytes: Arc<AtomicUsize>,
    }

    async fn spawn_harness(payload: String) -> Harness {
        spawn_harness_many(vec![payload]).await
    }

    /// Several payloads over one connection, which is the only way context
    /// takeover shows up: each message reuses the previous one's sliding window.
    async fn spawn_harness_many(payloads: Vec<String>) -> Harness {
        let app = Router::new().route(
            "/",
            get(move |mut req: Request<Body>| {
                let payloads = payloads.clone();
                async move {
                    let negotiated = negotiate(
                        req.headers()
                            .get(header::SEC_WEBSOCKET_EXTENSIONS)
                            .and_then(|v| v.to_str().ok()),
                    )
                    .expect("the test client always offers deflate");
                    try_upgrade(
                        &mut req,
                        negotiated,
                        1 << 20,
                        move |mut socket| async move {
                            for payload in payloads {
                                socket
                                    .send(WsMessage::Text(payload.into()))
                                    .await
                                    .expect("send the payload");
                            }
                            socket.flush().await.expect("flush");
                            // Hold the socket open until the client has read them.
                            let _ = socket.next().await;
                        },
                    )
                    .expect("the test client always sends a valid upgrade")
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

        // Counting proxy: every byte the client and server exchange passes here.
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
        let wire_bytes = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&wire_bytes);
        tokio::spawn(async move {
            let (mut client_side, _) = proxy_listener.accept().await.expect("proxy accept");
            let mut server_side = tokio::net::TcpStream::connect(server_addr)
                .await
                .expect("proxy dial");
            let (mut cr, mut cw) = client_side.split();
            let (mut sr, mut sw) = server_side.split();
            let up = async {
                let mut buf = [0u8; 8192];
                loop {
                    match cr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if sw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            };
            let down = async {
                let mut buf = [0u8; 8192];
                loop {
                    match sr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            counter.fetch_add(n, Ordering::Relaxed);
                            if cw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            };
            tokio::join!(up, down);
        });

        Harness {
            proxy_addr,
            wire_bytes,
        }
    }

    /// Negative control on the instrument: the same harness, a client that offers
    /// no extensions, so the server declines and nothing is compressed. If this
    /// also reported a small wire cost, the counter would be measuring nothing.
    #[tokio::test]
    async fn without_an_offer_the_same_payload_costs_its_full_size() {
        let payload = "buzz relay event payload ".repeat(10_000);
        let payload_len = payload.len();
        let harness = spawn_harness_declining(payload.clone()).await;

        let (mut client, response) =
            tokio_tungstenite_pmd::connect_async(format!("ws://{}/", harness.proxy_addr))
                .await
                .expect("client connects");

        assert!(
            response
                .headers()
                .get(header::SEC_WEBSOCKET_EXTENSIONS)
                .is_none(),
            "a client that offered nothing must not be told an extension was agreed"
        );

        let received = tokio::time::timeout(std::time::Duration::from_secs(10), client.next())
            .await
            .expect("payload arrives")
            .expect("stream is open")
            .expect("no protocol error");
        match received {
            tungstenite_pmd::protocol::Message::Text(text) => {
                assert_eq!(text.as_str(), payload);
            }
            other => panic!("expected the text payload, got {other:?}"),
        }

        let wire = harness.wire_bytes.load(Ordering::Relaxed);
        assert!(
            wire >= payload_len,
            "uncompressed, the wire cost must be at least the {payload_len} byte payload, saw {wire}"
        );
        println!("CONTROL payload={payload_len} wire={wire} (uncompressed)");
    }

    /// Same payload and same counting proxy, but served through axum's plain
    /// upgrade — the path a client that offers nothing takes today.
    async fn spawn_harness_declining(payload: String) -> Harness {
        spawn_harness_declining_many(vec![payload]).await
    }

    async fn spawn_harness_declining_many(payloads: Vec<String>) -> Harness {
        let app = Router::new().route(
            "/",
            get(move |ws: axum::extract::ws::WebSocketUpgrade| {
                let payloads = payloads.clone();
                async move {
                    ws.on_upgrade(move |mut socket| async move {
                        for payload in payloads {
                            socket
                                .send(WsMessage::Text(payload.into()))
                                .await
                                .expect("send the payload");
                        }
                        let _ = socket.recv().await;
                    })
                }
            }),
        );
        serve_through_counting_proxy(app).await
    }

    /// Measurement against captured relay traffic, on demand rather than in CI:
    ///
    /// ```sh
    /// BUZZ_DEFLATE_FRAMES=/path/to/events.jsonl \
    ///   cargo test -p buzz-relay --lib real_relay_traffic -- --ignored --nocapture
    /// ```
    ///
    /// One frame per line, all over a single connection so context takeover
    /// applies — that is where most of the ratio comes from, and a per-message
    /// measurement understates it substantially.
    #[tokio::test]
    #[ignore = "needs a captured frame file; see the doc comment"]
    async fn real_relay_traffic() {
        let path = std::env::var("BUZZ_DEFLATE_FRAMES")
            .expect("set BUZZ_DEFLATE_FRAMES to a file of one frame per line");
        let frames: Vec<String> = std::fs::read_to_string(&path)
            .expect("read the frame file")
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(str::to_owned)
            .collect();
        assert!(!frames.is_empty(), "the frame file is empty");
        let raw: usize = frames.iter().map(String::len).sum();

        for deflate in [true, false] {
            let harness = if deflate {
                spawn_harness_many(frames.clone()).await
            } else {
                spawn_harness_declining_many(frames.clone()).await
            };
            let mut client = if deflate {
                tokio_tungstenite_pmd::connect_async_with_config(
                    format!("ws://{}/", harness.proxy_addr),
                    Some(client_config()),
                    false,
                )
                .await
                .expect("client connects")
                .0
            } else {
                tokio_tungstenite_pmd::connect_async(format!("ws://{}/", harness.proxy_addr))
                    .await
                    .expect("client connects")
                    .0
            };

            let mut seen = 0usize;
            while seen < frames.len() {
                let msg = tokio::time::timeout(std::time::Duration::from_secs(30), client.next())
                    .await
                    .expect("frames arrive")
                    .expect("stream stays open")
                    .expect("no protocol error");
                if let tungstenite_pmd::protocol::Message::Text(t) = msg {
                    assert_eq!(t.as_str(), frames[seen], "frame {seen} must round-trip");
                    seen += 1;
                }
            }

            let wire = harness.wire_bytes.load(Ordering::Relaxed);
            let label = if deflate { "deflate" } else { "plain  " };
            println!(
                "REAL {label} frames={} raw={raw} wire={wire} ratio={:.2}x",
                frames.len(),
                raw as f64 / wire as f64
            );
        }
    }

    /// Compression level sweep on captured traffic. Ratio is deterministic --
    /// same input, same settings, same output bytes -- so this needs no
    /// repetition and no control.
    ///
    /// ```sh
    /// BUZZ_DEFLATE_FRAMES=/path/to/events.jsonl \
    ///   cargo test -p buzz-relay --lib level_sweep -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "needs a captured frame file; see the doc comment"]
    async fn level_sweep() {
        let path = std::env::var("BUZZ_DEFLATE_FRAMES").expect("set BUZZ_DEFLATE_FRAMES");
        let frames: Vec<String> = std::fs::read_to_string(&path)
            .expect("read the frame file")
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(str::to_owned)
            .collect();
        let raw: usize = frames.iter().map(String::len).sum();

        for level in 1..=9u32 {
            let harness = spawn_harness_many_at_level(frames.clone(), level).await;
            let mut client = tokio_tungstenite_pmd::connect_async_with_config(
                format!("ws://{}/", harness.proxy_addr),
                Some(client_config()),
                false,
            )
            .await
            .expect("client connects")
            .0;
            let mut seen = 0usize;
            while seen < frames.len() {
                let msg = tokio::time::timeout(std::time::Duration::from_secs(30), client.next())
                    .await
                    .expect("frames arrive")
                    .expect("open")
                    .expect("no error");
                if matches!(msg, tungstenite_pmd::protocol::Message::Text(_)) {
                    seen += 1;
                }
            }
            let wire = harness.wire_bytes.load(Ordering::Relaxed);
            println!(
                "SWEEP level={level} raw={raw} wire={wire} ratio={:.3}x",
                raw as f64 / wire as f64
            );
        }
    }

    /// Ratio against the server's compression window, 9..=15. Deterministic, so
    /// no repetition or control -- same shape as the level sweep.
    ///
    /// The server can set this unilaterally: an inflater with a wider window
    /// reads a narrower stream, so we compress at N and put nothing in the
    /// response. That is what makes it the one memory knob usable without
    /// client cooperation.
    ///
    /// ```sh
    /// BUZZ_DEFLATE_FRAMES=/path/to/events.jsonl \
    ///   cargo test -p buzz-relay --lib window_sweep -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "needs a captured frame file; see the doc comment"]
    async fn window_sweep() {
        let path = std::env::var("BUZZ_DEFLATE_FRAMES").expect("set BUZZ_DEFLATE_FRAMES");
        let frames: Vec<String> = std::fs::read_to_string(&path)
            .expect("read the frame file")
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(str::to_owned)
            .collect();
        let raw: usize = frames.iter().map(String::len).sum();

        for wb in 9..=15u8 {
            let harness = spawn_harness_many_at_window(frames.clone(), wb).await;
            let mut client = tokio_tungstenite_pmd::connect_async_with_config(
                format!("ws://{}/", harness.proxy_addr),
                Some(client_config()),
                false,
            )
            .await
            .expect("client connects")
            .0;
            let mut seen = 0usize;
            while seen < frames.len() {
                let msg = tokio::time::timeout(std::time::Duration::from_secs(30), client.next())
                    .await
                    .expect("frames arrive")
                    .expect("open")
                    .expect("no error");
                if matches!(msg, tungstenite_pmd::protocol::Message::Text(_)) {
                    seen += 1;
                }
            }
            let wire = harness.wire_bytes.load(Ordering::Relaxed);
            println!(
                "WINDOW wb={wb} raw={raw} wire={wire} ratio={:.3}x",
                raw as f64 / wire as f64
            );
        }
    }

    async fn spawn_harness_many_at_window(payloads: Vec<String>, wb: u8) -> Harness {
        let app = Router::new().route(
            "/",
            get(move |mut req: Request<Body>| {
                let payloads = payloads.clone();
                async move {
                    let mut negotiated = negotiate(
                        req.headers()
                            .get(header::SEC_WEBSOCKET_EXTENSIONS)
                            .and_then(|v| v.to_str().ok()),
                    )
                    .expect("offered");
                    negotiated.deflate = negotiated
                        .deflate
                        .set_max_window_bits(Role::Server, wb)
                        .expect("9..=15 is the supported range");
                    try_upgrade(
                        &mut req,
                        negotiated,
                        1 << 20,
                        move |mut socket| async move {
                            for payload in payloads {
                                socket
                                    .send(WsMessage::Text(payload.into()))
                                    .await
                                    .expect("send");
                            }
                            socket.flush().await.expect("flush");
                            let _ = socket.next().await;
                        },
                    )
                    .expect("valid upgrade")
                }
            }),
        );
        serve_through_counting_proxy(app).await
    }

    async fn spawn_harness_many_at_level(payloads: Vec<String>, level: u32) -> Harness {
        let app = Router::new().route(
            "/",
            get(move |mut req: Request<Body>| {
                let payloads = payloads.clone();
                async move {
                    let mut negotiated = negotiate(
                        req.headers()
                            .get(header::SEC_WEBSOCKET_EXTENSIONS)
                            .and_then(|v| v.to_str().ok()),
                    )
                    .expect("offered");
                    negotiated.deflate.compression = flate2::Compression::new(level);
                    try_upgrade(
                        &mut req,
                        negotiated,
                        1 << 20,
                        move |mut socket| async move {
                            for payload in payloads {
                                socket
                                    .send(WsMessage::Text(payload.into()))
                                    .await
                                    .expect("send");
                            }
                            socket.flush().await.expect("flush");
                            let _ = socket.next().await;
                        },
                    )
                    .expect("valid upgrade")
                }
            }),
        );
        serve_through_counting_proxy(app).await
    }

    fn client_config() -> tungstenite_pmd::protocol::WebSocketConfig {
        let mut config = tungstenite_pmd::protocol::WebSocketConfig::default();
        config.extensions.permessage_deflate = Some(DeflateConfig::default());
        config
    }

    #[tokio::test]
    async fn the_handshake_negotiates_deflate_and_the_payload_is_compressed_on_the_wire() {
        // 256 KB of highly compressible text: if deflate is not actually running,
        // the wire cost cannot be small.
        let payload = "buzz relay event payload ".repeat(10_000);
        let payload_len = payload.len();
        let harness = spawn_harness(payload.clone()).await;

        let (mut client, response) = tokio_tungstenite_pmd::connect_async_with_config(
            format!("ws://{}/", harness.proxy_addr),
            Some(client_config()),
            false,
        )
        .await
        .expect("client connects");

        // 1. The server answered the offer in the 101.
        let negotiated_header = response
            .headers()
            .get(header::SEC_WEBSOCKET_EXTENSIONS)
            .expect("the 101 carries Sec-WebSocket-Extensions")
            .to_str()
            .expect("header is ASCII");
        assert_eq!(negotiated_header, "permessage-deflate");

        // 2. The payload survives the round trip byte for byte.
        let received = tokio::time::timeout(std::time::Duration::from_secs(10), client.next())
            .await
            .expect("payload arrives")
            .expect("stream is open")
            .expect("no protocol error");
        match received {
            tungstenite_pmd::protocol::Message::Text(text) => {
                assert_eq!(text.as_str(), payload, "payload must round-trip intact");
            }
            other => panic!("expected the text payload, got {other:?}"),
        }

        // 3. It cost far fewer bytes than it occupies. This is the whole point.
        let wire = harness.wire_bytes.load(Ordering::Relaxed);
        assert!(
            wire < payload_len / 10,
            "expected the wire cost to be under a tenth of {payload_len} bytes, saw {wire}"
        );
        println!(
            "PROOF payload={payload_len} wire={wire} ratio={:.1}x",
            payload_len as f64 / wire as f64
        );
    }
}
