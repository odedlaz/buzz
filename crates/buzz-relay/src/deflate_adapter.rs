//! How the server offers `permessage-deflate`, for [`super::deflate_measure`].
//!
//! This is the swappable half of the measurement harness: the instrument beside
//! it is byte-identical to the one that measured the relay's own negotiation
//! module, so the two paths are compared by the same code and only this file
//! differs. Everything here is axum's own API.

use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::PerMessageDeflate;
use axum::routing::get;
use axum::Router;

/// The default configuration: RFC 7692's own defaults, accepted as offered.
pub(super) fn echo_app(frames: Vec<String>) -> Router {
    echo_app_with(frames, Some(PerMessageDeflate::new()))
}

/// The negative control: no `compression` call, so the client's offer is
/// declined and nothing is compressed.
pub(super) fn echo_app_without_compression(frames: Vec<String>) -> Router {
    echo_app_with(frames, None)
}

pub(super) fn echo_app_at_level(frames: Vec<String>, level: u8) -> Router {
    echo_app_with(frames, Some(PerMessageDeflate::new().level(level)))
}

pub(super) fn echo_app_at_window(frames: Vec<String>, bits: u8) -> Router {
    echo_app_with(frames, Some(PerMessageDeflate::new().max_window_bits(bits)))
}

fn echo_app_with(frames: Vec<String>, config: Option<PerMessageDeflate>) -> Router {
    Router::new().route(
        "/",
        get(move |ws: WebSocketUpgrade| {
            let frames = frames.clone();
            async move {
                let ws = match config {
                    Some(config) => ws.compression(config),
                    None => ws,
                };
                ws.on_upgrade(move |socket| push_all(socket, frames))
            }
        }),
    )
}

async fn push_all(mut socket: WebSocket, frames: Vec<String>) {
    for frame in frames {
        socket
            .send(WsMessage::Text(frame.into()))
            .await
            .expect("send the frame");
    }
    // Hold the socket open until the client has read everything.
    let _ = socket.recv().await;
}
