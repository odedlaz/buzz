//! axum routers — app (WebSocket + REST), health (K8s probes), metrics (Prometheus).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{ConnectInfo, FromRequest, PerMessageDeflate, State, WebSocketUpgrade},
    http::{HeaderMap, Request, StatusCode},
    middleware,
    response::{IntoResponse, Json},
    routing::{get, post, put},
    Router,
};
use serde_json::json;
use tower::ServiceExt;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::services::ServeDir;
use tower_http::trace::{HttpMakeClassifier, TraceLayer};

use crate::api;
use crate::audio;
use crate::connection::handle_connection;
use crate::metrics::track_metrics;
use crate::nip11::{nip11_document, relay_info_handler};
use crate::state::AppState;

/// Build the axum [`Router`] with all relay routes, middleware, and CORS configuration.
///
/// Pure Nostr protocol: WebSocket (NIP-01), HTTP bridge (NIP-98), media (Blossom),
/// git (smart HTTP), NIP-05, and health probes.
pub fn build_router(state: Arc<AppState>) -> Router {
    let media_body_limit = state
        .config
        .media
        .max_image_bytes
        .max(state.config.media.max_video_bytes) as usize;
    let media_router = Router::new()
        .route("/upload", put(api::media::upload_blob))
        .route("/media/upload", put(api::media::upload_blob))
        .route(
            "/media/{sha256_ext}",
            get(api::media::get_blob).head(api::media::head_blob),
        )
        .layer(RequestBodyLimitLayer::new(media_body_limit))
        .with_state(state.clone());

    let git_router = api::git::git_router(state.clone());

    let git_policy_router = api::git::git_policy_router(state.clone());

    let admin_enabled = state.config.admin.is_some();
    let admin_web_dir = state
        .config
        .admin
        .as_ref()
        .and_then(|config| config.web_dir.clone());
    let admin_router = admin_enabled
        .then(|| Router::new().nest("/api/admin/v1", api::admin::router(state.clone())));

    let api_router = Router::new()
        // WebSocket + NIP-11
        .route("/", get(nip11_or_ws_handler))
        .route("/info", get(relay_info_handler))
        .route("/.well-known/nostr.json", get(api::nip05::nostr_nip05))
        // Health endpoints
        .route("/health", get(health_handler))
        .route("/_liveness", get(liveness_handler))
        .route("/_readiness", get(readiness_handler))
        // Nostr HTTP bridge (NIP-98 auth)
        .route("/events", post(api::bridge::submit_event))
        .route("/query", post(api::bridge::query_events))
        .route("/count", post(api::bridge::count_events))
        .route(
            "/workflows/{workflow_id}/runs",
            get(api::workflows::workflow_runs),
        )
        .route(
            "/workflows/{workflow_id}/runs/{run_id}/approvals",
            get(api::workflows::run_approvals),
        )
        .route(
            "/operator/communities",
            get(api::operator::list_owned_communities).post(api::operator::provision_community),
        )
        .route(
            "/operator/communities/archive",
            post(api::operator::archive_community),
        )
        .route(
            "/operator/communities/unarchive",
            post(api::operator::unarchive_community),
        )
        .route(
            "/operator/communities/availability",
            get(api::operator::community_availability),
        )
        .route(
            "/operator/communities/transfer",
            post(api::operator::transfer_community),
        )
        // Relay invites: mint (owner/admin) + claim (membership-gate exempt)
        .route("/api/invites", post(api::invites::mint_invite))
        .route("/api/join-policy", get(api::invites::join_policy))
        // Policy documents as standalone pages — desktop opens these in the
        // system browser instead of rendering the Markdown in-app.
        .route(
            "/api/join-policy/terms",
            get(api::invites::join_policy_terms),
        )
        .route(
            "/api/join-policy/privacy",
            get(api::invites::join_policy_privacy),
        )
        .route(
            "/api/invites/accept-policy",
            post(api::invites::accept_policy),
        )
        .route("/api/invites/claim", post(api::invites::claim_invite))
        // Moderation queue reads (NIP-98 auth + mod-authz gate, L6)
        .route("/moderation/reports", get(api::bridge::moderation_reports))
        .route("/moderation/audit", get(api::bridge::moderation_audit))
        .route(
            "/moderation/restricted",
            get(api::bridge::moderation_restricted),
        )
        // Webhook trigger (secret-authenticated, no NIP-98)
        .route("/hooks/{id}", post(api::bridge::workflow_webhook))
        // Mesh demo echo probe — testbed-only; 404 unless BUZZ_MESH=on and
        // BUZZ_MESH_DEMO_ECHO=on (see api::mesh_demo).
        .route("/_mesh/demo/echo", post(api::mesh_demo::demo_echo))
        // Huddle audio WebSocket route
        .route(
            "/huddle/{channel_id}/audio",
            get(audio::handler::ws_audio_handler),
        )
        // Reject request bodies larger than 1 MB to prevent resource exhaustion.
        .layer(RequestBodyLimitLayer::new(1024 * 1024))
        .with_state(state.clone());

    // Merge — each sub-router carries its own body limit.
    // Metrics → Trace → CORS applied once over the combined router.
    let mut merged = api_router
        .merge(media_router)
        .merge(git_router)
        .merge(git_policy_router);
    if let Some(admin_router) = admin_router {
        merged = merged.merge(admin_router);
    }

    // Serve both bundles from one fallback. The admin host is checked first so
    // it can never fall through to the public web bundle.
    let web_dir = state.config.web_dir.clone();
    if admin_web_dir.is_some() || web_dir.is_some() {
        let admin_index = admin_web_dir.as_ref().map(|dir| dir.join("index.html"));
        let admin_files = admin_web_dir.map(ServeDir::new);
        let web_index = web_dir.as_ref().map(|dir| dir.join("index.html"));
        let web_files = web_dir.map(ServeDir::new);
        let serve_git_web_gui = state.config.serve_git_web_gui;
        let fallback_state = state.clone();
        let spa_fallback = tower::service_fn(move |req: axum::extract::Request| {
            let admin_index = admin_index.clone();
            let admin_files = admin_files.clone();
            let web_index = web_index.clone();
            let web_files = web_files.clone();
            let state = fallback_state.clone();
            async move {
                let path = req.uri().path();
                let admin_host = api::admin::is_admin_host(&state, req.headers());
                if admin_host {
                    if let (Some(index), Some(files)) = (admin_index, admin_files) {
                        if path.starts_with("/assets/") {
                            return files.oneshot(req).await.map(IntoResponse::into_response);
                        }
                        if is_admin_spa_path(path) {
                            return Ok(read_spa_index(&index).await);
                        }
                    }
                    return Ok(StatusCode::NOT_FOUND.into_response());
                }

                if let (Some(index), Some(files)) = (web_index, web_files) {
                    if path.starts_with("/assets/") {
                        return files.oneshot(req).await.map(IntoResponse::into_response);
                    }
                    if should_serve_spa(path, serve_git_web_gui) {
                        return Ok(read_spa_index(&index).await);
                    }
                }
                Ok(StatusCode::NOT_FOUND.into_response())
            }
        });
        merged = merged.fallback_service(spa_fallback);
    }

    merged
        .layer(middleware::from_fn(track_metrics))
        .layer(http_trace_layer())
        .layer(build_cors_layer(&state.config.cors_origins))
}

fn http_trace_layer() -> TraceLayer<HttpMakeClassifier, fn(&Request<Body>) -> tracing::Span> {
    TraceLayer::new_for_http().make_span_with(make_http_span as fn(&Request<Body>) -> tracing::Span)
}

fn make_http_span(request: &Request<Body>) -> tracing::Span {
    tracing::info_span!(
        target: "buzz_relay",
        "http.request",
        otel.kind = "server",
        http.request.method = %request.method(),
    )
}

fn is_admin_spa_path(path: &str) -> bool {
    path == "/"
        || path == "/reports"
        || path.starts_with("/reports/")
        || path == "/feedback"
        || path.starts_with("/feedback/")
}

fn is_invite_landing_path(path: &str) -> bool {
    path.strip_prefix("/invite/")
        .is_some_and(|code| !code.is_empty() && !code.contains('/'))
}

fn should_serve_spa(path: &str, serve_git_web_gui: bool) -> bool {
    is_invite_landing_path(path) || (serve_git_web_gui && is_git_web_gui_path(path))
}

fn is_git_web_gui_path(path: &str) -> bool {
    path == "/" || path == "/repos" || path.starts_with("/repos/")
}

async fn read_spa_index(index: &std::path::Path) -> axum::response::Response {
    match tokio::fs::read(index).await {
        Ok(body) => axum::response::Html(body).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Build the health-only router for K8s probes (port 8080 in CAKE).
///
/// No metrics middleware, no auth, no CORS, no body limit.
pub fn build_health_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/_liveness", get(liveness_handler))
        .route("/_readiness", get(readiness_handler))
        .route("/_status", get(status_handler))
        .route("/_mesh", get(mesh_status_handler))
        .with_state(state)
}

/// Content-negotiated: NIP-11 JSON for plain HTTP, WebSocket upgrade otherwise.
async fn nip11_or_ws_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    req: axum::extract::Request,
) -> impl IntoResponse {
    let addr = req
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0)
        .unwrap_or_else(|| std::net::SocketAddr::from(([0, 0, 0, 0], 0)));

    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let raw_host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // `/` is an explicit relay route, so it never reaches the SPA fallback.
    // Short-circuit the exact admin authority here and never let it serve the
    // public web bundle, NIP-11 document, or WebSocket endpoint.
    if api::admin::is_admin_host(&state, &headers) {
        if !accept.contains("text/html") {
            return StatusCode::NOT_FOUND.into_response();
        }
        let Some(index) = state
            .config
            .admin
            .as_ref()
            .and_then(|config| config.web_dir.as_ref())
            .map(|dir| dir.join("index.html"))
        else {
            return StatusCode::NOT_FOUND.into_response();
        };
        return read_spa_index(&index).await;
    }

    if accept.contains("application/nostr+json") {
        return Json(nip11_document(&state, raw_host).await).into_response();
    }

    // Row zero: bind the connection to its community from the request host
    // BEFORE the WebSocket upgrade, so no frame is ever read on an unbound
    // connection. The host is the authoritative selector; an unmapped host or a
    // lookup failure fails closed with a generic rejection — never a default
    // tenant. NIP-11 above is served before binding and stays fail-open: an
    // unmapped host still gets the document (with host-scoped fields like
    // `icon` simply absent), so the doc cannot leak which hosts are mapped.
    let tenant = match crate::tenant::bind_community(&state.db, raw_host).await {
        Ok(ctx) => ctx,
        Err(_) => {
            // Generic rejection: do not distinguish "unmapped" from "lookup
            // error", and never echo the host, so an unauthenticated caller
            // cannot probe which communities exist on this deployment.
            return (
                StatusCode::NOT_FOUND,
                "relay: no community is configured for this host",
            )
                .into_response();
        }
    };

    let max_frame_bytes = state.config.max_frame_bytes;
    let permessage_deflate_enabled = state.config.permessage_deflate_enabled;

    match WebSocketUpgrade::from_request(req, &state).await {
        Ok(ws) => {
            // Shutting down: refuse new sockets instead of accepting a
            // connection onto a dying pod. Readiness already returns 503, but
            // that only stops K8s routing — direct and in-flight upgrades
            // still reach here during the pre-drain grace window. Clients
            // treat the refusal as a normal dial failure and retry, landing
            // on a healthy pod.
            if state.shutting_down.load(Ordering::Relaxed) {
                return (StatusCode::SERVICE_UNAVAILABLE, "relay restarting").into_response();
            }
            limit_relay_websocket(ws, max_frame_bytes, permessage_deflate_enabled)
                .on_upgrade(move |socket| handle_connection(socket, state, addr, tenant))
                .into_response()
        }
        Err(_) => {
            // Browser requesting HTML and Git web GUI is enabled → serve SPA.
            if state.config.serve_git_web_gui {
                if let Some(ref dir) = state.config.web_dir {
                    if accept.contains("text/html") {
                        let index = dir.join("index.html");
                        if let Ok(body) = tokio::fs::read(&index).await {
                            return axum::response::Html(body).into_response();
                        }
                    }
                }
            }
            // Not a WS request and not asking for nostr+json — serve NIP-11 as fallback.
            Json(nip11_document(&state, raw_host).await).into_response()
        }
    }
}

fn limit_relay_websocket<F>(
    ws: WebSocketUpgrade<F>,
    max_frame_bytes: usize,
    permessage_deflate_enabled: bool,
) -> WebSocketUpgrade<F> {
    // recv_loop keeps the application-level check as defense in depth, but
    // parser limits must be set before tungstenite assembles the message.
    let limited = ws
        .max_message_size(max_frame_bytes)
        .max_frame_size(max_frame_bytes);

    // Off must skip the call rather than weaken the policy: `new()` only
    // installs a policy, and any policy still offers the extension — the fork
    // documents `no_context_takeover` as not reducing memory. The cost lands
    // when an offer negotiates, holding a compressor and a decompressor for the
    // lifetime of every such connection.
    if permessage_deflate_enabled {
        limited.compression(PerMessageDeflate::new())
    } else {
        limited
    }
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn liveness_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Readiness probe — checks shutdown flag, Postgres, and Redis connectivity.
async fn readiness_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    use std::time::Duration;

    if state.shutting_down.load(Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "shutting_down"})),
        )
            .into_response();
    }

    let check = async {
        let (pg_ok, redis_ok, deletion_catalog_ok) = tokio::join!(
            state.db.ping(),
            async { state.redis_pool.get().await.is_ok() },
            async { state.db.validate_deletion_serving_catalog().await.is_ok() },
        );
        (pg_ok, redis_ok, deletion_catalog_ok)
    };

    let (pg_ok, redis_ok, deletion_catalog_ok) =
        tokio::time::timeout(Duration::from_secs(2), check)
            .await
            .unwrap_or((false, false, false));

    if pg_ok && redis_ok && deletion_catalog_ok {
        (StatusCode::OK, Json(json!({"status": "ready"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "postgres": pg_ok,
                "redis": redis_ok,
                "deletion_catalog": deletion_catalog_ok
            })),
        )
            .into_response()
    }
}

/// Status endpoint — service name, version, uptime.
async fn status_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let uptime_secs = state.started_at.elapsed().as_secs();
    Json(json!({
        "service": "buzz-relay",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": uptime_secs,
    }))
}

/// `/_mesh` — live mesh status: peer table, connection/phi state, per-peer
/// counters, fence-rejection totals. Mesh-off reports `{"enabled": false}` so
/// operators can distinguish "off" from "on with zero peers".
async fn mesh_status_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.mesh() {
        Some(handle) => Json(serde_json::to_value(handle.status()).unwrap_or_else(
            |e| json!({"enabled": true, "error": format!("status serialize: {e}")}),
        )),
        None => Json(json!({"enabled": false})),
    }
}

/// Build a CORS layer from the configured origins list.
fn build_cors_layer(cors_origins: &[String]) -> CorsLayer {
    if cors_origins.is_empty() {
        return CorsLayer::permissive();
    }

    let origins: Vec<axum::http::HeaderValue> = cors_origins
        .iter()
        .filter_map(|o| o.parse::<axum::http::HeaderValue>().ok())
        .collect();

    if origins.is_empty() {
        tracing::error!(
            "BUZZ_CORS_ORIGINS set but no valid origins could be parsed — \
             refusing to fall back to permissive CORS. Fix the origins or unset \
             the variable for development mode."
        );
        return CorsLayer::new();
    }

    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any)
}

#[cfg(test)]
mod tests {
    use axum::http::header::SEC_WEBSOCKET_EXTENSIONS;
    use axum::{routing::get, Router};
    use futures_util::SinkExt;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_tungstenite::{connect_async, tungstenite::Message};
    use tower::ServiceBuilder;
    use tracing::Instrument as _;
    use tracing_subscriber::prelude::*;

    use super::*;

    #[test]
    fn invite_landing_path_requires_exactly_one_nonempty_code_segment() {
        assert!(is_invite_landing_path("/invite/payload.mac"));
        assert!(!is_invite_landing_path("/invite/"));
        assert!(!is_invite_landing_path("/invite/code/extra"));
        assert!(!is_invite_landing_path("/repos"));
        assert!(!is_invite_landing_path("/"));
    }

    #[test]
    fn git_web_gui_paths_are_explicit() {
        assert!(is_git_web_gui_path("/"));
        assert!(is_git_web_gui_path("/repos"));
        assert!(is_git_web_gui_path("/repos/example"));
        assert!(!is_git_web_gui_path("/repository"));
        assert!(!is_git_web_gui_path("/arbitrary"));
        assert!(!is_git_web_gui_path("/api/invites"));
    }

    #[test]
    fn invite_is_always_served_but_git_gui_requires_opt_in() {
        assert!(should_serve_spa("/invite/payload.mac", false));
        assert!(should_serve_spa("/invite/payload.mac", true));
        assert!(!should_serve_spa("/", false));
        assert!(!should_serve_spa("/repos/example", false));
        assert!(should_serve_spa("/", true));
        assert!(should_serve_spa("/repos/example", true));
        assert!(!should_serve_spa("/arbitrary", true));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn http_and_datastore_spans_are_exported_in_the_same_trace() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("test"))
                .with_filter(crate::telemetry::otel_env_filter(None)),
        );
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);
        let service = ServiceBuilder::new()
            .layer(http_trace_layer())
            .service(tower::service_fn(
                |_: axum::http::Request<axum::body::Body>| async {
                    async {}
                        .instrument(tracing::info_span!(
                            target: "buzz_datastore",
                            "SELECT",
                            otel.kind = "client",
                            db.system.name = "postgresql",
                        ))
                        .await;
                    Ok::<_, std::convert::Infallible>(axum::response::Response::new(
                        axum::body::Body::empty(),
                    ))
                },
            ));

        service
            .oneshot(
                axum::http::Request::get("/")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let http = spans
            .iter()
            .find(|span| span.name == "http.request")
            .unwrap();
        let datastore = spans.iter().find(|span| span.name == "SELECT").unwrap();

        assert_eq!(
            datastore.span_context.trace_id(),
            http.span_context.trace_id()
        );
        assert_eq!(datastore.parent_span_id, http.span_context.span_id());
    }

    async fn handler_receives_message_with_limit(limit: usize, size: usize) -> bool {
        let (received_tx, mut received_rx) = mpsc::unbounded_channel();
        let app = Router::new().route(
            "/",
            get(move |ws: WebSocketUpgrade| {
                let received_tx = received_tx.clone();
                async move {
                    limit_relay_websocket(ws, limit, false).on_upgrade(
                        move |mut socket| async move {
                            let _ = received_tx.send(matches!(socket.recv().await, Some(Ok(_))));
                        },
                    )
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test WebSocket listener");
        let addr = listener.local_addr().expect("test listener address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await;
        });

        let (mut client, _) = connect_async(format!("ws://{addr}/"))
            .await
            .expect("connect test WebSocket client");
        client
            .send(Message::Text("x".repeat(size).into()))
            .await
            .expect("send test WebSocket message");

        let received = tokio::time::timeout(std::time::Duration::from_secs(2), received_rx.recv())
            .await
            .expect("server should process the test message")
            .expect("server should report whether it received the message");

        server.abort();
        let _ = server.await;

        received
    }

    #[tokio::test]
    async fn relay_websocket_parser_rejects_oversized_messages_before_handler_reads_them() {
        let limit = 64;

        assert!(
            handler_receives_message_with_limit(limit, limit).await,
            "messages at the relay limit should still reach the handler"
        );
        assert!(
            !handler_receives_message_with_limit(limit, limit + 1).await,
            "oversized messages must be rejected by the WebSocket parser before the handler sees them"
        );
    }

    /// Drives the production route with a deflate-offering client and reports
    /// whether a Ping of `size` bytes reached the handler.
    async fn negotiated_handler_receives_ping_with_limit(limit: usize, size: usize) -> bool {
        let (received_tx, mut received_rx) = mpsc::unbounded_channel();
        let app = Router::new().route(
            "/",
            get(move |ws: WebSocketUpgrade| {
                let received_tx = received_tx.clone();
                async move {
                    limit_relay_websocket(ws, limit, true).on_upgrade(
                        move |mut socket| async move {
                            let _ = received_tx.send(matches!(socket.recv().await, Some(Ok(_))));
                        },
                    )
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test WebSocket listener");
        let addr = listener.local_addr().expect("test listener address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await;
        });

        let (mut client, response) = tokio_tungstenite_pmd::connect_async_with_config(
            format!("ws://{addr}/"),
            Some(tungstenite_pmd::protocol::WebSocketConfig::default().enable_deflate()),
            false,
        )
        .await
        .expect("connect deflate-offering test client");
        assert_eq!(
            response
                .headers()
                .get(SEC_WEBSOCKET_EXTENSIONS)
                .and_then(|value| value.to_str().ok()),
            Some("permessage-deflate"),
            "this row only means something on a negotiated socket"
        );
        client
            .send(tungstenite_pmd::protocol::Message::Ping(
                vec![0; size].into(),
            ))
            .await
            .expect("send test Ping");

        let received = tokio::time::timeout(std::time::Duration::from_secs(5), received_rx.recv())
            .await
            .expect("server should process the test Ping")
            .expect("server should report whether it received the Ping");

        server.abort();
        let _ = server.await;

        received
    }

    /// Keeps `max_frame_size` bound to the production route once compression is
    /// negotiated. Buzz sets both limits to one value, so a text message cannot
    /// say which one rejected it; a control frame can, because tungstenite
    /// checks the frame limit before opcode dispatch and never consults
    /// `max_message_size` on the control path.
    #[tokio::test]
    async fn negotiated_relay_websocket_keeps_the_frame_limit_on_control_frames() {
        let limit = 64;

        assert!(
            negotiated_handler_receives_ping_with_limit(limit, limit).await,
            "a Ping at the relay limit should still reach the handler"
        );
        assert!(
            !negotiated_handler_receives_ping_with_limit(limit, limit + 1).await,
            "an oversized Ping must be rejected by the frame limit, which the message limit cannot do"
        );
    }

    /// Binds the production compression policy. The `deflate_integration_tests`
    /// rows build their own upgrade, so without this assertion deleting
    /// `.compression(...)` from `limit_relay_websocket` leaves every row green.
    #[tokio::test]
    async fn relay_websocket_route_negotiates_permessage_deflate() {
        let app = Router::new().route(
            "/",
            get(|ws: WebSocketUpgrade| async move {
                limit_relay_websocket(ws, 1 << 20, true).on_upgrade(|mut socket| async move {
                    let _ = socket.recv().await;
                })
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test WebSocket listener");
        let addr = listener.local_addr().expect("test listener address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await;
        });

        let (_client, response) = tokio_tungstenite_pmd::connect_async_with_config(
            format!("ws://{addr}/"),
            Some(tungstenite_pmd::protocol::WebSocketConfig::default().enable_deflate()),
            false,
        )
        .await
        .expect("connect deflate-offering test client");
        let negotiated = response
            .headers()
            .get(SEC_WEBSOCKET_EXTENSIONS)
            .and_then(|value| value.to_str().ok());

        server.abort();
        let _ = server.await;

        assert_eq!(
            negotiated,
            Some("permessage-deflate"),
            "the relay route must negotiate permessage-deflate for an offering client"
        );
    }

    /// The gate's off state must decline the extension, not merely tolerate the
    /// offer. A deflate-offering client connects either way, so only the absent
    /// response header distinguishes "declined" from "negotiated".
    #[tokio::test]
    async fn relay_websocket_route_declines_permessage_deflate_when_gated_off() {
        let app = Router::new().route(
            "/",
            get(|ws: WebSocketUpgrade| async move {
                limit_relay_websocket(ws, 1 << 20, false).on_upgrade(|mut socket| async move {
                    let _ = socket.recv().await;
                })
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test WebSocket listener");
        let addr = listener.local_addr().expect("test listener address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await;
        });

        let (_client, response) = tokio_tungstenite_pmd::connect_async_with_config(
            format!("ws://{addr}/"),
            Some(tungstenite_pmd::protocol::WebSocketConfig::default().enable_deflate()),
            false,
        )
        .await
        .expect("a declined extension must still complete the handshake");
        let negotiated = response
            .headers()
            .get(SEC_WEBSOCKET_EXTENSIONS)
            .and_then(|value| value.to_str().ok());

        server.abort();
        let _ = server.await;

        assert_eq!(
            negotiated, None,
            "gated off, the relay must not negotiate permessage-deflate"
        );
    }

    /// Build a production `AppState` whose route can actually bind a tenant.
    ///
    /// `bind_community` is a real query, so the pool connects eagerly here: a
    /// lazy pool would defer the failure into a 404 that this test cannot tell
    /// apart from a gated-off relay.
    async fn production_state(enabled: bool, authority: &str) -> Arc<AppState> {
        let mut config = crate::config::Config::from_env().expect("default config loads");
        config.permessage_deflate_enabled = enabled;
        config.require_relay_membership = false;
        config.require_auth_token = false;

        let pool = sqlx::PgPool::connect(&config.database_url)
            .await
            .expect("the production route binds its tenant from Postgres");
        sqlx::query(
            "INSERT INTO communities (id, host) VALUES ($1, $2) ON CONFLICT (lower(host)) DO NOTHING",
        )
        .bind(uuid::Uuid::new_v4())
        .bind(authority)
        .execute(&pool)
        .await
        .expect("seed the community this test's host maps to");

        let db = buzz_db::Db::from_pool(pool.clone());
        let redis_pool = deadpool_redis::Config::from_url(&config.redis_url)
            .create_pool(Some(deadpool_redis::Runtime::Tokio1))
            .expect("redis pool");
        let pubsub = Arc::new(
            buzz_pubsub::PubSubManager::new(&config.redis_url, redis_pool.clone())
                .await
                .expect("pubsub manager"),
        );
        let audit = buzz_audit::AuditService::new(pool.clone());
        let auth = buzz_auth::AuthService::new(config.auth.clone());
        let search = buzz_search::SearchService::new(pool.clone());
        let workflow_engine = Arc::new(buzz_workflow::WorkflowEngine::new(
            db.clone(),
            buzz_workflow::WorkflowConfig::default(),
        ));
        let media_storage = buzz_media::MediaStorage::new(&config.media).expect("media storage");
        let (state, _audit_shutdown) = AppState::new(
            config,
            db,
            redis_pool,
            audit,
            pubsub,
            auth,
            search,
            workflow_engine,
            nostr::Keys::generate(),
            media_storage,
        );
        Arc::new(state)
    }

    /// Handshake a deflate-offering client against the real router and report
    /// what the production composition negotiated.
    async fn negotiate_through_production_route(enabled: bool) -> (StatusCode, Option<String>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind production route listener");
        let addr = listener.local_addr().expect("test listener address");
        let authority = format!("127.0.0.1:{}", addr.port());
        let state = production_state(enabled, &authority).await;

        let server = tokio::spawn(async move {
            axum::serve(listener, build_router(state)).await;
        });

        let (_client, response) = tokio_tungstenite_pmd::connect_async_with_config(
            format!("ws://{authority}/"),
            Some(tungstenite_pmd::protocol::WebSocketConfig::default().enable_deflate()),
            false,
        )
        .await
        .expect("the production route must complete the handshake");
        let negotiated = response
            .headers()
            .get(SEC_WEBSOCKET_EXTENSIONS)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let status = response.status();

        server.abort();
        let _ = server.await;

        (status, negotiated)
    }

    /// The four rows above call `limit_relay_websocket` with literals, so
    /// replacing the production read at `nip11_or_ws_handler` with a constant
    /// leaves every one of them green. This row is the only one that fails.
    ///
    /// Each arm asserts the 101 before the header: the gate sits downstream of
    /// the host-to-tenant bind, and an unmapped host answers 404 carrying no
    /// extension header — byte-identical to a correctly gated-off relay, and
    /// green even with the branch deleted.
    #[tokio::test]
    async fn production_route_negotiates_deflate_only_when_the_config_enables_it() {
        let (off_status, off_negotiated) = negotiate_through_production_route(false).await;
        assert_eq!(
            off_status,
            StatusCode::SWITCHING_PROTOCOLS,
            "the off arm must reach the gate, not stop at a tenant rejection"
        );
        assert_eq!(
            off_negotiated, None,
            "default-off must decline the extension on the production route"
        );

        let (on_status, on_negotiated) = negotiate_through_production_route(true).await;
        assert_eq!(
            on_status,
            StatusCode::SWITCHING_PROTOCOLS,
            "the on arm must reach the gate, not stop at a tenant rejection"
        );
        assert_eq!(
            on_negotiated.as_deref(),
            Some("permessage-deflate"),
            "explicit on must negotiate exactly the parameter-free extension"
        );
    }
}
