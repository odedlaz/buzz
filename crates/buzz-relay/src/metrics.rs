//! Prometheus metrics: recorder setup, upkeep task, policy-gauge refresh,
//! and HTTP middleware.
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │  metrics-rs facade (metrics::counter!, histogram!, etc.) │
//! │         ↓                                                │
//! │  PrometheusBuilder → HTTP listener on :9102              │
//! │         ↓                                                │
//! │  GET /metrics → Prometheus text format                   │
//! └──────────────────────────────────────────────────────────┘
//! ```
//!
//! Framework metrics (`http_requests_total`, `http_request_latency_ms`) are
//! recorded by [`track_metrics`] middleware on the app router. Buzz-specific
//! metrics are recorded inline at their call sites.

use std::time::{Duration, Instant};

use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
use metrics_util::MetricKindMask;

/// HTTP latency buckets (milliseconds) — only for `http_request_latency_ms`.
const LATENCY_BUCKETS_MS: [f64; 11] = [
    5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
];

/// Seconds-scale buckets for internal processing histograms (event, search, audit).
const DURATION_BUCKETS_S: [f64; 10] = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0];

/// Seconds-scale buckets for Git hydration and pack streams.
const GIT_DURATION_BUCKETS_S: [f64; 13] = [
    0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

/// Byte buckets for hydrated repositories and streamed clone/fetch responses.
const GIT_BYTES_BUCKETS: [f64; 9] = [
    0.0,
    64.0 * 1024.0,
    1024.0 * 1024.0,
    10.0 * 1024.0 * 1024.0,
    50.0 * 1024.0 * 1024.0,
    100.0 * 1024.0 * 1024.0,
    250.0 * 1024.0 * 1024.0,
    500.0 * 1024.0 * 1024.0,
    1024.0 * 1024.0 * 1024.0,
];

/// Pack-count buckets bounded by the manifest's maximum pack count.
const GIT_PACK_BUCKETS: [f64; 9] = [0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0];

/// Integer-count buckets for fan-out recipient histograms.
const FANOUT_BUCKETS: [f64; 9] = [0.0, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 500.0, 1000.0];

/// Floor on the usage poll interval. The idle window is floored against the
/// interval, so this also sets the shortest window the relay can be given.
const USAGE_METRICS_MIN_INTERVAL_SECS: u64 = 5;

/// Policy-gauge refreshes per idle window. Three leaves a whole spare refresh
/// inside the window, so one missed wakeup cannot drop a series.
const POLICY_GAUGE_REFRESHES_PER_WINDOW: u64 = 3;

/// Install the global metrics recorder and spawn the Prometheus HTTP exporter.
///
/// `build()` returns the recorder + exporter future and internally spawns
/// the upkeep task, so no separate upkeep call is needed.
///
/// Must be called from within a Tokio runtime.
/// Panics if a recorder is already installed or the port is in use.
pub fn install(port: u16, gauge_idle_timeout_secs: u64) {
    let (recorder, exporter) = PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        // Remove gauge series that the relay intentionally stops emitting.
        .idle_timeout(
            MetricKindMask::GAUGE,
            Some(Duration::from_secs(gauge_idle_timeout_secs)),
        )
        // Per-metric buckets: ms for HTTP latency, seconds for internal processing.
        .set_buckets_for_metric(
            Matcher::Full("http_request_latency_ms".to_owned()),
            &LATENCY_BUCKETS_MS,
        )
        .expect("valid ms bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_git_hydrate_seconds".to_owned()),
            &GIT_DURATION_BUCKETS_S,
        )
        .expect("valid git hydration duration bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_git_upload_pack_stream_seconds".to_owned()),
            &GIT_DURATION_BUCKETS_S,
        )
        .expect("valid git stream duration bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_git_pack_cache_populate_seconds".to_owned()),
            &GIT_DURATION_BUCKETS_S,
        )
        .expect("valid git cache population duration bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_git_pack_cache_population_wait_seconds".to_owned()),
            &GIT_DURATION_BUCKETS_S,
        )
        .expect("valid git cache population wait bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_git_pack_compaction_seconds".to_owned()),
            &GIT_DURATION_BUCKETS_S,
        )
        .expect("valid git compaction duration bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_git_hydrate_bytes".to_owned()),
            &GIT_BYTES_BUCKETS,
        )
        .expect("valid git hydration byte bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_git_upload_pack_stream_bytes".to_owned()),
            &GIT_BYTES_BUCKETS,
        )
        .expect("valid git stream byte bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_git_pack_compaction_bytes".to_owned()),
            &GIT_BYTES_BUCKETS,
        )
        .expect("valid git compaction byte bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_git_hydrate_packs".to_owned()),
            &GIT_PACK_BUCKETS,
        )
        .expect("valid git pack-count bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_git_pack_compaction_packs_before".to_owned()),
            &GIT_PACK_BUCKETS,
        )
        .expect("valid git compaction input pack-count bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_git_pack_compaction_packs_after".to_owned()),
            &GIT_PACK_BUCKETS,
        )
        .expect("valid git compaction output pack-count bucket boundaries")
        .set_buckets_for_metric(Matcher::Suffix("_seconds".to_owned()), &DURATION_BUCKETS_S)
        .expect("valid seconds bucket boundaries")
        .set_buckets_for_metric(
            Matcher::Full("buzz_fanout_recipients".to_owned()),
            &FANOUT_BUCKETS,
        )
        .expect("valid fanout bucket boundaries")
        .build()
        .expect("metrics exporter must build exactly once");

    metrics::set_global_recorder(recorder).expect("global recorder must be set exactly once");
    tokio::spawn(exporter);
}

/// Return the usage poll interval, with a floor that prevents a busy loop.
pub fn usage_metrics_interval_secs() -> u64 {
    std::env::var("BUZZ_USAGE_METRICS_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(300)
        .max(USAGE_METRICS_MIN_INTERVAL_SECS)
}

/// Return a gauge lifetime that always outlives several usage-poller ticks, so
/// that the poller's own gauges survive between its ticks.
pub fn usage_metrics_idle_timeout_secs(interval_secs: u64) -> u64 {
    let configured = std::env::var("BUZZ_USAGE_METRICS_IDLE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok());
    idle_timeout_secs(configured, interval_secs)
}

fn idle_timeout_secs(configured: Option<u64>, interval_secs: u64) -> u64 {
    configured
        .unwrap_or(900)
        .max(interval_secs.saturating_mul(3))
}

/// Return the policy-gauge refresh period for a gauge idle window.
///
/// `tokio::time::interval` panics on a zero period, so keep the period at one
/// second even for a window [`usage_metrics_idle_timeout_secs`] cannot produce.
fn policy_gauge_refresh_secs(gauge_idle_timeout_secs: u64) -> u64 {
    (gauge_idle_timeout_secs / POLICY_GAUGE_REFRESHES_PER_WINDOW).max(1)
}

/// Spawn the task that keeps the static policy gauges from aging out.
///
/// Nothing but its own timer may sit in front of the emission. Refreshing from
/// the usage poller's loop put it behind that tick's unbounded database and lock
/// work, so the gap between writes was the tick duration, not the cadence.
///
/// Must be called from within a Tokio runtime.
pub fn spawn_policy_gauge_refresh(config: crate::config::Config, gauge_idle_timeout_secs: u64) {
    let period = Duration::from_secs(policy_gauge_refresh_secs(gauge_idle_timeout_secs));
    tokio::spawn(async move {
        let mut refresh = tokio::time::interval(period);
        loop {
            refresh.tick().await;
            emit_policy_gauges(&config);
        }
    });
}

/// Write the relay's static policy gauges.
///
/// [`install`] evicts any gauge that goes idle, and nothing else ever writes
/// these two series, so a boot-only emission leaves the scrape output one idle
/// window later. [`spawn_policy_gauge_refresh`] is what keeps them present.
pub fn emit_policy_gauges(config: &crate::config::Config) {
    metrics::gauge!("buzz_audit_enabled").set(if config.audit_enabled { 1.0 } else { 0.0 });
    metrics::gauge!("buzz_permessage_deflate_enabled").set(if config.permessage_deflate_enabled {
        1.0
    } else {
        0.0
    });
}

/// Axum middleware that records CAKE framework HTTP metrics.
///
/// Emits:
/// - `http_requests_total{code, caller, action}` — counter
/// - `http_request_latency_ms{code, caller, action}` — histogram
///
/// Skips health/metrics paths (`/_*`, `/health`) to avoid polluting dashboards.
///
/// Labels:
/// - `code`: exact HTTP status code (e.g. "200", "404")
/// - `caller`: upstream service from Istio `x-envoy-downstream-service-cluster` header
/// - `action`: matched route pattern (e.g. `/api/channels/{channel_id}`)
pub async fn track_metrics(req: Request, next: Next) -> Response {
    // Use the route pattern (e.g. "/api/channels/{channel_id}"), NOT the raw URI.
    // Falling back to raw URI on 404s would create unbounded cardinality from scanners.
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned());

    // Skip health probes, metrics endpoint, and unmatched paths (404 scanners).
    match path.as_deref() {
        Some(p) if p.starts_with("/_") || p == "/health" || p == "/metrics" => {
            return next.run(req).await;
        }
        None => {
            // No matched route — 404/scanner traffic. Skip to avoid cardinality bomb.
            return next.run(req).await;
        }
        _ => {}
    }
    let action = path.unwrap(); // safe: None case returned above

    // Caller from Istio header. In CAKE, this is set by the mesh (trusted).
    // On the public TCP listener it's client-controlled, so validate format:
    // only accept short alphanumeric-with-hyphens service names.
    let caller = req
        .headers()
        .get("x-envoy-downstream-service-cluster")
        .and_then(|v| v.to_str().ok())
        .filter(|s| {
            s.len() <= 64
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        })
        .unwrap_or("unknown")
        .to_owned();

    let start = Instant::now();
    let response = next.run(req).await;
    let status = response.status().as_u16().to_string();
    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

    let labels = [("code", status), ("caller", caller), ("action", action)];
    metrics::counter!("http_requests_total", &labels).increment(1);
    metrics::histogram!("http_request_latency_ms", &labels).record(latency_ms);

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_idle_window_outlives_three_usage_poller_ticks() {
        assert_eq!(idle_timeout_secs(None, 300), 900);
        assert_eq!(idle_timeout_secs(Some(10), 1_000), 3_000);
    }

    /// The cadence has to fit every window the relay can be given, so sweep the
    /// reachable domain through the production helpers, not one chosen pair.
    #[test]
    fn the_policy_refresh_fits_inside_every_reachable_idle_window() {
        let intervals = [
            USAGE_METRICS_MIN_INTERVAL_SECS,
            USAGE_METRICS_MIN_INTERVAL_SECS + 1,
            300,
            3_600,
            u64::MAX,
        ];
        for interval_secs in intervals {
            for configured in [None, Some(0), Some(1), Some(15), Some(900), Some(u64::MAX)] {
                let window = idle_timeout_secs(configured, interval_secs);
                let refresh = policy_gauge_refresh_secs(window);
                assert!(refresh > 0, "tokio::time::interval panics on a zero period");
                assert!(
                    refresh * POLICY_GAUGE_REFRESHES_PER_WINDOW <= window,
                    "a {refresh}s refresh must fit {POLICY_GAUGE_REFRESHES_PER_WINDOW} times \
                     into a {window}s window (interval {interval_secs}s, configured {configured:?})"
                );
            }
        }
    }

    /// The arithmetic above only bounds the cadence; this pins that the task
    /// actually keeps the series alive with nothing driving it.
    #[test]
    fn the_spawned_refresh_restores_an_evicted_gauge_unattended() {
        const IDLE: Duration = Duration::from_millis(50);
        // The shortest window whose refresh period is still a whole second.
        const WINDOW_SECS: u64 = POLICY_GAUGE_REFRESHES_PER_WINDOW;
        let period = Duration::from_secs(policy_gauge_refresh_secs(WINDOW_SECS));
        let recorder = PrometheusBuilder::new()
            .idle_timeout(MetricKindMask::GAUGE, Some(IDLE))
            .build_recorder();
        let handle = recorder.handle();
        let config = crate::config::Config::from_env().expect("default config loads");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("current-thread runtime with a timer");

        metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                spawn_policy_gauge_refresh(config, WINDOW_SECS);

                tokio::time::sleep(IDLE / 5).await;
                assert!(
                    handle.render().contains("buzz_permessage_deflate_enabled"),
                    "the task must emit on its first tick, not one period later"
                );

                // Eviction is measured against the render above: the recorder
                // tracks idleness from the last observation, so an unrendered
                // series is never reaped.
                tokio::time::sleep(IDLE * 2).await;
                handle.run_upkeep();
                assert!(
                    !handle.render().contains("buzz_permessage_deflate_enabled"),
                    "the series must be evicted first, or the restore proves nothing"
                );

                tokio::time::sleep(period).await;
                assert!(
                    handle.render().contains("buzz_permessage_deflate_enabled"),
                    "the refresh task must restore the series with no other caller"
                );
            });
        });
    }

    /// Re-emitting on a cadence only helps if idle eviction is real and a later
    /// write restores the series, so pin both rather than trusting the
    /// exporter's documentation.
    #[test]
    fn policy_gauges_are_evicted_when_idle_and_restored_by_re_emission() {
        const IDLE: Duration = Duration::from_millis(50);
        let recorder = PrometheusBuilder::new()
            .idle_timeout(MetricKindMask::GAUGE, Some(IDLE))
            .build_recorder();
        let handle = recorder.handle();
        let mut config = crate::config::Config::from_env().expect("default config loads");
        config.audit_enabled = true;
        config.permessage_deflate_enabled = false;

        metrics::with_local_recorder(&recorder, || emit_policy_gauges(&config));
        // Distinct values, so a swap inside the helper fails here too.
        let fresh = handle.render();
        assert!(fresh.contains("buzz_audit_enabled 1"), "{fresh}");
        assert!(
            fresh.contains("buzz_permessage_deflate_enabled 0"),
            "{fresh}"
        );

        std::thread::sleep(IDLE * 2);
        handle.run_upkeep();
        let idle = handle.render();
        assert!(
            !idle.contains("buzz_permessage_deflate_enabled"),
            "a boot-only gauge does not survive its idle window: {idle}"
        );

        metrics::with_local_recorder(&recorder, || emit_policy_gauges(&config));
        assert!(
            handle
                .render()
                .contains("buzz_permessage_deflate_enabled 0"),
            "re-emission must restore an evicted series"
        );
    }
}
