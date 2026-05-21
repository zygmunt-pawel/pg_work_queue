#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Companion to `retry_in_override.rs` — testuje clamp warn event w izolacji
//! (separate test binary → swój własny global tracing subscriber). Łączenie
//! z `retry_in_override.rs` zawodzi bo `init_tracing()` z `common::pg18_pool`
//! claim'uje global default jako pierwszy test → drugi test (capture-side)
//! nie może zainstalować swojego `Registry`.
//!
//! Handler zwraca `Err(JobError::retry_in("tiny", 10ms))` — clamp do
//! `max(poll_interval, 100ms)`. Verifikacja: event `pgwq.retry_in.clamped`
//! z `requested_ms=10`, `applied_ms >= 100`.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use sqlx::Row;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::Registry;

use pg_work_queue::{BackoffPolicy, JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[derive(Default, Debug, Clone)]
#[allow(dead_code)]
struct CapturedEvent {
    target: String,
    fields: Vec<(String, String)>,
}

#[derive(Default)]
struct CaptureLayer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

struct FieldGrab<'a>(&'a mut Vec<(String, String)>);

impl<'a> Visit for FieldGrab<'a> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
}

impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        let target = meta.target().to_string();
        if !target.starts_with("pgwq.retry_in.clamped") {
            return;
        }
        let mut fields = Vec::new();
        let mut visitor = FieldGrab(&mut fields);
        event.record(&mut visitor);
        if let Ok(mut g) = self.events.lock() {
            g.push(CapturedEvent { target, fields });
        }
    }
}

async fn push_one(pool: &sqlx::PgPool, queue: &str) {
    let pusher = Pusher::new(queue);
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 }, None)
        .await
        .expect("push");
    tx.commit().await.expect("commit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_in_below_floor_emits_clamped_warn() {
    // Install capture FIRST — przed common::pg18_pool() które claim'uje global
    // przez init_tracing().
    let events = Arc::new(Mutex::new(Vec::<CapturedEvent>::new()));
    let layer = CaptureLayer {
        events: events.clone(),
    };
    let subscriber = Registry::default().with(layer);
    tracing::subscriber::set_global_default(subscriber).expect("set_global_default");

    let (pool, _c) = common::pg18_pool().await;
    push_one(&pool, "ric_q").await;

    let worker = Worker::<Payload, _>::builder()
        .queue("ric_q")
        .pool(pool.clone())
        .max_attempts(3)
        .retry_backoff(BackoffPolicy::fixed(Duration::from_millis(100)))
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(2))
        .poll_interval(Duration::from_millis(50))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            // 10ms < floor (max(poll_interval=50ms, 100ms) = 100ms).
            Err::<(), _>(JobError::retry_in("tiny", Duration::from_millis(10)))
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Wait until row went through awaiting_retry at least once.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row = sqlx::query(
            "SELECT status::text AS s FROM pgwq.jobs WHERE queue = 'ric_q' ORDER BY id LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .expect("fetch");
        let s: String = row.try_get("s").expect("s");
        if s == "awaiting_retry" || s == "dead" {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for retry/dead; s={s}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    handle.cancel();
    let _ = handle.join().await;

    let captured = events.lock().unwrap().clone();
    assert!(
        !captured.is_empty(),
        "expected at least one pgwq.retry_in.clamped event, got none"
    );
    let first = &captured[0];
    let requested = first
        .fields
        .iter()
        .find(|(k, _)| k == "requested_ms")
        .map(|(_, v)| v.as_str())
        .expect("requested_ms field");
    let applied = first
        .fields
        .iter()
        .find(|(k, _)| k == "applied_ms")
        .map(|(_, v)| v.as_str())
        .expect("applied_ms field");
    assert_eq!(requested, "10", "requested_ms should be 10");
    let applied_ms: u64 = applied.parse().expect("parse applied_ms");
    assert!(
        applied_ms >= 100,
        "applied_ms must be >= 100ms (floor); got {applied_ms}"
    );
}
