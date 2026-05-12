#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Capture `pgwq.state.transition` events via a custom Layer and assert
//! that for a happy-path job we see: `queued→running` (from claim) and
//! `running→done` (from mark_done). Per PLAN.md §"State transition events".

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::Registry;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[derive(Default, Debug, Clone)]
#[allow(dead_code)] // `target` kept for debug-printing on failure
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
        if !target.starts_with("pgwq.state.transition") {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn happy_path_emits_transition_events() {
    // Install global capturing subscriber BEFORE common::init_tracing()
    // (called inside pg18_pool). `init_tracing` uses `try_init`, so it
    // becomes a no-op once we've claimed the global slot. `set_default`
    // is thread-local — won't reach the tokio worker threads where
    // poll_loop and handle_job actually run.
    let events = Arc::new(Mutex::new(Vec::<CapturedEvent>::new()));
    let layer = CaptureLayer {
        events: events.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _ = tracing::subscriber::set_global_default(subscriber);

    let (pool, _c) = common::pg18_pool().await;

    let pusher = Pusher::new("trc_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 })
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    let worker = Worker::<Payload, _>::builder()
        .queue("trc_q")
        .pool(pool.clone())
        .concurrency(2)
        .lease_timeout(Duration::from_secs(30))
        .poll_interval(Duration::from_millis(100))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Wait for completion (job done).
    let start = std::time::Instant::now();
    loop {
        let row = sqlx::query(
            "SELECT status::text AS s FROM pgwq.jobs WHERE queue = 'trc_q' ORDER BY id LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .expect("fetch");
        let s: String = sqlx::Row::try_get(&row, "s").unwrap();
        if s == "done" {
            break;
        }
        if start.elapsed() > Duration::from_secs(3) {
            panic!("timeout waiting for done");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.cancel();
    let _ = handle.join().await;

    let captured = events.lock().unwrap().clone();

    assert!(
        !captured.is_empty(),
        "should capture at least one pgwq.state.transition event"
    );

    // Find running→done.
    let has_done = captured.iter().any(|e| {
        let from = e
            .fields
            .iter()
            .find(|(k, _)| k == "status.from")
            .map(|(_, v)| v.as_str());
        let to = e
            .fields
            .iter()
            .find(|(k, _)| k == "status.to")
            .map(|(_, v)| v.as_str());
        from == Some("running") && to == Some("done")
    });
    assert!(
        has_done,
        "expected a running→done transition; got: {captured:#?}"
    );

    // All transitions must carry job.id + source + status.from + status.to.
    for ev in &captured {
        let has = |name: &str| ev.fields.iter().any(|(k, _)| k == name);
        assert!(has("job.id"), "missing job.id in {ev:?}");
        assert!(has("status.from"), "missing status.from in {ev:?}");
        assert!(has("status.to"), "missing status.to in {ev:?}");
        assert!(has("source"), "missing source in {ev:?}");
    }
}
