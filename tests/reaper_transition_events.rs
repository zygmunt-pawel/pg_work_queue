#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Reaper musi emitować jeden `pgwq.state.transition` event PER REAPED ROW
//! ze `source="reaper"`. 2 rows w awaiting_retry + 3 rows w dead → 5 events
//! z odpowiednim `status.to`. Każdy event ma `job.id` i `job.attempts`.

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
        if !meta.target().starts_with("pgwq.state.transition") {
            return;
        }
        let mut fields = Vec::new();
        let mut visitor = FieldGrab(&mut fields);
        event.record(&mut visitor);
        if let Ok(mut g) = self.events.lock() {
            g.push(CapturedEvent {
                target: meta.target().to_string(),
                fields,
            });
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reaper_emits_one_transition_per_reaped_row() {
    let events = Arc::new(Mutex::new(Vec::<CapturedEvent>::new()));
    let layer = CaptureLayer {
        events: events.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _ = tracing::subscriber::set_global_default(subscriber);

    let (pool, _c) = common::pg18_pool().await;

    // Push 5 jobs (2 for retry, 3 for dead).
    let pusher = Pusher::new("rt_q");
    let payloads: Vec<(Payload, Option<String>)> =
        (0..5u32).map(|seq| (Payload { seq }, None)).collect();
    let mut tx = pool.begin().await.expect("tx");
    let ids = pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");
    assert_eq!(ids.len(), 5);

    // 2 rows: attempts=1, max=3 (→ awaiting_retry).
    sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'running',
             lease_token = gen_random_uuid(),
             lease_expires_at = now() - interval '1 second',
             attempts = 1,
             max_attempts = 3,
             last_attempted_at = now(),
             first_attempted_at = now(),
             run_at = now() + interval '1 hour'
         WHERE queue = 'rt_q' AND public_id = ANY($1)",
    )
    .bind(&ids[0..2])
    .execute(&pool)
    .await
    .expect("update retry rows");

    // 3 rows: attempts=3, max=3 (→ dead).
    sqlx::query(
        "UPDATE pgwq.jobs
         SET status = 'running',
             lease_token = gen_random_uuid(),
             lease_expires_at = now() - interval '1 second',
             attempts = 3,
             max_attempts = 3,
             last_attempted_at = now(),
             first_attempted_at = now(),
             run_at = now() + interval '1 hour'
         WHERE queue = 'rt_q' AND public_id = ANY($1)",
    )
    .bind(&ids[2..5])
    .execute(&pool)
    .await
    .expect("update dead rows");

    let worker = Worker::<Payload, _>::builder()
        .queue("rt_q")
        .pool(pool.clone())
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(1))
        .poll_interval(Duration::from_secs(1))
        .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    handle.cancel();
    let _ = handle.join().await;

    let captured = events.lock().expect("lock").clone();
    let reaper_events: Vec<_> = captured
        .iter()
        .filter(|e| e.fields.iter().any(|(k, v)| k == "source" && v == "reaper"))
        .collect();

    assert_eq!(
        reaper_events.len(),
        5,
        "expected exactly 5 reaper transition events; got {} (all: {captured:#?})",
        reaper_events.len()
    );

    let mut retry_count = 0;
    let mut dead_count = 0;
    for ev in &reaper_events {
        let to = ev
            .fields
            .iter()
            .find(|(k, _)| k == "status.to")
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        match to {
            "awaiting_retry" => retry_count += 1,
            "dead" => dead_count += 1,
            other => panic!("unexpected status.to={other} in {ev:?}"),
        }
        // Every reaper event must carry job.id and job.attempts.
        assert!(
            ev.fields.iter().any(|(k, _)| k == "job.id"),
            "missing job.id in {ev:?}"
        );
        assert!(
            ev.fields.iter().any(|(k, _)| k == "job.attempts"),
            "missing job.attempts in {ev:?}"
        );
    }
    assert_eq!(retry_count, 2, "expected 2 awaiting_retry transitions");
    assert_eq!(dead_count, 3, "expected 3 dead transitions");
}
