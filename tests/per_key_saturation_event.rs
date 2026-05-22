#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Asserts that when a per-key concurrency limit is saturated the Worker emits
//! a DEBUG event on target `pgwq.claim` with message "per-key concurrency
//! saturated" and a `saturated_keys` field containing the saturated key.
mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::Registry;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct Payload {
    n: u32,
}

#[derive(Default, Debug, Clone)]
struct CapturedEvent {
    #[allow(dead_code)] // kept for debug-printing on failure
    target: String,
    message: String,
    fields: Vec<(String, String)>,
}

#[derive(Default)]
struct CaptureLayer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

struct FieldGrab<'a>(&'a mut Vec<(String, String)>, &'a mut String);

impl<'a> Visit for FieldGrab<'a> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            *self.1 = format!("{value:?}");
        } else {
            self.0
                .push((field.name().to_string(), format!("{value:?}")));
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            *self.1 = value.to_string();
        } else {
            self.0.push((field.name().to_string(), value.to_string()));
        }
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
        if target != "pgwq.claim" {
            return;
        }
        let mut fields = Vec::new();
        let mut message = String::new();
        let mut visitor = FieldGrab(&mut fields, &mut message);
        event.record(&mut visitor);
        if let Ok(mut g) = self.events.lock() {
            g.push(CapturedEvent {
                target,
                message,
                fields,
            });
        }
    }
}

/// Push `limit + 1` jobs with the same key, start a Worker with `limit = 1`
/// and a handler that sleeps long enough that the key is saturated on the
/// second poll tick, and assert the `pgwq.claim` saturation event fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_key_saturation_emits_pgwq_claim_event() {
    // Install capturing subscriber BEFORE common::init_tracing() so
    // `init_tracing`'s `try_init` becomes a no-op.
    let events = Arc::new(Mutex::new(Vec::<CapturedEvent>::new()));
    let layer = CaptureLayer {
        events: events.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _ = tracing::subscriber::set_global_default(subscriber);

    let (pool, _c) = common::pg18_pool().await;

    // Push 3 jobs for key "sat_key"; limit is 1 → after the first job is
    // claimed and still running, headroom = 0, saturation fires on next tick.
    let key = "sat_key";
    let mut tx = pool.begin().await.expect("begin");
    let items: Vec<(Payload, Option<String>)> = (0..3)
        .map(|i| (Payload { n: i }, Some(key.to_string())))
        .collect();
    Pusher::new("sat_q")
        .push_batch(&mut tx, &items)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    // Handler sleeps long enough to keep the key saturated across at least
    // two poll ticks (poll_interval = 100 ms, sleep = 2 s).
    let handle = Worker::<Payload, _>::builder()
        .queue("sat_q")
        .pool(pool.clone())
        .concurrency(4)
        .concurrency_limits([("sat_key".to_string(), 1u32)])
        .poll_interval(Duration::from_millis(100))
        .lease_timeout(Duration::from_secs(60))
        .handler_timeout(Duration::from_secs(30))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok::<(), JobError>(())
        })
        .build()
        .expect("build")
        .start()
        .await
        .expect("start");

    // Allow several poll ticks so the saturation event has time to fire.
    tokio::time::sleep(Duration::from_millis(800)).await;

    handle.cancel();
    let _ = handle.join().await;

    let captured = events.lock().unwrap().clone();

    let sat_event = captured.iter().find(|e| {
        // Accept both the exact message string and its Debug-quoted variant.
        e.message.contains("per-key concurrency saturated")
            || e.message.contains("\"per-key concurrency saturated\"")
    });

    assert!(
        sat_event.is_some(),
        "expected a pgwq.claim saturation event; captured pgwq.claim events: {captured:#?}"
    );

    let ev = sat_event.unwrap();
    // The saturated_keys field must mention our key.
    let keys_field = ev
        .fields
        .iter()
        .find(|(k, _)| k == "saturated_keys")
        .map(|(_, v)| v.as_str())
        .unwrap_or("");
    assert!(
        keys_field.contains(key),
        "saturated_keys field should contain \"{key}\"; got: {keys_field}"
    );
}
