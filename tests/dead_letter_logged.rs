#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Dead-letter musi emitować `pgwq.state.transition` event z `status.to="dead"`
//! przy ERROR level. PII safety: ERROR ma `reason_present`/`reason_length` ale
//! nie body — testujemy że event jest emitowany (CaptureLayer), `status.to`
//! to "dead", `reason_present="true"`.

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
    level: String,
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
                level: meta.level().to_string(),
                fields,
            });
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_letter_emits_error_transition_event() {
    let events = Arc::new(Mutex::new(Vec::<CapturedEvent>::new()));
    let layer = CaptureLayer {
        events: events.clone(),
    };
    let subscriber = Registry::default().with(layer);
    let _ = tracing::subscriber::set_global_default(subscriber);

    let (pool, _c) = common::pg18_pool().await;

    let pusher = Pusher::new("dl_q");
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push(&mut tx, &Payload { seq: 1 })
        .await
        .expect("push");
    tx.commit().await.expect("commit");

    let worker = Worker::<Payload, _>::builder()
        .queue("dl_q")
        .pool(pool.clone())
        .max_attempts(1)
        .lease_timeout(Duration::from_secs(10))
        .reaper_interval(Duration::from_secs(1))
        .poll_interval(Duration::from_millis(100))
        .handler(|_p: Payload, _ctx: JobContext| async move {
            Err::<(), _>(JobError::retry("transient_err"))
        })
        .build()
        .expect("build");
    let handle = worker.start().await.expect("start");

    // Wait until row is dead (max_attempts=1 + first failure = mark_dead).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = sqlx::query("SELECT status::text AS s FROM pgwq.jobs WHERE queue = 'dl_q'")
            .fetch_one(&pool)
            .await
            .expect("fetch");
        let s: String = sqlx::Row::try_get(&row, "s").expect("s");
        if s == "dead" {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for dead status");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.cancel();
    let _ = handle.join().await;

    let captured = events.lock().expect("lock").clone();
    let dead_events: Vec<_> = captured
        .iter()
        .filter(|e| {
            e.fields
                .iter()
                .any(|(k, v)| k == "status.to" && v == "dead")
        })
        .collect();
    assert!(
        !dead_events.is_empty(),
        "expected at least one dead transition event; got: {captured:#?}"
    );
    for ev in &dead_events {
        assert_eq!(ev.level, "ERROR", "dead-letter event must be ERROR level");
        // PII safety: reason_present must be set; reason body omitted.
        let reason_present = ev
            .fields
            .iter()
            .find(|(k, _)| k == "reason_present")
            .map(|(_, v)| v.as_str());
        assert_eq!(
            reason_present,
            Some("true"),
            "ERROR dead-letter must carry reason_present=true; got: {ev:?}"
        );
    }
}
