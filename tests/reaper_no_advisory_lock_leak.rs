#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Regression: pg_work_queue NIE używa advisory locków (Anti-pattern #7).
//! Po przejściu 50 jobów przez 3 workery, `pg_locks WHERE locktype='advisory'`
//! musi być pusty. Apalis `register.sql` leak'ował advisory locki — chcemy
//! pewności że nikt nie wprowadzi tego silnie typed-and-not-tested.

mod common;

use std::time::Duration;

use sqlx::Row;

use pg_work_queue::{JobContext, JobError, Pusher, Worker};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Payload {
    seq: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_advisory_locks_after_workers_drain_queue() {
    let (pool, _c) = common::pg18_pool().await;

    // Push 50 jobs.
    let pusher = Pusher::new("lock_q");
    let payloads: Vec<(Payload, Option<String>)> =
        (0..50u32).map(|seq| (Payload { seq }, None)).collect();
    let mut tx = pool.begin().await.expect("tx");
    pusher
        .push_batch(&mut tx, &payloads)
        .await
        .expect("push_batch");
    tx.commit().await.expect("commit");

    // Spawn 3 workers, all on the same queue, default settings.
    let mut handles = Vec::new();
    for _ in 0..3 {
        let worker = Worker::<Payload, _>::builder()
            .queue("lock_q")
            .pool(pool.clone())
            .concurrency(4)
            .lease_timeout(Duration::from_secs(10))
            .reaper_interval(Duration::from_secs(1))
            .poll_interval(Duration::from_millis(100))
            .handler(|_p: Payload, _ctx: JobContext| async move { Ok::<(), JobError>(()) })
            .build()
            .expect("build");
        handles.push(worker.start().await.expect("start"));
    }

    // Wait for the queue to drain (all 50 rows status='done').
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let done: i64 = sqlx::query(
            "SELECT count(*)::bigint AS c FROM pgwq.jobs WHERE queue = 'lock_q' AND status = 'done'",
        )
        .fetch_one(&pool)
        .await
        .expect("count done")
        .try_get("c")
        .expect("c");
        if done == 50 {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("timeout: drained only {done}/50 jobs in 15s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Shut down all 3 workers.
    for h in handles {
        h.cancel();
        let _ = h.join().await;
    }

    // No advisory locks must remain.
    let advisory_locks: i64 =
        sqlx::query("SELECT count(*)::bigint AS c FROM pg_locks WHERE locktype = 'advisory'")
            .fetch_one(&pool)
            .await
            .expect("count advisory")
            .try_get("c")
            .expect("c");
    assert_eq!(
        advisory_locks, 0,
        "pg_work_queue MUST NOT acquire advisory locks (Anti-pattern #7)"
    );
}
