//! `Pusher` — enqueue jobs from within the user's own transaction.
//!
//! See PLAN.md §"Batch push" and §"Public API surface".
//!
//! Validation is **fail-late** — `Pusher::new` is infallible; queue-name
//! validity is checked on each `push*` call (matches the API sketch).

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::codec::{Codec, JsonCodec};
use crate::error::PushError;
use crate::limits::{
    MAX_BATCH_BYTES, MAX_BATCH_SIZE, MAX_CONCURRENCY_KEY_LEN, MAX_PAYLOAD_BYTES, MAX_QUEUE_LEN,
};

/// Push-side handle for a single queue. Holds queue name + codec; cheap to
/// `clone()`. Use [`Pusher::with_codec`] to swap the codec (consumes self).
///
/// Generic over [`Codec`] so an application may swap `JsonCodec` for a
/// custom binary codec without changing call sites.
#[derive(Debug, Clone)]
pub struct Pusher<C = JsonCodec> {
    queue: String,
    codec: C,
}

impl Pusher<JsonCodec> {
    /// Construct a pusher for `queue` using the default `JsonCodec`.
    ///
    /// Queue-name validation is deferred to push time (fail-late) — see
    /// [`PushError::QueueNameInvalid`].
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            codec: JsonCodec,
        }
    }
}

impl<C: Codec> Pusher<C> {
    /// Swap the codec while keeping the queue name. Consumes `self`.
    #[must_use]
    pub fn with_codec<C2: Codec>(self, codec: C2) -> Pusher<C2> {
        Pusher {
            queue: self.queue,
            codec,
        }
    }

    /// Validate the queue name once per push call (fail-late per the API sketch).
    fn validate_queue(&self) -> Result<(), PushError> {
        let len = self.queue.chars().count();
        if len == 0 || len > MAX_QUEUE_LEN {
            return Err(PushError::QueueNameInvalid(self.queue.clone()));
        }
        Ok(())
    }

    /// Validate an optional `concurrency_key` — character count in
    /// `1..=MAX_CONCURRENCY_KEY_LEN`. Called before the codec encode step.
    fn validate_concurrency_key(key: Option<&str>) -> Result<(), PushError> {
        if let Some(k) = key {
            let len = k.chars().count();
            if len == 0 || len > MAX_CONCURRENCY_KEY_LEN {
                return Err(PushError::ConcurrencyKeyInvalid(k.to_string()));
            }
        }
        Ok(())
    }

    /// Encode a single payload and validate its size. `index` is the position
    /// within a batch (0 for `push`/`push_at`).
    fn encode_one<T: Serialize>(&self, value: &T, index: usize) -> Result<Vec<u8>, PushError> {
        let bytes = self.codec.encode(value).map_err(|e| {
            if index == 0 {
                PushError::Codec(Box::new(e))
            } else {
                PushError::BatchCodec {
                    index,
                    source: Box::new(e),
                }
            }
        })?;
        if bytes.len() > MAX_PAYLOAD_BYTES {
            return Err(PushError::PayloadTooLarge {
                index,
                size: bytes.len(),
                max: MAX_PAYLOAD_BYTES,
            });
        }
        Ok(bytes)
    }

    /// Enqueue a single job. `run_at` defaults to `now()` server-side.
    ///
    /// `concurrency_key` — optional per-key concurrency bucket. `None` =
    /// unlimited. See the README "Per-key concurrency" section.
    ///
    /// # Errors
    /// - [`PushError::QueueNameInvalid`] — queue is empty or too long.
    /// - [`PushError::ConcurrencyKeyInvalid`] — key empty or too long.
    /// - [`PushError::Codec`] — codec rejected `payload`.
    /// - [`PushError::PayloadTooLarge`] — encoded size > `MAX_PAYLOAD_BYTES`.
    /// - [`PushError::Constraint`] / [`PushError::Transient`] — DB faults.
    #[tracing::instrument(skip(self, tx, payload), fields(queue = %self.queue))]
    pub async fn push<T: Serialize + Sync>(
        &self,
        tx: &mut PgConnection,
        payload: &T,
        concurrency_key: Option<&str>,
    ) -> Result<Uuid, PushError> {
        self.validate_queue()?;
        Self::validate_concurrency_key(concurrency_key)?;
        let bytes = self.encode_one(payload, 0)?;
        let public_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO pgwq.jobs (queue, payload, public_id, concurrency_key)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&self.queue)
        .bind(&bytes)
        .bind(public_id)
        .bind(concurrency_key)
        .execute(&mut *tx)
        .await?;
        Ok(public_id)
    }

    /// Enqueue a single job with an explicit scheduled `run_at` (UTC).
    ///
    /// # Errors
    /// Same as [`Pusher::push`].
    #[tracing::instrument(skip(self, tx, payload), fields(queue = %self.queue))]
    pub async fn push_at<T: Serialize + Sync>(
        &self,
        tx: &mut PgConnection,
        payload: &T,
        run_at: DateTime<Utc>,
        concurrency_key: Option<&str>,
    ) -> Result<Uuid, PushError> {
        self.validate_queue()?;
        Self::validate_concurrency_key(concurrency_key)?;
        let bytes = self.encode_one(payload, 0)?;
        let public_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO pgwq.jobs (queue, payload, public_id, run_at, concurrency_key)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&self.queue)
        .bind(&bytes)
        .bind(public_id)
        .bind(run_at)
        .bind(concurrency_key)
        .execute(&mut *tx)
        .await?;
        Ok(public_id)
    }

    /// Enqueue many jobs in a single round-trip. Each item is a
    /// `(payload, concurrency_key)` pair. Returns `Vec<Uuid>` in **input
    /// order** (client-side `Uuid::now_v7()` per item — no `RETURNING`).
    ///
    /// # Errors
    /// - [`PushError::BatchEmpty`] — `items.is_empty()`.
    /// - [`PushError::BatchTooLarge`] — `items.len() > MAX_BATCH_SIZE`.
    /// - [`PushError::ConcurrencyKeyInvalid`] — an item's key is out of range.
    /// - [`PushError::PayloadTooLarge`] — one payload exceeds `MAX_PAYLOAD_BYTES`.
    /// - [`PushError::BatchPayloadTooLarge`] — total bytes exceed `MAX_BATCH_BYTES`.
    /// - [`PushError::BatchCodec`] — codec rejected an item.
    /// - [`PushError::QueueNameInvalid`].
    /// - DB faults (`Constraint`/`Transient`).
    #[tracing::instrument(skip(self, tx, items), fields(queue = %self.queue, count = items.len()))]
    pub async fn push_batch<T: Serialize + Sync>(
        &self,
        tx: &mut PgConnection,
        items: &[(T, Option<String>)],
    ) -> Result<Vec<Uuid>, PushError> {
        self.validate_queue()?;
        if items.is_empty() {
            return Err(PushError::BatchEmpty);
        }
        if items.len() > MAX_BATCH_SIZE {
            return Err(PushError::BatchTooLarge {
                size: items.len(),
                max: MAX_BATCH_SIZE,
            });
        }

        let mut payload_bytes: Vec<Vec<u8>> = Vec::with_capacity(items.len());
        let mut keys: Vec<Option<String>> = Vec::with_capacity(items.len());
        let mut total_bytes: usize = 0;
        for (i, (payload, key)) in items.iter().enumerate() {
            Self::validate_concurrency_key(key.as_deref())?;
            let bytes = self.encode_one(payload, i)?;
            total_bytes = total_bytes.saturating_add(bytes.len());
            if total_bytes > MAX_BATCH_BYTES {
                return Err(PushError::BatchPayloadTooLarge {
                    total_bytes,
                    max: MAX_BATCH_BYTES,
                });
            }
            payload_bytes.push(bytes);
            keys.push(key.clone());
        }

        let public_ids: Vec<Uuid> = (0..items.len()).map(|_| Uuid::now_v7()).collect();

        let result = sqlx::query(
            "INSERT INTO pgwq.jobs (queue, payload, public_id, concurrency_key)
             SELECT $1, payload, public_id, concurrency_key
             FROM unnest($2::bytea[], $3::uuid[], $4::text[])
                  AS u(payload, public_id, concurrency_key)",
        )
        .bind(&self.queue)
        .bind(&payload_bytes)
        .bind(&public_ids)
        .bind(&keys)
        .execute(&mut *tx)
        .await?;

        let inserted = usize::try_from(result.rows_affected()).unwrap_or(usize::MAX);
        if inserted != public_ids.len() {
            return Err(PushError::BatchPartial {
                inserted,
                expected: public_ids.len(),
            });
        }
        Ok(public_ids)
    }
}
