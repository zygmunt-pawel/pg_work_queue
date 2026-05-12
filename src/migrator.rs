//! Embedded schema migrations.
//!
//! Re-exports `sqlx::Migrator` z embedded migrations w `migrations/`.
//! Library nie zarządza tym automatycznie — user wywołuje `migrator().run(&pool)`
//! ze swojego startup path (Cardinal v3: explicit nad implicit).

/// Returns the embedded migrator. Call `.run(&pool)` to apply the schema.
///
/// ```ignore
/// pg_work_queue::migrator().run(&pool).await?;
/// ```
#[must_use]
pub fn migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("./migrations")
}
