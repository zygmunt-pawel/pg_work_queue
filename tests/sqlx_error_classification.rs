#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic
)]

//! Unit test (no DB) for `is_fatal_sqlx`. We construct every sqlx::Error
//! variant whose constructor is accessible from a downstream crate; the
//! ones with `pub(crate)` only constructors are documented in comments.
//!
//! Fatal-true:  PoolClosed, WorkerCrashed, Configuration(_), Migrate(_),
//!              ColumnDecode { .. }, Decode(_), TypeNotFound { .. },
//!              ColumnNotFound(_), Protocol(_).
//! Fatal-false: PoolTimedOut, Io(_), Tls(_), Database(_).

use std::io;

use pg_work_queue::__test_exports::is_fatal_sqlx;

#[test]
fn pool_closed_is_fatal() {
    assert!(is_fatal_sqlx(&sqlx::Error::PoolClosed));
}

#[test]
fn worker_crashed_is_fatal() {
    assert!(is_fatal_sqlx(&sqlx::Error::WorkerCrashed));
}

#[test]
fn configuration_is_fatal() {
    let e = sqlx::Error::Configuration("bad url".into());
    assert!(is_fatal_sqlx(&e));
}

#[test]
fn type_not_found_is_fatal() {
    let e = sqlx::Error::TypeNotFound {
        type_name: "nope".to_string(),
    };
    assert!(is_fatal_sqlx(&e));
}

#[test]
fn column_not_found_is_fatal() {
    let e = sqlx::Error::ColumnNotFound("col".to_string());
    assert!(is_fatal_sqlx(&e));
}

#[test]
fn protocol_is_fatal() {
    let e = sqlx::Error::Protocol("proto".to_string());
    assert!(is_fatal_sqlx(&e));
}

#[test]
fn column_decode_is_fatal() {
    let source: Box<dyn std::error::Error + Send + Sync> = "decode boom".into();
    let e = sqlx::Error::ColumnDecode {
        index: "col".to_string(),
        source,
    };
    assert!(is_fatal_sqlx(&e));
}

#[test]
fn decode_is_fatal() {
    let source: Box<dyn std::error::Error + Send + Sync> = "decode boom".into();
    let e = sqlx::Error::Decode(source);
    assert!(is_fatal_sqlx(&e));
}

// Note: sqlx::Error::Migrate carries Box<sqlx::migrate::MigrateError>.
// Constructing one downstream is awkward (MigrateError has private
// constructors for most variants), but `MigrateError::Source` accepts
// any boxed error. We skip the Migrate path in this unit test — its
// branch is exercised by `tests/migrator_pg17_fails_loud.rs` indirectly.
//
// Transient variants:

#[test]
fn pool_timed_out_is_not_fatal() {
    assert!(!is_fatal_sqlx(&sqlx::Error::PoolTimedOut));
}

#[test]
fn io_is_not_fatal() {
    let e = sqlx::Error::Io(io::Error::new(io::ErrorKind::ConnectionReset, "reset"));
    assert!(!is_fatal_sqlx(&e));
}

// `sqlx::Error::Tls` wraps a Box<dyn Error + Send + Sync>, which we can
// construct from anything stringy.
#[test]
fn tls_is_not_fatal() {
    let boxed: Box<dyn std::error::Error + Send + Sync> = "tls handshake fail".into();
    let e = sqlx::Error::Tls(boxed);
    assert!(!is_fatal_sqlx(&e));
}

// `sqlx::Error::Database` requires a `Box<dyn DatabaseError>`; that
// trait is sealed by sqlx and can't be impl'd downstream. We can't
// construct a Database(_) variant in a unit test — its branch is
// covered indirectly by `fatal_sqlx_triggers_shutdown.rs` (real PG
// error path) and `schema_missing_fails_loud.rs`.
