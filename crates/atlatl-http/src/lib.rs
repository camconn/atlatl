//! REST engine (spec 07): HTTP client, limiter, response pipeline, auth, pagination and records.
//!
//! Build an [`ApiClient`] from a parsed [`ApiSpec`] and the connection's secrets, then run an
//! endpoint with [`ApiClient::endpoint`]: the [`EndpointRun`] yields `(ordinal, record)` pairs for
//! staging and keeps [`EndpointStats`] for the run report.

pub mod auth;
pub mod cassette;
pub mod client;
pub mod explain;
pub mod expr;
pub mod funcs;
pub mod iterate;
pub mod json;
pub mod limiter;
pub mod paginate;
pub mod records;
pub mod rejects;
pub mod request;
pub mod response;
pub mod rules;
pub mod spec;
pub mod transport;

pub use cassette::Cassette;
pub use client::{
    ApiClient, ClientOptions, EndpointRun, EndpointStats, HttpStats, Live, LiveSnapshot, Record,
    Replayed, RunVars, Skip, StatsHandle,
};
pub use iterate::{DriveOptions, DriveOutcome, Driver, RangeOverride};
pub use spec::ApiSpec;
