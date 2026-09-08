// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Portable performance-counter configuration and direct OTAP gauge projection.

mod config;
mod otap_builder;

pub use config::{Config, CounterConfig};
pub use otap_builder::into_otap;

/// A successful point-in-time PDH reading, independent of Windows handles.
#[derive(Debug, Clone)]
pub struct Sample {
    /// Collection time as nanoseconds since the Unix epoch.
    pub timestamp_unix_nano: i64,
    /// Values in the same order as the configured counters.
    pub values: Vec<i64>,
}
