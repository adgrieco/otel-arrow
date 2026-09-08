// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Portable fixed-counter configuration and direct OTAP gauge projection.

mod config;
mod otap_builder;

pub use config::{Config, Counter};
pub use otap_builder::into_otap;

/// The only supported input counter; English PDH paths are locale independent.
pub const COUNTER_PATH: &str = r"\Memory\Available Bytes";

/// Development metric identity, not a finalized host counter profile.
pub const METRIC_NAME: &str = "windows.memory.available";

/// A successful point-in-time PDH reading, independent of Windows handles.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    /// Collection time as nanoseconds since the Unix epoch.
    pub timestamp_unix_nano: i64,
    /// Available physical memory, in bytes, without floating-point conversion.
    pub available_bytes: i64,
}
