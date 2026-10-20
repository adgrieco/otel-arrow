// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Native PDH access, query-owned sampling state, discovery, and worker-thread lifecycle.
//!
//! Windows handles remain on the synchronous worker thread that owns them.

use super::model::{InstanceIdentity, Number};
mod counter_type;
mod error;
mod expansion;
mod native;
mod query;
mod timing;
mod worker;

pub(super) use error::{Error, PdhOperation};
pub(super) use worker::{QueryCadence, Worker};

#[cfg(test)]
static QUERY_OPENS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
static QUERY_CLOSES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The numeric representation requested from PDH.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ValueFormat {
    Integer,
    Double,
}

/// Checked native fields used to validate receiver-side sampling state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RawSample {
    pub(super) second_value: i64,
    pub(super) multi_count: u32,
}

/// Owns an open query and its counters; raw handles and unchecked statuses stay native.
pub(super) trait PdhApi {
    type Counter;

    fn add(&mut self, path: &str, localized: bool) -> Result<Self::Counter, Error>;
    fn remove(&mut self, counter: &Self::Counter, path: &str) -> Result<(), Error>;
    fn counter_type(&mut self, counter: &Self::Counter, path: &str) -> Result<u32, Error>;
    /// Returns whether collection has data; an expected omission returns false.
    fn collect(&mut self) -> Result<bool, Error>;
    /// Returns checked raw sampling fields, an expected omission, or a validated failure.
    fn raw(&mut self, counter: &Self::Counter, path: &str) -> Result<Option<RawSample>, Error>;
    /// Returns an unscaled value, an expected omission, or a validated failure.
    fn formatted(
        &mut self,
        counter: &Self::Counter,
        path: &str,
        format: ValueFormat,
    ) -> Result<Option<Number>, Error>;
    fn timestamp(&mut self) -> Result<i64, Error>;
    /// Returns concrete localized paths for one configured wildcard template.
    fn expand(&mut self, path: &str) -> Result<Vec<String>, Error>;
    /// Parses the instance identity from one concrete localized path.
    fn parse_instance(&mut self, path: &str) -> Result<InstanceIdentity, Error>;
}

#[cfg(test)]
mod tests;
