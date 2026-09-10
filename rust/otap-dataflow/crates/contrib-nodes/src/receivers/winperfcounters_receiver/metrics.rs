// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded operational metrics for the Windows performance-counter receiver.

use crate::receivers::winperfcounters::SampleDiagnostics;
use otel_arrow_dfe_telemetry::instrument::{Counter, Gauge, HistogramNormal};
use otel_arrow_dfe_telemetry_macros::metric_set;

/// Lifecycle, recovery, and collection metrics.
#[metric_set(name = "receiver.winperfcounters")]
#[derive(Debug, Default, Clone)]
pub(super) struct WinPerfCountersMetrics {
    /// Configured exact counter paths.
    #[metric(unit = "{counter}")]
    pub configured_exact: Gauge<u64>,
    /// Configured wildcard counter paths.
    #[metric(unit = "{counter}")]
    pub configured_wildcard: Gauge<u64>,
    /// Currently active expanded wildcard counters.
    #[metric(unit = "{counter}")]
    pub active_expanded: Gauge<u64>,
    /// Successful collections.
    #[metric(unit = "{scrape}")]
    pub scrapes: Counter<u64>,
    /// Query-level collection failures.
    #[metric(unit = "{scrape}")]
    pub scrape_failures: Counter<u64>,
    /// Collection duration.
    #[metric(unit = "s")]
    pub scrape_duration: HistogramNormal,
    /// Wildcard discovery refreshes attempted.
    #[metric(unit = "{refresh}")]
    pub discovery_refreshes: Counter<u64>,
    /// Wildcard discovery refreshes that failed.
    #[metric(unit = "{refresh}")]
    pub discovery_failures: Counter<u64>,
    /// Expanded instances added.
    #[metric(unit = "{instance}")]
    pub instances_added: Counter<u64>,
    /// Expanded instances removed.
    #[metric(unit = "{instance}")]
    pub instances_removed: Counter<u64>,
    /// Expanded instances omitted by configured limits.
    #[metric(unit = "{instance}")]
    pub instances_omitted_over_limit: Counter<u64>,
    /// Counter add attempts that failed.
    #[metric(unit = "{failure}")]
    pub counter_add_failures: Counter<u64>,
    /// Counter reads, statuses, or projections that failed.
    #[metric(unit = "{failure}")]
    pub counter_read_failures: Counter<u64>,
    /// Deferred counter retry attempts.
    #[metric(unit = "{attempt}")]
    pub retry_attempts: Counter<u64>,
    /// Deferred counter retries that recovered.
    #[metric(unit = "{recovery}")]
    pub retry_recoveries: Counter<u64>,
    /// Worker-owned query rebuild attempts.
    #[metric(unit = "{attempt}")]
    pub query_rebuild_attempts: Counter<u64>,
    /// Worker-owned query rebuilds that recovered.
    #[metric(unit = "{recovery}")]
    pub query_rebuild_recoveries: Counter<u64>,
    /// Points omitted while independently warming.
    #[metric(unit = "{point}")]
    pub warmup_omissions: Counter<u64>,
}

impl WinPerfCountersMetrics {
    pub(super) fn apply(&mut self, diagnostics: &SampleDiagnostics) {
        self.active_expanded
            .set(diagnostics.active_expanded_counters as u64);
        self.discovery_refreshes
            .add(diagnostics.discovery_refreshes);
        self.discovery_failures.add(diagnostics.discovery_failures);
        self.instances_added.add(diagnostics.instances_added);
        self.instances_removed.add(diagnostics.instances_removed);
        self.instances_omitted_over_limit
            .add(diagnostics.instances_omitted_over_limit);
        self.counter_add_failures
            .add(diagnostics.counter_add_failures);
        self.counter_read_failures
            .add(diagnostics.counter_read_failures);
        self.retry_attempts.add(diagnostics.retry_attempts);
        self.retry_recoveries.add(diagnostics.retry_recoveries);
        self.query_rebuild_attempts
            .add(diagnostics.query_rebuild_attempts);
        self.query_rebuild_recoveries
            .add(diagnostics.query_rebuild_recoveries);
        self.warmup_omissions.add(diagnostics.warmup_omissions);
    }
}
