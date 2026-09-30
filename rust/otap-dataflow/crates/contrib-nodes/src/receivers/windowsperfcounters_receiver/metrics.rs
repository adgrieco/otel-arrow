// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Internal telemetry for the Windows performance-counter receiver.

use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_telemetry::common_attributes::{Outcome, OutcomeAttributes};
use otel_arrow_dfe_telemetry::error::Error;
use otel_arrow_dfe_telemetry::instrument::Counter;
use otel_arrow_dfe_telemetry::metrics::{MeasurementMetricSet, MetricSet, MetricSetSnapshot};
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
use otel_arrow_dfe_telemetry_macros::metric_set;

/// PDH scrape outcomes for one Windows performance-counter receiver node.
#[metric_set(
    name = "receiver.windowsperfcounters.scrapes",
    measurement_attributes = OutcomeAttributes
)]
#[derive(Debug, Default, Clone)]
pub(super) struct WindowsPerfCountersScrapeMetrics {
    /// Number of PDH collection attempts by terminal outcome.
    #[metric(unit = "{scrape}")]
    pub attempts: Counter<u64>,
}

/// Receiver-specific counter-local failure metrics.
#[metric_set(name = "receiver.windowsperfcounters")]
#[derive(Debug, Default, Clone)]
pub(super) struct WindowsPerfCountersHealthMetrics {
    /// Number of configured counter values omitted because their read or calculation failed.
    #[metric(unit = "{metric}")]
    pub failed_counter_values: Counter<u64>,

    /// Number of scrape timeouts and ticks skipped while the worker remained busy.
    #[metric(unit = "{scrape}")]
    pub scrape_overruns: Counter<u64>,
}

/// Bounded operational metrics for one receiver node.
pub(super) struct WindowsPerfCountersReceiverMetrics {
    scrapes: MeasurementMetricSet<WindowsPerfCountersScrapeMetrics>,
    pub(super) health: MetricSet<WindowsPerfCountersHealthMetrics>,
}

impl WindowsPerfCountersReceiverMetrics {
    /// Register receiver-specific metric sets for a pipeline node.
    pub(super) fn register(pipeline: &PipelineContext) -> Self {
        Self {
            scrapes: WindowsPerfCountersScrapeMetrics::register(pipeline),
            health: WindowsPerfCountersHealthMetrics::register(pipeline),
        }
    }

    /// Record one terminal scrape outcome.
    pub(super) fn record_scrape(&mut self, outcome: Outcome) {
        self.scrapes
            .with(OutcomeAttributes { outcome })
            .attempts
            .inc();
    }

    /// Report all receiver-specific metric sets.
    pub(super) fn report(&mut self, reporter: &mut MetricsReporter) -> Result<(), Error> {
        reporter.report_measurement(&mut self.scrapes)?;
        reporter.report(&mut self.health)
    }

    /// Take terminal snapshots for all receiver-specific metric sets.
    pub(super) fn terminal_snapshots(&mut self) -> Vec<MetricSetSnapshot> {
        let mut snapshots = self.scrapes.terminal_snapshots();
        snapshots.extend(self.health.terminal_snapshots());
        snapshots
    }
}
