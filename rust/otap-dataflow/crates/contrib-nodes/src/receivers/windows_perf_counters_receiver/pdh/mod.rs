// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Persistent synchronous PDH worker. Windows handles never leave its thread.

use super::config::{Aggregation, CounterConfig, MetricKind};
use super::model::{
    ExpansionOverflow, InstanceIdentity, Number, Sample, SampleDiagnostics, SampleFailure,
    SamplePoint, SampleValue, ScaleError, scale_double, scale_integer,
};
mod counter_type;
mod native;
use counter_type::{CounterKind, UnsupportedNativeType, classify_native_type};
use native::NativePdh;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use windows_sys::Win32::System::Performance::PDH_MAX_COUNTER_PATH;

const INIT_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_REBUILD_AFTER_FAILURES: u32 = 3;
#[cfg(test)]
static QUERY_OPENS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
static QUERY_CLOSES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("{operation} for {path} failed with PDH status 0x{status:08X}")]
    Pdh {
        operation: &'static str,
        path: String,
        status: u32,
    },
    /// A counter's CStatus failure, separate from a PDH API return code.
    #[error("{operation} for {path} failed with counter data status 0x{status:08X}")]
    CounterData {
        operation: &'static str,
        path: String,
        status: u32,
    },
    #[error(transparent)]
    UnsupportedType(#[from] UnsupportedNativeType),
    #[error("invalid metric mapping for {path}: {message}")]
    InvalidMetricMapping { path: String, message: String },
    #[error("counter setup for {path} failed: {message}")]
    CounterSetup {
        path: String,
        message: String,
        permanent: bool,
    },
    #[error("invalid performance-counter token for {path}")]
    InvalidCounterToken { path: String },
    #[error("invalid performance-counter sample: {0}")]
    InvalidSample(&'static str),
    #[error("counter calculation for {path} failed: {message}")]
    Calculation { path: String, message: String },
    #[error("counter scaling for {path} failed: {source}")]
    Scale { path: String, source: ScaleError },
    #[error("failed to start PDH worker thread: {0}")]
    WorkerStart(String),
    #[error("PDH worker initialization timed out after 30 seconds")]
    WorkerInitTimeout,
    #[error("PDH worker stopped before initialization completed")]
    WorkerInitStopped,
    #[error("a performance-counter collection request is already pending")]
    WorkerBusy,
    #[error("PDH worker stopped unexpectedly")]
    WorkerStopped,
    #[error("PDH worker thread panicked")]
    WorkerPanicked,
    #[error("PDH query recovery is deferred for another {retry_after_ms} ms")]
    QueryRecoveryPending { retry_after_ms: u128 },
}

impl Error {
    pub(super) fn is_collection_failure(&self) -> bool {
        matches!(
            self,
            Self::Pdh { .. }
                | Self::CounterData { .. }
                | Self::InvalidSample(_)
                | Self::Calculation { .. }
                | Self::Scale { .. }
                | Self::WorkerBusy
                | Self::QueryRecoveryPending { .. }
        )
    }

    pub(super) fn is_overrun(&self) -> bool {
        matches!(self, Self::WorkerBusy)
    }

    fn is_query_collection_failure(&self) -> bool {
        matches!(
            self,
            Self::Pdh {
                operation: "PdhCollectQueryData" | "PdhCollectQueryData(prime)",
                ..
            }
        )
    }

    fn requires_immediate_query_rebuild(&self) -> bool {
        matches!(
            self,
            Self::Pdh { operation, .. }
                if matches!(*operation, "PdhRemoveCounter" | "PdhRemoveCounter(retry)")
        )
    }

    fn reason(&self) -> &'static str {
        match self {
            Self::Pdh { operation, .. } | Self::CounterData { operation, .. } => operation,
            Self::UnsupportedType(_) => "unsupported_type",
            Self::InvalidMetricMapping { .. } => "invalid_metric_mapping",
            Self::CounterSetup { .. } => "counter_setup",
            Self::InvalidCounterToken { .. } => "invalid_counter_token",
            Self::InvalidSample(_) => "invalid_sample",
            Self::Calculation { .. } => "calculation",
            Self::Scale { .. } => "scale",
            Self::WorkerStart(_) => "worker_start",
            Self::WorkerInitTimeout => "worker_init_timeout",
            Self::WorkerInitStopped => "worker_init_stopped",
            Self::WorkerBusy => "worker_busy",
            Self::WorkerStopped => "worker_stopped",
            Self::WorkerPanicked => "worker_panicked",
            Self::QueryRecoveryPending { .. } => "query_recovery_pending",
        }
    }

    fn bounded_detail(&self) -> String {
        match self {
            Self::Pdh { status, .. } => format!("PDH status 0x{status:08X}"),
            Self::CounterData { status, .. } => {
                format!("counter data status 0x{status:08X}")
            }
            Self::UnsupportedType(error) => {
                format!("unsupported native type 0x{:08X}", error.native_type)
            }
            Self::InvalidMetricMapping { message, .. } => message.clone(),
            Self::CounterSetup { message, .. } => message.clone(),
            Self::InvalidCounterToken { .. } => self.to_string(),
            Self::InvalidSample(message) => (*message).to_owned(),
            Self::Calculation { message, .. } => message.clone(),
            Self::Scale { source, .. } => source.to_string(),
            other => other.to_string(),
        }
    }
}

/// The numeric representation requested from PDH.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ValueFormat {
    Integer,
    Double,
}

/// Checked native fields used to validate receiver-side sampling state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RawSample {
    second_value: i64,
    multi_count: u32,
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

impl Error {
    fn is_permanent_counter_error(&self) -> bool {
        matches!(
            self,
            Self::UnsupportedType(_)
                | Self::InvalidMetricMapping { .. }
                | Self::CounterSetup {
                    permanent: true,
                    ..
                }
        )
    }
}

fn setup_error_with_cleanup<A: PdhApi>(
    api: &mut A,
    counter: &A::Counter,
    path: &str,
    error: Error,
) -> Error {
    let permanent = error.is_permanent_counter_error();
    match api.remove(counter, path) {
        Ok(()) => error,
        Err(cleanup) => Error::CounterSetup {
            path: path.to_owned(),
            message: format!("{error}; {cleanup}"),
            permanent,
        },
    }
}

fn validate_positive_base(path: &str, base: i64) -> Result<(), Error> {
    if base > 0 {
        Ok(())
    } else {
        Err(Error::Calculation {
            path: path.to_owned(),
            message: format!("base denominator must be positive, got {base}"),
        })
    }
}

fn validate_positive_multi_base(path: &str, base: u32) -> Result<(), Error> {
    if base > 0 {
        Ok(())
    } else {
        Err(Error::Calculation {
            path: path.to_owned(),
            message: "multi-counter base denominator must be positive, got 0".to_owned(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExactAddFailureDisposition {
    Diagnostic,
    Retry,
}

fn exact_add_failure_disposition(error: &Error) -> ExactAddFailureDisposition {
    if error.is_permanent_counter_error() {
        ExactAddFailureDisposition::Diagnostic
    } else {
        ExactAddFailureDisposition::Retry
    }
}

fn parse_instance(path: &str) -> Result<InstanceIdentity, Error> {
    NativePdh::parse_instance_path(path)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CounterKey {
    config_index: usize,
    normalized_path: String,
}

impl CounterKey {
    fn new(config_index: usize, path: &str) -> Self {
        Self {
            config_index,
            normalized_path: path.to_lowercase(),
        }
    }
}

#[derive(Clone)]
struct CounterTarget {
    config_index: usize,
    path: String,
    localized: bool,
    instance: Option<InstanceIdentity>,
}

#[derive(Clone)]
struct RetryState {
    target: CounterTarget,
    attempts: u32,
    next_attempt: Instant,
}

fn retry_delay(attempts: u32, cap: Duration) -> Duration {
    let exponent = attempts.saturating_sub(1).min(20);
    Duration::from_secs(1_u64 << exponent).min(cap)
}

fn should_rebuild_query(consecutive_failures: u32) -> bool {
    consecutive_failures >= QUERY_REBUILD_AFTER_FAILURES
}

fn schedule_retry_state(
    states: &mut BTreeMap<CounterKey, RetryState>,
    key: CounterKey,
    target: CounterTarget,
    now: Instant,
    cap: Duration,
) {
    let attempts = states
        .get(&key)
        .map_or(1, |state| state.attempts.saturating_add(1));
    let _ = states.insert(
        key,
        RetryState {
            target,
            attempts,
            next_attempt: now + retry_delay(attempts, cap),
        },
    );
}

fn limit_expanded_paths(
    paths: &mut Vec<String>,
    per_wildcard_limit: usize,
    total_remaining: usize,
) -> (usize, usize) {
    let (retained, per_wildcard_omitted, total_omitted) =
        expansion_allocation(paths.len(), per_wildcard_limit, total_remaining);
    paths.truncate(retained);
    (per_wildcard_omitted, total_omitted)
}

fn expansion_allocation(
    discovered: usize,
    per_wildcard_limit: usize,
    total_remaining: usize,
) -> (usize, usize, usize) {
    let per_wildcard_retained = discovered.min(per_wildcard_limit);
    let per_wildcard_omitted = discovered - per_wildcard_retained;
    let retained = per_wildcard_retained.min(total_remaining);
    let total_omitted = per_wildcard_retained - retained;
    (retained, per_wildcard_omitted, total_omitted)
}

fn sample_failure(counter_index: usize, error: &Error) -> SampleFailure {
    SampleFailure {
        counter_index,
        reason: error.reason(),
        error: error.bounded_detail(),
    }
}

fn positive_timestamp(timestamp: i64) -> Result<i64, Error> {
    if timestamp > 0 {
        Ok(timestamp)
    } else {
        Err(Error::InvalidSample("timestamp must be positive"))
    }
}

fn cumulative_start_time(
    start_time_unix_nano: &mut i64,
    previous_timestamp_unix_nano: &mut i64,
    timestamp_unix_nano: i64,
) -> i64 {
    if timestamp_unix_nano < *previous_timestamp_unix_nano {
        *start_time_unix_nano = timestamp_unix_nano;
    }
    *previous_timestamp_unix_nano = timestamp_unix_nano;
    *start_time_unix_nano
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MonotonicSequence {
    start_time_unix_nano: Option<i64>,
    previous_value: Option<i64>,
    previous_timestamp_unix_nano: Option<i64>,
}

fn monotonic_start_time(
    path: &str,
    sequence: &mut MonotonicSequence,
    value: i64,
    timestamp_unix_nano: i64,
) -> Result<i64, Error> {
    if value < 0 {
        return Err(Error::Calculation {
            path: path.to_owned(),
            message: format!("monotonic Sum value must be non-negative, got {value}"),
        });
    }
    if sequence.start_time_unix_nano.is_none()
        || sequence.previous_value.is_none()
        || sequence
            .previous_value
            .is_some_and(|previous| value < previous)
        || sequence
            .previous_timestamp_unix_nano
            .is_some_and(|previous| timestamp_unix_nano < previous)
    {
        sequence.start_time_unix_nano = Some(timestamp_unix_nano);
    }
    sequence.previous_value = Some(value);
    sequence.previous_timestamp_unix_nano = Some(timestamp_unix_nano);
    sequence.start_time_unix_nano.ok_or(Error::InvalidSample(
        "monotonic Sum start time was not initialized",
    ))
}

fn reset_monotonic_sequences_on_query_rollback<'a>(
    previous_query_timestamp_unix_nano: i64,
    timestamp_unix_nano: i64,
    sequences: impl Iterator<Item = &'a mut MonotonicSequence>,
) {
    if timestamp_unix_nano < previous_query_timestamp_unix_nano {
        for sequence in sequences {
            *sequence = MonotonicSequence::default();
        }
    }
}

fn advance_collection_time<'a>(
    start_time_unix_nano: &mut i64,
    previous_timestamp_unix_nano: &mut i64,
    timestamp_unix_nano: i64,
    sequences: impl Iterator<Item = &'a mut MonotonicSequence>,
) -> i64 {
    reset_monotonic_sequences_on_query_rollback(
        *previous_timestamp_unix_nano,
        timestamp_unix_nano,
        sequences,
    );
    cumulative_start_time(
        start_time_unix_nano,
        previous_timestamp_unix_nano,
        timestamp_unix_nano,
    )
}

fn validate_metric_mapping(
    path: &str,
    metric_kind: MetricKind,
    counter_kind: CounterKind,
) -> Result<(), Error> {
    match metric_kind {
        MetricKind::Sum {
            aggregation: Aggregation::Delta,
            ..
        } if counter_kind != CounterKind::Delta => {
            return Err(Error::InvalidMetricMapping {
                path: path.to_owned(),
                message: "delta Sum requires a native delta-count counter type \
                          (PERF_COUNTER_DELTA / LARGE_DELTA)"
                    .to_owned(),
            });
        }
        MetricKind::Sum {
            aggregation: Aggregation::Cumulative,
            monotonic: true,
        } if counter_kind != CounterKind::Direct => {
            return Err(Error::InvalidMetricMapping {
                path: path.to_owned(),
                message: "monotonic Sum requires a direct raw-count counter type \
                          (PERF_COUNTER_RAWCOUNT / LARGE_RAWCOUNT)"
                    .to_owned(),
            });
        }
        _ => {}
    }
    Ok(())
}

fn delta_interval_start(
    path: &str,
    baseline_unix_nano: &mut Option<i64>,
    number: Number,
    monotonic: bool,
    timestamp_unix_nano: i64,
) -> Result<Option<i64>, Error> {
    let Number::Double(value) = number else {
        return Err(Error::InvalidSample(
            "delta Sum was not backed by a formatted double value",
        ));
    };
    let previous = baseline_unix_nano.replace(timestamp_unix_nano);
    let Some(previous) = previous else {
        return Ok(None);
    };
    if timestamp_unix_nano <= previous || (monotonic && value < 0.0) {
        return Ok(None);
    }
    if !value.is_finite() {
        return Err(Error::Calculation {
            path: path.to_owned(),
            message: "delta Sum value must be finite".to_owned(),
        });
    }
    Ok(Some(previous))
}

fn is_aggregation_instance(path: &str, aggregation_name: &str) -> bool {
    parse_instance(path).is_ok_and(|instance| instance.name.eq_ignore_ascii_case(aggregation_name))
}

fn remove_aggregation_instances(paths: &mut Vec<String>, aggregation_name: Option<&str>) {
    if let Some(aggregation_name) = aggregation_name {
        paths.retain(|path| !is_aggregation_instance(path, aggregation_name));
    }
}

fn merge_diagnostics(target: &mut SampleDiagnostics, delta: SampleDiagnostics) {
    target.discovery_refreshes += delta.discovery_refreshes;
    target.discovery_failures += delta.discovery_failures;
    target.instances_added += delta.instances_added;
    target.instances_removed += delta.instances_removed;
    target.instances_omitted_over_limit += delta.instances_omitted_over_limit;
    target.counter_add_failures += delta.counter_add_failures;
    target.counter_read_failures += delta.counter_read_failures;
    target.retry_attempts += delta.retry_attempts;
    target.retry_recoveries += delta.retry_recoveries;
    target.query_rebuild_attempts += delta.query_rebuild_attempts;
    target.query_rebuild_recoveries += delta.query_rebuild_recoveries;
    target.warmup_omissions += delta.warmup_omissions;
}

fn merge_sample_metadata(
    target_diagnostics: &mut SampleDiagnostics,
    target_failures: &mut Vec<SampleFailure>,
    target_overflows: &mut Vec<ExpansionOverflow>,
    diagnostics: SampleDiagnostics,
    failures: Vec<SampleFailure>,
    overflows: Vec<ExpansionOverflow>,
) {
    merge_diagnostics(target_diagnostics, diagnostics);
    for failure in failures {
        if let Some(existing) = target_failures.iter_mut().find(|existing| {
            existing.counter_index == failure.counter_index && existing.reason == failure.reason
        }) {
            *existing = failure;
        } else {
            target_failures.push(failure);
        }
    }
    for overflow in overflows {
        if let Some(existing) = target_overflows.iter_mut().find(|existing| {
            existing.counter_index == overflow.counter_index && existing.reason == overflow.reason
        }) {
            *existing = overflow;
        } else {
            target_overflows.push(overflow);
        }
    }
}

#[derive(Default)]
struct PendingMetadata {
    diagnostics: SampleDiagnostics,
    failures: Vec<SampleFailure>,
    overflows: Vec<ExpansionOverflow>,
}

impl PendingMetadata {
    fn merge(
        &mut self,
        diagnostics: SampleDiagnostics,
        failures: Vec<SampleFailure>,
        overflows: Vec<ExpansionOverflow>,
    ) {
        merge_sample_metadata(
            &mut self.diagnostics,
            &mut self.failures,
            &mut self.overflows,
            diagnostics,
            failures,
            overflows,
        );
    }

    fn merge_into_sample(&mut self, sample: &mut Sample) {
        let mut pending = std::mem::take(self);
        merge_diagnostics(&mut sample.diagnostics, pending.diagnostics);
        pending.failures.append(&mut sample.failures);
        sample.failures = pending.failures;
        pending.overflows.append(&mut sample.overflows);
        sample.overflows = pending.overflows;
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InstanceGroupKey {
    config_index: usize,
    normalized_name: String,
    normalized_parent: Option<String>,
}

impl InstanceGroupKey {
    fn new(config_index: usize, instance: &InstanceIdentity) -> Self {
        Self {
            config_index,
            normalized_name: instance.name.to_lowercase(),
            normalized_parent: instance.parent.as_ref().map(|value| value.to_lowercase()),
        }
    }
}

fn instance_groups<'a>(
    counters: impl Iterator<Item = (&'a CounterKey, &'a InstanceIdentity)>,
) -> BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>> {
    let mut groups = BTreeMap::new();
    for (key, instance) in counters {
        let _ = groups
            .entry(InstanceGroupKey::new(key.config_index, instance))
            .or_insert_with(BTreeSet::new)
            .insert(key.clone());
    }
    groups
}

fn changed_instance_groups(
    existing: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
    wanted: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
) -> BTreeSet<InstanceGroupKey> {
    existing
        .keys()
        .chain(wanted.keys())
        .filter(|group| existing.get(*group) != wanted.get(*group))
        .cloned()
        .collect()
}

fn refresh_changes_with_group_resets(
    live: &BTreeSet<CounterKey>,
    retrying: &BTreeSet<CounterKey>,
    wanted: &BTreeSet<CounterKey>,
    known_groups: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
    wanted_groups: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
) -> (Vec<CounterKey>, Vec<CounterKey>) {
    let changed_groups = changed_instance_groups(known_groups, wanted_groups);
    let reset_live = changed_groups
        .iter()
        .filter_map(|group| known_groups.get(group))
        .flatten()
        .filter(|key| live.contains(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    let reset_wanted = changed_groups
        .iter()
        .filter_map(|group| wanted_groups.get(group))
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut removed = live.difference(wanted).cloned().collect::<Vec<_>>();
    let mut added = wanted
        .difference(live)
        .filter(|key| !retrying.contains(*key))
        .cloned()
        .collect::<Vec<_>>();
    removed.extend(reset_live);
    removed.sort();
    removed.dedup();
    added.extend(reset_wanted);
    added.sort();
    added.dedup();
    (removed, added)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CounterHistory {
    previous_base: Option<i64>,
    monotonic_sequence: MonotonicSequence,
    delta_baseline_unix_nano: Option<i64>,
}

impl CounterHistory {
    fn restart_pdh_sampling(&mut self) {
        self.previous_base = None;
        self.delta_baseline_unix_nano = None;
    }
}

fn restart_query_pdh_sampling<'a>(histories: impl Iterator<Item = &'a mut CounterHistory>) {
    for history in histories {
        history.restart_pdh_sampling();
    }
}

struct CounterHandle<C> {
    config_index: usize,
    path: String,
    handle: C,
    kind: CounterKind,
    scale_power10: i32,
    history: CounterHistory,
    wildcard: bool,
    instance: Option<InstanceIdentity>,
}

impl<C> CounterHandle<C> {
    fn reset_readiness(&mut self) {
        self.history.restart_pdh_sampling();
    }

    fn read(
        &mut self,
        api: &mut impl PdhApi<Counter = C>,
        metric_kind: MetricKind,
        timestamp_unix_nano: i64,
    ) -> Result<(SampleValue, Option<i64>), Error> {
        if !matches!(
            self.kind,
            CounterKind::Direct | CounterKind::FormattedSingleSample
        ) {
            let raw = match api.raw(&self.handle, &self.path) {
                Ok(Some(raw)) => raw,
                Ok(None) => {
                    self.reset_readiness();
                    return Ok((SampleValue::NoObservation, None));
                }
                Err(error) => {
                    self.reset_readiness();
                    return Err(error);
                }
            };
            if self.kind == CounterKind::RawFraction {
                validate_positive_base(&self.path, raw.second_value)?;
            } else {
                let previous = self.history.previous_base.replace(raw.second_value);
                if self.kind == CounterKind::CalculatedTwoSampleWithMultiBase
                    && let Err(error) = validate_positive_multi_base(&self.path, raw.multi_count)
                {
                    return Err(error);
                }
                let Some(previous) = previous else {
                    if self.kind == CounterKind::Delta {
                        self.history.delta_baseline_unix_nano = Some(timestamp_unix_nano);
                    }
                    return Ok((SampleValue::Warming, None));
                };
                if self.kind == CounterKind::CalculatedTwoSampleWithBase {
                    match raw.second_value.cmp(&previous) {
                        std::cmp::Ordering::Less => {
                            self.reset_readiness();
                            return Ok((SampleValue::NoObservation, None));
                        }
                        std::cmp::Ordering::Equal => {
                            return Ok((SampleValue::NoObservation, None));
                        }
                        std::cmp::Ordering::Greater => {}
                    }
                }
            }
        }

        let format = if self.kind == CounterKind::Direct {
            ValueFormat::Integer
        } else {
            ValueFormat::Double
        };
        let number = match api.formatted(&self.handle, &self.path, format) {
            Ok(Some(number)) => number,
            Ok(None) => {
                self.reset_readiness();
                return Ok((SampleValue::NoObservation, None));
            }
            Err(error) => {
                if self.kind == CounterKind::Delta || matches!(error, Error::CounterData { .. }) {
                    self.reset_readiness();
                }
                return Err(error);
            }
        };
        let direct_value = match number {
            Number::Integer(value) => Some(value),
            Number::Double(_) => None,
        };
        let delta_start_time = if let MetricKind::Sum {
            aggregation: Aggregation::Delta,
            monotonic,
        } = metric_kind
        {
            match delta_interval_start(
                &self.path,
                &mut self.history.delta_baseline_unix_nano,
                number,
                monotonic,
                timestamp_unix_nano,
            )? {
                Some(start_time) => Some(start_time),
                None => return Ok((SampleValue::NoObservation, None)),
            }
        } else {
            None
        };
        let scaled = match number {
            Number::Integer(value) => scale_integer(value, self.scale_power10),
            Number::Double(value) => scale_double(value, self.scale_power10),
        }
        .map_err(|source| Error::Scale {
            path: self.path.clone(),
            source,
        })?;
        let point_start_time = match metric_kind {
            MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: true,
            } => {
                let Some(direct_value) = direct_value else {
                    return Err(Error::InvalidSample(
                        "monotonic Sum was not backed by a direct formatted value",
                    ));
                };
                Some(monotonic_start_time(
                    &self.path,
                    &mut self.history.monotonic_sequence,
                    direct_value,
                    timestamp_unix_nano,
                )?)
            }
            MetricKind::Sum {
                aggregation: Aggregation::Delta,
                ..
            } => delta_start_time,
            MetricKind::Gauge
            | MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: false,
            } => None,
        };
        Ok((SampleValue::Value(scaled), point_start_time))
    }
}

/// The query owns every added counter; closing it releases all handles.
struct Query<A: PdhApi = NativePdh> {
    api: A,
    start_time_unix_nano: i64,
    previous_timestamp_unix_nano: i64,
    configs: Vec<CounterConfig>,
    counters: BTreeMap<CounterKey, CounterHandle<A::Counter>>,
    retry_states: BTreeMap<CounterKey, RetryState>,
    pending: PendingMetadata,
    wildcard_refresh_interval: Duration,
    max_instances_per_wildcard: usize,
    max_expanded_counters: usize,
    last_wildcard_refresh: Instant,
}

impl Query {
    fn open(
        configs: Vec<CounterConfig>,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_expanded_counters: usize,
        node: String,
    ) -> Result<Self, Error> {
        Self::open_with(
            NativePdh::open(node)?,
            configs,
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_expanded_counters,
        )
    }
}

impl<A: PdhApi> Query<A> {
    fn open_with(
        mut api: A,
        configs: Vec<CounterConfig>,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_expanded_counters: usize,
    ) -> Result<Self, Error> {
        let start_time_unix_nano = positive_timestamp(api.timestamp()?)?;
        let mut query = Self {
            api,
            start_time_unix_nano,
            previous_timestamp_unix_nano: start_time_unix_nano,
            configs,
            counters: BTreeMap::new(),
            retry_states: BTreeMap::new(),
            pending: PendingMetadata::default(),
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_expanded_counters,
            last_wildcard_refresh: Instant::now(),
        };
        for config_index in 0..query.configs.len() {
            if !query.configs[config_index].path.contains('*') {
                let path = query.configs[config_index].path.clone();
                let key = CounterKey::new(config_index, &path);
                let instance = if query.configs[config_index].has_instance {
                    match query.api.parse_instance(&path) {
                        Ok(instance) => Some(instance),
                        Err(error) => {
                            query.pending.diagnostics.counter_add_failures += 1;
                            query
                                .pending
                                .failures
                                .push(sample_failure(config_index, &error));
                            continue;
                        }
                    }
                } else {
                    None
                };
                let target = CounterTarget {
                    config_index,
                    path,
                    localized: false,
                    instance: instance.clone(),
                };
                match query.add_counter(
                    target.config_index,
                    target.path.clone(),
                    target.localized,
                    instance,
                ) {
                    Ok(()) => {}
                    Err(error) => match exact_add_failure_disposition(&error) {
                        ExactAddFailureDisposition::Diagnostic => {
                            query.pending.diagnostics.counter_add_failures += 1;
                            query
                                .pending
                                .failures
                                .push(sample_failure(config_index, &error));
                        }
                        ExactAddFailureDisposition::Retry => {
                            query.pending.diagnostics.counter_add_failures += 1;
                            query
                                .pending
                                .failures
                                .push(sample_failure(config_index, &error));
                            query.schedule_retry(key, target, Instant::now());
                        }
                    },
                }
            }
        }
        let mut diagnostics = SampleDiagnostics::default();
        let mut failures = Vec::new();
        let mut overflows = Vec::new();
        query.refresh_wildcards(&mut diagnostics, &mut failures, &mut overflows)?;
        query.pending.merge(diagnostics, failures, overflows);
        Ok(query)
    }

    fn add_counter(
        &mut self,
        config_index: usize,
        path: String,
        localized: bool,
        instance: Option<InstanceIdentity>,
    ) -> Result<(), Error> {
        let counter = self.api.add(&path, localized)?;
        let kind = match self
            .api
            .counter_type(&counter, &path)
            .and_then(|native_type| classify_native_type(&path, native_type).map_err(Error::from))
        {
            Ok(kind) => kind,
            Err(error) => {
                return Err(setup_error_with_cleanup(
                    &mut self.api,
                    &counter,
                    &path,
                    error,
                ));
            }
        };
        if let Err(error) =
            validate_metric_mapping(&path, self.configs[config_index].metric_kind, kind)
        {
            return Err(setup_error_with_cleanup(
                &mut self.api,
                &counter,
                &path,
                error,
            ));
        }
        let key = CounterKey::new(config_index, &path);
        let _ = self.counters.insert(
            key,
            CounterHandle {
                config_index,
                path,
                handle: counter,
                kind,
                scale_power10: self.configs[config_index].scale_power10,
                history: CounterHistory::default(),
                wildcard: localized,
                instance,
            },
        );
        Ok(())
    }

    fn schedule_retry(&mut self, key: CounterKey, target: CounterTarget, now: Instant) {
        schedule_retry_state(
            &mut self.retry_states,
            key,
            target,
            now,
            self.wildcard_refresh_interval,
        );
    }

    fn try_add_target(
        &mut self,
        key: CounterKey,
        target: CounterTarget,
        now: Instant,
        diagnostics: &mut SampleDiagnostics,
        failures: &mut Vec<SampleFailure>,
    ) -> bool {
        match self.add_counter(
            target.config_index,
            target.path.clone(),
            target.localized,
            target.instance.clone(),
        ) {
            Ok(()) => {
                if self.retry_states.remove(&key).is_some() {
                    diagnostics.retry_recoveries += 1;
                }
                true
            }
            Err(error) => {
                diagnostics.counter_add_failures += 1;
                failures.push(sample_failure(target.config_index, &error));
                if !error.is_permanent_counter_error() {
                    self.schedule_retry(key, target, now);
                }
                false
            }
        }
    }

    fn retry_counters(
        &mut self,
        now: Instant,
        diagnostics: &mut SampleDiagnostics,
        failures: &mut Vec<SampleFailure>,
    ) {
        let due = self
            .retry_states
            .iter()
            .filter(|(_, state)| state.next_attempt <= now)
            .map(|(key, state)| (key.clone(), state.target.clone()))
            .collect::<Vec<_>>();
        for (key, target) in due {
            diagnostics.retry_attempts += 1;
            let _ = self.try_add_target(key, target, now, diagnostics, failures);
        }
    }

    fn refresh_wildcards(
        &mut self,
        diagnostics: &mut SampleDiagnostics,
        failures: &mut Vec<SampleFailure>,
        overflows: &mut Vec<ExpansionOverflow>,
    ) -> Result<(), Error> {
        diagnostics.discovery_refreshes += 1;
        let mut desired = BTreeMap::new();
        let mut expanded_total = 0;
        let wildcard_paths = self
            .configs
            .iter()
            .enumerate()
            .filter(|(_, config)| config.path.contains('*'))
            .map(|(config_index, config)| (config_index, config.path.clone()))
            .collect::<Vec<_>>();
        let discoveries = wildcard_paths
            .into_iter()
            .map(|(config_index, path)| (config_index, self.api.expand(&path)))
            .collect::<Vec<_>>();

        // Retain failed templates before admitting successful discoveries so
        // their live handles and retry identities reserve global capacity
        // independently of configuration order.
        for (config_index, discovery) in &discoveries {
            let Err(error) = discovery else {
                continue;
            };
            diagnostics.discovery_failures += 1;
            failures.push(sample_failure(*config_index, error));
            let mut retained = self
                .counters
                .iter()
                .filter_map(|(key, counter)| {
                    (key.config_index == *config_index && counter.wildcard).then(|| {
                        counter
                            .instance
                            .clone()
                            .map(|instance| (key.clone(), (counter.path.clone(), instance)))
                    })?
                })
                .chain(self.retry_states.iter().filter_map(|(key, state)| {
                    (key.config_index == *config_index && state.target.localized).then(|| {
                        state
                            .target
                            .instance
                            .clone()
                            .map(|instance| (key.clone(), (state.target.path.clone(), instance)))
                    })?
                }))
                .collect::<BTreeMap<_, _>>();
            let discovered = retained.len();
            let (allowed, per_wildcard_omitted, total_omitted) = expansion_allocation(
                discovered,
                self.max_instances_per_wildcard,
                self.max_expanded_counters.saturating_sub(expanded_total),
            );
            if per_wildcard_omitted > 0 {
                overflows.push(ExpansionOverflow {
                    counter_index: *config_index,
                    reason: "per_wildcard",
                    discovered,
                    retained: discovered - per_wildcard_omitted,
                    omitted: per_wildcard_omitted,
                });
            }
            if total_omitted > 0 {
                overflows.push(ExpansionOverflow {
                    counter_index: *config_index,
                    reason: "receiver_total",
                    discovered: discovered - per_wildcard_omitted,
                    retained: allowed,
                    omitted: total_omitted,
                });
            }
            diagnostics.instances_omitted_over_limit +=
                u64::try_from(per_wildcard_omitted.saturating_add(total_omitted))
                    .unwrap_or(u64::MAX);
            while retained.len() > allowed {
                let Some(key) = retained.keys().next_back().cloned() else {
                    break;
                };
                let _ = retained.remove(&key);
            }
            expanded_total += retained.len();
            for (key, (path, instance)) in retained {
                let _ = desired.insert(key, (path, instance));
            }
        }

        for (config_index, discovery) in discoveries {
            let Ok(mut paths) = discovery else {
                continue;
            };
            remove_aggregation_instances(
                &mut paths,
                self.configs[config_index]
                    .excluded_aggregation_instance
                    .as_deref(),
            );
            let oversized = paths
                .iter()
                .filter(|path| path.encode_utf16().count() >= PDH_MAX_COUNTER_PATH as usize)
                .count();
            if oversized > 0 {
                diagnostics.counter_add_failures += u64::try_from(oversized).unwrap_or(u64::MAX);
                failures.push(SampleFailure {
                    counter_index: config_index,
                    reason: "expanded_path_too_long",
                    error: format!(
                        "{oversized} expanded paths reached the PDH {PDH_MAX_COUNTER_PATH}-unit limit"
                    ),
                });
                paths.retain(|path| path.encode_utf16().count() < PDH_MAX_COUNTER_PATH as usize);
            }
            paths.sort_by_key(|path| path.to_lowercase());
            paths.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
            let discovered = paths.len();
            let (per_wildcard_omitted, total_omitted) = limit_expanded_paths(
                &mut paths,
                self.max_instances_per_wildcard,
                self.max_expanded_counters.saturating_sub(expanded_total),
            );
            if per_wildcard_omitted > 0 {
                overflows.push(ExpansionOverflow {
                    counter_index: config_index,
                    reason: "per_wildcard",
                    discovered,
                    retained: discovered - per_wildcard_omitted,
                    omitted: per_wildcard_omitted,
                });
            }
            if total_omitted > 0 {
                overflows.push(ExpansionOverflow {
                    counter_index: config_index,
                    reason: "receiver_total",
                    discovered: discovered - per_wildcard_omitted,
                    retained: paths.len(),
                    omitted: total_omitted,
                });
            }
            diagnostics.instances_omitted_over_limit +=
                u64::try_from(per_wildcard_omitted.saturating_add(total_omitted))
                    .unwrap_or(u64::MAX);
            expanded_total += paths.len();
            for path in paths {
                let key = CounterKey::new(config_index, &path);
                match self.api.parse_instance(&path) {
                    Ok(instance) => {
                        let _ = desired.insert(key, (path, instance));
                    }
                    Err(error) => {
                        diagnostics.counter_add_failures += 1;
                        failures.push(sample_failure(config_index, &error));
                    }
                }
            }
        }

        let live = self
            .counters
            .iter()
            .filter_map(|(key, counter)| counter.wildcard.then_some(key.clone()))
            .collect::<BTreeSet<_>>();
        let retrying = self
            .retry_states
            .iter()
            .filter_map(|(key, state)| state.target.localized.then_some(key.clone()))
            .collect::<BTreeSet<_>>();
        let wanted = desired.keys().cloned().collect::<BTreeSet<_>>();
        let removed_retry_instances = self
            .retry_states
            .iter()
            .filter(|(key, state)| state.target.localized && !wanted.contains(*key))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        diagnostics.instances_removed +=
            u64::try_from(removed_retry_instances.len()).unwrap_or(u64::MAX);
        for key in removed_retry_instances {
            let _ = self.retry_states.remove(&key);
        }
        let known_groups = instance_groups(
            self.counters
                .iter()
                .filter_map(|(key, counter)| {
                    counter.instance.as_ref().map(|instance| (key, instance))
                })
                .chain(self.retry_states.iter().filter_map(|(key, state)| {
                    state
                        .target
                        .instance
                        .as_ref()
                        .map(|instance| (key, instance))
                })),
        );
        let wanted_groups =
            instance_groups(desired.iter().map(|(key, (_, instance))| (key, instance)));
        let (removed, added) = refresh_changes_with_group_resets(
            &live,
            &retrying,
            &wanted,
            &known_groups,
            &wanted_groups,
        );
        for key in removed {
            let _ = self.retry_states.remove(&key);
            let counter = self.counters.remove(&key).ok_or(Error::InvalidSample(
                "wildcard refresh lost an existing counter",
            ));
            let Ok(counter) = counter else {
                diagnostics.discovery_failures += 1;
                continue;
            };
            self.api.remove(&counter.handle, &counter.path)?;
            if !wanted.contains(&key) {
                diagnostics.instances_removed += 1;
            }
        }
        let now = Instant::now();
        for key in added {
            let Some((path, instance)) = desired.remove(&key) else {
                diagnostics.discovery_failures += 1;
                continue;
            };
            let target = CounterTarget {
                config_index: key.config_index,
                path,
                localized: true,
                instance: Some(instance),
            };
            let was_retry = self.retry_states.contains_key(&key);
            if self.try_add_target(key.clone(), target, now, diagnostics, failures)
                && !live.contains(&key)
                && !was_retry
            {
                diagnostics.instances_added += 1;
            }
        }
        self.last_wildcard_refresh = Instant::now();
        Ok(())
    }

    fn restore_pending_metadata(
        &mut self,
        diagnostics: SampleDiagnostics,
        failures: Vec<SampleFailure>,
        overflows: Vec<ExpansionOverflow>,
    ) {
        self.pending.merge(diagnostics, failures, overflows);
    }

    fn move_pending_metadata_to(
        &mut self,
        diagnostics: &mut SampleDiagnostics,
        failures: &mut Vec<SampleFailure>,
        overflows: &mut Vec<ExpansionOverflow>,
    ) {
        let pending = std::mem::take(&mut self.pending);
        merge_sample_metadata(
            diagnostics,
            failures,
            overflows,
            pending.diagnostics,
            pending.failures,
            pending.overflows,
        );
    }

    fn collect(&mut self) -> Result<Sample, Error> {
        let pending = std::mem::take(&mut self.pending);
        let mut diagnostics = pending.diagnostics;
        let mut failures = pending.failures;
        let mut overflows = pending.overflows;
        if self.last_wildcard_refresh.elapsed() >= self.wildcard_refresh_interval
            && let Err(error) =
                self.refresh_wildcards(&mut diagnostics, &mut failures, &mut overflows)
        {
            self.restore_pending_metadata(diagnostics, failures, overflows);
            return Err(error);
        }
        self.retry_counters(Instant::now(), &mut diagnostics, &mut failures);
        if self.counters.is_empty() {
            let timestamp_unix_nano = match self.api.timestamp().and_then(positive_timestamp) {
                Ok(timestamp) => timestamp,
                Err(error) => {
                    self.restore_pending_metadata(diagnostics, failures, overflows);
                    return Err(error);
                }
            };
            let _ = advance_collection_time(
                &mut self.start_time_unix_nano,
                &mut self.previous_timestamp_unix_nano,
                timestamp_unix_nano,
                std::iter::empty(),
            );
            return Ok(Sample {
                timestamp_unix_nano,
                points: Vec::new(),
                failures,
                overflows,
                diagnostics,
            });
        }
        let has_data = match self.api.collect() {
            Ok(has_data) => has_data,
            Err(error) => {
                restart_query_pdh_sampling(
                    self.counters
                        .values_mut()
                        .map(|counter| &mut counter.history),
                );
                self.restore_pending_metadata(diagnostics, failures, overflows);
                return Err(error);
            }
        };
        if !has_data {
            restart_query_pdh_sampling(
                self.counters
                    .values_mut()
                    .map(|counter| &mut counter.history),
            );
        }
        let timestamp_unix_nano = match self.api.timestamp().and_then(positive_timestamp) {
            Ok(timestamp) => timestamp,
            Err(error) => {
                restart_query_pdh_sampling(
                    self.counters
                        .values_mut()
                        .map(|counter| &mut counter.history),
                );
                self.restore_pending_metadata(diagnostics, failures, overflows);
                return Err(error);
            }
        };
        let start_time_unix_nano = advance_collection_time(
            &mut self.start_time_unix_nano,
            &mut self.previous_timestamp_unix_nano,
            timestamp_unix_nano,
            self.counters
                .values_mut()
                .map(|counter| &mut counter.history.monotonic_sequence),
        );
        let mut points = Vec::with_capacity(self.counters.len());
        for counter in self.counters.values_mut() {
            let metric_kind = self.configs[counter.config_index].metric_kind;
            let result = if has_data {
                counter.read(&mut self.api, metric_kind, timestamp_unix_nano)
            } else {
                Ok((SampleValue::NoObservation, None))
            };
            let (value, point_start_time) = match result {
                Ok(result) => result,
                Err(error) => {
                    failures.push(sample_failure(counter.config_index, &error));
                    diagnostics.counter_read_failures += 1;
                    continue;
                }
            };
            if matches!(value, SampleValue::Warming) {
                diagnostics.warmup_omissions += 1;
            }
            let point_start_time = match (metric_kind, value, point_start_time) {
                (
                    MetricKind::Sum {
                        aggregation: Aggregation::Cumulative,
                        monotonic: false,
                    },
                    SampleValue::Value(_),
                    None,
                ) => Some(start_time_unix_nano),
                (_, _, point_start_time) => point_start_time,
            };
            points.push(SamplePoint {
                counter_index: counter.config_index,
                path: counter.path.clone(),
                instance: counter.instance.clone(),
                start_time_unix_nano: point_start_time,
                value,
            });
        }
        diagnostics.active_expanded_counters = self
            .counters
            .values()
            .filter(|counter| counter.wildcard)
            .count();
        Ok(Sample {
            timestamp_unix_nano,
            points,
            failures,
            overflows,
            diagnostics,
        })
    }
}

enum Command {
    Collect(oneshot::Sender<Result<Sample, Error>>),
    Shutdown,
}

struct AcceptedCollectGuard {
    accepted: Arc<AtomicBool>,
}

impl AcceptedCollectGuard {
    fn new(accepted: Arc<AtomicBool>) -> Self {
        Self { accepted }
    }
}

impl Drop for AcceptedCollectGuard {
    fn drop(&mut self) {
        self.accepted.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
struct QuerySettings {
    counters: Vec<CounterConfig>,
    wildcard_refresh_interval: Duration,
    max_instances_per_wildcard: usize,
    max_expanded_counters: usize,
    node: String,
}

impl QuerySettings {
    fn open(&self) -> Result<Query, Error> {
        Query::open(
            self.counters.clone(),
            self.wildcard_refresh_interval,
            self.max_instances_per_wildcard,
            self.max_expanded_counters,
            self.node.clone(),
        )
    }
}

/// Capacity-one command client for the thread that owns the persistent query.
pub(super) struct Worker {
    tx: mpsc::SyncSender<Command>,
    accepted: Arc<AtomicBool>,
    shutdown_requested: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    completion: Option<oneshot::Receiver<()>>,
}

impl Worker {
    pub(super) async fn start(
        counters: Vec<CounterConfig>,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_expanded_counters: usize,
        node: String,
    ) -> Result<Self, Error> {
        let (tx, rx) = mpsc::sync_channel(1);
        let (init_tx, init_rx) = oneshot::channel();
        let (completion_tx, completion_rx) = oneshot::channel();
        let accepted = Arc::new(AtomicBool::new(false));
        let worker_accepted = Arc::clone(&accepted);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown_requested);
        let settings = QuerySettings {
            counters,
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_expanded_counters,
            node,
        };
        let join = std::thread::Builder::new()
            .name("windowsperfcounters-pdh".to_owned())
            .spawn(move || {
                {
                    let query = match settings.open() {
                        Ok(query) => query,
                        Err(error) => {
                            let _ = init_tx.send(Err(error));
                            return;
                        }
                    };
                    if init_tx.send(Ok(())).is_err() {
                        return;
                    }
                    let mut query = Some(query);
                    let mut query_failure_attempts = 0_u32;
                    let mut next_query_retry = Instant::now();
                    let mut rebuild_attempts = 0_u32;
                    let mut next_rebuild = Instant::now();
                    let mut pending = PendingMetadata::default();
                    while !worker_shutdown.load(Ordering::Acquire) {
                        match rx.recv() {
                            Ok(Command::Collect(response)) => {
                                let _accepted =
                                    AcceptedCollectGuard::new(Arc::clone(&worker_accepted));
                                let now = Instant::now();
                                let was_query_retry = query.is_some()
                                    && query_failure_attempts > 0
                                    && now >= next_query_retry;
                                if was_query_retry {
                                    pending.diagnostics.retry_attempts += 1;
                                }
                                let mut result = if let Some(active_query) = query.as_mut() {
                                    if now < next_query_retry {
                                        Err(Error::QueryRecoveryPending {
                                            retry_after_ms: next_query_retry
                                                .duration_since(now)
                                                .as_millis(),
                                        })
                                    } else {
                                        active_query.collect()
                                    }
                                } else if now < next_rebuild {
                                    Err(Error::QueryRecoveryPending {
                                        retry_after_ms: next_rebuild
                                            .duration_since(now)
                                            .as_millis(),
                                    })
                                } else {
                                    pending.diagnostics.query_rebuild_attempts += 1;
                                    match settings.open() {
                                        Ok(rebuilt) => {
                                            pending.diagnostics.query_rebuild_recoveries += 1;
                                            rebuild_attempts = 0;
                                            query_failure_attempts = 0;
                                            next_query_retry = now;
                                            query = Some(rebuilt);
                                            query
                                                .as_mut()
                                                .expect("query was just restored")
                                                .collect()
                                        }
                                        Err(error) => {
                                            rebuild_attempts = rebuild_attempts.saturating_add(1);
                                            next_rebuild = now
                                                + retry_delay(
                                                    rebuild_attempts,
                                                    settings.wildcard_refresh_interval,
                                                );
                                            Err(error)
                                        }
                                    }
                                };
                                if result
                                    .as_ref()
                                    .is_err_and(Error::is_query_collection_failure)
                                {
                                    query_failure_attempts =
                                        query_failure_attempts.saturating_add(1);
                                    next_query_retry = now
                                        + retry_delay(
                                            query_failure_attempts,
                                            settings.wildcard_refresh_interval,
                                        );
                                    if should_rebuild_query(query_failure_attempts) {
                                        if let Some(active_query) = query.as_mut() {
                                            active_query.move_pending_metadata_to(
                                                &mut pending.diagnostics,
                                                &mut pending.failures,
                                                &mut pending.overflows,
                                            );
                                        }
                                        query = None;
                                        rebuild_attempts = 0;
                                        next_rebuild = next_query_retry;
                                    }
                                } else if result
                                    .as_ref()
                                    .is_err_and(Error::requires_immediate_query_rebuild)
                                {
                                    if let Some(active_query) = query.as_mut() {
                                        active_query.move_pending_metadata_to(
                                            &mut pending.diagnostics,
                                            &mut pending.failures,
                                            &mut pending.overflows,
                                        );
                                    }
                                    query = None;
                                    rebuild_attempts = 0;
                                    next_rebuild =
                                        now + retry_delay(1, settings.wildcard_refresh_interval);
                                } else if result.is_ok() {
                                    if was_query_retry {
                                        pending.diagnostics.retry_recoveries += 1;
                                    }
                                    query_failure_attempts = 0;
                                    next_query_retry = now;
                                }
                                if let Ok(sample) = &mut result {
                                    pending.merge_into_sample(sample);
                                }
                                let _ = response.send(result);
                            }
                            Ok(Command::Shutdown) | Err(_) => break,
                        }
                    }
                }
                let _ = completion_tx.send(());
            })
            .map_err(|err| Error::WorkerStart(err.to_string()))?;

        match tokio::time::timeout(INIT_TIMEOUT, init_rx).await {
            Ok(Ok(Ok(()))) => Ok(Self {
                tx,
                accepted,
                shutdown_requested,
                join: Some(join),
                completion: Some(completion_rx),
            }),
            Ok(Ok(Err(error))) => {
                let _ = join.join();
                Err(error)
            }
            Err(_) => {
                shutdown_requested.store(true, Ordering::Release);
                drop(join);
                Err(Error::WorkerInitTimeout)
            }
            Ok(Err(_)) => {
                let _ = join.join();
                Err(Error::WorkerInitStopped)
            }
        }
    }

    pub(super) async fn collect(&self) -> Result<Sample, Error> {
        if self
            .accepted
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::WorkerBusy);
        }
        let (response_tx, response_rx) = oneshot::channel();
        match self.tx.try_send(Command::Collect(response_tx)) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(_)) => {
                self.accepted.store(false, Ordering::Release);
                return Err(Error::WorkerBusy);
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                self.accepted.store(false, Ordering::Release);
                return Err(Error::WorkerStopped);
            }
        }
        response_rx.await.map_err(|_| Error::WorkerStopped)?
    }

    /// Request shutdown and return `false` if the worker outlives the deadline.
    pub(super) async fn shutdown(&mut self, deadline: Instant) -> Result<bool, Error> {
        self.shutdown_requested.store(true, Ordering::Release);
        match self.tx.try_send(Command::Shutdown) {
            Ok(()) | Err(mpsc::TrySendError::Full(_)) => {}
            Err(mpsc::TrySendError::Disconnected(_)) => {}
        }
        let Some(join) = self.join.take() else {
            return Ok(true);
        };
        let Some(completion) = self.completion.as_mut() else {
            self.join = Some(join);
            return Err(Error::WorkerStopped);
        };
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), completion).await {
            Ok(_) => {
                self.completion = None;
                join.join().map_err(|_| Error::WorkerPanicked)?;
                Ok(true)
            }
            Err(_) => {
                self.join = Some(join);
                Ok(false)
            }
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.shutdown_requested.store(true, Ordering::Release);
        let _ = self.tx.try_send(Command::Shutdown);
    }
}

#[cfg(test)]
mod tests;
