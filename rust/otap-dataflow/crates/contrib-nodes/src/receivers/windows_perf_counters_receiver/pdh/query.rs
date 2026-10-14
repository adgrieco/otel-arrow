// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Query-owned counters, sampling histories, discovery reconciliation, and retry state.

use super::super::config::{Aggregation, CounterConfig, MetricKind};
use super::super::model::{
    ExpansionOverflow, InstanceIdentity, Number, Sample, SampleDiagnostics, SampleFailure,
    SamplePoint, SampleValue, scale_double, scale_integer,
};
use super::counter_type::{CounterKind, classify_native_type};
use super::error::Error;
use super::expansion::{
    CounterKey, cleanup_aggregation_instances, expansion_allocation, instance_groups,
    limit_expanded_paths, refresh_changes_with_group_resets,
};
use super::native::NativePdh;
use super::timing::{
    MonotonicSequence, advance_collection_time, delta_interval_start, monotonic_start_time,
    positive_timestamp,
};
use super::{PdhApi, ValueFormat};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};
use windows_sys::Win32::System::Performance::PDH_MAX_COUNTER_PATH;

pub(super) const QUERY_REBUILD_AFTER_FAILURES: u32 = 3;

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
pub(super) enum ExactAddFailureDisposition {
    Diagnostic,
    Retry,
}

pub(super) fn exact_add_failure_disposition(error: &Error) -> ExactAddFailureDisposition {
    if error.is_permanent_counter_error() {
        ExactAddFailureDisposition::Diagnostic
    } else {
        ExactAddFailureDisposition::Retry
    }
}

#[derive(Clone)]
pub(super) struct CounterTarget {
    pub(super) config_index: usize,
    pub(super) path: String,
    pub(super) localized: bool,
    pub(super) instance: Option<InstanceIdentity>,
    pub(super) emit_instance_attribute: bool,
}

#[derive(Clone)]
pub(super) struct RetryState {
    target: CounterTarget,
    pub(super) attempts: u32,
    pub(super) next_attempt: Instant,
}

pub(super) fn retry_delay(attempts: u32, cap: Duration) -> Duration {
    let exponent = attempts.saturating_sub(1).min(20);
    Duration::from_secs(1_u64 << exponent).min(cap)
}

pub(super) fn should_rebuild_query(consecutive_failures: u32) -> bool {
    consecutive_failures >= QUERY_REBUILD_AFTER_FAILURES
}

pub(super) fn schedule_retry_state(
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

pub(super) fn sample_failure(counter_index: usize, error: &Error) -> SampleFailure {
    SampleFailure {
        counter_index,
        reason: error.reason(),
        error: error.bounded_detail(),
        diagnostic_only: false,
    }
}

pub(super) fn validate_metric_mapping(
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

pub(super) fn merge_diagnostics(target: &mut SampleDiagnostics, delta: SampleDiagnostics) {
    target.discovery_refreshes += delta.discovery_refreshes;
    target.discovery_failures += delta.discovery_failures;
    target.instances_added += delta.instances_added;
    target.instances_removed += delta.instances_removed;
    target.instances_omitted_over_limit += delta.instances_omitted_over_limit;
    target.counter_add_failures += delta.counter_add_failures;
    target.counter_read_failures += delta.counter_read_failures;
    target.scrape_failures += delta.scrape_failures;
    target.retry_attempts += delta.retry_attempts;
    target.retry_recoveries += delta.retry_recoveries;
    target.query_rebuild_attempts += delta.query_rebuild_attempts;
    target.query_rebuild_recoveries += delta.query_rebuild_recoveries;
    target.query_recreation_attempts += delta.query_recreation_attempts;
    target.query_recreation_failures += delta.query_recreation_failures;
    target.warmup_omissions += delta.warmup_omissions;
}

pub(super) fn merge_sample_metadata(
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
pub(super) struct PendingMetadata {
    pub(super) diagnostics: SampleDiagnostics,
    pub(super) failures: Vec<SampleFailure>,
    pub(super) overflows: Vec<ExpansionOverflow>,
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

    pub(super) fn merge_into_sample(&mut self, sample: &mut Sample) {
        let mut pending = std::mem::take(self);
        merge_diagnostics(&mut sample.diagnostics, pending.diagnostics);
        pending.failures.append(&mut sample.failures);
        sample.failures = pending.failures;
        pending.overflows.append(&mut sample.overflows);
        sample.overflows = pending.overflows;
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct CounterHistory {
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

pub(super) fn restart_query_pdh_sampling<'a>(
    histories: impl Iterator<Item = &'a mut CounterHistory>,
) {
    for history in histories {
        history.restart_pdh_sampling();
    }
}

pub(super) struct CounterHandle<C> {
    config_index: usize,
    path: String,
    handle: C,
    kind: CounterKind,
    scale_power10: i32,
    history: CounterHistory,
    wildcard: bool,
    instance: Option<InstanceIdentity>,
    emit_instance_attribute: bool,
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
pub(super) struct Query<A: PdhApi = NativePdh> {
    api: A,
    pub(super) start_time_unix_nano: i64,
    pub(super) previous_timestamp_unix_nano: i64,
    configs: Vec<CounterConfig>,
    counters: BTreeMap<CounterKey, CounterHandle<A::Counter>>,
    retry_states: BTreeMap<CounterKey, RetryState>,
    pub(super) pending: PendingMetadata,
    wildcard_refresh_interval: Duration,
    max_instances_per_wildcard: usize,
    max_expanded_counters: usize,
    pub(super) last_wildcard_refresh: Instant,
}

impl Query {
    pub(super) fn open(
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
    pub(super) fn open_with(
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
                            let mut failure = sample_failure(config_index, &error);
                            failure.diagnostic_only = true;
                            query.pending.failures.push(failure);
                            None
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
                    emit_instance_attribute: query.configs[config_index].emit_instance_attribute
                        && instance.is_some(),
                };
                match query.add_counter(
                    target.config_index,
                    target.path.clone(),
                    target.localized,
                    instance,
                    target.emit_instance_attribute,
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
        emit_instance_attribute: bool,
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
                emit_instance_attribute,
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
            target.emit_instance_attribute,
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
                        counter.instance.clone().map(|instance| {
                            (
                                key.clone(),
                                (
                                    counter.path.clone(),
                                    instance,
                                    counter.emit_instance_attribute,
                                ),
                            )
                        })
                    })?
                })
                .chain(self.retry_states.iter().filter_map(|(key, state)| {
                    (key.config_index == *config_index && state.target.localized).then(|| {
                        state.target.instance.clone().map(|instance| {
                            (
                                key.clone(),
                                (
                                    state.target.path.clone(),
                                    instance,
                                    state.target.emit_instance_attribute,
                                ),
                            )
                        })
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
            for (key, (path, instance, emit_instance_attribute)) in retained {
                let _ = desired.insert(key, (path, instance, emit_instance_attribute));
            }
        }

        for (config_index, discovery) in discoveries {
            let Ok(mut paths) = discovery else {
                continue;
            };
            let aggregation_only = cleanup_aggregation_instances(
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
                    diagnostic_only: false,
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
                        let emit_instance_attribute =
                            self.configs[config_index].emit_instance_attribute && !aggregation_only;
                        let _ = desired.insert(key, (path, instance, emit_instance_attribute));
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
        let wanted_groups = instance_groups(
            desired
                .iter()
                .map(|(key, (_, instance, _))| (key, instance)),
        );
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
            let Some((path, instance, emit_instance_attribute)) = desired.remove(&key) else {
                diagnostics.discovery_failures += 1;
                continue;
            };
            let target = CounterTarget {
                config_index: key.config_index,
                path,
                localized: true,
                instance: Some(instance),
                emit_instance_attribute,
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

    pub(super) fn move_pending_metadata_to(
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

    pub(super) fn active_expanded_count(&self) -> usize {
        self.counters
            .values()
            .filter(|counter| counter.wildcard)
            .count()
    }

    pub(super) fn set_max_expanded_counters(&mut self, max_expanded_counters: usize) {
        self.max_expanded_counters = max_expanded_counters;
    }

    pub(super) fn prime(&mut self) -> Result<(), Error> {
        let sample = self.collect()?;
        self.pending
            .merge(sample.diagnostics, sample.failures, sample.overflows);
        Ok(())
    }

    pub(super) fn record_prime_failure(&mut self, error: &Error) {
        self.pending.diagnostics.scrape_failures += 1;
        self.pending
            .failures
            .extend((0..self.configs.len()).map(|counter_index| SampleFailure {
                counter_index,
                reason: error.reason(),
                error: error.bounded_detail(),
                diagnostic_only: true,
            }));
    }

    pub(super) fn collect(&mut self) -> Result<Sample, Error> {
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
                emit_instance_attribute: counter.emit_instance_attribute,
                timestamp_unix_nano,
                start_time_unix_nano: point_start_time,
                value,
            });
        }
        diagnostics.active_expanded_counters = self.active_expanded_count();
        Ok(Sample {
            timestamp_unix_nano,
            points,
            failures,
            overflows,
            diagnostics,
        })
    }
}
