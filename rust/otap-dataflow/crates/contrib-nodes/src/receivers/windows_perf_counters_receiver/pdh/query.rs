// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Query-owned counters, sampling histories, discovery reconciliation, and retry state.

use super::super::config::{Aggregation, CounterConfig, MetricKind};
use super::super::model::{
    ExpansionOverflow, InstanceIdentity, Number, Sample, SampleDiagnostics, SampleFailure,
    SamplePoint, SampleValue, merge_diagnostics, scale_double, scale_integer,
};
use super::counter_type::{CounterKind, classify_native_type, validate_metric_mapping};
use super::error::{Error, sample_failure};
use super::expansion::{
    CounterKey, ExpansionAllocation, cleanup_aggregation_instances, expansion_allocation,
    instance_groups, limit_expanded_paths, refresh_changes_with_group_resets,
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
        let mut metadata = PendingMetadata::default();
        query.refresh_wildcards(&mut metadata)?;
        query.pending.merge(metadata);
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

    fn refresh_wildcards(&mut self, metadata: &mut PendingMetadata) -> Result<(), Error> {
        metadata.diagnostics.discovery_refreshes += 1;
        let discoveries = self.discover_wildcards();
        let mut desired = BTreeMap::new();
        let expanded_total = self.reserve_failed_discoveries(&discoveries, &mut desired, metadata);
        self.prepare_successful_discoveries(discoveries, &mut desired, expanded_total, metadata);
        self.reconcile_wildcards(desired, metadata)?;
        self.last_wildcard_refresh = Instant::now();
        Ok(())
    }

    fn discover_wildcards(&mut self) -> Vec<WildcardDiscovery> {
        let wildcard_paths = self
            .configs
            .iter()
            .enumerate()
            .filter(|(_, config)| config.path.contains('*'))
            .map(|(config_index, config)| (config_index, config.path.clone()))
            .collect::<Vec<_>>();
        wildcard_paths
            .into_iter()
            .map(|(config_index, path)| WildcardDiscovery {
                config_index,
                paths: self.api.expand(&path),
            })
            .collect()
    }

    fn reserve_failed_discoveries(
        &self,
        discoveries: &[WildcardDiscovery],
        desired: &mut DesiredCounters,
        metadata: &mut PendingMetadata,
    ) -> usize {
        let mut expanded_total = 0;
        // Retain failed templates before admitting successful discoveries so
        // their live handles and retry identities reserve global capacity
        // independently of configuration order.
        for discovery in discoveries {
            let Err(error) = &discovery.paths else {
                continue;
            };
            metadata.diagnostics.discovery_failures += 1;
            metadata
                .failures
                .push(sample_failure(discovery.config_index, error));
            let mut retained = self
                .counters
                .iter()
                .filter_map(|(key, counter)| {
                    (key.config_index == discovery.config_index && counter.wildcard).then(|| {
                        counter.instance.clone().map(|instance| {
                            (
                                key.clone(),
                                DesiredCounter::new(
                                    counter.path.clone(),
                                    instance,
                                    counter.emit_instance_attribute,
                                ),
                            )
                        })
                    })?
                })
                .chain(self.retry_states.iter().filter_map(|(key, state)| {
                    (key.config_index == discovery.config_index && state.target.localized).then(
                        || {
                            state.target.instance.clone().map(|instance| {
                                (
                                    key.clone(),
                                    DesiredCounter::new(
                                        state.target.path.clone(),
                                        instance,
                                        state.target.emit_instance_attribute,
                                    ),
                                )
                            })
                        },
                    )?
                }))
                .collect::<BTreeMap<_, _>>();
            let discovered = retained.len();
            let allocation = expansion_allocation(
                discovered,
                self.max_instances_per_wildcard,
                self.max_expanded_counters.saturating_sub(expanded_total),
            );
            metadata.record_expansion_overflows(discovery.config_index, discovered, allocation);
            while retained.len() > allocation.retained {
                let Some(key) = retained.keys().next_back().cloned() else {
                    break;
                };
                let _ = retained.remove(&key);
            }
            expanded_total += retained.len();
            for (key, counter) in retained {
                let _ = desired.insert(key, counter);
            }
        }
        expanded_total
    }

    fn prepare_successful_discoveries(
        &mut self,
        discoveries: Vec<WildcardDiscovery>,
        desired: &mut DesiredCounters,
        mut expanded_total: usize,
        metadata: &mut PendingMetadata,
    ) {
        for discovery in discoveries {
            let Ok(mut paths) = discovery.paths else {
                continue;
            };
            let config_index = discovery.config_index;
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
                metadata.diagnostics.counter_add_failures +=
                    u64::try_from(oversized).unwrap_or(u64::MAX);
                metadata.failures.push(SampleFailure {
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
            let allocation = limit_expanded_paths(
                &mut paths,
                self.max_instances_per_wildcard,
                self.max_expanded_counters.saturating_sub(expanded_total),
            );
            metadata.record_expansion_overflows(config_index, discovered, allocation);
            expanded_total += paths.len();
            for path in paths {
                let key = CounterKey::new(config_index, &path);
                match self.api.parse_instance(&path) {
                    Ok(instance) => {
                        let emit_instance_attribute =
                            self.configs[config_index].emit_instance_attribute && !aggregation_only;
                        let _ = desired.insert(
                            key,
                            DesiredCounter::new(path, instance, emit_instance_attribute),
                        );
                    }
                    Err(error) => {
                        metadata.diagnostics.counter_add_failures += 1;
                        metadata.failures.push(sample_failure(config_index, &error));
                    }
                }
            }
        }
    }

    fn reconcile_wildcards(
        &mut self,
        mut desired: DesiredCounters,
        metadata: &mut PendingMetadata,
    ) -> Result<(), Error> {
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
        metadata.diagnostics.instances_removed +=
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
                .map(|(key, counter)| (key, &counter.instance)),
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
                metadata.diagnostics.discovery_failures += 1;
                continue;
            };
            self.api.remove(&counter.handle, &counter.path)?;
            if !wanted.contains(&key) {
                metadata.diagnostics.instances_removed += 1;
            }
        }
        let now = Instant::now();
        for key in added {
            let Some(DesiredCounter {
                path,
                instance,
                emit_instance_attribute,
            }) = desired.remove(&key)
            else {
                metadata.diagnostics.discovery_failures += 1;
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
            if self.try_add_target(
                key.clone(),
                target,
                now,
                &mut metadata.diagnostics,
                &mut metadata.failures,
            ) && !live.contains(&key)
                && !was_retry
            {
                metadata.diagnostics.instances_added += 1;
            }
        }
        Ok(())
    }

    fn restore_pending_metadata(&mut self, metadata: PendingMetadata) {
        self.pending.merge(metadata);
    }

    fn restore_collection_error(&mut self, metadata: PendingMetadata, error: Error) -> Error {
        self.restore_pending_metadata(metadata);
        error
    }

    fn restart_sampling(&mut self) {
        restart_query_pdh_sampling(
            self.counters
                .values_mut()
                .map(|counter| &mut counter.history),
        );
    }

    fn restart_sampling_and_restore_collection_error(
        &mut self,
        metadata: PendingMetadata,
        error: Error,
    ) -> Error {
        self.restart_sampling();
        self.restore_collection_error(metadata, error)
    }

    pub(super) fn move_pending_metadata_to(&mut self, target: &mut PendingMetadata) {
        target.merge(std::mem::take(&mut self.pending));
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
        let mut sample = self.collect()?;
        self.pending
            .merge(PendingMetadata::take_from_sample(&mut sample));
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
        let mut metadata = std::mem::take(&mut self.pending);
        if self.last_wildcard_refresh.elapsed() >= self.wildcard_refresh_interval
            && let Err(error) = self.refresh_wildcards(&mut metadata)
        {
            return Err(self.restore_collection_error(metadata, error));
        }
        self.retry_counters(
            Instant::now(),
            &mut metadata.diagnostics,
            &mut metadata.failures,
        );
        if self.counters.is_empty() {
            let timestamp_unix_nano = match self.api.timestamp().and_then(positive_timestamp) {
                Ok(timestamp) => timestamp,
                Err(error) => {
                    return Err(self.restore_collection_error(metadata, error));
                }
            };
            let _ = advance_collection_time(
                &mut self.start_time_unix_nano,
                &mut self.previous_timestamp_unix_nano,
                timestamp_unix_nano,
                std::iter::empty(),
            );
            let mut sample = Sample::empty(timestamp_unix_nano);
            metadata.restore_to_sample(&mut sample);
            return Ok(sample);
        }
        let has_data = match self.api.collect() {
            Ok(has_data) => has_data,
            Err(error) => {
                return Err(self.restart_sampling_and_restore_collection_error(metadata, error));
            }
        };
        if !has_data {
            self.restart_sampling();
        }
        let timestamp_unix_nano = match self.api.timestamp().and_then(positive_timestamp) {
            Ok(timestamp) => timestamp,
            Err(error) => {
                return Err(self.restart_sampling_and_restore_collection_error(metadata, error));
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
                counter.read(
                    &mut self.api,
                    metric_kind,
                    start_time_unix_nano,
                    timestamp_unix_nano,
                )
            } else {
                Ok((SampleValue::NoObservation, None))
            };
            let (value, point_start_time) = match result {
                Ok(result) => result,
                Err(error) => {
                    metadata
                        .failures
                        .push(sample_failure(counter.config_index, &error));
                    metadata.diagnostics.counter_read_failures += 1;
                    continue;
                }
            };
            if matches!(value, SampleValue::Warming) {
                metadata.diagnostics.warmup_omissions += 1;
            }
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
        metadata.diagnostics.active_expanded_counters = self.active_expanded_count();
        Ok(Sample {
            timestamp_unix_nano,
            points,
            failures: metadata.failures,
            overflows: metadata.overflows,
            diagnostics: metadata.diagnostics,
        })
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

struct DesiredCounter {
    path: String,
    instance: InstanceIdentity,
    emit_instance_attribute: bool,
}

impl DesiredCounter {
    fn new(path: String, instance: InstanceIdentity, emit_instance_attribute: bool) -> Self {
        Self {
            path,
            instance,
            emit_instance_attribute,
        }
    }
}

type DesiredCounters = BTreeMap<CounterKey, DesiredCounter>;

struct WildcardDiscovery {
    config_index: usize,
    paths: Result<Vec<String>, Error>,
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
struct CounterTarget {
    config_index: usize,
    path: String,
    localized: bool,
    instance: Option<InstanceIdentity>,
    emit_instance_attribute: bool,
}

#[derive(Clone)]
struct RetryState {
    target: CounterTarget,
    attempts: u32,
    next_attempt: Instant,
}

pub(super) fn retry_delay(attempts: u32, cap: Duration) -> Duration {
    let exponent = attempts.saturating_sub(1).min(20);
    Duration::from_secs(1_u64 << exponent).min(cap)
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

#[derive(Default)]
pub(super) struct PendingMetadata {
    pub(super) diagnostics: SampleDiagnostics,
    pub(super) failures: Vec<SampleFailure>,
    pub(super) overflows: Vec<ExpansionOverflow>,
}

impl PendingMetadata {
    fn record_expansion_overflows(
        &mut self,
        counter_index: usize,
        discovered: usize,
        allocation: ExpansionAllocation,
    ) {
        if allocation.per_wildcard_omitted > 0 {
            self.overflows.push(ExpansionOverflow {
                counter_index,
                reason: "per_wildcard",
                discovered,
                retained: discovered - allocation.per_wildcard_omitted,
                omitted: allocation.per_wildcard_omitted,
            });
        }
        if allocation.total_omitted > 0 {
            self.overflows.push(ExpansionOverflow {
                counter_index,
                reason: "receiver_total",
                discovered: discovered - allocation.per_wildcard_omitted,
                retained: allocation.retained,
                omitted: allocation.total_omitted,
            });
        }
        self.diagnostics.instances_omitted_over_limit += u64::try_from(
            allocation
                .per_wildcard_omitted
                .saturating_add(allocation.total_omitted),
        )
        .unwrap_or(u64::MAX);
    }

    pub(super) fn take_from_sample(sample: &mut Sample) -> Self {
        Self {
            diagnostics: std::mem::take(&mut sample.diagnostics),
            failures: std::mem::take(&mut sample.failures),
            overflows: std::mem::take(&mut sample.overflows),
        }
    }

    pub(super) fn merge(&mut self, delta: Self) {
        merge_diagnostics(&mut self.diagnostics, delta.diagnostics);
        for failure in delta.failures {
            if let Some(existing) = self.failures.iter_mut().find(|existing| {
                existing.counter_index == failure.counter_index && existing.reason == failure.reason
            }) {
                *existing = failure;
            } else {
                self.failures.push(failure);
            }
        }
        for overflow in delta.overflows {
            if let Some(existing) = self.overflows.iter_mut().find(|existing| {
                existing.counter_index == overflow.counter_index
                    && existing.reason == overflow.reason
            }) {
                *existing = overflow;
            } else {
                self.overflows.push(overflow);
            }
        }
    }

    pub(super) fn restore_to_sample(self, sample: &mut Sample) {
        sample.diagnostics = self.diagnostics;
        sample.failures = self.failures;
        sample.overflows = self.overflows;
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
        query_start_time_unix_nano: i64,
        timestamp_unix_nano: i64,
    ) -> Result<(SampleValue, Option<i64>), Error> {
        if let Some(value) = self.prepare_raw_sample(api, timestamp_unix_nano)? {
            return Ok((value, None));
        }

        let Some(number) = self.read_formatted_number(api)? else {
            return Ok((SampleValue::NoObservation, None));
        };
        let delta_start_time = self.delta_start_time(metric_kind, number, timestamp_unix_nano)?;
        if matches!(
            metric_kind,
            MetricKind::Sum {
                aggregation: Aggregation::Delta,
                ..
            }
        ) && delta_start_time.is_none()
        {
            return Ok((SampleValue::NoObservation, None));
        }
        let scaled = self.scale_number(number)?;
        let point_start_time = self.point_start_time(
            metric_kind,
            number,
            delta_start_time,
            query_start_time_unix_nano,
            timestamp_unix_nano,
        )?;
        Ok((SampleValue::Value(scaled), point_start_time))
    }

    fn prepare_raw_sample(
        &mut self,
        api: &mut impl PdhApi<Counter = C>,
        timestamp_unix_nano: i64,
    ) -> Result<Option<SampleValue>, Error> {
        if matches!(
            self.kind,
            CounterKind::Direct | CounterKind::FormattedSingleSample
        ) {
            return Ok(None);
        }
        let raw = match api.raw(&self.handle, &self.path) {
            Ok(Some(raw)) => raw,
            Ok(None) => {
                self.reset_readiness();
                return Ok(Some(SampleValue::NoObservation));
            }
            Err(error) => {
                self.reset_readiness();
                return Err(error);
            }
        };
        if self.kind == CounterKind::RawFraction {
            validate_positive_base(&self.path, raw.second_value)?;
            return Ok(None);
        }

        let previous = self.history.previous_base.replace(raw.second_value);
        if self.kind == CounterKind::CalculatedTwoSampleWithMultiBase {
            validate_positive_multi_base(&self.path, raw.multi_count)?;
        }
        let Some(previous) = previous else {
            if self.kind == CounterKind::Delta {
                self.history.delta_baseline_unix_nano = Some(timestamp_unix_nano);
            }
            return Ok(Some(SampleValue::Warming));
        };
        if self.kind == CounterKind::CalculatedTwoSampleWithBase {
            match raw.second_value.cmp(&previous) {
                std::cmp::Ordering::Less => {
                    self.reset_readiness();
                    return Ok(Some(SampleValue::NoObservation));
                }
                std::cmp::Ordering::Equal => {
                    return Ok(Some(SampleValue::NoObservation));
                }
                std::cmp::Ordering::Greater => {}
            }
        }
        Ok(None)
    }

    fn read_formatted_number(
        &mut self,
        api: &mut impl PdhApi<Counter = C>,
    ) -> Result<Option<Number>, Error> {
        let format = if self.kind == CounterKind::Direct {
            ValueFormat::Integer
        } else {
            ValueFormat::Double
        };
        match api.formatted(&self.handle, &self.path, format) {
            Ok(Some(number)) => Ok(Some(number)),
            Ok(None) => {
                self.reset_readiness();
                Ok(None)
            }
            Err(error) => {
                if self.kind == CounterKind::Delta || matches!(error, Error::CounterData { .. }) {
                    self.reset_readiness();
                }
                Err(error)
            }
        }
    }

    fn delta_start_time(
        &mut self,
        metric_kind: MetricKind,
        number: Number,
        timestamp_unix_nano: i64,
    ) -> Result<Option<i64>, Error> {
        if let MetricKind::Sum {
            aggregation: Aggregation::Delta,
            monotonic,
        } = metric_kind
        {
            delta_interval_start(
                &self.path,
                &mut self.history.delta_baseline_unix_nano,
                number,
                monotonic,
                timestamp_unix_nano,
            )
        } else {
            Ok(None)
        }
    }

    fn scale_number(&self, number: Number) -> Result<Number, Error> {
        match number {
            Number::Integer(value) => scale_integer(value, self.scale_power10),
            Number::Double(value) => scale_double(value, self.scale_power10),
        }
        .map_err(|source| Error::Scale {
            path: self.path.clone(),
            source,
        })
    }

    fn point_start_time(
        &mut self,
        metric_kind: MetricKind,
        number: Number,
        delta_start_time: Option<i64>,
        query_start_time_unix_nano: i64,
        timestamp_unix_nano: i64,
    ) -> Result<Option<i64>, Error> {
        Ok(match metric_kind {
            MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: true,
            } => {
                let Number::Integer(direct_value) = number else {
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
            MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: false,
            } => Some(query_start_time_unix_nano),
            MetricKind::Gauge => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: The same failed counter is scheduled repeatedly before it recovers.
    /// Guarantees: One stable identity owns one retry entry and repeated failures advance its bounded backoff.
    #[test]
    fn retry_state_is_unique_per_stable_identity() {
        let key = CounterKey::new(0, r"\Process(worker)\Private Bytes");
        let target = CounterTarget {
            config_index: 0,
            path: r"\Process(worker)\Private Bytes".to_owned(),
            localized: true,
            instance: Some(InstanceIdentity {
                name: "worker".to_owned(),
                parent: None,
                index: 0,
            }),
            emit_instance_attribute: true,
        };
        let now = Instant::now();
        let mut states = BTreeMap::new();

        schedule_retry_state(
            &mut states,
            key.clone(),
            target.clone(),
            now,
            Duration::from_secs(30),
        );
        schedule_retry_state(
            &mut states,
            key.clone(),
            target,
            now,
            Duration::from_secs(30),
        );

        assert_eq!(states.len(), 1);
        let state = states.get(&key).unwrap();
        assert_eq!(state.attempts, 2);
        assert_eq!(state.next_attempt, now + Duration::from_secs(2));
    }
}
