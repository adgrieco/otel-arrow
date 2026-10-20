// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Query-owned counters, sampling histories, discovery reconciliation, and retry state.

use super::super::config::{Aggregation, CounterConfig, MetricKind};
use super::super::model::{
    ExpansionOverflow, InstanceIdentity, MonotonicPublicationCandidate, Number, PendingMetadata,
    Sample, SampleFailure, SampleMember, SamplePoint, SampleValue, scale_double, scale_integer,
};
use super::counter_type::{CounterKind, classify_native_type, validate_metric_mapping};
use super::error::{Error, sample_failure};
use super::expansion::{
    CounterKey, ExpansionAllocation, cleanup_aggregation_instances, expansion_allocation,
    instance_groups, keyed_expanded_paths, limit_expanded_paths, normalize_expanded_path,
    refresh_changes_with_group_resets,
};
use super::native::NativePdh;
use super::timing::{
    MonotonicSequence, advance_collection_time, authorize_monotonic_start, delta_interval_start,
    pending_monotonic_start_time, positive_timestamp, prime_monotonic_sequence,
    publish_monotonic_start,
};
use super::{PdhApi, ValueFormat};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use windows_sys::Win32::System::Performance::PDH_MAX_COUNTER_PATH;

/// The query owns every added counter; closing it releases all handles.
pub(super) struct Query<A: PdhApi = NativePdh> {
    api: A,
    pub(super) start_time_unix_nano: i64,
    pub(super) previous_timestamp_unix_nano: i64,
    configs: Vec<CounterConfig>,
    counters: BTreeMap<CounterKey, CounterHandle<A::Counter>>,
    pending_removals: BTreeMap<CounterKey, PendingRemoval<A::Counter>>,
    retry_states: BTreeMap<CounterKey, RetryState>,
    pub(super) pending: PendingMetadata,
    exact_retry_cap: Duration,
    wildcard_refresh_interval: Duration,
    max_instances_per_wildcard: usize,
    max_native_counters: usize,
    exact_counter_reservations: usize,
    next_wildcard_admission_order: u64,
    pub(super) last_wildcard_refresh: Instant,
    wildcard_refresh_pending: bool,
}

impl Query {
    #[cfg(test)]
    pub(super) fn open(
        configs: Vec<CounterConfig>,
        exact_retry_cap: Duration,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_native_counters: usize,
        node: String,
    ) -> Result<Self, Error> {
        Self::open_with_cadences(
            NativePdh::open(node)?,
            configs,
            exact_retry_cap,
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_native_counters,
        )
    }

    pub(super) fn open_replacement(
        configs: Vec<CounterConfig>,
        exact_retry_cap: Duration,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_native_counters: usize,
        node: String,
        seed: Option<&QueryRecreationSeed>,
    ) -> Result<Self, Error> {
        Self::open_with_cadences_and_seed(
            NativePdh::open(node)?,
            configs,
            exact_retry_cap,
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_native_counters,
            seed,
            true,
        )
    }
}

impl<A: PdhApi> Query<A> {
    #[cfg(test)]
    pub(super) fn open_with(
        api: A,
        configs: Vec<CounterConfig>,
        exact_retry_cap: Duration,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_native_counters: usize,
    ) -> Result<Self, Error> {
        Self::open_with_cadences(
            api,
            configs,
            exact_retry_cap,
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_native_counters,
        )
    }

    #[cfg(test)]
    pub(super) fn open_with_seed(
        api: A,
        configs: Vec<CounterConfig>,
        exact_retry_cap: Duration,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_native_counters: usize,
        seed: Option<&QueryRecreationSeed>,
    ) -> Result<Self, Error> {
        Self::open_with_cadences_and_seed(
            api,
            configs,
            exact_retry_cap,
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_native_counters,
            seed,
            true,
        )
    }

    #[cfg(test)]
    pub(super) fn open_managed_with_seed(
        api: A,
        configs: Vec<CounterConfig>,
        exact_retry_cap: Duration,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_native_counters: usize,
        seed: Option<&QueryRecreationSeed>,
    ) -> Result<Self, Error> {
        Self::open_with_cadences_and_seed(
            api,
            configs,
            exact_retry_cap,
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_native_counters,
            seed,
            false,
        )
    }

    #[cfg(test)]
    pub(super) fn open_with_cadences(
        api: A,
        configs: Vec<CounterConfig>,
        exact_retry_cap: Duration,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_native_counters: usize,
    ) -> Result<Self, Error> {
        Self::open_with_cadences_and_seed(
            api,
            configs,
            exact_retry_cap,
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_native_counters,
            None,
            true,
        )
    }

    fn open_with_cadences_and_seed(
        mut api: A,
        configs: Vec<CounterConfig>,
        exact_retry_cap: Duration,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_native_counters: usize,
        seed: Option<&QueryRecreationSeed>,
        refresh_immediately: bool,
    ) -> Result<Self, Error> {
        let exact_counter_reservations = configs
            .iter()
            .filter(|config| !config.is_expanded())
            .count();
        if exact_counter_reservations > max_native_counters {
            return Err(Error::InvalidSample(
                "query native-counter limit is below its exact-counter reservation",
            ));
        }
        let (start_time_unix_nano, previous_timestamp_unix_nano) = if let Some(seed) = seed {
            (seed.start_time_unix_nano, seed.previous_timestamp_unix_nano)
        } else {
            let timestamp = positive_timestamp(api.timestamp()?)?;
            (timestamp, timestamp)
        };
        let mut query = Self {
            api,
            start_time_unix_nano,
            previous_timestamp_unix_nano,
            configs,
            counters: BTreeMap::new(),
            pending_removals: BTreeMap::new(),
            retry_states: BTreeMap::new(),
            pending: PendingMetadata::default(),
            exact_retry_cap,
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_native_counters,
            exact_counter_reservations,
            next_wildcard_admission_order: 0,
            last_wildcard_refresh: seed
                .map_or_else(Instant::now, |seed| seed.last_wildcard_refresh),
            wildcard_refresh_pending: false,
        };
        query.initialize_exact_counters(seed)?;
        let mut metadata = PendingMetadata::default();
        if let Some(seed) = seed {
            query.initialize_seeded_wildcards(seed, &mut metadata)?;
        }
        query.wildcard_refresh_pending = query.has_wildcard_configs()
            && seed.is_none_or(|seed| {
                seed.last_wildcard_refresh.elapsed() >= wildcard_refresh_interval
            });
        if refresh_immediately && query.wildcard_refresh_pending {
            query.refresh_wildcards(&mut metadata)?;
            query.wildcard_refresh_pending = false;
        }
        query.pending.merge(metadata);
        Ok(query)
    }

    fn has_wildcard_configs(&self) -> bool {
        self.configs.iter().any(CounterConfig::is_expanded)
    }

    fn initialize_seeded_wildcards(
        &mut self,
        seed: &QueryRecreationSeed,
        metadata: &mut PendingMetadata,
    ) -> Result<(), Error> {
        let now = Instant::now();
        for (key, target) in &seed.wildcard_targets {
            if let Some(admission_order) = target.admission_order {
                self.next_wildcard_admission_order = self
                    .next_wildcard_admission_order
                    .max(admission_order.saturating_add(1));
            }
            let _ = self.try_add_target(key.clone(), target.clone(), now, metadata)?;
        }
        Ok(())
    }

    pub(super) fn recreation_seed(&self) -> QueryRecreationSeed {
        let wildcard_targets = self
            .counters
            .iter()
            .filter_map(|(key, counter)| {
                counter
                    .origin
                    .is_wildcard()
                    .then_some((key.clone(), counter.target().for_recreated_query()))
            })
            .chain(self.retry_states.iter().filter_map(|(key, state)| {
                state
                    .target
                    .origin
                    .is_wildcard()
                    .then_some((key.clone(), state.target.clone().for_recreated_query()))
            }))
            .chain(self.pending_removals.iter().filter_map(|(key, removal)| {
                removal.retry_target.as_ref().and_then(|target| {
                    target
                        .origin
                        .is_wildcard()
                        .then_some((key.clone(), target.clone().for_recreated_query()))
                })
            }))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .collect();
        let exact_histories = self
            .counters
            .iter()
            .filter(|(_, counter)| !counter.origin.is_wildcard())
            .map(|(key, counter)| (key.clone(), counter.history.for_recreated_query()))
            .chain(self.retry_states.iter().filter_map(|(key, state)| {
                (!state.target.origin.is_wildcard())
                    .then_some((key.clone(), state.target.history.for_recreated_query()))
            }))
            .chain(self.pending_removals.iter().filter_map(|(key, removal)| {
                removal.retry_target.as_ref().and_then(|target| {
                    (!target.origin.is_wildcard())
                        .then_some((key.clone(), target.history.for_recreated_query()))
                })
            }))
            .collect();
        QueryRecreationSeed {
            start_time_unix_nano: self.start_time_unix_nano,
            previous_timestamp_unix_nano: self.previous_timestamp_unix_nano,
            last_wildcard_refresh: self.last_wildcard_refresh,
            exact_histories,
            wildcard_targets,
        }
    }

    pub(super) fn admitted_members(&self) -> Vec<SampleMember> {
        let mut members = self
            .counters
            .iter()
            .map(|(key, counter)| (key.clone(), counter.member()))
            .chain(
                self.retry_states
                    .iter()
                    .map(|(key, state)| (key.clone(), state.target.member())),
            )
            .chain(self.pending_removals.iter().filter_map(|(key, removal)| {
                removal
                    .retry_target
                    .as_ref()
                    .map(|target| (key.clone(), target.member()))
            }))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();
        members.sort_by(|left, right| {
            left.counter_index
                .cmp(&right.counter_index)
                .then_with(|| left.path.cmp(&right.path))
        });
        members
    }

    fn initialize_exact_counters(
        &mut self,
        seed: Option<&QueryRecreationSeed>,
    ) -> Result<(), Error> {
        for config_index in 0..self.configs.len() {
            if self.configs[config_index].is_expanded() {
                continue;
            }
            let path = Arc::<str>::from(self.configs[config_index].path.as_str());
            let key = CounterKey::new(self.configs[config_index].source_mapping_index, &path);
            let instance = if self.configs[config_index].instance_config.has_instance() {
                match self.api.parse_instance(&path) {
                    Ok(instance) => Some(Arc::new(instance)),
                    Err(error) => {
                        let mut failure = sample_failure(config_index, &error);
                        failure.diagnostic_only = true;
                        self.pending.failures.push(failure);
                        None
                    }
                }
            } else {
                None
            };
            let target = CounterTarget {
                config_index,
                path,
                origin: CounterOrigin::Exact,
                instance: instance.clone(),
                counter_name: None,
                emit_instance_attribute: self.configs[config_index]
                    .instance_config
                    .emit_attribute()
                    && instance.is_some(),
                admission_order: None,
                removal_counted: false,
                history: seed
                    .and_then(|seed| seed.exact_histories.get(&key).copied())
                    .unwrap_or_else(|| CounterHistory::new(self.start_time_unix_nano)),
            };
            if let Err(error) = self.add_counter(target.clone()) {
                if error.requires_immediate_query_rebuild() {
                    return Err(error);
                }
                self.pending.diagnostics.counter_add_failures += 1;
                self.pending
                    .failures
                    .push(sample_failure(config_index, &error));
                if exact_add_failure_disposition(&error) == ExactAddFailureDisposition::Retry {
                    self.schedule_retry(key, target, Instant::now());
                }
            }
        }
        Ok(())
    }

    fn add_counter(&mut self, target: CounterTarget) -> Result<(), Error> {
        let counter = self.api.add(&target.path)?;
        let kind = match self
            .api
            .counter_type(&counter, &target.path)
            .and_then(|native_type| {
                classify_native_type(&target.path, native_type).map_err(Error::from)
            }) {
            Ok(kind) => kind,
            Err(error) => {
                return Err(setup_error_with_cleanup(
                    &mut self.api,
                    &counter,
                    &target.path,
                    error,
                ));
            }
        };
        if let Err(error) = validate_metric_mapping(
            &target.path,
            self.configs[target.config_index].metric_kind,
            kind,
        ) {
            return Err(setup_error_with_cleanup(
                &mut self.api,
                &counter,
                &target.path,
                error,
            ));
        }
        let key = CounterKey::new(
            self.configs[target.config_index].source_mapping_index,
            &target.path,
        );
        let _ = self.counters.insert(
            key,
            CounterHandle {
                config_index: target.config_index,
                path: target.path,
                handle: counter,
                kind,
                scale_power10: self.configs[target.config_index].scale_power10,
                history: target.history,
                origin: target.origin,
                instance: target.instance,
                counter_name: target.counter_name,
                emit_instance_attribute: target.emit_instance_attribute,
                admission_order: target.admission_order,
                removal_counted: target.removal_counted,
            },
        );
        Ok(())
    }

    fn schedule_retry(&mut self, key: CounterKey, target: CounterTarget, now: Instant) {
        let cap = target
            .origin
            .retry_cap(self.exact_retry_cap, self.wildcard_refresh_interval);
        schedule_retry_state(&mut self.retry_states, key, target, now, cap);
    }

    fn try_add_target(
        &mut self,
        key: CounterKey,
        target: CounterTarget,
        now: Instant,
        metadata: &mut PendingMetadata,
    ) -> Result<bool, Error> {
        match self.add_counter(target.clone()) {
            Ok(()) => {
                if self.retry_states.remove(&key).is_some() {
                    metadata.diagnostics.retry_recoveries += 1;
                }
                Ok(true)
            }
            Err(error) => {
                metadata.diagnostics.counter_add_failures += 1;
                metadata
                    .failures
                    .push(sample_failure(target.config_index, &error));
                if error.requires_immediate_query_rebuild() {
                    return Err(error);
                }
                if error.is_permanent_counter_error() {
                    let _ = self.retry_states.remove(&key);
                } else {
                    self.schedule_retry(key, target, now);
                }
                Ok(false)
            }
        }
    }

    fn retry_counters(
        &mut self,
        now: Instant,
        metadata: &mut PendingMetadata,
    ) -> Result<(), Error> {
        let due = self
            .retry_states
            .iter()
            .filter(|(_, state)| state.next_attempt <= now)
            .map(|(key, state)| (key.clone(), state.target.clone()))
            .collect::<Vec<_>>();
        for (key, target) in due {
            metadata.diagnostics.retry_attempts += 1;
            let _ = self.try_add_target(key, target, now, metadata)?;
        }
        Ok(())
    }

    fn refresh_wildcards(&mut self, metadata: &mut PendingMetadata) -> Result<(), Error> {
        metadata.diagnostics.discovery_refreshes += 1;
        let wildcard_configs = self
            .configs
            .iter()
            .enumerate()
            .filter_map(|(config_index, config)| config.is_expanded().then_some(config_index))
            .collect::<Vec<_>>();
        for config_index in wildcard_configs {
            self.refresh_wildcard_config(config_index, metadata)?;
        }
        self.last_wildcard_refresh = Instant::now();
        self.wildcard_refresh_pending = false;
        Ok(())
    }

    pub(super) fn wildcard_refresh_due(&self) -> bool {
        self.wildcard_refresh_pending
            || (self.has_wildcard_configs()
                && self.last_wildcard_refresh.elapsed() >= self.wildcard_refresh_interval)
    }

    #[cfg(test)]
    pub(super) fn refresh_all_wildcards_managed(&mut self) -> Result<(), Error> {
        if !self.wildcard_refresh_due() {
            return Ok(());
        }
        let mut metadata = PendingMetadata::default();
        let result = self.refresh_wildcards(&mut metadata);
        self.pending.merge(metadata);
        result
    }

    #[cfg(test)]
    pub(super) fn refresh_wildcard_config_managed(
        &mut self,
        config_index: usize,
    ) -> Result<(), Error> {
        if !self
            .configs
            .get(config_index)
            .is_some_and(CounterConfig::is_expanded)
        {
            return Err(Error::InvalidSample(
                "managed wildcard refresh selected a non-wildcard counter",
            ));
        }
        let mut metadata = PendingMetadata::default();
        let result = self.refresh_wildcard_config(config_index, &mut metadata);
        self.pending.merge(metadata);
        result
    }

    fn expanded_identity(
        &mut self,
        config_index: usize,
        path: &str,
    ) -> Result<(Option<Arc<InstanceIdentity>>, Option<Arc<str>>), Error> {
        let instance = if self.configs[config_index].instance_config.has_instance() {
            Some(Arc::new(self.api.parse_instance(path)?))
        } else {
            None
        };
        let counter_name = if self.configs[config_index].counter_name_wildcard {
            Some(Arc::from(self.api.parse_counter_name(path)?))
        } else {
            None
        };
        Ok((instance, counter_name))
    }

    pub(super) fn plan_wildcard_refresh_managed(
        &mut self,
        config_index: usize,
    ) -> WildcardRefreshPlan {
        let mut metadata = PendingMetadata::default();
        let paths: Vec<WildcardCandidate> = match self.api.expand(&self.configs[config_index].path)
        {
            Ok(paths) => {
                let mut paths = paths
                    .into_iter()
                    .map(normalize_expanded_path)
                    .collect::<Vec<_>>();
                let aggregation_only = cleanup_aggregation_instances(
                    &mut paths,
                    self.configs[config_index]
                        .instance_config
                        .excluded_aggregation(),
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
                        path: Arc::from(self.configs[config_index].path.as_str()),
                        reason: "expanded_path_too_long",
                        error: format!(
                            "{oversized} expanded paths reached the PDH {PDH_MAX_COUNTER_PATH}-unit limit"
                        ),
                        diagnostic_only: false,
                    });
                    paths
                        .retain(|path| path.encode_utf16().count() < PDH_MAX_COUNTER_PATH as usize);
                }
                keyed_expanded_paths(self.configs[config_index].source_mapping_index, paths)
                    .into_iter()
                    .map(|(key, path)| {
                        let satisfied_by_exact = self
                            .counters
                            .get(&key)
                            .is_some_and(|counter| !counter.origin.is_wildcard())
                            || self
                                .retry_states
                                .get(&key)
                                .is_some_and(|state| !state.target.origin.is_wildcard());
                        let desired = self.expanded_identity(config_index, &path).map(
                            |(instance, counter_name)| {
                                let emit_instance_attribute =
                                    self.configs[config_index].instance_config.emit_attribute()
                                        && instance.is_some()
                                        && !aggregation_only;
                                DesiredCounter::new(
                                    config_index,
                                    path.clone(),
                                    instance,
                                    counter_name,
                                    emit_instance_attribute,
                                )
                            },
                        );
                        WildcardCandidate {
                            key,
                            desired,
                            satisfied_by_exact,
                        }
                    })
                    .collect()
            }
            Err(error) => {
                metadata.diagnostics.discovery_failures += 1;
                let mut failure = sample_failure(config_index, &error);
                failure.diagnostic_only = true;
                metadata.failures.push(failure);
                self.current_wildcard_desired(config_index)
                    .into_iter()
                    .map(|(key, desired)| WildcardCandidate {
                        key,
                        desired: Ok(desired),
                        satisfied_by_exact: false,
                    })
                    .collect()
            }
        };
        WildcardRefreshPlan {
            config_index,
            discovered: paths.len(),
            paths,
            metadata,
        }
    }

    pub(super) fn prepare_managed_wildcard_refresh(
        &mut self,
        selections: Vec<WildcardRefreshSelection>,
    ) -> WildcardRefreshAdditions {
        let planned_configs = selections
            .iter()
            .map(|selection| selection.plan.config_index)
            .collect::<BTreeSet<_>>();
        let mut desired = self.current_wildcards_excluding(&planned_configs);
        let mut metadata = PendingMetadata::default();
        for selection in selections {
            metadata.merge(selection.plan.metadata);
            metadata.record_expansion_overflows(
                selection.plan.config_index,
                selection.plan.discovered,
                selection.allocation,
            );
            let selected_keys = selection
                .plan
                .paths
                .iter()
                .filter(|candidate| selection.selected.contains(&candidate.key))
                .map(|candidate| candidate.key.clone())
                .collect::<BTreeSet<_>>();
            self.restore_rediscovered_pending_removals(&selected_keys);
            for candidate in selection.plan.paths {
                if !selection.selected.contains(&candidate.key) {
                    continue;
                }
                match candidate.desired {
                    Ok(counter) => {
                        let _ = desired.insert(candidate.key, counter);
                    }
                    Err(error) => {
                        metadata.diagnostics.counter_add_failures += 1;
                        metadata
                            .failures
                            .push(sample_failure(selection.plan.config_index, &error));
                    }
                }
            }
        }
        let changes = self.plan_wildcard_reconciliation(&desired);
        self.discard_unwanted_wildcard_retries(&changes.wanted, &mut metadata);
        self.remove_wildcard_counters(&changes.removed, &changes.wanted, &mut metadata);
        self.pending.merge(metadata);
        WildcardRefreshAdditions {
            desired,
            added: changes.added,
            live: changes.live,
        }
    }

    pub(super) fn complete_managed_wildcard_addition(
        &mut self,
        key: &CounterKey,
        additions: &mut WildcardRefreshAdditions,
    ) -> Result<(), Error> {
        let mut metadata = PendingMetadata::default();
        let result = self.add_wildcard_counters(
            std::slice::from_ref(key),
            &additions.live,
            &mut additions.desired,
            &mut metadata,
        );
        self.pending.merge(metadata);
        result
    }

    pub(super) fn admitted_wildcard_keys(&self) -> BTreeSet<CounterKey> {
        self.counters
            .iter()
            .filter_map(|(key, counter)| counter.origin.is_wildcard().then_some(key.clone()))
            .chain(self.retry_states.iter().filter_map(|(key, state)| {
                state.target.origin.is_wildcard().then_some(key.clone())
            }))
            .chain(self.pending_removals.iter().filter_map(|(key, removal)| {
                removal
                    .retry_target
                    .as_ref()
                    .is_some_and(|target| target.origin.is_wildcard())
                    .then_some(key.clone())
            }))
            .collect()
    }

    pub(super) fn blocked_wildcard_removal_keys(&self) -> BTreeSet<CounterKey> {
        self.pending_removals
            .iter()
            .filter_map(|(key, removal)| {
                (removal.counter.origin.is_wildcard() && removal.retry_target.is_none())
                    .then_some(key.clone())
            })
            .collect()
    }

    pub(super) fn exact_counter_reservations(&self) -> usize {
        self.exact_counter_reservations
    }

    pub(super) fn max_instances_per_wildcard(&self) -> usize {
        self.max_instances_per_wildcard
    }

    pub(super) fn finish_managed_wildcard_refresh(&mut self) {
        self.pending.diagnostics.discovery_refreshes += 1;
        self.last_wildcard_refresh = Instant::now();
        self.wildcard_refresh_pending = false;
    }

    fn refresh_wildcard_config(
        &mut self,
        config_index: usize,
        metadata: &mut PendingMetadata,
    ) -> Result<(), Error> {
        let mut desired = self.desired_wildcards_excluding(config_index);
        let expanded_total = self.reserved_wildcards_excluding(config_index);
        let paths = match self.api.expand(&self.configs[config_index].path) {
            Ok(paths) => paths,
            Err(error) => {
                self.reserve_failed_discovery(
                    config_index,
                    &mut desired,
                    expanded_total,
                    metadata,
                    &error,
                );
                return self.reconcile_wildcards(desired, metadata);
            }
        };
        self.prepare_successful_discoveries(
            vec![WildcardDiscovery {
                config_index,
                paths: Ok(paths),
            }],
            &mut desired,
            expanded_total,
            metadata,
        );
        self.reconcile_wildcards(desired, metadata)
    }

    fn desired_wildcards_excluding(&self, config_index: usize) -> DesiredCounters {
        self.counters
            .iter()
            .filter(|(_, counter)| {
                counter.config_index != config_index && counter.origin.is_wildcard()
            })
            .map(|(key, counter)| {
                (
                    key.clone(),
                    DesiredCounter::new(
                        counter.config_index,
                        counter.path.clone(),
                        counter.instance.clone(),
                        counter.counter_name.clone(),
                        counter.emit_instance_attribute,
                    ),
                )
            })
            .chain(
                self.retry_states
                    .iter()
                    .filter(|(_, state)| {
                        state.target.config_index != config_index
                            && state.target.origin.is_wildcard()
                    })
                    .map(|(key, state)| {
                        (
                            key.clone(),
                            DesiredCounter::new(
                                state.target.config_index,
                                state.target.path.clone(),
                                state.target.instance.clone(),
                                state.target.counter_name.clone(),
                                state.target.emit_instance_attribute,
                            ),
                        )
                    }),
            )
            .chain(self.pending_removals.iter().filter_map(|(key, removal)| {
                (removal.counter.config_index != config_index)
                    .then(|| {
                        removal.retry_target.as_ref().map(|target| {
                            (
                                key.clone(),
                                DesiredCounter::new(
                                    target.config_index,
                                    target.path.clone(),
                                    target.instance.clone(),
                                    target.counter_name.clone(),
                                    target.emit_instance_attribute,
                                ),
                            )
                        })
                    })
                    .flatten()
            }))
            .collect()
    }

    fn current_wildcard_desired(&self, config_index: usize) -> DesiredCounters {
        self.current_wildcards_excluding(&BTreeSet::new())
            .into_iter()
            .filter(|(_, desired)| desired.config_index == config_index)
            .collect()
    }

    fn current_wildcards_excluding(&self, excluded_configs: &BTreeSet<usize>) -> DesiredCounters {
        self.counters
            .iter()
            .filter(|(_, counter)| {
                counter.origin.is_wildcard() && !excluded_configs.contains(&counter.config_index)
            })
            .map(|(key, counter)| {
                (
                    key.clone(),
                    DesiredCounter::new(
                        counter.config_index,
                        counter.path.clone(),
                        counter.instance.clone(),
                        counter.counter_name.clone(),
                        counter.emit_instance_attribute,
                    ),
                )
            })
            .chain(
                self.retry_states
                    .iter()
                    .filter(|(_, state)| {
                        state.target.origin.is_wildcard()
                            && !excluded_configs.contains(&state.target.config_index)
                    })
                    .map(|(key, state)| {
                        (
                            key.clone(),
                            DesiredCounter::new(
                                state.target.config_index,
                                state.target.path.clone(),
                                state.target.instance.clone(),
                                state.target.counter_name.clone(),
                                state.target.emit_instance_attribute,
                            ),
                        )
                    }),
            )
            .chain(self.pending_removals.iter().filter_map(|(key, removal)| {
                removal.retry_target.as_ref().and_then(|target| {
                    (target.origin.is_wildcard()
                        && !excluded_configs.contains(&target.config_index))
                    .then(|| {
                        (
                            key.clone(),
                            DesiredCounter::new(
                                target.config_index,
                                target.path.clone(),
                                target.instance.clone(),
                                target.counter_name.clone(),
                                target.emit_instance_attribute,
                            ),
                        )
                    })
                })
            }))
            .collect()
    }

    fn reserved_wildcards_excluding(&self, config_index: usize) -> usize {
        self.counters
            .iter()
            .filter_map(|(key, counter)| {
                (counter.config_index != config_index && counter.origin.is_wildcard())
                    .then_some(key.clone())
            })
            .chain(self.retry_states.iter().filter_map(|(key, state)| {
                (state.target.config_index != config_index && state.target.origin.is_wildcard())
                    .then_some(key.clone())
            }))
            .collect::<BTreeSet<_>>()
            .len()
    }

    fn reserve_failed_discovery(
        &self,
        config_index: usize,
        desired: &mut DesiredCounters,
        expanded_total: usize,
        metadata: &mut PendingMetadata,
        error: &Error,
    ) {
        metadata.diagnostics.discovery_failures += 1;
        let mut failure = sample_failure(config_index, error);
        failure.diagnostic_only = true;
        metadata.failures.push(failure);
        let mut retained = self
            .counters
            .iter()
            .filter_map(|(key, counter)| {
                (counter.config_index == config_index && counter.origin.is_wildcard()).then_some((
                    counter.admission_order.unwrap_or(u64::MAX),
                    key,
                    &counter.path,
                    &counter.instance,
                    &counter.counter_name,
                    counter.emit_instance_attribute,
                ))
            })
            .chain(self.retry_states.iter().filter_map(|(key, state)| {
                (state.target.config_index == config_index && state.target.origin.is_wildcard())
                    .then_some((
                        state.target.admission_order.unwrap_or(u64::MAX),
                        key,
                        &state.target.path,
                        &state.target.instance,
                        &state.target.counter_name,
                        state.target.emit_instance_attribute,
                    ))
            }))
            .map(
                |(admission_order, key, path, instance, counter_name, emit_instance_attribute)| {
                    (
                        admission_order,
                        key.clone(),
                        DesiredCounter::new(
                            config_index,
                            path.clone(),
                            instance.clone(),
                            counter_name.clone(),
                            emit_instance_attribute,
                        ),
                    )
                },
            )
            .collect::<Vec<_>>();
        retained.sort_by_key(|(admission_order, _, _)| *admission_order);
        let discovered = retained.len();
        let allocation = expansion_allocation(
            discovered,
            self.max_instances_per_wildcard,
            self.remaining_expansion_capacity(expanded_total),
        );
        metadata.record_expansion_overflows(config_index, discovered, allocation);
        for (_, key, counter) in retained.into_iter().take(allocation.retained) {
            let _ = desired.insert(key, counter);
        }
    }

    fn prepare_successful_discoveries(
        &mut self,
        discoveries: Vec<WildcardDiscovery>,
        desired: &mut DesiredCounters,
        mut expanded_total: usize,
        metadata: &mut PendingMetadata,
    ) {
        for discovery in discoveries {
            let Ok(paths) = discovery.paths else {
                continue;
            };
            let mut paths = paths
                .into_iter()
                .map(normalize_expanded_path)
                .collect::<Vec<_>>();
            let config_index = discovery.config_index;
            let aggregation_only = cleanup_aggregation_instances(
                &mut paths,
                self.configs[config_index]
                    .instance_config
                    .excluded_aggregation(),
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
                    path: Arc::from(self.configs[config_index].path.as_str()),
                    reason: "expanded_path_too_long",
                    error: format!(
                        "{oversized} expanded paths reached the PDH {PDH_MAX_COUNTER_PATH}-unit limit"
                    ),
                    diagnostic_only: false,
                });
                paths.retain(|path| path.encode_utf16().count() < PDH_MAX_COUNTER_PATH as usize);
            }
            let mut paths =
                keyed_expanded_paths(self.configs[config_index].source_mapping_index, paths);
            let discovered = paths.len();
            let discovered_keys = paths
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<BTreeSet<_>>();
            self.restore_rediscovered_pending_removals(&discovered_keys);
            let admitted = self
                .counters
                .iter()
                .filter_map(|(key, counter)| {
                    (counter.config_index == config_index && counter.origin.is_wildcard())
                        .then_some((key.clone(), counter.admission_order.unwrap_or(u64::MAX)))
                })
                .chain(self.retry_states.iter().filter_map(|(key, state)| {
                    (state.target.config_index == config_index && state.target.origin.is_wildcard())
                        .then_some((
                            key.clone(),
                            state.target.admission_order.unwrap_or(u64::MAX),
                        ))
                }))
                .collect::<BTreeMap<_, _>>();
            paths.sort_by_key(|(key, _)| {
                admitted
                    .get(key)
                    .copied()
                    .map_or((true, u64::MAX), |order| (false, order))
            });
            let allocation = limit_expanded_paths(
                &mut paths,
                self.max_instances_per_wildcard,
                self.remaining_expansion_capacity(expanded_total),
            );
            metadata.record_expansion_overflows(config_index, discovered, allocation);
            expanded_total += paths.len();
            for (key, path) in paths {
                match self.expanded_identity(config_index, &path) {
                    Ok((instance, counter_name)) => {
                        let emit_instance_attribute =
                            self.configs[config_index].instance_config.emit_attribute()
                                && instance.is_some()
                                && !aggregation_only;
                        let _ = desired.insert(
                            key,
                            DesiredCounter::new(
                                config_index,
                                path,
                                instance,
                                counter_name,
                                emit_instance_attribute,
                            ),
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

    fn restore_rediscovered_pending_removals(&mut self, discovered: &BTreeSet<CounterKey>) {
        let restored = self
            .pending_removals
            .keys()
            .filter(|key| discovered.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in restored {
            let Some(mut removal) = self.pending_removals.remove(&key) else {
                continue;
            };
            removal.counter.reset_readiness();
            let _ = self.counters.insert(key, removal.counter);
        }
    }

    fn reconcile_wildcards(
        &mut self,
        mut desired: DesiredCounters,
        metadata: &mut PendingMetadata,
    ) -> Result<(), Error> {
        let changes = self.plan_wildcard_reconciliation(&desired);
        self.discard_unwanted_wildcard_retries(&changes.wanted, metadata);
        self.remove_wildcard_counters(&changes.removed, &changes.wanted, metadata);
        self.add_wildcard_counters(&changes.added, &changes.live, &mut desired, metadata)?;
        Ok(())
    }

    fn plan_wildcard_reconciliation(&self, desired: &DesiredCounters) -> WildcardReconciliation {
        let live = self
            .counters
            .iter()
            .filter_map(|(key, counter)| counter.origin.is_wildcard().then_some(key.clone()))
            .collect::<BTreeSet<_>>();
        let retrying = self
            .retry_states
            .iter()
            .filter_map(|(key, state)| state.target.origin.is_wildcard().then_some(key.clone()))
            .collect::<BTreeSet<_>>();
        let wanted = desired.keys().cloned().collect::<BTreeSet<_>>();
        let known_groups = instance_groups(
            self.counters
                .iter()
                .filter_map(|(key, counter)| {
                    counter.instance.as_deref().map(|instance| (key, instance))
                })
                .chain(self.retry_states.iter().filter_map(|(key, state)| {
                    state
                        .target
                        .instance
                        .as_deref()
                        .map(|instance| (key, instance))
                })),
        );
        let wanted_groups = instance_groups(desired.iter().filter_map(|(key, counter)| {
            counter.instance.as_deref().map(|instance| (key, instance))
        }));
        let (removed, mut added) = refresh_changes_with_group_resets(
            &live,
            &retrying,
            &wanted,
            &known_groups,
            &wanted_groups,
        );
        added.retain(|key| {
            !self
                .counters
                .get(key)
                .is_some_and(|counter| !counter.origin.is_wildcard())
                && !self
                    .retry_states
                    .get(key)
                    .is_some_and(|state| !state.target.origin.is_wildcard())
        });
        WildcardReconciliation {
            live,
            wanted,
            removed,
            added,
        }
    }

    fn discard_unwanted_wildcard_retries(
        &mut self,
        wanted: &BTreeSet<CounterKey>,
        metadata: &mut PendingMetadata,
    ) {
        let removed_retry_instances = self
            .retry_states
            .iter()
            .filter(|(key, state)| state.target.origin.is_wildcard() && !wanted.contains(*key))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        metadata.diagnostics.instances_removed +=
            u64::try_from(removed_retry_instances.len()).unwrap_or(u64::MAX);
        for key in removed_retry_instances {
            let _ = self.retry_states.remove(&key);
        }
    }

    fn remove_wildcard_counters(
        &mut self,
        removed: &[CounterKey],
        wanted: &BTreeSet<CounterKey>,
        metadata: &mut PendingMetadata,
    ) {
        for key in removed {
            let _ = self.retry_states.remove(key);
            let counter = self.counters.remove(key).ok_or(Error::InvalidSample(
                "wildcard refresh lost an existing counter",
            ));
            let Ok(mut counter) = counter else {
                metadata.diagnostics.discovery_failures += 1;
                continue;
            };
            if !wanted.contains(key) && !counter.removal_counted {
                metadata.diagnostics.instances_removed += 1;
                counter.removal_counted = true;
            }
            if let Err(error) = self.api.remove(&counter.handle, &counter.path) {
                metadata.diagnostics.discovery_failures += 1;
                metadata
                    .failures
                    .push(sample_failure(counter.config_index, &error));
                let _ = self.pending_removals.insert(
                    key.clone(),
                    PendingRemoval {
                        counter,
                        retry_target: None,
                        retry_ready: false,
                    },
                );
            }
        }
    }

    fn add_wildcard_counters(
        &mut self,
        added: &[CounterKey],
        live: &BTreeSet<CounterKey>,
        desired: &mut DesiredCounters,
        metadata: &mut PendingMetadata,
    ) -> Result<(), Error> {
        let now = Instant::now();
        for key in added {
            if self.pending_removals.contains_key(key) {
                continue;
            }
            let Some(DesiredCounter {
                config_index,
                path,
                instance,
                counter_name,
                emit_instance_attribute,
            }) = desired.remove(key)
            else {
                metadata.diagnostics.discovery_failures += 1;
                continue;
            };
            let admission_order = self.next_wildcard_admission_order;
            self.next_wildcard_admission_order =
                self.next_wildcard_admission_order.saturating_add(1);
            let target = CounterTarget {
                config_index,
                path,
                origin: CounterOrigin::Wildcard,
                instance,
                counter_name,
                emit_instance_attribute,
                admission_order: Some(admission_order),
                removal_counted: false,
                history: CounterHistory::new(self.previous_timestamp_unix_nano),
            };
            let was_retry = self.retry_states.contains_key(key);
            if self.try_add_target(key.clone(), target, now, metadata)?
                && !live.contains(key)
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
            .filter(|counter| counter.origin.is_wildcard())
            .count()
    }

    pub(super) fn reserved_native_entry_count(&self) -> usize {
        self.exact_counter_reservations + self.reserved_expanded_count()
    }

    pub(super) fn publish_monotonic_start(
        &mut self,
        config_index: usize,
        path: &Arc<str>,
        timestamp_unix_nano: i64,
        sequence_token: u64,
    ) -> bool {
        let key = CounterKey::new(self.configs[config_index].source_mapping_index, path);
        self.counters.get_mut(&key).is_some_and(|counter| {
            publish_monotonic_start(
                &mut counter.history.monotonic_sequence,
                timestamp_unix_nano,
                sequence_token,
            )
        })
    }

    pub(super) fn authorize_monotonic_start(
        &mut self,
        config_index: usize,
        path: &Arc<str>,
        timestamp_unix_nano: i64,
        sequence_token: u64,
    ) -> bool {
        let key = CounterKey::new(self.configs[config_index].source_mapping_index, path);
        self.counters.get_mut(&key).is_some_and(|counter| {
            authorize_monotonic_start(
                &mut counter.history.monotonic_sequence,
                timestamp_unix_nano,
                sequence_token,
            )
        })
    }

    #[cfg(test)]
    pub(super) fn monotonic_sequence(
        &self,
        config_index: usize,
        path: &Arc<str>,
    ) -> Option<MonotonicSequence> {
        self.counters
            .get(&CounterKey::new(
                self.configs[config_index].source_mapping_index,
                path,
            ))
            .map(|counter| counter.history.monotonic_sequence)
    }

    pub(super) fn set_max_native_counters(
        &mut self,
        max_native_counters: usize,
    ) -> Result<(), Error> {
        if self.exact_counter_reservations > max_native_counters {
            return Err(Error::InvalidSample(
                "query native-counter limit is below its exact-counter reservation",
            ));
        }
        self.max_native_counters = max_native_counters;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn prime(&mut self) -> Result<(), Error> {
        let sample = self.collect_with_monotonic_candidate(false, true)?;
        self.pending.merge(sample.metadata);
        Ok(())
    }

    pub(super) fn prime_managed(&mut self) -> Result<(), Error> {
        let sample = self.collect_with_monotonic_candidate(false, false)?;
        self.pending.merge(sample.metadata);
        Ok(())
    }

    pub(super) fn record_prime_failure(&mut self, error: &Error) {
        self.pending
            .failures
            .extend(
                self.configs
                    .iter()
                    .enumerate()
                    .map(|(counter_index, config)| SampleFailure {
                        counter_index,
                        path: Arc::from(config.path.as_str()),
                        reason: error.reason(),
                        error: error.bounded_detail(),
                        diagnostic_only: true,
                    }),
            );
    }

    fn remaining_expansion_capacity(&self, allocated_expanded: usize) -> usize {
        self.max_native_counters
            .saturating_sub(self.exact_counter_reservations)
            .saturating_sub(self.pending_wildcard_removal_count())
            .saturating_sub(allocated_expanded)
    }

    fn reserved_expanded_count(&self) -> usize {
        let live = self
            .counters
            .values()
            .filter(|counter| counter.origin.is_wildcard())
            .count();
        let retrying = self
            .retry_states
            .iter()
            .filter(|(key, state)| {
                state.target.origin.is_wildcard() && !self.counters.contains_key(*key)
            })
            .count();
        live.saturating_add(retrying)
            .saturating_add(self.pending_wildcard_removal_count())
    }

    fn pending_wildcard_removal_count(&self) -> usize {
        self.pending_removals
            .values()
            .filter(|removal| removal.counter.origin.is_wildcard())
            .count()
    }

    fn retry_pending_removals(&mut self, now: Instant, metadata: &mut PendingMetadata) {
        let keys = self.pending_removals.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let Some(mut removal) = self.pending_removals.remove(&key) else {
                continue;
            };
            if !removal.retry_ready {
                removal.retry_ready = true;
                let _ = self.pending_removals.insert(key, removal);
                continue;
            }
            match self
                .api
                .remove(&removal.counter.handle, &removal.counter.path)
            {
                Ok(()) => {
                    if let Some(target) = removal.retry_target {
                        self.schedule_retry(key, target, now);
                    }
                }
                Err(error) => {
                    metadata
                        .failures
                        .push(sample_failure(removal.counter.config_index, &error));
                    let _ = self.pending_removals.insert(key, removal);
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn collect(&mut self) -> Result<Sample, Error> {
        self.collect_with_monotonic_candidate(true, true)
    }

    pub(super) fn collect_managed(&mut self) -> Result<Sample, Error> {
        let mut sample = self.collect_with_monotonic_candidate(true, false)?;
        sample.metadata.diagnostics.any_query_collected = true;
        Ok(sample)
    }

    fn collect_with_monotonic_candidate(
        &mut self,
        emit_monotonic_candidate: bool,
        refresh_wildcards: bool,
    ) -> Result<Sample, Error> {
        let mut metadata = std::mem::take(&mut self.pending);
        let now = Instant::now();
        if refresh_wildcards
            && self.wildcard_refresh_due()
            && let Err(error) = self.refresh_wildcards(&mut metadata)
        {
            return Err(self.restore_collection_error(metadata, error));
        }
        self.retry_pending_removals(now, &mut metadata);
        if let Err(error) = self.retry_counters(now, &mut metadata) {
            return Err(self.restore_collection_error(metadata, error));
        }
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
            return Ok(Sample {
                timestamp_unix_nano,
                members: self.admitted_members(),
                points: Vec::new(),
                metadata,
            });
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
        if timestamp_unix_nano < self.previous_timestamp_unix_nano {
            self.reset_histories_for_clock_rollback(timestamp_unix_nano);
        }
        let _ = advance_collection_time(
            &mut self.start_time_unix_nano,
            &mut self.previous_timestamp_unix_nano,
            timestamp_unix_nano,
            self.counters
                .values_mut()
                .map(|counter| &mut counter.history.monotonic_sequence),
        );
        let points = match self.collect_points(
            has_data,
            timestamp_unix_nano,
            emit_monotonic_candidate,
            &mut metadata,
        ) {
            Ok(points) => points,
            Err(error) => return Err(self.restore_collection_error(metadata, error)),
        };
        metadata.diagnostics.active_expanded_counters = self.active_expanded_count();
        Ok(Sample {
            timestamp_unix_nano,
            members: self.admitted_members(),
            points,
            metadata,
        })
    }

    fn collect_points(
        &mut self,
        has_data: bool,
        timestamp_unix_nano: i64,
        emit_monotonic_candidate: bool,
        metadata: &mut PendingMetadata,
    ) -> Result<Vec<SamplePoint>, Error> {
        let mut points = Vec::with_capacity(self.counters.len());
        let mut retired = Vec::new();
        for (key, counter) in &mut self.counters {
            let metric_kind = self.configs[counter.config_index].metric_kind;
            let result = if has_data {
                counter.read(
                    &mut self.api,
                    metric_kind,
                    timestamp_unix_nano,
                    emit_monotonic_candidate,
                )
            } else {
                Ok((SampleValue::Unavailable, None))
            };
            let (value, point_start_time) = match result {
                Ok(result) => result,
                Err(error) => {
                    metadata
                        .failures
                        .push(sample_failure(counter.config_index, &error));
                    metadata.diagnostics.counter_read_failures += 1;
                    if error.is_invalid_counter_handle_read() {
                        let mut target = counter.target();
                        target.history.restart_pdh_sampling();
                        retired.push((key.clone(), target));
                    }
                    continue;
                }
            };
            if matches!(value, SampleValue::Warming | SampleValue::Unavailable) {
                metadata.diagnostics.warmup_omissions += 1;
            }
            points.push(SamplePoint {
                counter_index: counter.config_index,
                path: counter.path.clone(),
                instance: counter.instance.clone(),
                counter_name: counter.counter_name.clone(),
                emit_instance_attribute: counter.emit_instance_attribute,
                timestamp_unix_nano,
                start_time_unix_nano: point_start_time,
                monotonic_publication: (matches!(
                    metric_kind,
                    MetricKind::Sum {
                        aggregation: Aggregation::Cumulative,
                        monotonic: true,
                    }
                ) && counter
                    .history
                    .monotonic_sequence
                    .start_time_unix_nano
                    .is_none())
                .then_some(MonotonicPublicationCandidate::Pending),
                value,
            });
        }
        points.sort_by(|left, right| {
            left.counter_index
                .cmp(&right.counter_index)
                .then_with(|| left.path.cmp(&right.path))
        });
        let now = Instant::now();
        for (key, target) in retired {
            let counter = self.counters.remove(&key).ok_or(Error::InvalidSample(
                "invalid-handle recovery lost an existing counter",
            ))?;
            if let Err(error) = self.api.remove(&counter.handle, &counter.path) {
                metadata
                    .failures
                    .push(sample_failure(counter.config_index, &error));
                return Err(error);
            }
            self.schedule_retry(key, target, now);
        }
        Ok(points)
    }

    fn reset_histories_for_clock_rollback(&mut self, timestamp_unix_nano: i64) {
        for history in self
            .counters
            .values_mut()
            .map(|counter| &mut counter.history)
        {
            history.cumulative_start_time_unix_nano = timestamp_unix_nano;
            history.monotonic_sequence = MonotonicSequence::default();
        }
        for history in self
            .retry_states
            .values_mut()
            .map(|state| &mut state.target.history)
            .chain(self.pending_removals.values_mut().filter_map(|removal| {
                removal
                    .retry_target
                    .as_mut()
                    .map(|target| &mut target.history)
            }))
        {
            *history = CounterHistory::new(timestamp_unix_nano);
        }
    }
}

fn setup_error_with_cleanup<A: PdhApi>(
    api: &mut A,
    counter: &A::Counter,
    path: &str,
    error: Error,
) -> Error {
    match api.remove(counter, path) {
        Ok(()) => error,
        Err(cleanup) => Error::with_cleanup_failure(path, Some(error), cleanup),
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
    config_index: usize,
    path: Arc<str>,
    instance: Option<Arc<InstanceIdentity>>,
    counter_name: Option<Arc<str>>,
    emit_instance_attribute: bool,
}

impl DesiredCounter {
    fn new(
        config_index: usize,
        path: impl Into<Arc<str>>,
        instance: Option<Arc<InstanceIdentity>>,
        counter_name: Option<Arc<str>>,
        emit_instance_attribute: bool,
    ) -> Self {
        Self {
            config_index,
            path: path.into(),
            instance,
            counter_name,
            emit_instance_attribute,
        }
    }
}

type DesiredCounters = BTreeMap<CounterKey, DesiredCounter>;

pub(super) struct QueryRecreationSeed {
    start_time_unix_nano: i64,
    previous_timestamp_unix_nano: i64,
    last_wildcard_refresh: Instant,
    exact_histories: BTreeMap<CounterKey, CounterHistory>,
    wildcard_targets: Vec<(CounterKey, CounterTarget)>,
}

impl QueryRecreationSeed {
    pub(super) fn reserved_native_entry_count(&self, exact_counter_reservations: usize) -> usize {
        exact_counter_reservations.saturating_add(self.wildcard_targets.len())
    }

    pub(super) fn publish_monotonic_start(
        &mut self,
        key: &CounterKey,
        timestamp_unix_nano: i64,
        sequence_token: u64,
    ) -> bool {
        if let Some(history) = self.exact_histories.get_mut(key) {
            return publish_monotonic_start(
                &mut history.monotonic_sequence,
                timestamp_unix_nano,
                sequence_token,
            );
        }
        self.wildcard_targets
            .iter_mut()
            .find(|(candidate, _)| candidate == key)
            .is_some_and(|(_, target)| {
                publish_monotonic_start(
                    &mut target.history.monotonic_sequence,
                    timestamp_unix_nano,
                    sequence_token,
                )
            })
    }
}

struct WildcardReconciliation {
    live: BTreeSet<CounterKey>,
    wanted: BTreeSet<CounterKey>,
    removed: Vec<CounterKey>,
    added: Vec<CounterKey>,
}

struct WildcardDiscovery {
    config_index: usize,
    paths: Result<Vec<String>, Error>,
}

pub(super) struct WildcardCandidate {
    pub(super) key: CounterKey,
    desired: Result<DesiredCounter, Error>,
    pub(super) satisfied_by_exact: bool,
}

pub(super) struct WildcardRefreshPlan {
    pub(super) config_index: usize,
    pub(super) discovered: usize,
    pub(super) paths: Vec<WildcardCandidate>,
    metadata: PendingMetadata,
}

pub(super) struct WildcardRefreshSelection {
    pub(super) plan: WildcardRefreshPlan,
    pub(super) selected: BTreeSet<CounterKey>,
    pub(super) allocation: ExpansionAllocation,
}

pub(super) struct WildcardRefreshAdditions {
    desired: DesiredCounters,
    pub(super) added: Vec<CounterKey>,
    live: BTreeSet<CounterKey>,
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
    path: Arc<str>,
    origin: CounterOrigin,
    instance: Option<Arc<InstanceIdentity>>,
    counter_name: Option<Arc<str>>,
    emit_instance_attribute: bool,
    admission_order: Option<u64>,
    removal_counted: bool,
    history: CounterHistory,
}

#[derive(Clone, Copy)]
enum CounterOrigin {
    Exact,
    Wildcard,
}

impl CounterOrigin {
    fn is_wildcard(self) -> bool {
        matches!(self, Self::Wildcard)
    }

    fn retry_cap(self, exact_retry_cap: Duration, wildcard_retry_cap: Duration) -> Duration {
        match self {
            Self::Exact => exact_retry_cap,
            Self::Wildcard => wildcard_retry_cap,
        }
    }
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
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CounterHistory {
    cumulative_start_time_unix_nano: i64,
    previous_base: Option<i64>,
    monotonic_sequence: MonotonicSequence,
    delta_baseline_unix_nano: Option<i64>,
}

impl CounterHistory {
    fn new(cumulative_start_time_unix_nano: i64) -> Self {
        Self {
            cumulative_start_time_unix_nano,
            ..Self::default()
        }
    }

    fn restart_pdh_sampling(&mut self) {
        self.previous_base = None;
        self.delta_baseline_unix_nano = None;
    }

    fn for_recreated_query(mut self) -> Self {
        self.restart_pdh_sampling();
        self
    }
}

impl CounterTarget {
    fn member(&self) -> SampleMember {
        SampleMember {
            counter_index: self.config_index,
            path: Arc::clone(&self.path),
            instance: self.instance.clone(),
            counter_name: self.counter_name.clone(),
            emit_instance_attribute: self.emit_instance_attribute,
        }
    }

    fn for_recreated_query(mut self) -> Self {
        self.history.restart_pdh_sampling();
        self
    }
}

fn restart_query_pdh_sampling<'a>(histories: impl Iterator<Item = &'a mut CounterHistory>) {
    for history in histories {
        history.restart_pdh_sampling();
    }
}

struct CounterHandle<C> {
    config_index: usize,
    path: Arc<str>,
    handle: C,
    kind: CounterKind,
    scale_power10: i32,
    history: CounterHistory,
    origin: CounterOrigin,
    instance: Option<Arc<InstanceIdentity>>,
    counter_name: Option<Arc<str>>,
    emit_instance_attribute: bool,
    admission_order: Option<u64>,
    removal_counted: bool,
}

struct PendingRemoval<C> {
    counter: CounterHandle<C>,
    retry_target: Option<CounterTarget>,
    retry_ready: bool,
}

impl<C> CounterHandle<C> {
    fn member(&self) -> SampleMember {
        SampleMember {
            counter_index: self.config_index,
            path: Arc::clone(&self.path),
            instance: self.instance.clone(),
            counter_name: self.counter_name.clone(),
            emit_instance_attribute: self.emit_instance_attribute,
        }
    }

    fn target(&self) -> CounterTarget {
        CounterTarget {
            config_index: self.config_index,
            path: self.path.clone(),
            origin: self.origin,
            instance: self.instance.clone(),
            counter_name: self.counter_name.clone(),
            emit_instance_attribute: self.emit_instance_attribute,
            admission_order: self.admission_order,
            removal_counted: self.removal_counted,
            history: self.history,
        }
    }

    fn reset_readiness(&mut self) {
        self.history.restart_pdh_sampling();
    }

    fn read(
        &mut self,
        api: &mut impl PdhApi<Counter = C>,
        metric_kind: MetricKind,
        timestamp_unix_nano: i64,
        emit_monotonic_candidate: bool,
    ) -> Result<(SampleValue, Option<i64>), Error> {
        if let Some(value) = self.prepare_raw_sample(api, timestamp_unix_nano)? {
            return Ok((value, None));
        }

        let Some(number) = self.read_formatted_number(api)? else {
            return Ok((SampleValue::Unavailable, None));
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
            return Ok((SampleValue::Warming, None));
        }
        let scaled = self.scale_number(number)?;
        let point_start_time = self.point_start_time(
            metric_kind,
            number,
            delta_start_time,
            timestamp_unix_nano,
            emit_monotonic_candidate,
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
                return Ok(Some(SampleValue::Unavailable));
            }
            Err(error) => {
                self.reset_readiness();
                return Err(error);
            }
        };
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
            path: self.path.to_string(),
            source,
        })
    }

    fn point_start_time(
        &mut self,
        metric_kind: MetricKind,
        number: Number,
        delta_start_time: Option<i64>,
        timestamp_unix_nano: i64,
        emit_monotonic_candidate: bool,
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
                if emit_monotonic_candidate {
                    Some(pending_monotonic_start_time(
                        &self.path,
                        &mut self.history.monotonic_sequence,
                        direct_value,
                        timestamp_unix_nano,
                    )?)
                } else {
                    prime_monotonic_sequence(
                        &self.path,
                        &mut self.history.monotonic_sequence,
                        direct_value,
                        timestamp_unix_nano,
                    )?;
                    None
                }
            }
            MetricKind::Sum {
                aggregation: Aggregation::Delta,
                ..
            } => delta_start_time,
            MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: false,
            } => Some(self.history.cumulative_start_time_unix_nano),
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
            path: Arc::from(r"\Process(worker)\Private Bytes"),
            origin: CounterOrigin::Wildcard,
            instance: Some(Arc::new(InstanceIdentity {
                name: "worker".to_owned(),
                parent: None,
                index: 0,
            })),
            counter_name: None,
            emit_instance_attribute: true,
            admission_order: Some(0),
            removal_counted: false,
            history: CounterHistory::new(100),
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

    /// Scenario: Exact and wildcard counter-add failures use distinct retry clocks.
    /// Guarantees: Exact targets cap backoff at the collection retry cadence while wildcard targets retain the discovery refresh cap.
    #[test]
    fn retry_cap_follows_counter_origin() {
        let exact_retry_cap = Duration::from_secs(2);
        let wildcard_retry_cap = Duration::from_secs(60);

        assert_eq!(
            CounterOrigin::Exact.retry_cap(exact_retry_cap, wildcard_retry_cap),
            exact_retry_cap
        );
        assert_eq!(
            CounterOrigin::Wildcard.retry_cap(exact_retry_cap, wildcard_retry_cap),
            wildcard_retry_cap
        );
    }
}
