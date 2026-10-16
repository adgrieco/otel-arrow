// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded worker-thread query ownership, recovery, recreation, and command lifecycle.

use super::super::config::CounterConfig;
use super::super::model::{PendingMetadata, Sample, SampleDiagnostics, SampleFailure};
use super::PdhApi;
use super::error::Error;
use super::query::{Query, retry_delay};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

pub(super) const QUERY_REBUILD_AFTER_FAILURES: u32 = 3;

pub(super) fn should_rebuild_query(consecutive_failures: u32) -> bool {
    consecutive_failures >= QUERY_REBUILD_AFTER_FAILURES
}

/// Capacity-one command client for the thread that owns the persistent query.
pub(in super::super) struct Worker {
    pub(super) tx: mpsc::SyncSender<Command>,
    pub(super) accepted: Arc<AtomicBool>,
    pub(super) shutdown_requested: Arc<AtomicBool>,
    pub(super) join: Option<JoinHandle<()>>,
    pub(super) completion: Option<oneshot::Receiver<()>>,
}

impl Worker {
    pub(in super::super) async fn start(
        counters: Vec<CounterConfig>,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_expanded_counters: usize,
        node: String,
    ) -> Result<Self, Error> {
        let settings = QuerySettings {
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            node,
        };
        Self::start_with(
            counters,
            wildcard_refresh_interval,
            max_expanded_counters,
            move |counters, query_limit| settings.open(counters, query_limit),
        )
        .await
    }

    pub(super) async fn start_with<A, F>(
        counters: Vec<CounterConfig>,
        query_retry_cap: Duration,
        max_expanded_counters: usize,
        mut open: F,
    ) -> Result<Self, Error>
    where
        A: PdhApi + 'static,
        F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error> + Send + 'static,
    {
        let plans = query_plans(counters);
        let (tx, rx) = mpsc::sync_channel(1);
        let (init_tx, init_rx) = oneshot::channel();
        let (completion_tx, completion_rx) = oneshot::channel();
        let accepted = Arc::new(AtomicBool::new(false));
        let worker_accepted = Arc::clone(&accepted);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown_requested);
        let join = std::thread::Builder::new()
            .name("windows-perf-counters-pdh".to_owned())
            .spawn(move || {
                run_worker_thread(
                    plans,
                    query_retry_cap,
                    max_expanded_counters,
                    &mut open,
                    rx,
                    init_tx,
                    completion_tx,
                    worker_accepted,
                    worker_shutdown,
                );
            })
            .map_err(|err| Error::WorkerStart(err.to_string()))?;

        let mut cancellation =
            StartupCancellation::new(tx.clone(), Arc::clone(&shutdown_requested));
        match init_rx.await {
            Ok(Ok(())) => {
                cancellation.disarm();
                Ok(Self {
                    tx,
                    accepted,
                    shutdown_requested,
                    join: Some(join),
                    completion: Some(completion_rx),
                })
            }
            Ok(Err(error)) => {
                cancellation.request();
                await_worker_completion(join, completion_rx).await?;
                Err(error)
            }
            Err(_) => {
                cancellation.request();
                await_worker_completion(join, completion_rx).await?;
                Err(Error::WorkerInitStopped)
            }
        }
    }

    pub(in super::super) async fn collect(&self) -> Result<Sample, Error> {
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
    pub(in super::super) async fn shutdown(&mut self, deadline: Instant) -> Result<bool, Error> {
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
                join_worker(join).await?;
                Ok(true)
            }
            Err(_) => {
                self.join = Some(join);
                Ok(false)
            }
        }
    }
}

struct StartupCancellation {
    tx: mpsc::SyncSender<Command>,
    shutdown_requested: Arc<AtomicBool>,
    armed: bool,
}

impl StartupCancellation {
    fn new(tx: mpsc::SyncSender<Command>, shutdown_requested: Arc<AtomicBool>) -> Self {
        Self {
            tx,
            shutdown_requested,
            armed: true,
        }
    }

    fn request(&self) {
        self.shutdown_requested.store(true, Ordering::Release);
        let _ = self.tx.try_send(Command::Shutdown);
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StartupCancellation {
    fn drop(&mut self) {
        if self.armed {
            self.request();
        }
    }
}

async fn await_worker_completion(
    join: JoinHandle<()>,
    completion: oneshot::Receiver<()>,
) -> Result<(), Error> {
    let completion_result = completion.await;
    join_worker(join).await?;
    completion_result.map_err(|_| Error::WorkerStopped)
}

async fn join_worker(join: JoinHandle<()>) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || join.join())
        .await
        .map_err(|_| Error::WorkerPanicked)?
        .map_err(|_| Error::WorkerPanicked)
}

struct CompletionSignal(Option<oneshot::Sender<()>>);

impl Drop for CompletionSignal {
    fn drop(&mut self) {
        if let Some(completion) = self.0.take() {
            let _ = completion.send(());
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_worker_thread<A, F>(
    plans: Vec<QueryPlan>,
    query_retry_cap: Duration,
    max_expanded_counters: usize,
    open: &mut F,
    rx: mpsc::Receiver<Command>,
    init_tx: oneshot::Sender<Result<(), Error>>,
    completion_tx: oneshot::Sender<()>,
    accepted: Arc<AtomicBool>,
    shutdown_requested: Arc<AtomicBool>,
) where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error>,
{
    let _completion = CompletionSignal(Some(completion_tx));
    let mut queries = match open_initial_queries(
        plans,
        query_retry_cap,
        max_expanded_counters,
        open,
        &shutdown_requested,
    ) {
        Ok(Some(queries)) => queries,
        Ok(None) => return,
        Err(error) => {
            let _ = init_tx.send(Err(error));
            return;
        }
    };
    if shutdown_requested.load(Ordering::Acquire) || init_tx.send(Ok(())).is_err() {
        return;
    }
    run_command_loop(
        &mut queries,
        query_retry_cap,
        max_expanded_counters,
        open,
        &rx,
        &accepted,
        &shutdown_requested,
    );
}

fn open_initial_queries<A, F>(
    plans: Vec<QueryPlan>,
    query_retry_cap: Duration,
    max_expanded_counters: usize,
    open: &mut F,
    shutdown_requested: &AtomicBool,
) -> Result<Option<Vec<QuerySlot<A>>>, Error>
where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error>,
{
    let mut queries = Vec::with_capacity(plans.len());
    for plan in plans {
        if shutdown_requested.load(Ordering::Acquire) {
            return Ok(None);
        }
        let query_limit = remaining_expansion_capacity(
            max_expanded_counters,
            queries.iter().map(QuerySlot::active_expanded_count),
        );
        queries.push(QuerySlot::open_initial_with_retry_cap(
            plan,
            query_retry_cap,
            query_limit,
            open,
        )?);
    }
    Ok(Some(queries))
}

fn run_command_loop<A, F>(
    queries: &mut [QuerySlot<A>],
    query_retry_cap: Duration,
    max_expanded_counters: usize,
    open: &mut F,
    rx: &mpsc::Receiver<Command>,
    accepted: &Arc<AtomicBool>,
    shutdown_requested: &Arc<AtomicBool>,
) where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error>,
{
    while !shutdown_requested.load(Ordering::Acquire) {
        match rx.recv() {
            Ok(Command::Collect(response)) => {
                let accepted = AcceptedCollectGuard::new(Arc::clone(accepted));
                let result = collect_queries(queries, query_retry_cap, max_expanded_counters, open);
                drop(accepted);
                let _ = response.send(result);
            }
            Ok(Command::Shutdown) | Err(_) => break,
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.shutdown_requested.store(true, Ordering::Release);
        let _ = self.tx.try_send(Command::Shutdown);
    }
}

pub(super) enum Command {
    Collect(oneshot::Sender<Result<Sample, Error>>),
    Shutdown,
}

pub(super) struct AcceptedCollectGuard {
    accepted: Arc<AtomicBool>,
}

impl AcceptedCollectGuard {
    pub(super) fn new(accepted: Arc<AtomicBool>) -> Self {
        Self { accepted }
    }
}

impl Drop for AcceptedCollectGuard {
    fn drop(&mut self) {
        self.accepted.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
pub(super) struct QuerySettings {
    wildcard_refresh_interval: Duration,
    max_instances_per_wildcard: usize,
    node: String,
}

impl QuerySettings {
    fn open(
        &self,
        counters: Vec<CounterConfig>,
        max_expanded_counters: usize,
    ) -> Result<Query, Error> {
        Query::open(
            counters,
            self.wildcard_refresh_interval,
            self.max_instances_per_wildcard,
            max_expanded_counters,
            self.node.clone(),
        )
    }
}

pub(super) struct QueryPlan {
    pub(super) configs: Vec<CounterConfig>,
    pub(super) global_indices: Vec<usize>,
    pub(super) recreate_query: bool,
}

pub(super) fn query_plans(counters: Vec<CounterConfig>) -> Vec<QueryPlan> {
    let mut plans = Vec::new();
    for (global_index, counter) in counters.into_iter().enumerate() {
        let append_to_current = plans.last().is_some_and(|plan: &QueryPlan| {
            if counter.recreate_query {
                plan.recreate_query
                    && plan.configs[0].source_mapping_index == counter.source_mapping_index
            } else {
                !plan.recreate_query
            }
        });
        if append_to_current && let Some(plan) = plans.last_mut() {
            plan.configs.push(counter);
            plan.global_indices.push(global_index);
            continue;
        }
        plans.push(QueryPlan {
            recreate_query: counter.recreate_query,
            configs: vec![counter],
            global_indices: vec![global_index],
        });
    }
    plans
}

pub(super) struct QuerySlot<A: PdhApi> {
    pub(super) plan: QueryPlan,
    pub(super) query: Option<Query<A>>,
    query_failure_attempts: u32,
    next_query_retry: Instant,
    rebuild_attempts: u32,
    next_rebuild: Instant,
    pending: PendingMetadata,
}

impl<A: PdhApi> QuerySlot<A> {
    #[cfg(test)]
    pub(super) fn open_initial<F>(
        plan: QueryPlan,
        max_expanded_counters: usize,
        open: &mut F,
    ) -> Result<Self, Error>
    where
        F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error>,
    {
        Self::open_initial_with_retry_cap(plan, Duration::ZERO, max_expanded_counters, open)
    }

    fn open_initial_with_retry_cap<F>(
        plan: QueryPlan,
        query_retry_cap: Duration,
        max_expanded_counters: usize,
        open: &mut F,
    ) -> Result<Self, Error>
    where
        F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error>,
    {
        let now = Instant::now();
        let mut query = match open(plan.configs.clone(), max_expanded_counters) {
            Ok(query) => query,
            Err(error) if error.is_recoverable_query_open_failure() => {
                let reason = error.reason();
                let detail = error.bounded_detail();
                return Ok(Self {
                    pending: PendingMetadata {
                        failures: plan
                            .configs
                            .iter()
                            .enumerate()
                            .map(|(counter_index, _)| SampleFailure {
                                counter_index,
                                reason,
                                error: detail.clone(),
                                diagnostic_only: true,
                            })
                            .collect(),
                        ..Default::default()
                    },
                    plan,
                    query: None,
                    query_failure_attempts: 0,
                    next_query_retry: now,
                    rebuild_attempts: 1,
                    next_rebuild: now + retry_delay(1, query_retry_cap),
                });
            }
            Err(error) => return Err(error),
        };
        let prime_error = query.prime().err();
        if let Some(error) = &prime_error {
            query.record_prime_failure(error);
        }
        Ok(Self {
            plan,
            query: Some(query),
            query_failure_attempts: u32::from(
                prime_error
                    .as_ref()
                    .is_some_and(Error::is_query_collection_failure),
            ),
            next_query_retry: now,
            rebuild_attempts: 0,
            next_rebuild: now,
            pending: PendingMetadata::default(),
        })
    }

    fn is_recoverable_collection_error(&self, error: &Error) -> bool {
        if self.query.is_some() {
            error.is_collection_failure()
        } else {
            error.is_recoverable_query_open_failure()
                || matches!(error, Error::QueryRecoveryPending { .. })
        }
    }

    pub(super) fn collect<F>(
        &mut self,
        now: Instant,
        query_retry_cap: Duration,
        max_expanded_counters: usize,
        open: &mut F,
    ) -> Result<Sample, Error>
    where
        F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error>,
    {
        let was_query_retry =
            self.query.is_some() && self.query_failure_attempts > 0 && now >= self.next_query_retry;
        if was_query_retry {
            self.pending.diagnostics.retry_attempts += 1;
        }
        let mut result = self.collect_or_rebuild(now, max_expanded_counters, query_retry_cap, open);
        self.update_recovery_state(now, query_retry_cap, was_query_retry, &result);
        if let Ok(sample) = &mut result {
            sample
                .metadata
                .merge_older(std::mem::take(&mut self.pending));
        }
        result
    }

    fn collect_or_rebuild<F>(
        &mut self,
        now: Instant,
        max_expanded_counters: usize,
        query_retry_cap: Duration,
        open: &mut F,
    ) -> Result<Sample, Error>
    where
        F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error>,
    {
        if let Some(active_query) = self.query.as_mut() {
            active_query.set_max_expanded_counters(max_expanded_counters);
            if now < self.next_query_retry {
                Err(Error::QueryRecoveryPending {
                    retry_after_ms: self.next_query_retry.duration_since(now).as_millis(),
                })
            } else {
                active_query.collect()
            }
        } else if now < self.next_rebuild {
            Err(Error::QueryRecoveryPending {
                retry_after_ms: self.next_rebuild.duration_since(now).as_millis(),
            })
        } else {
            self.rebuild(now, max_expanded_counters, query_retry_cap, open)
        }
    }

    fn rebuild<F>(
        &mut self,
        now: Instant,
        max_expanded_counters: usize,
        query_retry_cap: Duration,
        open: &mut F,
    ) -> Result<Sample, Error>
    where
        F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error>,
    {
        self.pending.diagnostics.query_rebuild_attempts += 1;
        let mut rebuilt = match open(self.plan.configs.clone(), max_expanded_counters) {
            Ok(rebuilt) => rebuilt,
            Err(error) if error.is_recoverable_query_open_failure() => {
                self.rebuild_attempts = self.rebuild_attempts.saturating_add(1);
                self.next_rebuild = now + retry_delay(self.rebuild_attempts, query_retry_cap);
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        self.rebuild_attempts = 0;
        self.query_failure_attempts = 0;
        self.next_query_retry = now;
        if let Err(error) = rebuilt.prime() {
            self.query = Some(rebuilt);
            return Err(error);
        }
        self.pending.diagnostics.query_rebuild_recoveries += 1;
        let mut sample = Sample::empty(rebuilt.previous_timestamp_unix_nano);
        rebuilt.move_pending_metadata_to(&mut sample.metadata);
        self.query = Some(rebuilt);
        Ok(sample)
    }

    fn update_recovery_state(
        &mut self,
        now: Instant,
        query_retry_cap: Duration,
        was_query_retry: bool,
        result: &Result<Sample, Error>,
    ) {
        if result
            .as_ref()
            .is_err_and(Error::is_query_collection_failure)
        {
            self.query_failure_attempts = self.query_failure_attempts.saturating_add(1);
            self.next_query_retry = now + retry_delay(self.query_failure_attempts, query_retry_cap);
            if should_rebuild_query(self.query_failure_attempts) {
                self.move_query_metadata_to_pending();
                self.query = None;
                self.rebuild_attempts = 0;
                self.next_rebuild = self.next_query_retry;
            }
        } else if result
            .as_ref()
            .is_err_and(Error::requires_immediate_query_rebuild)
        {
            self.move_query_metadata_to_pending();
            self.query = None;
            self.rebuild_attempts = 0;
            self.next_rebuild = now + retry_delay(1, query_retry_cap);
        } else if result.is_ok() {
            if was_query_retry {
                self.pending.diagnostics.retry_recoveries += 1;
            }
            self.query_failure_attempts = 0;
            self.next_query_retry = now;
        }
    }

    fn move_query_metadata_to_pending(&mut self) {
        if let Some(active_query) = self.query.as_mut() {
            active_query.move_pending_metadata_to(&mut self.pending);
        }
    }

    fn recreate<F>(
        &mut self,
        now: Instant,
        max_expanded_counters: usize,
        open: &mut F,
        metadata: &mut PendingMetadata,
    ) where
        F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error>,
    {
        if !self.plan.recreate_query {
            return;
        }
        metadata.diagnostics.query_recreation_attempts += 1;
        let replacement =
            open(self.plan.configs.clone(), max_expanded_counters).and_then(|mut query| {
                query.prime()?;
                Ok(query)
            });
        match replacement {
            Ok(query) => {
                self.query = Some(query);
                self.query_failure_attempts = 0;
                self.next_query_retry = now;
                self.rebuild_attempts = 0;
                self.next_rebuild = now;
            }
            Err(error) => {
                metadata.diagnostics.query_recreation_failures += 1;
                if error.is_query_collection_failure() {
                    metadata.diagnostics.scrape_failures += 1;
                }
                metadata
                    .failures
                    .extend(
                        self.plan
                            .global_indices
                            .iter()
                            .map(|&counter_index| SampleFailure {
                                counter_index,
                                reason: error.reason(),
                                error: error.bounded_detail(),
                                diagnostic_only: true,
                            }),
                    );
            }
        }
    }

    pub(super) fn remap_sample(&self, mut sample: Sample) -> Result<Sample, Error> {
        let remap = |local_index: usize| {
            self.plan
                .global_indices
                .get(local_index)
                .copied()
                .ok_or(Error::InvalidSample(
                    "query returned an unknown local counter index",
                ))
        };
        for point in &mut sample.points {
            point.counter_index = remap(point.counter_index)?;
        }
        for failure in &mut sample.metadata.failures {
            failure.counter_index = remap(failure.counter_index)?;
        }
        for overflow in &mut sample.metadata.overflows {
            overflow.counter_index = remap(overflow.counter_index)?;
        }
        Ok(sample)
    }

    pub(super) fn active_expanded_count(&self) -> usize {
        self.query.as_ref().map_or(0, Query::active_expanded_count)
    }
}

pub(super) fn merge_query_samples(samples: Vec<Sample>) -> Result<Sample, Error> {
    let mut merged = None::<Sample>;
    for sample in samples {
        if let Some(target) = &mut merged {
            target.timestamp_unix_nano = target.timestamp_unix_nano.max(sample.timestamp_unix_nano);
            target.metadata.diagnostics.active_expanded_counters +=
                sample.metadata.diagnostics.active_expanded_counters;
            target.metadata.merge(sample.metadata);
            target.points.extend(sample.points);
        } else {
            merged = Some(sample);
        }
    }
    let mut merged = merged.ok_or(Error::InvalidSample(
        "worker has no performance-counter query",
    ))?;
    merged.points.sort_by(|left, right| {
        (left.counter_index, &left.path).cmp(&(right.counter_index, &right.path))
    });
    merged
        .metadata
        .failures
        .sort_by_key(|failure| (failure.counter_index, failure.reason));
    merged
        .metadata
        .overflows
        .sort_by_key(|overflow| (overflow.counter_index, overflow.reason));
    Ok(merged)
}

pub(super) fn collect_queries<A, F>(
    queries: &mut [QuerySlot<A>],
    query_retry_cap: Duration,
    max_expanded_counters: usize,
    open: &mut F,
) -> Result<Sample, Error>
where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error>,
{
    let now = Instant::now();
    let mut samples = Vec::with_capacity(queries.len());
    let mut collection_succeeded = vec![false; queries.len()];
    let mut first_collection_error = None;
    let mut query_failures = Vec::new();
    for query_index in 0..queries.len() {
        let query_limit = query_expansion_limit(queries, query_index, max_expanded_counters);
        let query = &mut queries[query_index];
        match query.collect(now, query_retry_cap, query_limit, open) {
            Ok(sample) => {
                collection_succeeded[query_index] = true;
                samples.push(query.remap_sample(sample)?);
            }
            Err(error) if query.is_recoverable_collection_error(&error) => {
                append_query_failures(&mut query_failures, &query.plan, &error);
                if first_collection_error.is_none() {
                    first_collection_error = Some(error);
                }
            }
            Err(error) => return Err(error),
        }
    }
    if samples.is_empty() {
        return Err(first_collection_error.unwrap_or(Error::InvalidSample(
            "worker has no successful performance-counter query",
        )));
    }
    let mut sample = merge_query_samples(samples)?;
    if !query_failures.is_empty() {
        sample.metadata.merge(PendingMetadata {
            diagnostics: SampleDiagnostics {
                scrape_failures: 1,
                ..Default::default()
            },
            failures: query_failures,
            overflows: Vec::new(),
        });
    }
    recreate_successful_queries(
        queries,
        &collection_succeeded,
        now,
        max_expanded_counters,
        open,
        &mut sample,
    );
    sample
        .metadata
        .failures
        .sort_by_key(|failure| (failure.counter_index, failure.reason));
    Ok(sample)
}

fn query_expansion_limit<A: PdhApi>(
    queries: &[QuerySlot<A>],
    query_index: usize,
    max_expanded_counters: usize,
) -> usize {
    remaining_expansion_capacity(
        max_expanded_counters,
        queries
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != query_index)
            .map(|(_, query)| query.active_expanded_count()),
    )
}

fn remaining_expansion_capacity(
    max_expanded_counters: usize,
    active_counts: impl Iterator<Item = usize>,
) -> usize {
    max_expanded_counters.saturating_sub(active_counts.sum())
}

fn append_query_failures(failures: &mut Vec<SampleFailure>, plan: &QueryPlan, error: &Error) {
    let reason = error.reason();
    let detail = error.bounded_detail();
    failures.extend(
        plan.global_indices
            .iter()
            .map(|&counter_index| SampleFailure {
                counter_index,
                reason,
                error: detail.clone(),
                diagnostic_only: false,
            }),
    );
}

fn recreate_successful_queries<A, F>(
    queries: &mut [QuerySlot<A>],
    collection_succeeded: &[bool],
    now: Instant,
    max_expanded_counters: usize,
    open: &mut F,
    sample: &mut Sample,
) where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize) -> Result<Query<A>, Error>,
{
    for query_index in 0..queries.len() {
        if !collection_succeeded[query_index] {
            continue;
        }
        let query_limit = query_expansion_limit(queries, query_index, max_expanded_counters);
        queries[query_index].recreate(now, query_limit, open, &mut sample.metadata);
    }
}
