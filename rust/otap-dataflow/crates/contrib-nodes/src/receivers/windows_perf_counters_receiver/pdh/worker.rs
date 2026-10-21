// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded worker-thread query ownership, recovery, recreation, and command lifecycle.

use super::super::config::CounterConfig;
use super::super::model::{
    MonotonicPublication, MonotonicPublicationCandidate, PendingMetadata, Sample,
    SampleDiagnostics, SampleFailure, SampleMember,
};
use super::PdhApi;
use super::error::Error;
use super::query::{Query, QueryRecreationSeed, retry_delay};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

pub(super) const QUERY_REBUILD_AFTER_FAILURES: u32 = 3;

#[derive(Clone, Copy)]
pub(in super::super) struct QueryCadence {
    retry_cap: Duration,
    wildcard_refresh: Duration,
}

impl QueryCadence {
    pub(in super::super) fn new(retry_cap: Duration, wildcard_refresh: Duration) -> Self {
        Self {
            retry_cap,
            wildcard_refresh,
        }
    }

    #[cfg(test)]
    pub(super) fn uniform(cadence: Duration) -> Self {
        Self::new(cadence, cadence)
    }

    #[cfg(test)]
    pub(super) fn retry_cap(self) -> Duration {
        self.retry_cap
    }

    #[cfg(test)]
    pub(super) fn wildcard_refresh(self) -> Duration {
        self.wildcard_refresh
    }
}

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
    #[cfg(test)]
    pub(super) async fn start(
        counters: Vec<CounterConfig>,
        cadence: QueryCadence,
        max_instances_per_wildcard: usize,
        max_native_counters: usize,
        node: String,
    ) -> Result<Self, Error> {
        Self::start_with_cadence(
            counters,
            cadence,
            max_instances_per_wildcard,
            max_native_counters,
            node,
        )
        .await
    }

    pub(in super::super) async fn start_with_cadence(
        counters: Vec<CounterConfig>,
        cadence: QueryCadence,
        max_instances_per_wildcard: usize,
        max_native_counters: usize,
        node: String,
    ) -> Result<Self, Error> {
        let has_wildcards = counters
            .iter()
            .any(|counter| counter.instance_config.is_wildcard());
        let mut settings = QuerySettings {
            cadence,
            max_instances_per_wildcard,
            node,
            has_wildcards,
            initialized: false,
        };
        Self::start_controlled(
            counters,
            cadence.retry_cap,
            max_native_counters,
            move |counters, query_limit, seed, shutdown| {
                settings.open(counters, query_limit, seed, shutdown)
            },
        )
        .await
    }

    #[cfg(test)]
    pub(super) async fn start_with_cadence_and<A, F>(
        counters: Vec<CounterConfig>,
        cadence: QueryCadence,
        max_native_counters: usize,
        mut open: F,
    ) -> Result<Self, Error>
    where
        A: PdhApi + 'static,
        F: FnMut(
                Vec<CounterConfig>,
                usize,
                QueryCadence,
                Option<&QueryRecreationSeed>,
            ) -> Result<Query<A>, Error>
            + Send
            + 'static,
    {
        Self::start_with(
            counters,
            cadence.retry_cap,
            max_native_counters,
            move |counters, query_limit, seed| open(counters, query_limit, cadence, seed),
        )
        .await
    }

    #[cfg(test)]
    pub(super) async fn start_with<A, F>(
        counters: Vec<CounterConfig>,
        query_retry_cap: Duration,
        max_native_counters: usize,
        mut open: F,
    ) -> Result<Self, Error>
    where
        A: PdhApi + 'static,
        F: FnMut(
                Vec<CounterConfig>,
                usize,
                Option<&QueryRecreationSeed>,
            ) -> Result<Query<A>, Error>
            + Send
            + 'static,
    {
        Self::start_controlled(
            counters,
            query_retry_cap,
            max_native_counters,
            move |configs, limit, seed, _shutdown| open(configs, limit, seed),
        )
        .await
    }

    async fn start_controlled<A, F>(
        counters: Vec<CounterConfig>,
        query_retry_cap: Duration,
        max_native_counters: usize,
        mut open: F,
    ) -> Result<Self, Error>
    where
        A: PdhApi + 'static,
        F: FnMut(
                Vec<CounterConfig>,
                usize,
                Option<&QueryRecreationSeed>,
                Arc<AtomicBool>,
            ) -> Result<Query<A>, Error>
            + Send
            + 'static,
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
                let factory_shutdown = Arc::clone(&worker_shutdown);
                let mut open = |configs, limit, seed: Option<&QueryRecreationSeed>| {
                    open(configs, limit, seed, Arc::clone(&factory_shutdown))
                };
                run_worker_thread(
                    plans,
                    query_retry_cap,
                    max_native_counters,
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
    max_native_counters: usize,
    open: &mut F,
    rx: mpsc::Receiver<Command>,
    init_tx: oneshot::Sender<Result<(), Error>>,
    completion_tx: oneshot::Sender<()>,
    accepted: Arc<AtomicBool>,
    shutdown_requested: Arc<AtomicBool>,
) where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize, Option<&QueryRecreationSeed>) -> Result<Query<A>, Error>,
{
    let _completion = CompletionSignal(Some(completion_tx));
    let mut queries = match open_initial_queries(
        plans,
        query_retry_cap,
        max_native_counters,
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
        max_native_counters,
        open,
        &rx,
        &accepted,
        &shutdown_requested,
    );
}

pub(super) fn open_initial_queries<A, F>(
    plans: Vec<QueryPlan>,
    query_retry_cap: Duration,
    max_native_counters: usize,
    open: &mut F,
    shutdown_requested: &AtomicBool,
) -> Result<Option<Vec<QuerySlot<A>>>, Error>
where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize, Option<&QueryRecreationSeed>) -> Result<Query<A>, Error>,
{
    let mut queries = Vec::with_capacity(plans.len());
    let mut unopened_exact_reservations = plans
        .iter()
        .map(QueryPlan::exact_counter_reservations)
        .fold(0usize, usize::saturating_add);
    for plan in plans {
        if shutdown_requested.load(Ordering::Acquire) {
            return Ok(None);
        }
        unopened_exact_reservations =
            unopened_exact_reservations.saturating_sub(plan.exact_counter_reservations());
        let query_limit = remaining_native_capacity(
            max_native_counters,
            queries
                .iter()
                .map(QuerySlot::reserved_native_entry_count)
                .chain(std::iter::once(unopened_exact_reservations)),
        );
        queries.push(QuerySlot::open_initial_unprepared(plan, query_limit, open)?);
    }
    if !prepare_initial_queries(
        &mut queries,
        query_retry_cap,
        max_native_counters,
        shutdown_requested,
    )? {
        return Ok(None);
    }
    Ok(Some(queries))
}

fn run_command_loop<A, F>(
    queries: &mut [QuerySlot<A>],
    query_retry_cap: Duration,
    max_native_counters: usize,
    open: &mut F,
    rx: &mpsc::Receiver<Command>,
    accepted: &Arc<AtomicBool>,
    shutdown_requested: &Arc<AtomicBool>,
) where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize, Option<&QueryRecreationSeed>) -> Result<Query<A>, Error>,
{
    while !shutdown_requested.load(Ordering::Acquire) {
        match rx.recv() {
            Ok(Command::Collect(response)) => {
                let accepted = AcceptedCollectGuard::new(Arc::clone(accepted));
                let result = collect_queries(queries, query_retry_cap, max_native_counters, open);
                let terminal = result.as_ref().is_err_and(Error::is_invalid_query);
                drop(accepted);
                let _ = response.send(result);
                if terminal {
                    break;
                }
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
    cadence: QueryCadence,
    max_instances_per_wildcard: usize,
    node: String,
    has_wildcards: bool,
    initialized: bool,
}

impl QuerySettings {
    fn open(
        &mut self,
        counters: Vec<CounterConfig>,
        max_native_counters: usize,
        seed: Option<&QueryRecreationSeed>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Query, Error> {
        initialize_once(&mut self.initialized, || {
            super::native::initialize_worker(&self.node, self.has_wildcards, &shutdown)
        })?;
        let mut query = Query::open_replacement(
            counters,
            self.cadence.retry_cap,
            self.cadence.wildcard_refresh,
            self.max_instances_per_wildcard,
            max_native_counters,
            self.node.clone(),
            seed,
            shutdown,
        )?;
        query.freeze_membership()?;
        Ok(query)
    }
}

pub(super) fn initialize_once(
    initialized: &mut bool,
    initialize: impl FnOnce() -> Result<(), Error>,
) -> Result<(), Error> {
    if *initialized {
        return Ok(());
    }
    initialize()?;
    *initialized = true;
    Ok(())
}

pub(super) struct QueryPlan {
    pub(super) configs: Vec<CounterConfig>,
    pub(super) global_indices: Vec<usize>,
    pub(super) recreate_query: bool,
}

impl QueryPlan {
    fn exact_counter_reservations(&self) -> usize {
        self.configs
            .iter()
            .filter(|config| !config.instance_config.is_wildcard())
            .count()
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum QueryPlanKey {
    Shared,
    Recreated(usize),
}

pub(super) fn query_plans(counters: Vec<CounterConfig>) -> Vec<QueryPlan> {
    let mut plans = Vec::<QueryPlan>::new();
    let mut plan_indices = HashMap::<QueryPlanKey, usize>::new();
    for (global_index, counter) in counters.into_iter().enumerate() {
        let key = if counter.recreate_query {
            QueryPlanKey::Recreated(counter.source_mapping_index)
        } else {
            QueryPlanKey::Shared
        };
        if let Some(&plan_index) = plan_indices.get(&key) {
            let plan = &mut plans[plan_index];
            plan.configs.push(counter);
            plan.global_indices.push(global_index);
            continue;
        }
        let _ = plan_indices.insert(key, plans.len());
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
    query_generation: u64,
    query_failure_attempts: u32,
    next_query_retry: Instant,
    refresh_failure_attempts: u32,
    next_refresh_retry: Instant,
    rebuild_attempts: u32,
    next_rebuild: Instant,
    rebuild_seed: Option<QueryRecreationSeed>,
    last_active_expanded_count: usize,
    last_admitted_members: Vec<SampleMember>,
    pending: PendingMetadata,
}

impl<A: PdhApi> QuerySlot<A> {
    #[cfg(test)]
    pub(super) fn open_initial<F>(
        plan: QueryPlan,
        max_native_counters: usize,
        open: &mut F,
    ) -> Result<Self, Error>
    where
        F: FnMut(
            Vec<CounterConfig>,
            usize,
            Option<&QueryRecreationSeed>,
        ) -> Result<Query<A>, Error>,
    {
        let mut slot = Self::open_initial_unprepared(plan, max_native_counters, open)?;
        if let Some(query) = slot.query.as_mut() {
            query.refresh_all_wildcards_managed()?;
            let prime_error = query.prime_managed().err();
            if prime_error.as_ref().is_some_and(Error::is_invalid_query) {
                return Err(prime_error.expect("invalid-query prime error is present"));
            }
            if let Some(error) = &prime_error {
                query.record_prime_failure(error);
            }
            slot.query_failure_attempts = u32::from(
                prime_error
                    .as_ref()
                    .is_some_and(Error::is_query_collection_failure),
            );
        }
        Ok(slot)
    }

    fn open_initial_unprepared<F>(
        plan: QueryPlan,
        max_native_counters: usize,
        open: &mut F,
    ) -> Result<Self, Error>
    where
        F: FnMut(
            Vec<CounterConfig>,
            usize,
            Option<&QueryRecreationSeed>,
        ) -> Result<Query<A>, Error>,
    {
        let now = Instant::now();
        let query = open(plan.configs.clone(), max_native_counters, None)?;
        let last_active_expanded_count = query.active_expanded_count();
        let last_admitted_members = query.admitted_members();
        Ok(Self {
            plan,
            query: Some(query),
            query_generation: 0,
            query_failure_attempts: 0,
            next_query_retry: now,
            refresh_failure_attempts: 0,
            next_refresh_retry: now,
            rebuild_attempts: 0,
            next_rebuild: now,
            rebuild_seed: None,
            last_active_expanded_count,
            last_admitted_members,
            pending: PendingMetadata::default(),
        })
    }

    fn is_recoverable_collection_error(&self, error: &Error) -> bool {
        !error.is_invalid_query()
            && (error.is_collection_failure() || error.is_recoverable_query_open_failure())
    }

    pub(super) fn collect<F>(
        &mut self,
        now: Instant,
        query_retry_cap: Duration,
        max_native_counters: usize,
        open: &mut F,
    ) -> Result<Sample, Error>
    where
        F: FnMut(
            Vec<CounterConfig>,
            usize,
            Option<&QueryRecreationSeed>,
        ) -> Result<Query<A>, Error>,
    {
        if let Some(query) = self.query.as_mut()
            && query.membership_is_frozen()
        {
            return query.collect_managed();
        }
        let was_query_retry =
            self.query.is_some() && self.query_failure_attempts > 0 && now >= self.next_query_retry;
        if was_query_retry {
            self.pending.diagnostics.retry_attempts += 1;
        }
        let mut result = self.collect_or_rebuild(now, max_native_counters, query_retry_cap, open);
        if result.as_ref().is_err_and(Error::is_invalid_query) {
            return result;
        }
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
        max_native_counters: usize,
        query_retry_cap: Duration,
        open: &mut F,
    ) -> Result<Sample, Error>
    where
        F: FnMut(
            Vec<CounterConfig>,
            usize,
            Option<&QueryRecreationSeed>,
        ) -> Result<Query<A>, Error>,
    {
        if let Some(active_query) = self.query.as_mut() {
            active_query.set_max_native_counters(max_native_counters)?;
            if now < self.next_query_retry {
                Err(Error::QueryRecoveryPending {
                    retry_after_ms: self.next_query_retry.duration_since(now).as_millis(),
                })
            } else {
                active_query.collect_managed()
            }
        } else if now < self.next_rebuild {
            Err(Error::QueryRecoveryPending {
                retry_after_ms: self.next_rebuild.duration_since(now).as_millis(),
            })
        } else {
            self.rebuild(now, max_native_counters, query_retry_cap, open)
        }
    }

    fn rebuild<F>(
        &mut self,
        now: Instant,
        max_native_counters: usize,
        query_retry_cap: Duration,
        open: &mut F,
    ) -> Result<Sample, Error>
    where
        F: FnMut(
            Vec<CounterConfig>,
            usize,
            Option<&QueryRecreationSeed>,
        ) -> Result<Query<A>, Error>,
    {
        self.pending.diagnostics.query_rebuild_attempts += 1;
        let mut rebuilt = match open(
            self.plan.configs.clone(),
            max_native_counters,
            self.rebuild_seed.as_ref(),
        ) {
            Ok(rebuilt) => rebuilt,
            Err(error) => {
                self.rebuild_attempts = self.rebuild_attempts.saturating_add(1);
                self.next_rebuild = now + retry_delay(self.rebuild_attempts, query_retry_cap);
                return Err(error);
            }
        };
        self.rebuild_attempts = 0;
        self.query_failure_attempts = 0;
        self.next_query_retry = now;
        self.refresh_failure_attempts = 0;
        self.next_refresh_retry = now;
        if let Err(error) = rebuilt.prime_managed() {
            self.query_generation = self.query_generation.wrapping_add(1);
            self.rebuild_seed = None;
            self.query = Some(rebuilt);
            return Err(error);
        }
        self.pending.diagnostics.query_rebuild_recoveries += 1;
        let mut sample = Sample::empty(rebuilt.previous_timestamp_unix_nano);
        rebuilt.move_pending_metadata_to(&mut sample.metadata);
        self.last_admitted_members = rebuilt.admitted_members();
        self.query_generation = self.query_generation.wrapping_add(1);
        self.rebuild_seed = None;
        self.query = Some(rebuilt);
        Ok(sample)
    }

    fn recovery_timestamp_unix_nano(&self) -> i64 {
        self.query
            .as_ref()
            .map_or(0, |query| query.previous_timestamp_unix_nano)
    }

    fn move_pending_metadata_to(&mut self, target: &mut PendingMetadata) {
        if let Some(query) = self.query.as_mut() {
            query.move_pending_metadata_to(target);
        }
        target.merge(std::mem::take(&mut self.pending));
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
                self.schedule_rebuild(self.next_query_retry);
            }
        } else if result
            .as_ref()
            .is_err_and(Error::requires_immediate_query_rebuild)
        {
            self.schedule_rebuild(now + retry_delay(1, query_retry_cap));
        } else if result.is_ok() {
            if was_query_retry {
                self.pending.diagnostics.retry_recoveries += 1;
            }
            self.query_failure_attempts = 0;
            self.next_query_retry = now;
        }
    }

    fn update_refresh_recovery_state(
        &mut self,
        now: Instant,
        query_retry_cap: Duration,
        error: &Error,
    ) {
        self.refresh_failure_attempts = self.refresh_failure_attempts.saturating_add(1);
        self.next_refresh_retry = now + retry_delay(self.refresh_failure_attempts, query_retry_cap);
        if error.requires_immediate_query_rebuild() {
            self.schedule_rebuild(self.next_refresh_retry);
            return;
        }
        if should_rebuild_query(self.refresh_failure_attempts) {
            self.schedule_rebuild(self.next_refresh_retry);
        }
    }

    fn record_refresh_success(&mut self, now: Instant) {
        self.refresh_failure_attempts = 0;
        self.next_refresh_retry = now;
    }

    fn move_query_metadata_to_pending(&mut self) {
        if let Some(active_query) = self.query.as_mut() {
            active_query.move_pending_metadata_to(&mut self.pending);
        }
    }

    fn schedule_rebuild(&mut self, next_rebuild: Instant) {
        if self.query.is_none() {
            return;
        }
        self.last_active_expanded_count = self.query.as_ref().map_or(
            self.last_active_expanded_count,
            Query::active_expanded_count,
        );
        if let Some(query) = self.query.as_ref() {
            self.last_admitted_members = query.admitted_members();
        }
        self.move_query_metadata_to_pending();
        self.rebuild_seed = self.query.as_ref().map(Query::recreation_seed);
        self.query = None;
        self.rebuild_attempts = 0;
        self.next_rebuild = next_rebuild;
    }

    fn recreate<F>(
        &mut self,
        now: Instant,
        max_native_counters: usize,
        open: &mut F,
        metadata: &mut PendingMetadata,
    ) where
        F: FnMut(
            Vec<CounterConfig>,
            usize,
            Option<&QueryRecreationSeed>,
        ) -> Result<Query<A>, Error>,
    {
        if !self.plan.recreate_query {
            return;
        }
        if self.query.as_ref().is_some_and(Query::membership_is_frozen) {
            return;
        }
        metadata.diagnostics.query_recreation_attempts += 1;
        let reserved = self.reserved_native_entry_count();
        let replacement = if max_native_counters < reserved {
            Err(Error::QueryRecreationCapacity {
                available: max_native_counters,
                reserved,
            })
        } else {
            let seed = self.query.as_ref().map(Query::recreation_seed);
            open(
                self.plan.configs.clone(),
                max_native_counters,
                seed.as_ref(),
            )
            .and_then(|mut query| {
                query.prime_managed()?;
                Ok(query)
            })
        };
        match replacement {
            Ok(query) => {
                self.last_admitted_members = query.admitted_members();
                self.query = Some(query);
                self.query_failure_attempts = 0;
                self.next_query_retry = now;
                self.rebuild_attempts = 0;
                self.next_rebuild = now;
            }
            Err(error) => {
                metadata.diagnostics.query_recreation_failures += 1;
                metadata.failures.extend(
                    self.plan.global_indices.iter().zip(&self.plan.configs).map(
                        |(&counter_index, config)| SampleFailure {
                            counter_index,
                            path: Arc::from(config.path.as_str()),
                            reason: error.reason(),
                            error: error.bounded_detail(),
                            diagnostic_only: true,
                        },
                    ),
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
            if matches!(
                point.monotonic_publication,
                Some(MonotonicPublicationCandidate::Pending)
            ) {
                point.monotonic_publication =
                    Some(MonotonicPublicationCandidate::Ready(MonotonicPublication {
                        counter_index: point.counter_index,
                        path: Arc::clone(&point.path),
                        query_generation: self.query_generation,
                        timestamp_unix_nano: point.timestamp_unix_nano,
                    }));
            }
        }
        for failure in &mut sample.metadata.failures {
            failure.counter_index = remap(failure.counter_index)?;
        }
        for overflow in &mut sample.metadata.overflows {
            overflow.counter_index = remap(overflow.counter_index)?;
        }
        Ok(sample)
    }

    fn admitted_members(&self) -> Result<Vec<SampleMember>, Error> {
        let mut members = self.query.as_ref().map_or_else(
            || self.last_admitted_members.clone(),
            Query::admitted_members,
        );
        for member in &mut members {
            member.counter_index = self
                .plan
                .global_indices
                .get(member.counter_index)
                .copied()
                .ok_or(Error::InvalidSample(
                    "query membership references an unknown local counter index",
                ))?;
        }
        Ok(members)
    }

    pub(super) fn active_expanded_count(&self) -> usize {
        self.query.as_ref().map_or(
            self.last_active_expanded_count,
            Query::active_expanded_count,
        )
    }

    pub(super) fn reserved_native_entry_count(&self) -> usize {
        let exact_counter_reservations = self.plan.exact_counter_reservations();
        self.query.as_ref().map_or_else(
            || {
                self.rebuild_seed
                    .as_ref()
                    .map_or(exact_counter_reservations, |seed| {
                        seed.reserved_native_entry_count(exact_counter_reservations)
                    })
            },
            Query::reserved_native_entry_count,
        )
    }

    #[cfg(test)]
    pub(super) fn publish_monotonic_start(&mut self, publication: &MonotonicPublication) -> bool {
        if publication.query_generation != self.query_generation {
            return false;
        }
        let Ok(config_index) = self
            .plan
            .global_indices
            .binary_search(&publication.counter_index)
        else {
            return false;
        };
        self.query.as_mut().is_some_and(|query| {
            query.publish_monotonic_start(
                config_index,
                &publication.path,
                publication.timestamp_unix_nano,
            )
        })
    }
}

#[cfg(test)]
pub(super) fn publish_monotonic_starts<A: PdhApi>(
    queries: &mut [QuerySlot<A>],
    publications: Vec<MonotonicPublication>,
) {
    for publication in publications {
        let _ = queries
            .iter_mut()
            .any(|query| query.publish_monotonic_start(&publication));
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
            target.members.extend(sample.members);
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
    max_native_counters: usize,
    open: &mut F,
) -> Result<Sample, Error>
where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize, Option<&QueryRecreationSeed>) -> Result<Query<A>, Error>,
{
    let now = Instant::now();
    let (mut refresh_errors, _) =
        refresh_wildcards_in_config_order(queries, max_native_counters, None);
    let mut samples = Vec::with_capacity(queries.len());
    let mut recreation_eligible = vec![false; queries.len()];
    let mut first_collection_error = None;
    let mut query_failures = Vec::new();
    let mut scrape_failure_observed = false;
    let mut any_query_collected = false;
    for query_index in 0..queries.len() {
        let refresh_failed = refresh_errors[query_index].is_some();
        if let Some(error) = refresh_errors[query_index].take() {
            let query = &mut queries[query_index];
            let recoverable = query.is_recoverable_collection_error(&error)
                || error.requires_immediate_query_rebuild();
            if recoverable {
                query.update_refresh_recovery_state(now, query_retry_cap, &error);
            } else {
                return Err(error);
            }
        }
        let query_limit = query_native_limit(queries, query_index, max_native_counters);
        let query = &mut queries[query_index];
        let generation_before_collection = query.query_generation;
        match query.collect(now, query_retry_cap, query_limit, open) {
            Ok(sample) => {
                any_query_collected |= sample.metadata.diagnostics.any_query_collected;
                recreation_eligible[query_index] =
                    !refresh_failed && query.query_generation == generation_before_collection;
                samples.push(query.remap_sample(sample)?);
            }
            Err(error) if query.is_recoverable_collection_error(&error) => {
                let recovery_only = matches!(&error, Error::QueryRecoveryPending { .. })
                    || query.query_generation != generation_before_collection;
                if !recovery_only {
                    scrape_failure_observed = true;
                }
                append_query_failures(&mut query_failures, &query.plan, &error, recovery_only);
                if recovery_only {
                    let mut sample = Sample::empty(query.recovery_timestamp_unix_nano());
                    sample.metadata.diagnostics.active_expanded_counters =
                        query.active_expanded_count();
                    query.move_pending_metadata_to(&mut sample.metadata);
                    samples.push(sample);
                } else if first_collection_error.is_none() {
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
    sample.metadata.diagnostics.any_query_collected = any_query_collected;
    if !query_failures.is_empty() {
        sample.metadata.merge(PendingMetadata {
            diagnostics: SampleDiagnostics {
                scrape_failures: u64::from(scrape_failure_observed),
                ..Default::default()
            },
            failures: query_failures,
            overflows: Vec::new(),
        });
    }
    recreate_successful_queries(
        queries,
        &recreation_eligible,
        now,
        max_native_counters,
        open,
        &mut sample,
    );
    sample.members = queries
        .iter()
        .map(QuerySlot::admitted_members)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();
    sample.members.sort_by(|left, right| {
        (left.counter_index, &left.path).cmp(&(right.counter_index, &right.path))
    });
    sample
        .metadata
        .failures
        .sort_by_key(|failure| (failure.counter_index, failure.reason));
    Ok(sample)
}

fn prepare_initial_queries<A: PdhApi>(
    queries: &mut [QuerySlot<A>],
    query_retry_cap: Duration,
    max_native_counters: usize,
    shutdown_requested: &AtomicBool,
) -> Result<bool, Error> {
    let now = Instant::now();
    let (mut refresh_errors, stopped) =
        refresh_wildcards_in_config_order(queries, max_native_counters, Some(shutdown_requested));
    if stopped {
        return Ok(false);
    }
    for (query_index, error) in refresh_errors.iter_mut().enumerate() {
        let Some(error) = error.take() else {
            continue;
        };
        let slot = &mut queries[query_index];
        if !slot.is_recoverable_collection_error(&error)
            && !error.requires_immediate_query_rebuild()
        {
            return Err(error);
        }
        slot.update_refresh_recovery_state(now, query_retry_cap, &error);
    }
    for slot in queries {
        if shutdown_requested.load(Ordering::Acquire) {
            return Ok(false);
        }
        let Some(query) = slot.query.as_mut() else {
            continue;
        };
        let prime_error = query.prime_managed().err();
        if prime_error.as_ref().is_some_and(Error::is_invalid_query) {
            return Err(prime_error.expect("invalid-query prime error is present"));
        }
        if let Some(error) = &prime_error {
            query.record_prime_failure(error);
        }
        if slot.query_failure_attempts == 0
            && prime_error
                .as_ref()
                .is_some_and(Error::is_query_collection_failure)
        {
            slot.query_failure_attempts = 1;
            slot.next_query_retry = now;
        }
    }
    Ok(true)
}

fn refresh_wildcards_in_config_order<A: PdhApi>(
    queries: &mut [QuerySlot<A>],
    max_native_counters: usize,
    shutdown_requested: Option<&AtomicBool>,
) -> (Vec<Option<Error>>, bool) {
    let now = Instant::now();
    let due = queries
        .iter()
        .map(|slot| {
            now >= slot.next_refresh_retry
                && slot.query.as_ref().is_some_and(Query::wildcard_refresh_due)
        })
        .collect::<Vec<_>>();
    let mut tasks = queries
        .iter()
        .enumerate()
        .filter(|(query_index, _)| due[*query_index])
        .flat_map(|(query_index, slot)| {
            slot.plan
                .configs
                .iter()
                .enumerate()
                .filter(|(_, config)| config.instance_config.is_wildcard())
                .map(move |(config_index, _)| {
                    (
                        slot.plan.global_indices[config_index],
                        query_index,
                        config_index,
                    )
                })
        })
        .collect::<Vec<_>>();
    tasks.sort_by_key(|(global_index, _, _)| *global_index);

    let mut errors = std::iter::repeat_with(|| None)
        .take(queries.len())
        .collect::<Vec<_>>();
    for (_, query_index, config_index) in tasks {
        if shutdown_requested.is_some_and(|stop| stop.load(Ordering::Acquire)) {
            return (errors, true);
        }
        if errors[query_index].is_some() {
            continue;
        }
        let query_limit = query_native_limit(queries, query_index, max_native_counters);
        let query = queries[query_index]
            .query
            .as_mut()
            .expect("wildcard refresh task requires an active query");
        let result = query
            .set_max_native_counters(query_limit)
            .and_then(|()| query.refresh_wildcard_config_managed(config_index));
        if let Err(error) = result {
            errors[query_index] = Some(error);
        }
    }
    for (query_index, is_due) in due.into_iter().enumerate() {
        if is_due && errors[query_index].is_none() {
            let slot = &mut queries[query_index];
            if let Some(query) = slot.query.as_mut() {
                query.finish_managed_wildcard_refresh();
                slot.record_refresh_success(now);
            }
        }
    }
    (errors, false)
}

fn query_native_limit<A: PdhApi>(
    queries: &[QuerySlot<A>],
    query_index: usize,
    max_native_counters: usize,
) -> usize {
    remaining_native_capacity(
        max_native_counters,
        queries
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != query_index)
            .map(|(_, query)| query.reserved_native_entry_count()),
    )
}

pub(super) fn remaining_native_capacity(
    max_native_counters: usize,
    reserved_counts: impl Iterator<Item = usize>,
) -> usize {
    max_native_counters
        .saturating_sub(reserved_counts.fold(0usize, |total, count| total.saturating_add(count)))
}

fn append_query_failures(
    failures: &mut Vec<SampleFailure>,
    plan: &QueryPlan,
    error: &Error,
    diagnostic_only: bool,
) {
    let reason = error.reason();
    let detail = error.bounded_detail();
    failures.extend(plan.global_indices.iter().zip(&plan.configs).map(
        |(&counter_index, config)| SampleFailure {
            counter_index,
            path: Arc::from(config.path.as_str()),
            reason,
            error: detail.clone(),
            diagnostic_only,
        },
    ));
}

fn recreate_successful_queries<A, F>(
    queries: &mut [QuerySlot<A>],
    collection_succeeded: &[bool],
    now: Instant,
    max_native_counters: usize,
    open: &mut F,
    sample: &mut Sample,
) where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize, Option<&QueryRecreationSeed>) -> Result<Query<A>, Error>,
{
    for query_index in 0..queries.len() {
        if !collection_succeeded[query_index] {
            continue;
        }
        let query_limit = remaining_native_capacity(
            max_native_counters,
            queries.iter().map(QuerySlot::reserved_native_entry_count),
        );
        queries[query_index].recreate(now, query_limit, open, &mut sample.metadata);
    }
}
