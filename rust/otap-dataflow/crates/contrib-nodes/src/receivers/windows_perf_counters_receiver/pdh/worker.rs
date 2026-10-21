// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded worker-thread query ownership, failure recovery, and command lifecycle.

use super::super::config::CounterConfig;
use super::super::model::{PendingMetadata, Sample, SampleFailure, SampleMember};
use super::PdhApi;
use super::error::Error;
use super::query::{Query, QueryRecreationSeed, retry_delay};
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
    pub(in super::super) startup: StartupState,
}

impl Worker {
    pub(in super::super) async fn start(
        counters: Vec<CounterConfig>,
        cadence: QueryCadence,
        max_instances_per_wildcard: usize,
        max_native_counters: usize,
        node: String,
    ) -> Result<Self, Error> {
        let mut settings = QuerySettings {
            cadence,
            max_instances_per_wildcard,
            node,
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
                    counters,
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
            Ok(Ok(startup)) => {
                cancellation.disarm();
                Ok(Self {
                    tx,
                    accepted,
                    shutdown_requested,
                    join: Some(join),
                    completion: Some(completion_rx),
                    startup,
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
                Err(Error::WorkerStopRequested)
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

#[derive(Clone, Copy, Debug, Default)]
pub(in super::super) struct StartupState {
    pub(in super::super) pending: bool,
    pub(in super::super) active_expanded: usize,
}

#[allow(clippy::too_many_arguments)]
fn run_worker_thread<A, F>(
    configs: Vec<CounterConfig>,
    query_retry_cap: Duration,
    max_native_counters: usize,
    open: &mut F,
    rx: mpsc::Receiver<Command>,
    init_tx: oneshot::Sender<Result<StartupState, Error>>,
    completion_tx: oneshot::Sender<()>,
    accepted: Arc<AtomicBool>,
    shutdown_requested: Arc<AtomicBool>,
) where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize, Option<&QueryRecreationSeed>) -> Result<Query<A>, Error>,
{
    let _completion = CompletionSignal(Some(completion_tx));
    let mut slot = match open_initial_query(
        configs,
        query_retry_cap,
        max_native_counters,
        open,
        &shutdown_requested,
    ) {
        Ok(Some(slot)) => slot,
        Ok(None) => return,
        Err(error) => {
            let _ = init_tx.send(Err(error));
            return;
        }
    };
    let startup = StartupState {
        pending: slot.is_pending(),
        active_expanded: slot.active_expanded_count(),
    };
    if shutdown_requested.load(Ordering::Acquire) || init_tx.send(Ok(startup)).is_err() {
        return;
    }
    while !shutdown_requested.load(Ordering::Acquire) {
        match rx.recv() {
            Ok(Command::Collect(response)) => {
                let accepted = AcceptedCollectGuard::new(Arc::clone(&accepted));
                let result = collect_query(&mut slot, query_retry_cap, max_native_counters, open);
                drop(accepted);
                let _ = response.send(result);
            }
            Ok(Command::Shutdown) | Err(_) => break,
        }
    }
}

pub(super) fn open_initial_query<A, F>(
    configs: Vec<CounterConfig>,
    query_retry_cap: Duration,
    max_native_counters: usize,
    open: &mut F,
    shutdown: &AtomicBool,
) -> Result<Option<QuerySlot<A>>, Error>
where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize, Option<&QueryRecreationSeed>) -> Result<Query<A>, Error>,
{
    if shutdown.load(Ordering::Acquire) {
        return Ok(None);
    }
    let mut slot = QuerySlot::open_unprepared(configs, query_retry_cap, max_native_counters, open)?;
    if shutdown.load(Ordering::Acquire) {
        return Ok(None);
    }
    if let Some(query) = slot.query.as_mut() {
        match query.prime() {
            Ok(()) => {}
            Err(error) => {
                if shutdown.load(Ordering::Acquire) && error.is_stop() {
                    return Ok(None);
                }
                if !error.is_collection_failure() {
                    return Err(error);
                }
                query.record_prime_failure(&error);
                let retry_installed =
                    error.is_query_collection_failure() && !error.is_invalid_query();
                slot.update_recovery_state(Instant::now(), query_retry_cap, false, &Err(error));
                if retry_installed {
                    slot.next_query_retry = Instant::now();
                }
            }
        }
    }
    Ok(Some(slot))
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

struct QuerySettings {
    cadence: QueryCadence,
    max_instances_per_wildcard: usize,
    node: String,
    initialized: bool,
}

impl QuerySettings {
    fn open(
        &mut self,
        configs: Vec<CounterConfig>,
        max_native_counters: usize,
        seed: Option<&QueryRecreationSeed>,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Query, Error> {
        initialize_once(&mut self.initialized, || {
            super::native::initialize_worker(&self.node, &shutdown)
        })?;
        Query::open_replacement(
            configs,
            self.cadence.retry_cap,
            self.cadence.wildcard_refresh,
            self.max_instances_per_wildcard,
            max_native_counters,
            self.node.clone(),
            seed,
            shutdown,
        )
    }
}

pub(super) fn initialize_once(
    initialized: &mut bool,
    initialize: impl FnOnce() -> Result<(), Error>,
) -> Result<(), Error> {
    if !*initialized {
        initialize()?;
        *initialized = true;
    }
    Ok(())
}

pub(super) struct QuerySlot<A: PdhApi> {
    configs: Vec<CounterConfig>,
    pub(super) query: Option<Query<A>>,
    query_failure_attempts: u32,
    next_query_retry: Instant,
    rebuild_attempts: u32,
    next_rebuild: Instant,
    rebuild_seed: Option<QueryRecreationSeed>,
    last_admitted_members: Vec<SampleMember>,
    pending: PendingMetadata,
}

impl<A: PdhApi> QuerySlot<A> {
    #[cfg(test)]
    pub(super) fn open_initial<F>(
        configs: Vec<CounterConfig>,
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
        open_initial_query(
            configs,
            Duration::ZERO,
            max_native_counters,
            open,
            &AtomicBool::new(false),
        )?
        .ok_or(Error::WorkerStopRequested)
    }

    fn open_unprepared<F>(
        configs: Vec<CounterConfig>,
        retry_cap: Duration,
        limit: usize,
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
        let mut slot = Self {
            configs,
            query: None,
            query_failure_attempts: 0,
            next_query_retry: now,
            rebuild_attempts: 0,
            next_rebuild: now,
            rebuild_seed: None,
            last_admitted_members: Vec::new(),
            pending: PendingMetadata::default(),
        };
        match open(slot.configs.clone(), limit, None) {
            Ok(query) => {
                slot.last_admitted_members = query.admitted_members();
                slot.query = Some(query);
            }
            Err(error) if error.is_recoverable_query_open_failure() => {
                slot.record_query_failure(&error);
                slot.rebuild_attempts = 1;
                slot.next_rebuild = now + retry_delay(1, retry_cap);
            }
            Err(error) => return Err(error),
        }
        Ok(slot)
    }

    fn is_pending(&self) -> bool {
        self.query_failure_attempts > 0
            || self
                .query
                .as_ref()
                .is_none_or(|query| query.active_counter_count() == 0)
    }

    fn record_query_failure(&mut self, error: &Error) {
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

    pub(super) fn collect<F>(
        &mut self,
        now: Instant,
        retry_cap: Duration,
        limit: usize,
        open: &mut F,
    ) -> Result<Sample, Error>
    where
        F: FnMut(
            Vec<CounterConfig>,
            usize,
            Option<&QueryRecreationSeed>,
        ) -> Result<Query<A>, Error>,
    {
        let was_retry =
            self.query.is_some() && self.query_failure_attempts > 0 && now >= self.next_query_retry;
        if was_retry {
            self.pending.diagnostics.retry_attempts += 1;
        }
        let mut result = self.collect_or_rebuild(now, limit, retry_cap, open);
        self.update_recovery_state(now, retry_cap, was_retry, &result);
        if let Ok(sample) = &mut result {
            let collected = sample.metadata.diagnostics.any_query_collected;
            sample
                .metadata
                .merge_older(std::mem::take(&mut self.pending));
            sample.metadata.diagnostics.any_query_collected = collected;
        }
        result
    }

    fn collect_or_rebuild<F>(
        &mut self,
        now: Instant,
        limit: usize,
        retry_cap: Duration,
        open: &mut F,
    ) -> Result<Sample, Error>
    where
        F: FnMut(
            Vec<CounterConfig>,
            usize,
            Option<&QueryRecreationSeed>,
        ) -> Result<Query<A>, Error>,
    {
        if let Some(query) = self.query.as_mut() {
            if now < self.next_query_retry {
                Err(Error::QueryRecoveryPending {
                    retry_after_ms: self.next_query_retry.duration_since(now).as_millis(),
                })
            } else {
                query.collect()
            }
        } else if now < self.next_rebuild {
            Err(Error::QueryRecoveryPending {
                retry_after_ms: self.next_rebuild.duration_since(now).as_millis(),
            })
        } else {
            self.rebuild(now, limit, retry_cap, open)
        }
    }

    fn rebuild<F>(
        &mut self,
        now: Instant,
        limit: usize,
        retry_cap: Duration,
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
        let mut rebuilt = match open(self.configs.clone(), limit, self.rebuild_seed.as_ref()) {
            Ok(query) => query,
            Err(error) => {
                self.rebuild_attempts = self.rebuild_attempts.saturating_add(1);
                self.next_rebuild = now + retry_delay(self.rebuild_attempts, retry_cap);
                return Err(error);
            }
        };
        self.rebuild_attempts = 0;
        self.query_failure_attempts = 0;
        self.next_query_retry = now;
        let primed = rebuilt.prime();
        self.last_admitted_members = rebuilt.admitted_members();
        self.query = Some(rebuilt);
        self.rebuild_seed = None;
        primed?;
        self.pending.diagnostics.query_rebuild_recoveries += 1;
        let mut sample = Sample::empty(self.recovery_timestamp_unix_nano());
        self.move_pending_metadata_to(&mut sample.metadata);
        sample.metadata.diagnostics.any_query_collected = false;
        Ok(sample)
    }

    fn update_recovery_state(
        &mut self,
        now: Instant,
        retry_cap: Duration,
        was_retry: bool,
        result: &Result<Sample, Error>,
    ) {
        if result.as_ref().is_err_and(|error| {
            error.is_invalid_query() || error.requires_immediate_query_rebuild()
        }) {
            self.schedule_rebuild(now + retry_delay(1, retry_cap));
        } else if result
            .as_ref()
            .is_err_and(Error::is_query_collection_failure)
        {
            self.query_failure_attempts = self.query_failure_attempts.saturating_add(1);
            self.next_query_retry = now + retry_delay(self.query_failure_attempts, retry_cap);
            if should_rebuild_query(self.query_failure_attempts) {
                self.schedule_rebuild(self.next_query_retry);
            }
        } else if result.is_ok() {
            if was_retry {
                self.pending.diagnostics.retry_recoveries += 1;
            }
            self.query_failure_attempts = 0;
            self.next_query_retry = now;
        }
    }

    fn schedule_rebuild(&mut self, next_rebuild: Instant) {
        let Some(query) = self.query.as_mut() else {
            return;
        };
        self.last_admitted_members = query.admitted_members();
        query.move_pending_metadata_to(&mut self.pending);
        self.rebuild_seed = Some(query.recreation_seed());
        self.query = None;
        self.rebuild_attempts = 0;
        self.next_rebuild = next_rebuild;
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

    pub(super) fn active_expanded_count(&self) -> usize {
        self.query.as_ref().map_or(0, Query::active_expanded_count)
    }

    #[cfg(test)]
    pub(super) fn reserved_native_entry_count(&self) -> usize {
        let exact = self
            .configs
            .iter()
            .filter(|config| !config.instance_config.is_wildcard())
            .count();
        self.query.as_ref().map_or_else(
            || {
                self.rebuild_seed
                    .as_ref()
                    .map_or(exact, |seed| seed.reserved_native_entry_count(exact))
            },
            Query::reserved_native_entry_count,
        )
    }
}

pub(super) fn collect_query<A, F>(
    slot: &mut QuerySlot<A>,
    retry_cap: Duration,
    limit: usize,
    open: &mut F,
) -> Result<Sample, Error>
where
    A: PdhApi,
    F: FnMut(Vec<CounterConfig>, usize, Option<&QueryRecreationSeed>) -> Result<Query<A>, Error>,
{
    let now = Instant::now();
    let was_rebuilding = slot.query.is_none();
    let mut sample = match slot.collect(now, retry_cap, limit, open) {
        Ok(sample) => sample,
        Err(error)
            if error.is_collection_failure() || error.is_recoverable_query_open_failure() =>
        {
            let maintenance_cleanup = error.requires_immediate_query_rebuild()
                && !slot.pending.diagnostics.any_query_collected;
            if !was_rebuilding
                && !matches!(error, Error::QueryRecoveryPending { .. })
                && !maintenance_cleanup
            {
                return Err(error);
            }
            slot.record_query_failure(&error);
            let mut sample = Sample::empty(slot.recovery_timestamp_unix_nano());
            slot.move_pending_metadata_to(&mut sample.metadata);
            sample.metadata.diagnostics.any_query_collected = false;
            sample
        }
        Err(error) => return Err(error),
    };
    sample.members = slot.query.as_ref().map_or_else(
        || slot.last_admitted_members.clone(),
        Query::admitted_members,
    );
    sample.metadata.diagnostics.active_expanded_counters = slot.active_expanded_count();
    sample.metadata.diagnostics.query_pending = slot.is_pending();
    sample
        .metadata
        .failures
        .sort_by_key(|failure| (failure.counter_index, failure.reason));
    Ok(sample)
}
