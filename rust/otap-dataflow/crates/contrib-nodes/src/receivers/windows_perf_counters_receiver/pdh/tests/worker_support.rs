// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl QueryCadence {
    pub(in super::super) fn uniform(cadence: Duration) -> Self {
        Self::new(cadence, cadence)
    }

    pub(in super::super) fn retry_cap(self) -> Duration {
        self.retry_cap
    }

    pub(in super::super) fn wildcard_refresh(self) -> Duration {
        self.wildcard_refresh
    }
}

impl Worker {
    pub(in super::super) async fn start_with<A, F>(
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
}

impl<A: PdhApi> QuerySlot<A> {
    pub(in super::super) fn open_initial<F>(
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

    pub(in super::super) fn reserved_native_entry_count(&self) -> usize {
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
