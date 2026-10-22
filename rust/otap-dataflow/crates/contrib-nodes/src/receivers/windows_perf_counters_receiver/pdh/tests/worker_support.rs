// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl QueryCadence {
    pub(in super::super) fn uniform(cadence: Duration) -> Self {
        Self::new(cadence, cadence)
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
            "test".to_owned(),
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
}
