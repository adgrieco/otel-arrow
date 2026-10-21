// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl<A: PdhApi> Query<A> {
    pub(in super::super) fn reserved_native_entry_count(&self) -> usize {
        self.exact_counter_reservations + self.reserved_expanded_count()
    }

    pub(in super::super) fn set_max_native_counters(
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
}

impl QueryRecreationSeed {
    pub(in super::super) fn reserved_native_entry_count(
        &self,
        exact_counter_reservations: usize,
    ) -> usize {
        exact_counter_reservations.saturating_add(self.wildcard_targets.len())
    }
}
