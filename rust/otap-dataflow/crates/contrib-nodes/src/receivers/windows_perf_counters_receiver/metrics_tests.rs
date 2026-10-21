// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

impl WindowsPerfCountersReceiverMetrics {
    pub(in super::super) fn scrape_attempts(&self, outcome: Outcome) -> u64 {
        self.scrapes
            .get(OutcomeAttributes { outcome })
            .attempts
            .get()
    }
}
