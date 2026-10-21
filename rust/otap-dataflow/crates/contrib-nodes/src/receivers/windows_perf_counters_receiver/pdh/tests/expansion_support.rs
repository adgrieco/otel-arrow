// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::super::native::NativePdh;
use super::*;

pub(in super::super) fn parse_instance(path: &str) -> Result<InstanceIdentity, Error> {
    NativePdh::parse_instance_path(path)
}

pub(in super::super) fn limit_expanded_paths<T>(
    paths: &mut Vec<T>,
    per_wildcard_limit: usize,
    total_remaining: usize,
) -> ExpansionAllocation {
    let allocation = expansion_allocation(paths.len(), per_wildcard_limit, total_remaining);
    paths.truncate(allocation.retained);
    allocation
}

pub(in super::super) fn is_aggregation_instance(path: &str, aggregation_name: &str) -> bool {
    parse_instance(path).is_ok_and(|instance| matches_aggregation(instance, aggregation_name))
}
