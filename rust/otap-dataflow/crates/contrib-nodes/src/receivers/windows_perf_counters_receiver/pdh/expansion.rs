// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::super::model::InstanceIdentity;
use super::error::Error;
use super::native::NativePdh;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn parse_instance(path: &str) -> Result<InstanceIdentity, Error> {
    NativePdh::parse_instance_path(path)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CounterKey {
    pub(super) config_index: usize,
    normalized_path: String,
}

impl CounterKey {
    pub(super) fn new(config_index: usize, path: &str) -> Self {
        Self {
            config_index,
            normalized_path: path.to_lowercase(),
        }
    }
}

pub(super) fn limit_expanded_paths(
    paths: &mut Vec<String>,
    per_wildcard_limit: usize,
    total_remaining: usize,
) -> (usize, usize) {
    let (retained, per_wildcard_omitted, total_omitted) =
        expansion_allocation(paths.len(), per_wildcard_limit, total_remaining);
    paths.truncate(retained);
    (per_wildcard_omitted, total_omitted)
}

pub(super) fn expansion_allocation(
    discovered: usize,
    per_wildcard_limit: usize,
    total_remaining: usize,
) -> (usize, usize, usize) {
    let per_wildcard_retained = discovered.min(per_wildcard_limit);
    let per_wildcard_omitted = discovered - per_wildcard_retained;
    let retained = per_wildcard_retained.min(total_remaining);
    let total_omitted = per_wildcard_retained - retained;
    (retained, per_wildcard_omitted, total_omitted)
}

pub(super) fn is_aggregation_instance(path: &str, aggregation_name: &str) -> bool {
    parse_instance(path).is_ok_and(|instance| instance.name == aggregation_name)
}

pub(super) fn cleanup_aggregation_instances(
    paths: &mut Vec<String>,
    aggregation_name: Option<&str>,
) -> bool {
    if let Some(aggregation_name) = aggregation_name {
        if paths.len() == 1 && is_aggregation_instance(&paths[0], aggregation_name) {
            return true;
        }
        paths.retain(|path| !is_aggregation_instance(path, aggregation_name));
    }
    false
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct InstanceGroupKey {
    config_index: usize,
    normalized_name: String,
    normalized_parent: Option<String>,
}

impl InstanceGroupKey {
    fn new(config_index: usize, instance: &InstanceIdentity) -> Self {
        Self {
            config_index,
            normalized_name: instance.name.to_lowercase(),
            normalized_parent: instance.parent.as_ref().map(|value| value.to_lowercase()),
        }
    }
}

pub(super) fn instance_groups<'a>(
    counters: impl Iterator<Item = (&'a CounterKey, &'a InstanceIdentity)>,
) -> BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>> {
    let mut groups = BTreeMap::new();
    for (key, instance) in counters {
        let _ = groups
            .entry(InstanceGroupKey::new(key.config_index, instance))
            .or_insert_with(BTreeSet::new)
            .insert(key.clone());
    }
    groups
}

pub(super) fn changed_instance_groups(
    existing: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
    wanted: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
) -> BTreeSet<InstanceGroupKey> {
    existing
        .keys()
        .chain(wanted.keys())
        .filter(|group| existing.get(*group) != wanted.get(*group))
        .cloned()
        .collect()
}

pub(super) fn refresh_changes_with_group_resets(
    live: &BTreeSet<CounterKey>,
    retrying: &BTreeSet<CounterKey>,
    wanted: &BTreeSet<CounterKey>,
    known_groups: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
    wanted_groups: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
) -> (Vec<CounterKey>, Vec<CounterKey>) {
    let changed_groups = changed_instance_groups(known_groups, wanted_groups);
    let reset_live = changed_groups
        .iter()
        .filter_map(|group| known_groups.get(group))
        .flatten()
        .filter(|key| live.contains(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    let reset_wanted = changed_groups
        .iter()
        .filter_map(|group| wanted_groups.get(group))
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut removed = live.difference(wanted).cloned().collect::<Vec<_>>();
    let mut added = wanted
        .difference(live)
        .filter(|key| !retrying.contains(*key))
        .cloned()
        .collect::<Vec<_>>();
    removed.extend(reset_live);
    removed.sort();
    removed.dedup();
    added.extend(reset_wanted);
    added.sort();
    added.dedup();
    (removed, added)
}
