// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Scenario: Two metric mappings use the same exact PDH path.
/// Guarantees: The query adds two handles and routes one point to each configured mapping index.
#[test]
fn same_path_mappings_use_independent_query_entries() {
    let path = r"\Memory\Available Bytes";
    let configs = vec![
        counter(path, "windows.memory.available"),
        counter(path, "windows.memory.available_copy"),
    ];
    let (state, mut query) = fake_query(
        configs,
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x0001_0100,
            raw: None,
            multi_count: 1,
            formatted: Some(Number::Integer(42)),
            raw_error: false,
            formatted_error: false,
        }],
    );

    assert_eq!(state.borrow().added, [path, path]);
    let sample = query.collect().unwrap();
    assert_eq!(
        sample
            .points
            .iter()
            .map(|point| point.counter_index)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert!(
        sample
            .points
            .iter()
            .all(|point| matches!(point.value, SampleValue::Value(Number::Integer(42))))
    );
}

/// Scenario: Startup priming reads a calculated Gauge and native delta Sum before their first scrape.
/// Guarantees: The first requested scrape emits both values using receiver-side readiness and the prime timestamp.
#[test]
fn startup_prime_seeds_two_sample_histories() {
    let rate_path = r"\Processor(_Total)\% Processor Time";
    let delta_path = r"\Provider\Operations Delta";
    let (state, mut query) = fake_query(
        vec![
            counter(rate_path, "system.cpu.time"),
            delta_sum(delta_path, "provider.operations", false),
        ],
        vec![
            FakeCounter {
                path: rate_path.to_owned(),
                native_type: 0x2041_0500,
                raw: Some(10),
                multi_count: 1,
                formatted: Some(Number::Double(1.0)),
                raw_error: false,
                formatted_error: false,
            },
            FakeCounter {
                path: delta_path.to_owned(),
                native_type: 0x0040_0500,
                raw: Some(10),
                multi_count: 1,
                formatted: Some(Number::Double(2.0)),
                raw_error: false,
                formatted_error: false,
            },
        ],
    );

    query.prime().unwrap();
    {
        let mut state = state.borrow_mut();
        state.timestamp = 110;
        state.counters[0].raw = Some(20);
        state.counters[1].raw = Some(20);
        state.counters[1].formatted = Some(Number::Double(3.0));
    }
    let sample = query.collect().unwrap();
    assert!(matches!(
        sample.points[0].value,
        SampleValue::Value(Number::Double(1.0))
    ));
    assert_eq!(sample.points[1].start_time_unix_nano, Some(100));
    assert!(matches!(
        sample.points[1].value,
        SampleValue::Value(Number::Double(3.0))
    ));
    assert_eq!(sample.diagnostics.warmup_omissions, 2);
}

/// Scenario: A calculated counter is mapped to a cumulative non-monotonic Sum.
/// Guarantees: The emitted point receives the query start time directly from counter sampling.
#[test]
fn cumulative_non_monotonic_sum_uses_query_start_time() {
    let path = r"\Process(_Total)\Private Bytes";
    let config = CounterConfig {
        metric_kind: MetricKind::Sum {
            aggregation: Aggregation::Cumulative,
            monotonic: false,
        },
        ..counter(path, "windows.process.private")
    };
    let (state, mut query) = fake_query(
        vec![config],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x2041_0500,
            raw: Some(10),
            multi_count: 1,
            formatted: Some(Number::Double(1.0)),
            raw_error: false,
            formatted_error: false,
        }],
    );

    query.prime().unwrap();
    {
        let mut state = state.borrow_mut();
        state.timestamp = 110;
        state.counters[0].raw = Some(20);
        state.counters[0].formatted = Some(Number::Double(2.0));
    }

    let sample = query.collect().unwrap();
    assert_eq!(sample.points[0].start_time_unix_nano, Some(100));
    assert!(matches!(
        sample.points[0].value,
        SampleValue::Value(Number::Double(2.0))
    ));
}

/// Scenario: The native startup collection reports no data before a calculated counter becomes valid.
/// Guarantees: Priming fabricates no readiness, so the first valid sample warms and the following sample emits.
#[test]
fn startup_prime_no_data_requires_fresh_warmup() {
    let path = r"\Processor(_Total)\% Processor Time";
    let (state, mut query) = fake_query(
        vec![counter(path, "system.cpu.time")],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x2041_0500,
            raw: Some(10),
            multi_count: 1,
            formatted: Some(Number::Double(1.0)),
            raw_error: false,
            formatted_error: false,
        }],
    );
    state.borrow_mut().collect_has_data = false;

    query.prime().unwrap();
    {
        let mut state = state.borrow_mut();
        state.collect_has_data = true;
        state.timestamp = 110;
    }
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::Warming
    );
    {
        let mut state = state.borrow_mut();
        state.timestamp = 120;
        state.counters[0].raw = Some(20);
    }
    assert!(matches!(
        query.collect().unwrap().points[0].value,
        SampleValue::Value(Number::Double(1.0))
    ));
}

/// Scenario: A direct counter has a counter-local formatted failure during startup priming.
/// Guarantees: Initialization continues and the priming failure is delivered with the next successful scrape.
#[test]
fn startup_prime_preserves_counter_failures() {
    let path = r"\Memory\Available Bytes";
    let (state, mut query) = fake_query(
        vec![counter(path, "system.memory.available")],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x0001_0100,
            raw: None,
            multi_count: 1,
            formatted: Some(Number::Integer(42)),
            raw_error: false,
            formatted_error: true,
        }],
    );

    query.prime().unwrap();
    state.borrow_mut().counters[0].formatted_error = false;
    let sample = query.collect().unwrap();
    assert_eq!(sample.points.len(), 1);
    assert_eq!(sample.failures.len(), 1);
    assert_eq!(sample.failures[0].counter_index, 0);
    assert_eq!(sample.diagnostics.counter_read_failures, 1);
}

/// Scenario: One exact configured path names a duplicate process instance.
/// Guarantees: Query opening preserves parsed instance identity for non-wildcard counters.
#[test]
fn exact_instance_path_preserves_identity() {
    let path = r"\Process(worker#1)\Private Bytes";
    let mut config = counter(path, "windows.process.private");
    config.has_instance = true;
    config.emit_instance_attribute = true;
    let (_, mut query) = fake_query(
        vec![config],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x0001_0100,
            raw: None,
            multi_count: 1,
            formatted: Some(Number::Integer(42)),
            raw_error: false,
            formatted_error: false,
        }],
    );

    let sample = query.collect().unwrap();
    assert_eq!(
        sample.points[0].instance,
        Some(InstanceIdentity {
            name: "worker".to_owned(),
            parent: None,
            index: 1,
        })
    );
}

/// Scenario: An exact configured instance path cannot be parsed for optional identity attributes.
/// Guarantees: The native counter still emits its value and reports only diagnostic identity evidence.
#[test]
fn exact_instance_parse_failure_preserves_value() {
    let path = r"\Object\Counter";
    let mut config = counter(path, "object.counter");
    config.has_instance = true;
    config.emit_instance_attribute = true;
    let (_, mut query) = fake_query(
        vec![config],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x0001_0100,
            raw: None,
            multi_count: 1,
            formatted: Some(Number::Integer(42)),
            raw_error: false,
            formatted_error: false,
        }],
    );

    let sample = query.collect().unwrap();
    assert_eq!(sample.points.len(), 1);
    assert_eq!(sample.points[0].instance, None);
    assert!(!sample.points[0].emit_instance_attribute);
    assert_eq!(sample.failures.len(), 1);
    assert!(sample.failures[0].diagnostic_only);
    assert_eq!(sample.diagnostics.counter_add_failures, 0);
}

/// Scenario: Counter inspection fails and native cleanup also reports an error.
/// Guarantees: The counter-local diagnostic preserves both failures instead of silently dropping cleanup evidence.
#[test]
fn setup_diagnostic_preserves_cleanup_failure() {
    let path = r"\Object\Unsupported";
    let state = Rc::new(RefCell::new(FakeState {
        counters: vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x4003_0401,
            raw: None,
            multi_count: 1,
            formatted: None,
            raw_error: false,
            formatted_error: false,
        }],
        added: Vec::new(),
        collect_has_data: true,
        collect_error: false,
        timestamp: 100,
        timestamp_error: false,
        remove_error: true,
        removed: Vec::new(),
        expansions: BTreeMap::new(),
    }));
    let mut query = Query::open_with(
        FakePdh(Rc::clone(&state)),
        vec![counter(path, "windows.unsupported")],
        Duration::from_secs(30),
        256,
        4_096,
    )
    .unwrap();

    let sample = query.collect().unwrap();
    assert_eq!(sample.failures.len(), 1);
    assert!(
        sample.failures[0].error.contains("unsupported"),
        "{}",
        sample.failures[0].error
    );
    assert!(sample.failures[0].error.contains("PdhRemoveCounter"));
    assert_eq!(state.borrow().removed, vec![0]);
}

/// Scenario: Expanded paths arrive in different orders and include duplicate instance names.
/// Guarantees: Stable keys use configured identity plus the full case-insensitive PDH path.
#[test]
fn stable_keys_do_not_depend_on_expansion_order() {
    let first = [
        CounterKey::new(0, r"\Process(worker)\Private Bytes"),
        CounterKey::new(0, r"\Process(worker#1)\Private Bytes"),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let reordered = [
        CounterKey::new(0, r"\PROCESS(WORKER#1)\PRIVATE BYTES"),
        CounterKey::new(0, r"\PROCESS(WORKER)\PRIVATE BYTES"),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    assert_eq!(first, reordered);
    assert_ne!(
        CounterKey::new(0, r"\Process(worker)\Private Bytes"),
        CounterKey::new(0, r"\Process(worker#1)\Private Bytes")
    );
    assert_ne!(
        CounterKey::new(0, r"\Process(worker)\Private Bytes"),
        CounterKey::new(1, r"\Process(worker)\Private Bytes")
    );
}

/// Scenario: A refresh reorders retained instances while adding one and removing another.
/// Guarantees: Only stable-key set differences change handles, so retained history cannot move positionally.
#[test]
fn refresh_delta_preserves_retained_instance_keys() {
    let retained = CounterKey::new(0, r"\Process(worker#1)\Private Bytes");
    let removed = CounterKey::new(0, r"\Process(old)\Private Bytes");
    let added = CounterKey::new(0, r"\Process(new)\Private Bytes");
    let existing = [removed.clone(), retained.clone()]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let wanted = [added.clone(), retained.clone()]
        .into_iter()
        .rev()
        .collect::<BTreeSet<_>>();
    let (actual_removed, actual_added) = refresh_changes_with_group_resets(
        &existing,
        &BTreeSet::new(),
        &wanted,
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    assert_eq!(actual_removed, vec![removed]);
    assert_eq!(actual_added, vec![added]);
    assert!(existing.contains(&retained));
    assert!(wanted.contains(&retained));
}

/// Scenario: Discovery returns more sorted paths than either configured expansion bound allows.
/// Guarantees: The retained prefix is deterministic and every omitted instance is counted explicitly.
#[test]
fn expansion_limits_are_deterministic_and_report_omissions() {
    let mut paths = vec!["charlie".to_owned(), "alpha".to_owned(), "bravo".to_owned()];
    paths.sort();

    let omitted = limit_expanded_paths(&mut paths, 2, 10);

    assert_eq!(paths, ["alpha", "bravo"]);
    assert_eq!(omitted, (1, 0));

    let omitted = limit_expanded_paths(&mut paths, 2, 1);
    assert_eq!(paths, ["alpha"]);
    assert_eq!(omitted, (0, 1));
}

/// Scenario: An earlier configured wildcard succeeds while a later wildcard's discovery fails with retained identities.
/// Guarantees: Failed-discovery retention reserves global capacity before successful discoveries regardless of configuration order.
#[test]
fn failed_discovery_retention_reserves_global_capacity_first() {
    let total_limit = 4;
    let (retained_failed, failed_per_omitted, failed_total_omitted) =
        expansion_allocation(3, 10, total_limit);
    let (retained_success, success_per_omitted, success_total_omitted) =
        expansion_allocation(3, 10, total_limit - retained_failed);

    assert_eq!(retained_failed, 3);
    assert_eq!((failed_per_omitted, failed_total_omitted), (0, 0));
    assert_eq!(retained_success, 1);
    assert_eq!((success_per_omitted, success_total_omitted), (0, 2));
    assert_eq!(retained_failed + retained_success, total_limit);
}

/// Scenario: Refresh returns the same duplicate-name membership in a different order.
/// Guarantees: Stable membership does not recreate handles or reset calculated history.
#[test]
fn unchanged_duplicate_group_preserves_handles() {
    let worker = CounterKey::new(0, r"\Process(worker)\% Processor Time");
    let worker_duplicate = CounterKey::new(0, r"\Process(worker#1)\% Processor Time");
    let instance = InstanceIdentity {
        name: "worker".to_owned(),
        parent: None,
        index: 0,
    };
    let duplicate_instance = InstanceIdentity {
        name: "worker".to_owned(),
        parent: None,
        index: 1,
    };
    let existing = [worker.clone(), worker_duplicate.clone()]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let wanted = [worker_duplicate.clone(), worker.clone()]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let existing_groups = instance_groups(
        [
            (&worker, &instance),
            (&worker_duplicate, &duplicate_instance),
        ]
        .into_iter(),
    );
    let wanted_groups = instance_groups(
        [
            (&worker_duplicate, &duplicate_instance),
            (&worker, &instance),
        ]
        .into_iter(),
    );

    let (removed, added) = refresh_changes_with_group_resets(
        &existing,
        &BTreeSet::new(),
        &wanted,
        &existing_groups,
        &wanted_groups,
    );

    assert!(removed.is_empty());
    assert!(added.is_empty());
}

/// Scenario: One member of a duplicate-name group has a live handle while its peer waits in retry state and discovery still returns both.
/// Guarantees: Logical membership remains unchanged, so the healthy live handle is not recreated and the retry keeps its backoff.
#[test]
fn retrying_duplicate_does_not_reset_healthy_peer() {
    let live_worker = CounterKey::new(0, r"\Process(worker)\% Processor Time");
    let retrying_worker = CounterKey::new(0, r"\Process(worker#1)\% Processor Time");
    let live_instance = InstanceIdentity {
        name: "worker".to_owned(),
        parent: None,
        index: 0,
    };
    let retrying_instance = InstanceIdentity {
        name: "worker".to_owned(),
        parent: None,
        index: 1,
    };
    let live = [live_worker.clone()].into_iter().collect::<BTreeSet<_>>();
    let retrying = [retrying_worker.clone()]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let wanted = [live_worker.clone(), retrying_worker.clone()]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let known_groups = instance_groups(
        [
            (&live_worker, &live_instance),
            (&retrying_worker, &retrying_instance),
        ]
        .into_iter(),
    );
    let wanted_groups = instance_groups(
        [
            (&retrying_worker, &retrying_instance),
            (&live_worker, &live_instance),
        ]
        .into_iter(),
    );

    let (removed, added) =
        refresh_changes_with_group_resets(&live, &retrying, &wanted, &known_groups, &wanted_groups);

    assert!(removed.is_empty());
    assert!(added.is_empty());
}

/// Scenario: One of two duplicate-name processes exits and PDH renumbers the survivor.
/// Guarantees: Every handle in that duplicate group is recreated so no native or receiver history crosses process identities.
#[test]
fn duplicate_group_churn_recreates_retained_paths() {
    let worker = CounterKey::new(0, r"\Process(worker)\% Processor Time");
    let worker_duplicate = CounterKey::new(0, r"\Process(worker#1)\% Processor Time");
    let unrelated = CounterKey::new(0, r"\Process(other)\% Processor Time");
    let worker_instance = InstanceIdentity {
        name: "worker".to_owned(),
        parent: None,
        index: 0,
    };
    let worker_duplicate_instance = InstanceIdentity {
        name: "worker".to_owned(),
        parent: None,
        index: 1,
    };
    let unrelated_instance = InstanceIdentity {
        name: "other".to_owned(),
        parent: None,
        index: 0,
    };
    let existing = [worker.clone(), worker_duplicate.clone(), unrelated.clone()]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let wanted = [worker.clone(), unrelated.clone()]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let existing_groups = instance_groups(
        [
            (&worker, &worker_instance),
            (&worker_duplicate, &worker_duplicate_instance),
            (&unrelated, &unrelated_instance),
        ]
        .into_iter(),
    );
    let wanted_groups = instance_groups(
        [
            (&worker, &worker_instance),
            (&unrelated, &unrelated_instance),
        ]
        .into_iter(),
    );

    let (removed, added) = refresh_changes_with_group_resets(
        &existing,
        &BTreeSet::new(),
        &wanted,
        &existing_groups,
        &wanted_groups,
    );

    assert_eq!(
        removed.iter().cloned().collect::<BTreeSet<_>>(),
        [worker.clone(), worker_duplicate]
            .into_iter()
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(added, vec![worker]);
    assert!(!removed.contains(&unrelated));
    assert!(!added.contains(&unrelated));
}

/// Scenario: A wildcard expansion contains exact-case, case-distinct, and concrete instances.
/// Guarantees: Aggregate matching is case-sensitive and does not confuse other provider instances.
#[test]
fn identifies_provider_aggregation_instances() {
    assert!(is_aggregation_instance(
        r"\Process(_Total)\Private Bytes",
        "_Total"
    ));
    assert!(!is_aggregation_instance(
        r"\Custom(_GLOBAL_)\Counter",
        "_Global_"
    ));
    assert!(!is_aggregation_instance(
        r"\Process(worker)\Private Bytes",
        "_Total"
    ));
}

/// Scenario: A wildcard aggregate sorts before concrete instances under a one-instance limit.
/// Guarantees: Aggregate filtering happens before limits so capacity is available to real instances.
#[test]
fn filters_aggregation_before_expansion_limits() {
    let mut paths = vec![
        r"\Process(_Total)\Private Bytes".to_owned(),
        r"\Process(worker)\Private Bytes".to_owned(),
    ];
    assert!(!cleanup_aggregation_instances(&mut paths, Some("_Total")));
    let omitted = limit_expanded_paths(&mut paths, 1, 1);
    assert_eq!(omitted, (0, 0));
    assert_eq!(paths, [r"\Process(worker)\Private Bytes"]);
}

/// Scenario: A wildcard discovery returns only the configured aggregate instance.
/// Guarantees: The aggregate value is retained while its Go-compatible `instance` label is omitted.
#[test]
fn retains_wildcard_only_aggregation_without_instance_label() {
    let template = r"\Process(*)\Private Bytes";
    let aggregate = r"\Process(_Total)\Private Bytes";
    let mut config = counter(template, "windows.process.private");
    config.has_instance = true;
    config.emit_instance_attribute = true;
    config.excluded_aggregation_instance = Some("_Total".to_owned());
    let (state, mut query) = fake_query(
        vec![config],
        vec![FakeCounter {
            path: aggregate.to_owned(),
            native_type: 0x0001_0100,
            raw: None,
            multi_count: 1,
            formatted: Some(Number::Integer(42)),
            raw_error: false,
            formatted_error: false,
        }],
    );
    let _ = state
        .borrow_mut()
        .expansions
        .insert(template.to_owned(), vec![aggregate.to_owned()]);
    query.last_wildcard_refresh = Instant::now() - Duration::from_secs(31);

    let sample = query.collect().unwrap();
    assert_eq!(sample.points.len(), 1);
    assert_eq!(
        sample.points[0]
            .instance
            .as_ref()
            .map(|value| value.name.as_str()),
        Some("_Total")
    );
    assert!(!sample.points[0].emit_instance_attribute);
}
