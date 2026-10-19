// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

struct StartupTestPdh {
    dropped: Arc<AtomicBool>,
}

impl Drop for StartupTestPdh {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

impl PdhApi for StartupTestPdh {
    type Counter = ();

    fn add(&mut self, _path: &str, _localized: bool) -> Result<Self::Counter, Error> {
        Ok(())
    }

    fn remove(&mut self, _counter: &Self::Counter, _path: &str) -> Result<(), Error> {
        Ok(())
    }

    fn counter_type(&mut self, _counter: &Self::Counter, _path: &str) -> Result<u32, Error> {
        Ok(0x0001_0100)
    }

    fn collect(&mut self) -> Result<bool, Error> {
        Ok(true)
    }

    fn raw(&mut self, _counter: &Self::Counter, _path: &str) -> Result<Option<RawSample>, Error> {
        Ok(None)
    }

    fn formatted(
        &mut self,
        _counter: &Self::Counter,
        _path: &str,
        _format: ValueFormat,
    ) -> Result<Option<Number>, Error> {
        Ok(Some(Number::Integer(42)))
    }

    fn timestamp(&mut self) -> Result<i64, Error> {
        Ok(1)
    }

    fn expand(&mut self, _path: &str) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }

    fn parse_instance(&mut self, path: &str) -> Result<InstanceIdentity, Error> {
        Err(Error::InvalidCounterToken {
            path: path.to_owned(),
        })
    }
}

#[derive(Default)]
struct PeakOwnership {
    current: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

struct PeakPdh {
    ownership: Arc<PeakOwnership>,
    owned: usize,
    expansions: BTreeMap<String, Vec<String>>,
}

impl Drop for PeakPdh {
    fn drop(&mut self) {
        let _ = self
            .ownership
            .current
            .fetch_sub(self.owned, Ordering::AcqRel);
    }
}

impl PdhApi for PeakPdh {
    type Counter = String;

    fn add(&mut self, path: &str, _localized: bool) -> Result<Self::Counter, Error> {
        self.owned += 1;
        let current = self.ownership.current.fetch_add(1, Ordering::AcqRel) + 1;
        let _ = self.ownership.peak.fetch_max(current, Ordering::AcqRel);
        Ok(path.to_owned())
    }

    fn remove(&mut self, _counter: &Self::Counter, _path: &str) -> Result<(), Error> {
        self.owned -= 1;
        let _ = self.ownership.current.fetch_sub(1, Ordering::AcqRel);
        Ok(())
    }

    fn counter_type(&mut self, _counter: &Self::Counter, _path: &str) -> Result<u32, Error> {
        Ok(0x0001_0100)
    }

    fn collect(&mut self) -> Result<bool, Error> {
        Ok(true)
    }

    fn raw(&mut self, _counter: &Self::Counter, _path: &str) -> Result<Option<RawSample>, Error> {
        Ok(None)
    }

    fn formatted(
        &mut self,
        _counter: &Self::Counter,
        _path: &str,
        _format: ValueFormat,
    ) -> Result<Option<Number>, Error> {
        Ok(Some(Number::Integer(42)))
    }

    fn timestamp(&mut self) -> Result<i64, Error> {
        Ok(100)
    }

    fn expand(&mut self, path: &str) -> Result<Vec<String>, Error> {
        Ok(self.expansions.get(path).cloned().unwrap_or_default())
    }

    fn parse_instance(&mut self, path: &str) -> Result<InstanceIdentity, Error> {
        NativePdh::parse_instance_path(path)
    }
}

/// Scenario: Shared and independently recreated source mappings are interleaved after expansion.
/// Guarantees: Adjacency cannot split logical query ownership, which keeps query count and sampling history stable across configuration order.
#[test]
fn query_plans_isolate_recreated_source_mappings() {
    let mut shared_first = counter(r"\Shared(first)\Value", "shared.first");
    shared_first.source_mapping_index = 0;
    let mut recreated_first = counter(r"\Isolated(first)\Value", "isolated.first");
    recreated_first.recreate_query = true;
    recreated_first.source_mapping_index = 1;
    let mut shared_second = counter(r"\Shared(second)\Value", "shared.second");
    shared_second.source_mapping_index = 2;
    let mut recreated_second = counter(r"\Isolated(second)\Value", "isolated.second");
    recreated_second.recreate_query = true;
    recreated_second.source_mapping_index = 1;
    let mut another_recreated = counter(r"\Another\Value", "another");
    another_recreated.recreate_query = true;
    another_recreated.source_mapping_index = 3;

    let plans = query_plans(vec![
        shared_first,
        recreated_first,
        shared_second,
        recreated_second,
        another_recreated,
    ]);

    assert_eq!(plans.len(), 3);
    assert_eq!(plans[0].global_indices, [0, 2]);
    assert!(!plans[0].recreate_query);
    assert_eq!(plans[1].global_indices, [1, 3]);
    assert!(plans[1].recreate_query);
    assert_eq!(plans[2].global_indices, [4]);
    assert!(plans[2].recreate_query);
}

/// Scenario: Shared, isolated, and shared mappings collect from interleaved positions while isolated recreation and collection fail in sequence.
/// Guarantees: Shared values retain global indices 0 and 2, and both isolated failures report only global index 1.
#[test]
fn interleaved_query_failures_keep_global_indices() {
    let mut shared_first = counter(r"\Shared(first)\Value", "shared.first");
    shared_first.source_mapping_index = 0;
    let mut isolated = counter(r"\Isolated\Value", "isolated");
    isolated.recreate_query = true;
    isolated.source_mapping_index = 1;
    let mut shared_second = counter(r"\Shared(second)\Value", "shared.second");
    shared_second.source_mapping_index = 2;

    let states = Rc::new(RefCell::new(Vec::new()));
    let factory_states = Rc::clone(&states);
    let open_calls = Rc::new(Cell::new(0_usize));
    let factory_open_calls = Rc::clone(&open_calls);
    let mut open = move |configs: Vec<CounterConfig>, _max_expanded_counters: usize| {
        let open_index = factory_open_calls.get();
        factory_open_calls.set(open_index + 1);
        if open_index == 2 {
            return Err(Error::Pdh {
                operation: "PdhOpenQueryW",
                path: "<query>".to_owned(),
                status: 1,
            });
        }
        let counters = configs
            .iter()
            .map(|config| FakeCounter {
                path: config.path.clone(),
                native_type: 0x0001_0100,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Integer(42)),
                raw_error: false,
                formatted_error: false,
            })
            .collect();
        let (state, query) = fake_query(configs, counters);
        factory_states.borrow_mut().push(state);
        Ok(query)
    };
    let mut queries = query_plans(vec![shared_first, isolated, shared_second])
        .into_iter()
        .map(|plan| QuerySlot::open_initial(plan, 4_096, &mut open).unwrap())
        .collect::<Vec<_>>();
    states.borrow()[0].borrow_mut().timestamp = 110;
    states.borrow()[1].borrow_mut().timestamp = 115;

    let sample = collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();

    assert_eq!(sample.timestamp_unix_nano, 115);
    assert_eq!(
        sample
            .points
            .iter()
            .map(|point| (point.counter_index, point.timestamp_unix_nano))
            .collect::<Vec<_>>(),
        [(0, 110), (1, 115), (2, 110)]
    );
    assert_eq!(
        sample
            .points
            .iter()
            .map(|point| point.path.as_ref())
            .collect::<Vec<_>>(),
        [
            r"\Shared(first)\Value",
            r"\Isolated\Value",
            r"\Shared(second)\Value"
        ]
    );
    assert_eq!(sample.metadata.diagnostics.query_recreation_attempts, 1);
    assert_eq!(sample.metadata.diagnostics.query_recreation_failures, 1);
    assert_eq!(sample.metadata.failures.len(), 1);
    assert_eq!(sample.metadata.failures[0].counter_index, 1);
    assert_eq!(sample.metadata.failures[0].reason, "PdhOpenQueryW");
    assert!(sample.metadata.failures[0].diagnostic_only);

    {
        let states = states.borrow();
        states[0].borrow_mut().timestamp = 120;
        states[1].borrow_mut().collect_error = true;
    }
    let partial = collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();

    assert_eq!(
        partial
            .points
            .iter()
            .map(|point| (point.counter_index, point.path.as_ref()))
            .collect::<Vec<_>>(),
        [(0, r"\Shared(first)\Value"), (2, r"\Shared(second)\Value")]
    );
    assert_eq!(partial.metadata.diagnostics.query_recreation_attempts, 0);
    assert_eq!(partial.metadata.diagnostics.scrape_failures, 1);
    assert_eq!(partial.metadata.failures.len(), 1);
    assert_eq!(partial.metadata.failures[0].counter_index, 1);
    assert_eq!(partial.metadata.failures[0].reason, "PdhCollectQueryData");
    assert!(!partial.metadata.failures[0].diagnostic_only);
    assert_eq!(open_calls.get(), 3);
    assert_eq!(states.borrow().len(), 2);
}

/// Scenario: One exact mapping and multiple recreated wildcard mappings fill one receiver-wide native-entry limit.
/// Guarantees: Recreation retains each old query when no replacement capacity remains, so isolated queries cannot multiply or discard retained entries.
#[test]
fn recreated_queries_share_receiver_native_entry_limit() {
    let mut exact = counter(r"\Memory\Available Bytes", "memory.available");
    exact.source_mapping_index = 0;
    let mut first = counter(r"\Process(first*)\Private Bytes", "process.first");
    first.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    first.recreate_query = true;
    first.source_mapping_index = 1;
    let mut second = counter(r"\Process(second*)\Private Bytes", "process.second");
    second.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    second.recreate_query = true;
    second.source_mapping_index = 2;

    let observed_limits = Rc::new(RefCell::new(Vec::new()));
    let factory_limits = Rc::clone(&observed_limits);
    let mut open = move |configs: Vec<CounterConfig>, max_expanded_counters: usize| {
        factory_limits.borrow_mut().push(max_expanded_counters);
        let template = configs[0].path.clone();
        let paths = if configs[0].instance_config.is_wildcard() {
            let prefix = if configs[0].source_mapping_index == 1 {
                "first"
            } else {
                "second"
            };
            (0..3)
                .map(|index| format!(r"\Process({prefix}{index})\Private Bytes"))
                .collect::<Vec<_>>()
        } else {
            vec![template.clone()]
        };
        let state = Rc::new(RefCell::new(FakeState {
            counters: paths
                .iter()
                .map(|path| FakeCounter {
                    path: path.clone(),
                    native_type: 0x0001_0100,
                    raw: None,
                    multi_count: 1,
                    formatted: Some(Number::Integer(42)),
                    raw_error: false,
                    formatted_error: false,
                })
                .collect(),
            added: Vec::new(),
            calls: FakeNativeCalls::default(),
            collect_has_data: true,
            collect_error: false,
            timestamp: 100,
            timestamp_error: false,
            remove_error: false,
            removed: Vec::new(),
            expansions: configs[0]
                .instance_config
                .is_wildcard()
                .then_some((template, paths))
                .into_iter()
                .collect(),
        }));
        Query::open_with(
            FakePdh(state),
            configs,
            Duration::from_secs(30),
            Duration::from_secs(30),
            256,
            max_expanded_counters,
        )
    };

    let mut queries = Vec::new();
    for plan in query_plans(vec![exact, first, second]) {
        let reserved = queries
            .iter()
            .map(QuerySlot::reserved_native_entry_count)
            .sum::<usize>();
        queries.push(QuerySlot::open_initial(plan, 5 - reserved, &mut open).unwrap());
    }
    assert_eq!(
        queries
            .iter()
            .map(QuerySlot::active_expanded_count)
            .collect::<Vec<_>>(),
        [0, 3, 1]
    );

    let sample = collect_queries(&mut queries, Duration::from_secs(30), 5, &mut open).unwrap();
    assert_eq!(sample.metadata.diagnostics.active_expanded_counters, 4);
    assert_eq!(sample.metadata.diagnostics.query_recreation_attempts, 2);
    assert_eq!(sample.metadata.diagnostics.query_recreation_failures, 2);
    assert_eq!(sample.metadata.failures.len(), 2);
    assert!(
        sample
            .metadata
            .failures
            .iter()
            .all(|failure| failure.reason == "query_recreation_capacity")
    );
    assert_eq!(observed_limits.borrow().as_slice(), [5, 4, 1]);
    assert_eq!(
        queries
            .iter()
            .map(QuerySlot::reserved_native_entry_count)
            .sum::<usize>(),
        5
    );
}

/// Scenario: A wildcard initial plan can fill the receiver budget before a later isolated exact plan opens.
/// Guarantees: Startup reserves every unopened exact mapping, so both plans open within the validated receiver-wide limit.
#[test]
fn initial_wildcard_reserves_later_exact_native_entry() {
    let wildcard_path = r"\Process(*)\Private Bytes";
    let expanded = (0..3)
        .map(|index| format!(r"\Process(worker{index})\Private Bytes"))
        .collect::<Vec<_>>();
    let mut wildcard = counter(wildcard_path, "process.private");
    wildcard.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    let mut isolated = counter(r"\System\Processes", "system.processes");
    isolated.recreate_query = true;
    isolated.source_mapping_index = 1;

    let observed_limits = Rc::new(RefCell::new(Vec::new()));
    let factory_limits = Rc::clone(&observed_limits);
    let mut open = move |configs: Vec<CounterConfig>, max_native_counters: usize| {
        factory_limits.borrow_mut().push(max_native_counters);
        let counters = configs
            .iter()
            .flat_map(|config| {
                if config.instance_config.is_wildcard() {
                    expanded.clone()
                } else {
                    vec![config.path.clone()]
                }
            })
            .map(|path| FakeCounter {
                path,
                native_type: 0x0001_0100,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Integer(42)),
                raw_error: false,
                formatted_error: false,
            })
            .collect();
        let state = Rc::new(RefCell::new(FakeState {
            counters,
            added: Vec::new(),
            calls: FakeNativeCalls::default(),
            collect_has_data: true,
            collect_error: false,
            timestamp: 100,
            timestamp_error: false,
            remove_error: false,
            removed: Vec::new(),
            expansions: [(wildcard_path.to_owned(), expanded.clone())]
                .into_iter()
                .collect(),
        }));
        Query::open_with(
            FakePdh(state),
            configs,
            Duration::from_secs(30),
            Duration::from_secs(30),
            256,
            max_native_counters,
        )
    };

    let queries = open_initial_queries(
        query_plans(vec![wildcard, isolated]),
        Duration::from_secs(30),
        3,
        &mut open,
        &AtomicBool::new(false),
    )
    .unwrap()
    .unwrap();

    assert_eq!(observed_limits.borrow().as_slice(), [2, 1]);
    assert_eq!(
        queries
            .iter()
            .map(QuerySlot::active_expanded_count)
            .collect::<Vec<_>>(),
        [2, 0]
    );
    assert_eq!(
        queries
            .iter()
            .map(QuerySlot::reserved_native_entry_count)
            .sum::<usize>(),
        3
    );
}

/// Scenario: A recreated exact-and-wildcard plan discovers more instances while its old handles remain live beside an exact peer.
/// Guarantees: Replacement construction overlaps the old query without exceeding receiver-wide ownership and swaps only the entries that fit.
#[test]
fn recreation_peak_includes_old_query_ownership() {
    let exact_path = r"\Memory\Available Bytes";
    let wildcard_path = r"\Process(*)\Private Bytes";
    let expanded_paths = (0..3)
        .map(|index| format!(r"\Process(worker{index})\Private Bytes"))
        .collect::<Vec<_>>();
    let peer_path = r"\System\Processes";
    let mut exact = counter(exact_path, "memory.available");
    exact.recreate_query = true;
    exact.source_mapping_index = 1;
    let mut wildcard = counter(wildcard_path, "process.private");
    wildcard.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    wildcard.recreate_query = true;
    wildcard.source_mapping_index = 1;
    let peer = counter(peer_path, "system.processes");

    let ownership = Arc::new(PeakOwnership::default());
    let factory_ownership = Arc::clone(&ownership);
    let open_calls = Rc::new(Cell::new(0));
    let factory_open_calls = Rc::clone(&open_calls);
    let mut open = move |configs: Vec<CounterConfig>, max_native_counters: usize| {
        let open_index = factory_open_calls.get();
        factory_open_calls.set(open_index + 1);
        let expanded = if open_index == 2 {
            expanded_paths.clone()
        } else {
            expanded_paths[..1].to_vec()
        };
        Query::open_with(
            PeakPdh {
                ownership: Arc::clone(&factory_ownership),
                owned: 0,
                expansions: [(wildcard_path.to_owned(), expanded)].into_iter().collect(),
            },
            configs,
            Duration::from_secs(30),
            Duration::from_secs(30),
            256,
            max_native_counters,
        )
    };
    let mut queries = open_initial_queries(
        query_plans(vec![exact, wildcard, peer]),
        Duration::from_secs(30),
        5,
        &mut open,
        &AtomicBool::new(false),
    )
    .unwrap()
    .unwrap();

    assert_eq!(ownership.current.load(Ordering::Acquire), 3);
    assert_eq!(ownership.peak.load(Ordering::Acquire), 3);

    let sample = collect_queries(&mut queries, Duration::from_secs(30), 5, &mut open).unwrap();

    assert_eq!(sample.points.len(), 3);
    assert_eq!(sample.metadata.diagnostics.query_recreation_attempts, 1);
    assert_eq!(sample.metadata.diagnostics.query_recreation_failures, 0);
    assert_eq!(open_calls.get(), 3);
    assert_eq!(ownership.current.load(Ordering::Acquire), 3);
    assert_eq!(ownership.peak.load(Ordering::Acquire), 5);
    assert_eq!(
        queries
            .iter()
            .map(QuerySlot::reserved_native_entry_count)
            .sum::<usize>(),
        3
    );
}

/// Scenario: An exact-only query is temporarily unavailable while a later query opens.
/// Guarantees: The absent slot keeps its exact reservation so recovery cannot overcommit capacity already given to healthy peers.
#[test]
fn unavailable_query_reserves_exact_native_entry() {
    let unavailable = QueryPlan {
        configs: vec![counter(r"\Unavailable\Value", "windows.unavailable")],
        global_indices: vec![0],
        recreate_query: false,
    };
    let mut open = |_: Vec<CounterConfig>, _: usize| {
        Err::<Query<FakePdh>, _>(Error::Pdh {
            operation: "PdhOpenQueryW",
            path: "<query>".to_owned(),
            status: 1,
        })
    };
    let slot = QuerySlot::open_initial(unavailable, 2, &mut open).unwrap();

    assert_eq!(slot.reserved_native_entry_count(), 1);
    assert_eq!(
        remaining_native_capacity(2, std::iter::once(slot.reserved_native_entry_count())),
        1
    );
}

/// Scenario: One query combines an exact mapping with a wildcard that discovers three paths under a two-entry limit.
/// Guarantees: Exact ownership reduces same-query expansion capacity so wildcard discovery cannot overcommit the receiver budget.
#[test]
fn exact_reservation_reduces_same_query_expansion_capacity() {
    let exact_path = r"\Memory\Available Bytes";
    let wildcard_path = r"\Process(*)\Private Bytes";
    let expanded = (0..3)
        .map(|index| format!(r"\Process(worker{index})\Private Bytes"))
        .collect::<Vec<_>>();
    let state = Rc::new(RefCell::new(FakeState {
        counters: std::iter::once(exact_path.to_owned())
            .chain(expanded.iter().cloned())
            .map(|path| FakeCounter {
                path,
                native_type: 0x0001_0100,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Integer(42)),
                raw_error: false,
                formatted_error: false,
            })
            .collect(),
        added: Vec::new(),
        calls: FakeNativeCalls::default(),
        collect_has_data: true,
        collect_error: false,
        timestamp: 100,
        timestamp_error: false,
        remove_error: false,
        removed: Vec::new(),
        expansions: [(wildcard_path.to_owned(), expanded)].into_iter().collect(),
    }));
    let mut wildcard = counter(wildcard_path, "process.private");
    wildcard.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };

    let query = Query::open_with(
        FakePdh(Rc::clone(&state)),
        vec![counter(exact_path, "memory.available"), wildcard],
        Duration::from_secs(30),
        Duration::from_secs(30),
        256,
        2,
    )
    .unwrap();

    assert_eq!(query.active_expanded_count(), 1);
    assert_eq!(query.reserved_native_entry_count(), 2);
    assert_eq!(state.borrow().added.len(), 2);
}

/// Scenario: A worker allocation supplies fewer entries than one query's exact configurations require.
/// Guarantees: Internal accounting corruption fails explicitly before opening native handles instead of silently exceeding the receiver limit.
#[test]
fn query_rejects_limit_below_exact_reservation() {
    let result = Query::open_with(
        StartupTestPdh {
            dropped: Arc::new(AtomicBool::new(false)),
        },
        vec![
            counter(r"\Memory\Available Bytes", "memory.available"),
            counter(r"\System\Processes", "system.processes"),
        ],
        Duration::from_secs(30),
        Duration::from_secs(30),
        256,
        1,
    );

    assert!(matches!(
        result,
        Err(Error::InvalidSample(
            "query native-counter limit is below its exact-counter reservation"
        ))
    ));
}

/// Scenario: A monotonic collection is discarded before delivery, as with a positive runtime timeout, and a later scrape succeeds.
/// Guarantees: The discarded point does not consume the unknown start, so the next delivered candidate starts at its own timestamp and only its acknowledgement establishes the sequence.
#[test]
fn discarded_monotonic_collection_does_not_consume_start() {
    let path = r"\TCPv4\Connection Failures";
    let config = monotonic_sum(path, "windows.tcp.connection_failures", "{failure}");
    let states = Rc::new(RefCell::new(Vec::new()));
    let factory_states = Rc::clone(&states);
    let mut open = move |configs: Vec<CounterConfig>, _max_native_counters: usize| {
        let (state, query) = fake_query(
            configs,
            vec![FakeCounter {
                path: path.to_owned(),
                native_type: 0x0001_0100,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Integer(10)),
                raw_error: false,
                formatted_error: false,
            }],
        );
        factory_states.borrow_mut().push(state);
        Ok(query)
    };
    let mut queries = query_plans(vec![config])
        .into_iter()
        .map(|plan| QuerySlot::open_initial(plan, 4_096, &mut open).unwrap())
        .collect::<Vec<_>>();

    {
        let states = states.borrow();
        let mut state = states[0].borrow_mut();
        state.timestamp = 110;
        state.counters[0].formatted = Some(Number::Integer(20));
    }
    let discarded =
        collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
    assert_eq!(discarded.points[0].start_time_unix_nano, Some(110));

    {
        let states = states.borrow();
        let mut state = states[0].borrow_mut();
        state.timestamp = 120;
        state.counters[0].formatted = Some(Number::Integer(30));
    }
    let delivered =
        collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
    assert_eq!(delivered.points[0].start_time_unix_nano, Some(120));
    publish_monotonic_starts(&mut queries, collected_monotonic_publications(&delivered));

    {
        let states = states.borrow();
        let mut state = states[0].borrow_mut();
        state.timestamp = 130;
        state.counters[0].formatted = Some(Number::Integer(40));
    }
    let retained =
        collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
    assert_eq!(retained.points[0].start_time_unix_nano, Some(120));
}

/// Scenario: Projection rejects the only monotonic point, leaving no Arrow records to send for that scrape.
/// Guarantees: An all-point projection drop acknowledges nothing, so the next valid point starts a fresh exported sequence at its own timestamp.
#[test]
fn all_point_projection_drop_does_not_consume_monotonic_start() {
    let path = r"\TCPv4\Connection Failures";
    let config = monotonic_sum(path, "windows.tcp.connection_failures", "{failure}");
    let projection_config = config.clone();
    let states = Rc::new(RefCell::new(Vec::new()));
    let factory_states = Rc::clone(&states);
    let mut open = move |configs: Vec<CounterConfig>, _max_native_counters: usize| {
        let (state, query) = fake_query(
            configs,
            vec![FakeCounter {
                path: path.to_owned(),
                native_type: 0x0001_0100,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Integer(10)),
                raw_error: false,
                formatted_error: false,
            }],
        );
        factory_states.borrow_mut().push(state);
        Ok(query)
    };
    let mut queries = query_plans(vec![config])
        .into_iter()
        .map(|plan| QuerySlot::open_initial(plan, 4_096, &mut open).unwrap())
        .collect::<Vec<_>>();

    {
        let states = states.borrow();
        let mut state = states[0].borrow_mut();
        state.timestamp = 110;
        state.counters[0].formatted = Some(Number::Integer(20));
    }
    let mut dropped =
        collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
    dropped.points[0].start_time_unix_nano = Some(0);
    let projection = project_sample(std::slice::from_ref(&projection_config), &dropped).unwrap();
    assert!(projection.records.is_none());
    assert_eq!(projection.point_failures.len(), 1);
    assert!(projection.monotonic_publications.is_empty());
    publish_monotonic_starts(&mut queries, projection.monotonic_publications);

    {
        let states = states.borrow();
        let mut state = states[0].borrow_mut();
        state.timestamp = 120;
        state.counters[0].formatted = Some(Number::Integer(30));
    }
    let delivered =
        collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
    assert_eq!(delivered.points[0].start_time_unix_nano, Some(120));
}

/// Scenario: Projection accepts one monotonic point and rejects its peer before a successful batch send.
/// Guarantees: Delivery acknowledges only the projected point; its sequence retains the earlier start while the rejected peer starts fresh on its next valid output.
#[test]
fn per_point_projection_drop_commits_only_delivered_monotonic_start() {
    let first_path = r"\TCPv4\Connection Failures";
    let second_path = r"\TCPv4\Connections Reset";
    let configs = vec![
        monotonic_sum(first_path, "windows.tcp.connection_failures", "{failure}"),
        monotonic_sum(second_path, "windows.tcp.connections_reset", "{reset}"),
    ];
    let projection_configs = configs.clone();
    let states = Rc::new(RefCell::new(Vec::new()));
    let factory_states = Rc::clone(&states);
    let mut open = move |configs: Vec<CounterConfig>, _max_native_counters: usize| {
        let (state, query) = fake_query(
            configs,
            [first_path, second_path]
                .into_iter()
                .map(|path| FakeCounter {
                    path: path.to_owned(),
                    native_type: 0x0001_0100,
                    raw: None,
                    multi_count: 1,
                    formatted: Some(Number::Integer(10)),
                    raw_error: false,
                    formatted_error: false,
                })
                .collect(),
        );
        factory_states.borrow_mut().push(state);
        Ok(query)
    };
    let mut queries = query_plans(configs)
        .into_iter()
        .map(|plan| QuerySlot::open_initial(plan, 4_096, &mut open).unwrap())
        .collect::<Vec<_>>();

    {
        let states = states.borrow();
        let mut state = states[0].borrow_mut();
        state.timestamp = 110;
        for counter in &mut state.counters {
            counter.formatted = Some(Number::Integer(20));
        }
    }
    let mut mixed =
        collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
    mixed.points[1].start_time_unix_nano = Some(0);
    let projection = project_sample(&projection_configs, &mixed).unwrap();
    assert!(projection.records.is_some());
    assert_eq!(projection.point_failures.len(), 1);
    assert_eq!(projection.monotonic_publications.len(), 1);
    assert_eq!(projection.monotonic_publications[0].counter_index, 0);
    publish_monotonic_starts(&mut queries, projection.monotonic_publications);

    {
        let states = states.borrow();
        let mut state = states[0].borrow_mut();
        state.timestamp = 120;
        for counter in &mut state.counters {
            counter.formatted = Some(Number::Integer(30));
        }
    }
    let next = collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
    assert_eq!(
        next.points
            .iter()
            .map(|point| point.start_time_unix_nano)
            .collect::<Vec<_>>(),
        [Some(110), Some(120)]
    );
}

/// Scenario: A delta mapping and a recreated peer collect through separate queries with different clocks.
/// Guarantees: Each delta interval starts exactly at that series' previous point timestamp without overlap from the later peer timestamp.
#[test]
fn cross_query_delta_intervals_use_their_own_timestamps() {
    let delta_path = r"\Provider\Operations Delta";
    let peer_path = r"\Memory\Available Bytes";
    let mut delta = delta_sum(delta_path, "provider.operations", false);
    delta.source_mapping_index = 0;
    let mut recreated_peer = counter(peer_path, "windows.memory.available");
    recreated_peer.recreate_query = true;
    recreated_peer.source_mapping_index = 1;

    let states = Rc::new(RefCell::new(Vec::new()));
    let factory_states = Rc::clone(&states);
    let mut open = move |configs: Vec<CounterConfig>, _max_expanded_counters: usize| {
        let config = &configs[0];
        let native_counter = if config.path == delta_path {
            FakeCounter {
                path: config.path.clone(),
                native_type: 0x0040_0500,
                raw: Some(10),
                multi_count: 1,
                formatted: Some(Number::Double(1.0)),
                raw_error: false,
                formatted_error: false,
            }
        } else {
            FakeCounter {
                path: config.path.clone(),
                native_type: 0x0001_0100,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Integer(42)),
                raw_error: false,
                formatted_error: false,
            }
        };
        let (state, query) = fake_query(configs, vec![native_counter]);
        if state.borrow().counters[0].path == peer_path {
            state.borrow_mut().timestamp = 105;
        }
        factory_states.borrow_mut().push(state);
        Ok(query)
    };
    let mut queries = query_plans(vec![delta, recreated_peer])
        .into_iter()
        .map(|plan| QuerySlot::open_initial(plan, 4_096, &mut open).unwrap())
        .collect::<Vec<_>>();
    {
        let states = states.borrow();
        let mut delta = states[0].borrow_mut();
        delta.timestamp = 110;
        delta.counters[0].raw = Some(20);
        delta.counters[0].formatted = Some(Number::Double(2.0));
        states[1].borrow_mut().timestamp = 115;
    }

    let first = collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
    assert_eq!(first.timestamp_unix_nano, 115);
    assert_eq!(first.points[0].start_time_unix_nano, Some(100));
    assert_eq!(first.points[0].timestamp_unix_nano, 110);

    {
        let states = states.borrow();
        let mut delta = states[0].borrow_mut();
        delta.timestamp = 120;
        delta.counters[0].raw = Some(30);
        delta.counters[0].formatted = Some(Number::Double(3.0));
        states[2].borrow_mut().timestamp = 125;
    }
    let second = collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
    assert_eq!(second.timestamp_unix_nano, 125);
    assert_eq!(
        second.points[0].start_time_unix_nano,
        Some(first.points[0].timestamp_unix_nano)
    );
    assert_eq!(second.points[0].timestamp_unix_nano, 120);
}

/// Scenario: One isolated query fails collection while a shared peer remains healthy.
/// Guarantees: The healthy peer publishes its unknown monotonic start, reports the isolated failure, and does not recreate the failed query.
#[test]
fn isolated_query_failure_preserves_healthy_peer_output() {
    let shared_path = r"\Memory\Available Bytes";
    let isolated_path = r"\Processor(_Total)\% Processor Time";
    let mut shared = monotonic_sum(shared_path, "windows.memory.available", "By");
    shared.source_mapping_index = 0;
    let mut isolated = counter(isolated_path, "windows.processor.time");
    isolated.recreate_query = true;
    isolated.source_mapping_index = 1;

    let states = Rc::new(RefCell::new(Vec::new()));
    let factory_states = Rc::clone(&states);
    let open_calls = Rc::new(Cell::new(0_usize));
    let factory_open_calls = Rc::clone(&open_calls);
    let mut open = move |configs: Vec<CounterConfig>, _max_expanded_counters: usize| {
        factory_open_calls.set(factory_open_calls.get() + 1);
        let config = &configs[0];
        let native_counter = if config.path == shared_path {
            FakeCounter {
                path: config.path.clone(),
                native_type: 0x0001_0100,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Integer(42)),
                raw_error: false,
                formatted_error: false,
            }
        } else {
            FakeCounter {
                path: config.path.clone(),
                native_type: 0x2041_0500,
                raw: Some(10),
                multi_count: 1,
                formatted: Some(Number::Double(1.0)),
                raw_error: false,
                formatted_error: false,
            }
        };
        let (state, query) = fake_query(configs, vec![native_counter]);
        factory_states.borrow_mut().push(state);
        Ok(query)
    };
    let mut queries = query_plans(vec![shared, isolated])
        .into_iter()
        .map(|plan| QuerySlot::open_initial(plan, 4_096, &mut open).unwrap())
        .collect::<Vec<_>>();
    states.borrow()[1].borrow_mut().collect_error = true;
    advance_fake_queries(&states);

    let sample = collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();

    assert_eq!(sample.points.len(), 1);
    assert_eq!(sample.points[0].counter_index, 0);
    assert_eq!(
        sample.points[0].start_time_unix_nano,
        Some(sample.points[0].timestamp_unix_nano)
    );
    assert_eq!(sample.metadata.failures.len(), 1);
    assert_eq!(sample.metadata.failures[0].counter_index, 1);
    assert_eq!(sample.metadata.failures[0].reason, "PdhCollectQueryData");
    assert!(!sample.metadata.failures[0].diagnostic_only);
    assert_eq!(sample.metadata.diagnostics.scrape_failures, 1);
    assert_eq!(sample.metadata.diagnostics.query_recreation_attempts, 0);
    assert_eq!(open_calls.get(), 2);
}

/// Scenario: An isolated query is recreated repeatedly while construction and priming fail in sequence.
/// Guarantees: The old isolated query survives both failures, its replacement is primed before use, and the shared peer's cumulative sequence is unchanged.
#[test]
fn recreated_query_primes_before_swap_and_preserves_old_query_on_failure() {
    let shared_path = r"\Memory\Available Bytes";
    let isolated_path = r"\Processor(_Total)\% Processor Time";
    let mut shared = monotonic_sum(shared_path, "windows.memory.available", "By");
    shared.source_mapping_index = 0;
    let mut isolated = counter(isolated_path, "windows.processor.time");
    isolated.recreate_query = true;
    isolated.source_mapping_index = 1;

    let states = Rc::new(RefCell::new(Vec::new()));
    let factory_states = Rc::clone(&states);
    let open_calls = Rc::new(Cell::new(0_usize));
    let factory_open_calls = Rc::clone(&open_calls);
    let mut open = move |configs: Vec<CounterConfig>, _max_expanded_counters: usize| {
        let open_index = factory_open_calls.get();
        factory_open_calls.set(open_index + 1);
        if open_index == 2 {
            return Err(Error::Pdh {
                operation: "PdhOpenQueryW",
                path: "<query>".to_owned(),
                status: 1,
            });
        }
        let counters = configs
            .iter()
            .map(|config| {
                if config.path == shared_path {
                    FakeCounter {
                        path: config.path.clone(),
                        native_type: 0x0001_0100,
                        raw: None,
                        multi_count: 1,
                        formatted: Some(Number::Integer(42)),
                        raw_error: false,
                        formatted_error: false,
                    }
                } else {
                    FakeCounter {
                        path: config.path.clone(),
                        native_type: 0x2041_0500,
                        raw: Some(10),
                        multi_count: 1,
                        formatted: Some(Number::Double(1.0)),
                        raw_error: false,
                        formatted_error: false,
                    }
                }
            })
            .collect();
        let (state, query) = fake_query(configs, counters);
        if open_index == 3 {
            state.borrow_mut().collect_error = true;
        }
        factory_states.borrow_mut().push(state);
        Ok(query)
    };
    let plans = query_plans(vec![shared, isolated]);
    let mut queries = plans
        .into_iter()
        .map(|plan| QuerySlot::open_initial(plan, 4_096, &mut open).unwrap())
        .collect::<Vec<_>>();

    let mut shared_start = None;
    for scrape_index in 0..4 {
        advance_fake_queries(&states);
        let sample =
            collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
        assert_eq!(
            sample
                .points
                .iter()
                .map(|point| point.counter_index)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert!(matches!(
            sample.points[1].value,
            SampleValue::Value(Number::Double(1.0))
        ));
        let current_shared_start = sample.points[0].start_time_unix_nano;
        if let Some(shared_start) = shared_start {
            assert_eq!(current_shared_start, Some(shared_start));
        } else {
            shared_start = current_shared_start;
        }

        assert_eq!(sample.metadata.diagnostics.query_recreation_attempts, 1);
        match scrape_index {
            0 => {
                assert_eq!(sample.metadata.diagnostics.query_recreation_failures, 1);
                assert_eq!(sample.metadata.diagnostics.scrape_failures, 0);
                assert_eq!(sample.metadata.failures.len(), 1);
                assert_eq!(sample.metadata.failures[0].reason, "PdhOpenQueryW");
                assert!(sample.metadata.failures[0].diagnostic_only);
            }
            1 => {
                assert_eq!(sample.metadata.diagnostics.query_recreation_failures, 1);
                assert_eq!(sample.metadata.diagnostics.scrape_failures, 1);
                assert_eq!(sample.metadata.failures.len(), 1);
                assert_eq!(sample.metadata.failures[0].reason, "PdhCollectQueryData");
                assert!(sample.metadata.failures[0].diagnostic_only);
            }
            _ => {
                assert_eq!(sample.metadata.diagnostics.query_recreation_failures, 0);
                assert_eq!(sample.metadata.diagnostics.scrape_failures, 0);
                assert!(sample.metadata.failures.is_empty());
            }
        }
        publish_monotonic_starts(&mut queries, collected_monotonic_publications(&sample));
    }
    assert_eq!(open_calls.get(), 6);
}

/// Scenario: A recreated monotonic direct-count query is primed before each of two scrapes and receives the prior query's delivery acknowledgement.
/// Guarantees: Generation checks ignore the stale acknowledgement, so every replacement's first exported point uses start equal to time, including in decoded OTAP.
#[test]
fn recreated_monotonic_query_defers_start_until_publication() {
    let path = r"\TCPv4\Connection Failures";
    let mut config = monotonic_sum(path, "windows.tcp.connection_failures", "{failure}");
    config.recreate_query = true;
    let projection_config = config.clone();
    let states = Rc::new(RefCell::new(Vec::new()));
    let factory_states = Rc::clone(&states);
    let open_calls = Rc::new(Cell::new(0_usize));
    let factory_open_calls = Rc::clone(&open_calls);
    let mut open = move |configs: Vec<CounterConfig>, _max_native_counters: usize| {
        let open_index = factory_open_calls.get();
        factory_open_calls.set(open_index + 1);
        let offset = i64::try_from(open_index).unwrap() * 10;
        let (state, query) = fake_query(
            configs,
            vec![FakeCounter {
                path: path.to_owned(),
                native_type: 0x0001_0100,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Integer(10 + offset)),
                raw_error: false,
                formatted_error: false,
            }],
        );
        state.borrow_mut().timestamp = 100 + offset;
        factory_states.borrow_mut().push(state);
        Ok(query)
    };
    let plan = query_plans(vec![config]).pop().unwrap();
    let mut queries = vec![QuerySlot::open_initial(plan, 4_096, &mut open).unwrap()];

    for (scrape_index, timestamp) in [110, 120].into_iter().enumerate() {
        {
            let states = states.borrow();
            let mut state = states[scrape_index].borrow_mut();
            state.timestamp = timestamp;
            state.counters[0].formatted = Some(Number::Integer(timestamp / 10 + 10));
        }
        let sample =
            collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
        assert_eq!(sample.points[0].timestamp_unix_nano, timestamp);
        assert_eq!(sample.points[0].start_time_unix_nano, Some(timestamp));

        if scrape_index == 0 {
            let projection =
                project_sample(std::slice::from_ref(&projection_config), &sample).unwrap();
            let records = projection.records.unwrap();
            let points = records.get(ArrowPayloadType::NumberDataPoints).unwrap();
            let starts = points
                .column_by_name("start_time_unix_nano")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .unwrap();
            let times = points
                .column_by_name("time_unix_nano")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .unwrap();
            assert_eq!(starts.value(0), times.value(0));
            publish_monotonic_starts(&mut queries, projection.monotonic_publications);
        }
    }
    assert_eq!(open_calls.get(), 3);
}

/// Scenario: A monotonic cumulative Sum applies a nonzero decimal scale before OTAP delivery.
/// Guarantees: Delivery publishes the native monotonic start without requiring an integer projected value, and the next scrape retains that start.
#[test]
fn scaled_monotonic_sum_publishes_and_retains_start() {
    let path = r"\TCPv4\Connection Failures";
    let mut config = monotonic_sum(path, "windows.tcp.connection_failures", "{failure}");
    config.scale_power10 = -1;
    let projection_config = config.clone();
    let states = Rc::new(RefCell::new(Vec::new()));
    let factory_states = Rc::clone(&states);
    let mut open = move |configs: Vec<CounterConfig>, _max_native_counters: usize| {
        let (state, query) = fake_query(
            configs,
            vec![FakeCounter {
                path: path.to_owned(),
                native_type: 0x0001_0100,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Integer(10)),
                raw_error: false,
                formatted_error: false,
            }],
        );
        factory_states.borrow_mut().push(state);
        Ok(query)
    };
    let plan = query_plans(vec![config]).pop().unwrap();
    let mut queries = vec![QuerySlot::open_initial(plan, 4_096, &mut open).unwrap()];

    {
        let states = states.borrow();
        let mut state = states[0].borrow_mut();
        state.timestamp = 110;
        state.counters[0].formatted = Some(Number::Integer(10));
    }
    let first = collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
    assert_eq!(first.points[0].start_time_unix_nano, Some(110));
    let projection = project_sample(std::slice::from_ref(&projection_config), &first).unwrap();
    assert_eq!(projection.monotonic_publications.len(), 1);
    publish_monotonic_starts(&mut queries, projection.monotonic_publications);

    {
        let states = states.borrow();
        let mut state = states[0].borrow_mut();
        state.timestamp = 120;
        state.counters[0].formatted = Some(Number::Integer(20));
    }
    let second = collect_queries(&mut queries, Duration::from_secs(30), 4_096, &mut open).unwrap();
    assert_eq!(second.points[0].start_time_unix_nano, Some(110));
    let projection = project_sample(std::slice::from_ref(&projection_config), &second).unwrap();
    assert!(projection.records.is_some());
}

/// Scenario: A monotonic direct-count query is rebuilt after repeated collection failures and primed successfully.
/// Guarantees: Rebuild priming restores native readiness without backdating the first recovered cumulative point.
#[test]
fn rebuilt_monotonic_query_defers_start_until_publication() {
    let path = r"\TCPv4\Connection Failures";
    let states = Rc::new(RefCell::new(Vec::new()));
    let factory_states = Rc::clone(&states);
    let open_calls = Rc::new(Cell::new(0_usize));
    let factory_open_calls = Rc::clone(&open_calls);
    let mut open = move |configs: Vec<CounterConfig>, _max_native_counters: usize| {
        let open_index = factory_open_calls.get();
        factory_open_calls.set(open_index + 1);
        let offset = i64::try_from(open_index).unwrap() * 10;
        let (state, query) = fake_query(
            configs,
            vec![FakeCounter {
                path: path.to_owned(),
                native_type: 0x0001_0100,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Integer(10 + offset)),
                raw_error: false,
                formatted_error: false,
            }],
        );
        state.borrow_mut().timestamp = 100 + offset;
        factory_states.borrow_mut().push(state);
        Ok(query)
    };
    let plan = query_plans(vec![monotonic_sum(
        path,
        "windows.tcp.connection_failures",
        "{failure}",
    )])
    .pop()
    .unwrap();
    let mut query = QuerySlot::open_initial(plan, 4_096, &mut open).unwrap();
    states.borrow()[0].borrow_mut().collect_error = true;
    let now = Instant::now();
    for _ in 0..QUERY_REBUILD_AFTER_FAILURES {
        assert!(
            query
                .collect(now, Duration::ZERO, 4_096, &mut open)
                .is_err()
        );
    }

    let rebuilt = query
        .collect(now, Duration::ZERO, 4_096, &mut open)
        .unwrap();
    assert!(rebuilt.points.is_empty());
    {
        let states = states.borrow();
        let mut state = states[1].borrow_mut();
        state.timestamp = 120;
        state.counters[0].formatted = Some(Number::Integer(30));
    }
    let recovered = query
        .collect(now, Duration::ZERO, 4_096, &mut open)
        .unwrap();
    assert_eq!(recovered.points[0].timestamp_unix_nano, 120);
    assert_eq!(recovered.points[0].start_time_unix_nano, Some(120));
    assert_eq!(open_calls.get(), 2);
}

/// Scenario: One initial native open remains blocked while virtual time advances beyond the former deadline.
/// Guarantees: Slow provider work cannot become a fatal timeout; readiness waits for the completed initial pass.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn startup_longer_than_thirty_seconds_eventually_succeeds() {
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let dropped = Arc::new(AtomicBool::new(false));
    let factory_dropped = Arc::clone(&dropped);
    let startup = Worker::start_with(
        vec![counter(
            r"\Memory\Available Bytes",
            "windows.memory.available",
        )],
        Duration::from_secs(30),
        4_096,
        move |configs, max_expanded_counters| {
            let _ = entered_tx.send(());
            let _ = release_rx.recv();
            Query::open_with(
                StartupTestPdh {
                    dropped: Arc::clone(&factory_dropped),
                },
                configs,
                Duration::from_secs(30),
                Duration::from_secs(30),
                256,
                max_expanded_counters,
            )
        },
    );
    tokio::pin!(startup);
    tokio::select! {
        biased;
        _ = &mut startup => panic!("startup completed before the blocked native call"),
        () = tokio::task::yield_now() => {}
    }
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("worker did not begin initial query open");

    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::select! {
        biased;
        _ = &mut startup => panic!("startup completed before the blocked native call"),
        () = tokio::task::yield_now() => {}
    }

    release_tx.send(()).unwrap();
    let worker = startup.await.unwrap();
    tokio::time::resume();
    drop(worker);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !dropped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("slow startup worker did not release its query");
}

/// Scenario: Control abandons startup while the first of multiple native initialization units is blocked.
/// Guarantees: Cancellation requests cleanup and prevents later units from opening, avoiding overlapping orphan work.
#[tokio::test(flavor = "current_thread")]
async fn cancelling_startup_stops_between_initialization_units() {
    let mut isolated = counter(r"\System\Processes", "windows.system.processes");
    isolated.recreate_query = true;
    isolated.source_mapping_index = 1;
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let open_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let factory_open_calls = Arc::clone(&open_calls);
    let dropped = Arc::new(AtomicBool::new(false));
    let factory_dropped = Arc::clone(&dropped);
    let mut startup = Box::pin(Worker::start_with(
        vec![
            counter(r"\Memory\Available Bytes", "windows.memory.available"),
            isolated,
        ],
        Duration::from_secs(30),
        4_096,
        move |configs, max_expanded_counters| {
            let call = factory_open_calls.fetch_add(1, Ordering::AcqRel);
            if call == 0 {
                let _ = entered_tx.send(());
                let _ = release_rx.recv();
            }
            Query::open_with(
                StartupTestPdh {
                    dropped: Arc::clone(&factory_dropped),
                },
                configs,
                Duration::from_secs(30),
                Duration::from_secs(30),
                256,
                max_expanded_counters,
            )
        },
    ));
    tokio::select! {
        biased;
        _ = &mut startup => panic!("startup completed before cancellation"),
        () = tokio::task::yield_now() => {}
    }
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("worker did not begin initial query open");

    drop(startup);
    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !dropped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled startup did not release its query");
    assert_eq!(open_calls.load(Ordering::Acquire), 1);
}

/// Scenario: Initial query opens fail transiently for every slot or beside one healthy isolated slot.
/// Guarantees: Empty and partial initial state become ready, preserve bounded evidence, and recover without restarting the receiver.
#[tokio::test(flavor = "current_thread")]
async fn startup_retains_recoverable_empty_and_partial_slots() {
    for include_healthy in [false, true] {
        let failed_path = r"\Unavailable\Value";
        let mut counters = vec![counter(failed_path, "windows.unavailable")];
        if include_healthy {
            let mut healthy = counter(r"\Memory\Available Bytes", "windows.memory.available");
            healthy.recreate_query = true;
            healthy.source_mapping_index = 1;
            counters.push(healthy);
        }
        let failed_once = Arc::new(AtomicBool::new(false));
        let factory_failed_once = Arc::clone(&failed_once);
        let mut worker = Worker::start_with(
            counters,
            Duration::ZERO,
            4_096,
            move |configs, max_expanded_counters| {
                if configs[0].path == failed_path
                    && !factory_failed_once.swap(true, Ordering::AcqRel)
                {
                    return Err(Error::Pdh {
                        operation: "PdhOpenQueryW",
                        path: "<query>".to_owned(),
                        status: 1,
                    });
                }
                Query::open_with(
                    StartupTestPdh {
                        dropped: Arc::new(AtomicBool::new(false)),
                    },
                    configs,
                    Duration::from_secs(30),
                    Duration::from_secs(30),
                    256,
                    max_expanded_counters,
                )
            },
        )
        .await
        .unwrap();

        let first = worker.collect().await.unwrap();
        assert_eq!(
            first
                .points
                .iter()
                .map(|point| point.counter_index)
                .collect::<Vec<_>>(),
            if include_healthy { vec![1] } else { Vec::new() }
        );
        assert_eq!(first.metadata.failures.len(), 1);
        assert_eq!(first.metadata.failures[0].counter_index, 0);
        assert_eq!(first.metadata.failures[0].reason, "PdhOpenQueryW");
        assert!(first.metadata.failures[0].diagnostic_only);
        assert_eq!(first.metadata.diagnostics.query_rebuild_attempts, 1);
        assert_eq!(first.metadata.diagnostics.query_rebuild_recoveries, 1);

        let recovered = worker.collect().await.unwrap();
        assert_eq!(recovered.points.len(), usize::from(include_healthy) + 1);
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
        );
    }
}

/// Scenario: Initial query construction returns a non-availability invariant error.
/// Guarantees: Permanent startup corruption remains explicit instead of entering an endless recovery loop.
#[tokio::test(flavor = "current_thread")]
async fn startup_rejects_nonrecoverable_query_errors() {
    let result = Worker::start_with::<StartupTestPdh, _>(
        vec![counter(
            r"\Memory\Available Bytes",
            "windows.memory.available",
        )],
        Duration::from_secs(30),
        4_096,
        |_, _| Err(Error::InvalidSample("injected startup invariant")),
    )
    .await;
    let Err(error) = result else {
        panic!("nonrecoverable startup error unexpectedly succeeded");
    };

    assert!(matches!(
        error,
        Error::InvalidSample("injected startup invariant")
    ));
}

/// Scenario: The worker's startup prime receives a hard query collection failure.
/// Guarantees: Startup succeeds, the first scrape warms, and later collection recovers through normal retry state.
#[tokio::test(flavor = "current_thread")]
async fn startup_prime_query_failure_starts_and_recovers() {
    let path = r"\Processor(_Total)\% Processor Time";
    let counters = vec![counter(path, "system.cpu.time")];
    let collect_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let factory_calls = Arc::clone(&collect_calls);
    let mut worker = Worker::start_with(
        counters,
        Duration::from_secs(30),
        4_096,
        move |counters, max_expanded_counters| {
            Query::open_with(
                RecoveringPrimePdh {
                    collect_calls: Arc::clone(&factory_calls),
                },
                counters,
                Duration::from_secs(30),
                Duration::from_secs(30),
                256,
                max_expanded_counters,
            )
        },
    )
    .await
    .unwrap();

    let warming = worker.collect().await.unwrap();
    assert_eq!(warming.points[0].value, SampleValue::Warming);
    assert_eq!(warming.metadata.failures.len(), 1);
    assert_eq!(warming.metadata.failures[0].counter_index, 0);
    assert_eq!(warming.metadata.failures[0].reason, "PdhCollectQueryData");
    assert!(warming.metadata.failures[0].diagnostic_only);
    assert_eq!(warming.metadata.diagnostics.counter_read_failures, 0);
    assert_eq!(warming.metadata.diagnostics.scrape_failures, 1);
    assert_eq!(warming.metadata.diagnostics.retry_attempts, 1);
    assert_eq!(warming.metadata.diagnostics.retry_recoveries, 1);

    let recovered = worker.collect().await.unwrap();
    assert!(matches!(
        recovered.points[0].value,
        SampleValue::Value(Number::Double(1.0))
    ));
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
    );
}

/// Scenario: A calculated query reaches the failure threshold and is rebuilt during a scrape.
/// Guarantees: The rebuild scrape is empty after priming, and the following scrape emits over the full post-prime interval.
#[test]
fn rebuilt_query_defers_values_until_the_next_scrape() {
    let path = r"\Processor(_Total)\% Processor Time";
    let first_query_failure = Arc::new(AtomicBool::new(false));
    let factory_first_query_failure = Arc::clone(&first_query_failure);
    let open_calls = Rc::new(Cell::new(0_usize));
    let factory_open_calls = Rc::clone(&open_calls);
    let mut open = move |configs: Vec<CounterConfig>, max_expanded_counters: usize| {
        let open_index = factory_open_calls.get();
        factory_open_calls.set(open_index + 1);
        Query::open_with(
            RebuildPrimePdh {
                fail_collect: if open_index == 0 {
                    Arc::clone(&factory_first_query_failure)
                } else {
                    Arc::new(AtomicBool::new(false))
                },
                collect_calls: 0,
            },
            configs,
            Duration::from_secs(30),
            Duration::from_secs(30),
            256,
            max_expanded_counters,
        )
    };
    let plan = query_plans(vec![counter(path, "system.cpu.time")])
        .pop()
        .unwrap();
    let mut query = QuerySlot::open_initial(plan, 4_096, &mut open).unwrap();
    first_query_failure.store(true, Ordering::Relaxed);
    let now = Instant::now();
    for _ in 0..QUERY_REBUILD_AFTER_FAILURES {
        assert!(matches!(
            query.collect(now, Duration::ZERO, 4_096, &mut open),
            Err(Error::Pdh {
                operation: "PdhCollectQueryData",
                ..
            })
        ));
    }

    let rebuilt = query
        .collect(now, Duration::ZERO, 4_096, &mut open)
        .unwrap();

    assert!(rebuilt.points.is_empty());
    assert_eq!(rebuilt.timestamp_unix_nano, 110);
    assert_eq!(rebuilt.metadata.diagnostics.query_rebuild_attempts, 1);
    assert_eq!(rebuilt.metadata.diagnostics.query_rebuild_recoveries, 1);
    assert_eq!(rebuilt.metadata.diagnostics.warmup_omissions, 2);

    let recovered = query
        .collect(now, Duration::ZERO, 4_096, &mut open)
        .unwrap();
    assert!(matches!(
        recovered.points[0].value,
        SampleValue::Value(Number::Double(2.0))
    ));
    assert_eq!(recovered.points[0].timestamp_unix_nano, 120);
    assert_eq!(recovered.points[0].start_time_unix_nano, None);
    assert_eq!(recovered.metadata.diagnostics.query_rebuild_attempts, 0);
    assert_eq!(recovered.metadata.diagnostics.query_rebuild_recoveries, 0);
    assert_eq!(recovered.metadata.diagnostics.warmup_omissions, 0);
    assert_eq!(open_calls.get(), 2);
}

/// Scenario: Rebuild priming fails while another query succeeds in the same scrape.
/// Guarantees: The partial scrape counts one query failure and the rebuilt query carries no duplicate pending prime failure.
#[test]
fn rebuild_prime_failure_is_counted_once() {
    let healthy_path = r"\Processor(_Total)\% Processor Time";
    let rebuilding_path = r"\Processor(0)\% Processor Time";
    let first_rebuilding_failure = Arc::new(AtomicBool::new(false));
    let factory_first_rebuilding_failure = Arc::clone(&first_rebuilding_failure);
    let rebuild_prime_failure = Arc::new(AtomicBool::new(true));
    let factory_rebuild_prime_failure = Arc::clone(&rebuild_prime_failure);
    let open_counts = Rc::new(RefCell::new(BTreeMap::<String, usize>::new()));
    let factory_open_counts = Rc::clone(&open_counts);
    let mut open = move |configs: Vec<CounterConfig>, max_expanded_counters: usize| {
        let path = configs[0].path.clone();
        let open_index = {
            let mut counts = factory_open_counts.borrow_mut();
            let count = counts.entry(path.clone()).or_default();
            let open_index = *count;
            *count += 1;
            open_index
        };
        let fail_collect = if path == rebuilding_path {
            if open_index == 0 {
                Arc::clone(&factory_first_rebuilding_failure)
            } else {
                Arc::clone(&factory_rebuild_prime_failure)
            }
        } else {
            Arc::new(AtomicBool::new(false))
        };
        Query::open_with(
            RebuildPrimePdh {
                fail_collect,
                collect_calls: 0,
            },
            configs,
            Duration::from_secs(30),
            Duration::from_secs(30),
            256,
            max_expanded_counters,
        )
    };
    let healthy_plan = QueryPlan {
        configs: vec![counter(healthy_path, "system.cpu.total")],
        global_indices: vec![0],
        recreate_query: false,
    };
    let rebuilding_plan = QueryPlan {
        configs: vec![counter(rebuilding_path, "system.cpu.core")],
        global_indices: vec![1],
        recreate_query: false,
    };
    let mut queries = vec![
        QuerySlot::open_initial(healthy_plan, 4_096, &mut open).unwrap(),
        QuerySlot::open_initial(rebuilding_plan, 4_096, &mut open).unwrap(),
    ];
    first_rebuilding_failure.store(true, Ordering::Relaxed);
    let now = Instant::now();
    for _ in 0..QUERY_REBUILD_AFTER_FAILURES {
        assert!(
            queries[1]
                .collect(now, Duration::ZERO, 4_096, &mut open)
                .is_err()
        );
    }

    let sample = collect_queries(&mut queries, Duration::ZERO, 4_096, &mut open).unwrap();

    assert_eq!(sample.points.len(), 1);
    assert_eq!(sample.points[0].counter_index, 0);
    assert_eq!(sample.metadata.diagnostics.scrape_failures, 1);
    assert_eq!(sample.metadata.failures.len(), 1);
    assert_eq!(sample.metadata.failures[0].counter_index, 1);
    assert_eq!(sample.metadata.failures[0].reason, "PdhCollectQueryData");
    assert!(!sample.metadata.failures[0].diagnostic_only);
    let rebuilt = queries[1].query.as_ref().unwrap();
    assert_eq!(rebuilt.pending.diagnostics.scrape_failures, 0);
    assert!(rebuilt.pending.failures.is_empty());
}

fn simulated_blocked_worker(cleanup_complete: Arc<AtomicBool>) -> (Worker, mpsc::Sender<()>) {
    let (tx, rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::channel();
    let (completion_tx, completion_rx) = oneshot::channel();
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let join = std::thread::spawn(move || {
        let _commands = rx;
        let _ = release_rx.recv();
        cleanup_complete.store(true, Ordering::Release);
        let _ = completion_tx.send(());
    });
    (
        Worker {
            tx,
            accepted: Arc::new(AtomicBool::new(false)),
            shutdown_requested,
            join: Some(join),
            completion: Some(completion_rx),
        },
        release_tx,
    )
}

fn empty_sample() -> Sample {
    Sample {
        timestamp_unix_nano: 2,
        points: Vec::new(),
        metadata: PendingMetadata::default(),
    }
}

fn controlled_worker() -> (
    Worker,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
    oneshot::Receiver<()>,
) {
    let (tx, rx) = mpsc::sync_channel(1);
    let accepted = Arc::new(AtomicBool::new(false));
    let worker_accepted = Arc::clone(&accepted);
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let worker_shutdown = Arc::clone(&shutdown_requested);
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (released_tx, released_rx) = oneshot::channel();
    let (completion_tx, completion_rx) = oneshot::channel();
    let join = std::thread::spawn(move || {
        let mut first = Some((started_tx, release_rx, released_tx));
        while !worker_shutdown.load(Ordering::Acquire) {
            match rx.recv() {
                Ok(Command::Collect(response)) => {
                    let accepted = AcceptedCollectGuard::new(Arc::clone(&worker_accepted));
                    if let Some((started, release, released)) = first.take() {
                        let _ = started.send(());
                        let _ = release.blocking_recv();
                        drop(accepted);
                        let _ = response.send(Ok(empty_sample()));
                        let _ = released.send(());
                    } else {
                        drop(accepted);
                        let _ = response.send(Ok(empty_sample()));
                    }
                }
                Ok(Command::PublishMonotonic(_, response)) => {
                    let _ = response.send(());
                }
                Ok(Command::Shutdown) | Err(_) => break,
            }
        }
        let _ = completion_tx.send(());
    });
    (
        Worker {
            tx,
            accepted,
            shutdown_requested,
            join: Some(join),
            completion: Some(completion_rx),
        },
        started_rx,
        release_tx,
        released_rx,
    )
}

/// Scenario: A collection future times out while the worker remains inside a native call.
/// Guarantees: Later requests fail fast as busy and collection resumes after the call returns.
#[tokio::test(flavor = "current_thread")]
async fn bounds_timed_out_collection_and_recovers() {
    let (mut worker, started, release, released) = controlled_worker();
    {
        let first = tokio::time::timeout(Duration::from_millis(20), worker.collect());
        tokio::pin!(first);
        tokio::select! {
            result = &mut first => panic!("collection resolved before release: {result:?}"),
            result = started => result.expect("worker should start the first collection"),
        }
        assert!(first.await.is_err());
    }
    assert!(matches!(worker.collect().await, Err(Error::WorkerBusy)));
    release.send(()).expect("release worker collection");
    released
        .await
        .expect("worker should finish blocked collection");
    assert!(worker.collect().await.is_ok());
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .expect("worker shutdown")
    );
}

/// Scenario: The worker's capacity-one command queue already contains a request.
/// Guarantees: Additional work fails explicitly instead of growing an unbounded backlog.
#[test]
fn pending_work_is_bounded() {
    let (tx, _rx) = mpsc::sync_channel(1);
    let (first, _) = oneshot::channel();
    let (second, _) = oneshot::channel();
    tx.try_send(Command::Collect(first)).unwrap();
    assert!(matches!(
        tx.try_send(Command::Collect(second)),
        Err(mpsc::TrySendError::Full(_))
    ));
}

/// Scenario: Startup or recovery deltas are merged into a completed collection.
/// Guarantees: Cumulative diagnostic deltas are added without overwriting the collection's current active-counter gauge.
#[test]
fn diagnostic_merge_preserves_current_active_count() {
    let mut current = SampleDiagnostics {
        active_expanded_counters: 2,
        instances_added: 2,
        ..Default::default()
    };
    let pending = SampleDiagnostics {
        active_expanded_counters: 0,
        discovery_refreshes: 1,
        ..Default::default()
    };

    merge_diagnostics(&mut current, pending);

    assert_eq!(current.active_expanded_counters, 2);
    assert_eq!(current.instances_added, 2);
    assert_eq!(current.discovery_refreshes, 1);
}

/// Scenario: Query metadata survives a failed collection and is merged with later recovery evidence.
/// Guarantees: Diagnostic counts, counter failures, and expansion overflows remain ordered and observable.
#[test]
fn sample_metadata_merge_preserves_pending_evidence() {
    let mut metadata = PendingMetadata {
        diagnostics: SampleDiagnostics {
            counter_add_failures: 1,
            ..Default::default()
        },
        failures: vec![SampleFailure {
            counter_index: 0,
            reason: "initial",
            error: "initial failure".to_owned(),
            diagnostic_only: false,
        }],
        overflows: vec![ExpansionOverflow {
            counter_index: 0,
            reason: "per_wildcard",
            discovered: 3,
            retained: 2,
            omitted: 1,
        }],
    };

    metadata.merge(PendingMetadata {
        diagnostics: SampleDiagnostics {
            retry_recoveries: 1,
            ..Default::default()
        },
        failures: vec![SampleFailure {
            counter_index: 1,
            reason: "recovery",
            error: "recovery failure".to_owned(),
            diagnostic_only: false,
        }],
        overflows: vec![ExpansionOverflow {
            counter_index: 1,
            reason: "receiver_total",
            discovered: 4,
            retained: 3,
            omitted: 1,
        }],
    });

    assert_eq!(metadata.diagnostics.counter_add_failures, 1);
    assert_eq!(metadata.diagnostics.retry_recoveries, 1);
    assert_eq!(metadata.failures.len(), 2);
    assert_eq!(metadata.failures[0].reason, "initial");
    assert_eq!(metadata.failures[1].reason, "recovery");
    assert_eq!(metadata.overflows.len(), 2);
    assert_eq!(metadata.overflows[0].reason, "per_wildcard");
    assert_eq!(metadata.overflows[1].reason, "receiver_total");

    metadata.merge(PendingMetadata {
        diagnostics: SampleDiagnostics::default(),
        failures: vec![SampleFailure {
            counter_index: 0,
            reason: "initial",
            error: "latest failure".to_owned(),
            diagnostic_only: false,
        }],
        overflows: vec![ExpansionOverflow {
            counter_index: 0,
            reason: "per_wildcard",
            discovered: 5,
            retained: 2,
            omitted: 3,
        }],
    });

    assert_eq!(metadata.failures.len(), 2);
    assert_eq!(metadata.failures[0].error, "latest failure");
    assert_eq!(metadata.overflows.len(), 2);
    assert_eq!(metadata.overflows[0].discovered, 5);
}

/// Scenario: Older slot metadata and the current scrape report the same counter failure identity.
/// Guarantees: Diagnostic evidence accumulates while the newer scrape failure replaces the older detail.
#[test]
fn slot_metadata_merge_keeps_newer_failure() {
    let mut pending = PendingMetadata {
        diagnostics: SampleDiagnostics {
            retry_attempts: 1,
            ..Default::default()
        },
        failures: vec![SampleFailure {
            counter_index: 0,
            reason: "counter read",
            error: "pending failure".to_owned(),
            diagnostic_only: false,
        }],
        overflows: vec![ExpansionOverflow {
            counter_index: 0,
            reason: "per_wildcard",
            discovered: 3,
            retained: 2,
            omitted: 1,
        }],
    };
    let mut sample = Sample {
        timestamp_unix_nano: 2,
        points: Vec::new(),
        metadata: PendingMetadata {
            failures: vec![SampleFailure {
                counter_index: 0,
                reason: "counter read",
                error: "current failure".to_owned(),
                diagnostic_only: false,
            }],
            overflows: vec![ExpansionOverflow {
                counter_index: 1,
                reason: "receiver_total",
                discovered: 4,
                retained: 3,
                omitted: 1,
            }],
            diagnostics: SampleDiagnostics {
                active_expanded_counters: 2,
                retry_recoveries: 1,
                ..Default::default()
            },
        },
    };

    sample.metadata.merge_older(std::mem::take(&mut pending));

    assert_eq!(sample.metadata.diagnostics.active_expanded_counters, 2);
    assert_eq!(sample.metadata.diagnostics.retry_attempts, 1);
    assert_eq!(sample.metadata.diagnostics.retry_recoveries, 1);
    assert_eq!(sample.metadata.failures.len(), 1);
    assert_eq!(sample.metadata.failures[0].error, "current failure");
    assert_eq!(sample.metadata.overflows[0].reason, "receiver_total");
    assert_eq!(sample.metadata.overflows[1].reason, "per_wildcard");
    assert_eq!(pending.failures.len(), 0);
    assert_eq!(pending.overflows.len(), 0);
}

/// Scenario: A query repeatedly fails while the collection cadence caps recovery delay.
/// Guarantees: Query recovery stays responsive even when membership discovery uses a longer interval.
#[test]
fn retry_delay_is_exponential_and_capped() {
    let cap = Duration::from_secs(10);
    assert_eq!(retry_delay(1, cap), Duration::from_secs(1));
    assert_eq!(retry_delay(2, cap), Duration::from_secs(2));
    assert_eq!(retry_delay(3, cap), Duration::from_secs(4));
    assert_eq!(retry_delay(4, cap), Duration::from_secs(8));
    assert_eq!(retry_delay(5, cap), cap);
    assert_eq!(retry_delay(u32::MAX, cap), cap);
    assert_eq!(
        retry_delay(1, Duration::from_secs(1)),
        Duration::from_secs(1)
    );
}

/// Scenario: Query collection repeatedly fails with a two-second retry cap and a sixty-second membership refresh.
/// Guarantees: Recovery rebuilds on collection cadence instead of waiting for the unrelated wildcard discovery clock.
#[tokio::test(flavor = "current_thread")]
async fn query_recovery_cap_is_independent_of_membership_refresh() {
    let path = r"\Memory\Available Bytes";
    let fail_collect = Arc::new(AtomicBool::new(false));
    let factory_fail_collect = Arc::clone(&fail_collect);
    let open_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let factory_open_calls = Arc::clone(&open_calls);
    let observed_refreshes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let factory_refreshes = Arc::clone(&observed_refreshes);
    let cadence = QueryCadence::new(Duration::from_secs(2), Duration::from_secs(60));
    let mut worker = Worker::start_with_cadence_and(
        vec![counter(path, "system.memory.available")],
        cadence,
        4_096,
        move |configs, max_native_counters, cadence| {
            let _ = factory_open_calls.fetch_add(1, Ordering::AcqRel);
            factory_refreshes
                .lock()
                .unwrap()
                .push(cadence.wildcard_refresh());
            Query::open_with_cadences(
                RebuildPrimePdh {
                    fail_collect: Arc::clone(&factory_fail_collect),
                    collect_calls: 0,
                },
                configs,
                cadence.retry_cap(),
                cadence.wildcard_refresh(),
                256,
                max_native_counters,
            )
        },
    )
    .await
    .unwrap();
    fail_collect.store(true, Ordering::Release);

    assert!(matches!(
        worker.collect().await,
        Err(Error::Pdh {
            operation: "PdhCollectQueryData",
            ..
        })
    ));
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(matches!(
        worker.collect().await,
        Err(Error::Pdh {
            operation: "PdhCollectQueryData",
            ..
        })
    ));
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    assert!(matches!(
        worker.collect().await,
        Err(Error::Pdh {
            operation: "PdhCollectQueryData",
            ..
        })
    ));
    fail_collect.store(false, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(2_100)).await;

    let rebuilt = worker.collect().await.unwrap();

    assert!(rebuilt.points.is_empty());
    assert_eq!(rebuilt.metadata.diagnostics.query_rebuild_recoveries, 1);
    assert_eq!(open_calls.load(Ordering::Acquire), 2);
    assert_eq!(
        observed_refreshes.lock().unwrap().as_slice(),
        [Duration::from_secs(60), Duration::from_secs(60)]
    );
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
    );
}

/// Scenario: Query collection fails transiently and then remains unavailable.
/// Guarantees: Valid query history survives the first two retries and a rebuild is requested at the bounded threshold.
#[test]
fn query_rebuild_waits_for_bounded_retries() {
    assert!(!should_rebuild_query(1));
    assert!(!should_rebuild_query(2));
    assert!(should_rebuild_query(3));
    assert!(should_rebuild_query(u32::MAX));
}

/// Scenario: A simulated PDH call remains blocked past the shutdown deadline.
/// Guarantees: Shutdown returns at the deadline, cleanup occurs later on the worker,
/// and worker-owned resources remain alive until that cleanup completes.
#[tokio::test(flavor = "current_thread")]
async fn blocked_worker_releases_resources_after_timeout() {
    let _serial = TEST_PDH_LOCK.lock().await;
    let cleanup_complete = Arc::new(AtomicBool::new(false));
    let (mut worker, release) = simulated_blocked_worker(Arc::clone(&cleanup_complete));

    let start = Instant::now();
    assert!(
        !worker
            .shutdown(start + Duration::from_millis(25))
            .await
            .unwrap()
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(!cleanup_complete.load(Ordering::Acquire));
    release.send(()).unwrap();
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert!(cleanup_complete.load(Ordering::Acquire));
}
