// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Scenario: Shared mappings and independently recreated mappings expand to multiple normalized paths.
/// Guarantees: Ordinary paths share one query while every opted-in source mapping receives exactly one isolated query.
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
        shared_second,
        recreated_first,
        recreated_second,
        another_recreated,
    ]);

    assert_eq!(plans.len(), 3);
    assert_eq!(plans[0].global_indices, [0, 1]);
    assert!(!plans[0].recreate_query);
    assert_eq!(plans[1].global_indices, [2, 3]);
    assert!(plans[1].recreate_query);
    assert_eq!(plans[2].global_indices, [4]);
    assert!(plans[2].recreate_query);
}

/// Scenario: Multiple recreated wildcard mappings compete for one receiver-wide expansion limit.
/// Guarantees: Initial queries and overlapping replacements share the configured limit instead of multiplying it per query.
#[test]
fn recreated_queries_share_receiver_expansion_limit() {
    let mut first = counter(r"\Process(first*)\Private Bytes", "process.first");
    first.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    first.recreate_query = true;
    first.source_mapping_index = 0;
    let mut second = counter(r"\Process(second*)\Private Bytes", "process.second");
    second.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    second.recreate_query = true;
    second.source_mapping_index = 1;

    let observed_limits = Rc::new(RefCell::new(Vec::new()));
    let factory_limits = Rc::clone(&observed_limits);
    let mut open = move |configs: Vec<CounterConfig>, max_expanded_counters: usize| {
        factory_limits.borrow_mut().push(max_expanded_counters);
        let template = configs[0].path.clone();
        let prefix = if configs[0].source_mapping_index == 0 {
            "first"
        } else {
            "second"
        };
        let paths = (0..3)
            .map(|index| format!(r"\Process({prefix}{index})\Private Bytes"))
            .collect::<Vec<_>>();
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
            collect_has_data: true,
            collect_error: false,
            timestamp: 100,
            timestamp_error: false,
            remove_error: false,
            removed: Vec::new(),
            expansions: [(template, paths)].into_iter().collect(),
        }));
        Query::open_with(
            FakePdh(state),
            configs,
            Duration::from_secs(30),
            256,
            max_expanded_counters,
        )
    };

    let mut queries = Vec::new();
    for plan in query_plans(vec![first, second]) {
        let active = queries
            .iter()
            .map(QuerySlot::active_expanded_count)
            .sum::<usize>();
        queries.push(QuerySlot::open_initial(plan, 4 - active, &mut open).unwrap());
    }
    assert_eq!(
        queries
            .iter()
            .map(QuerySlot::active_expanded_count)
            .collect::<Vec<_>>(),
        [3, 1]
    );

    let sample = collect_queries(&mut queries, Duration::from_secs(30), 4, &mut open).unwrap();
    assert_eq!(sample.metadata.diagnostics.active_expanded_counters, 4);
    assert_eq!(observed_limits.borrow().as_slice(), [4, 1, 3, 1]);
    assert_eq!(
        queries
            .iter()
            .map(QuerySlot::active_expanded_count)
            .sum::<usize>(),
        4
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
/// Guarantees: The scrape emits the healthy peer, reports the isolated mapping failure, and does not recreate the failed query.
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
    assert_eq!(sample.points[0].start_time_unix_nano, Some(100));
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
    }
    assert_eq!(open_calls.get(), 6);
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

/// Scenario: A counter repeatedly fails while the refresh cadence caps recovery delay.
/// Guarantees: Retry delay grows exponentially, never becomes zero, and never exceeds the refresh cadence.
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
