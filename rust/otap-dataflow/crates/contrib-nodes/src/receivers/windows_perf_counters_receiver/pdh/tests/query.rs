// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use windows_sys::Win32::System::Performance::PDH_INVALID_HANDLE;

struct InvalidHandleState {
    next_handle: usize,
    paths: BTreeMap<usize, String>,
    active: BTreeSet<usize>,
    max_active: usize,
    add_attempts: Vec<(String, bool)>,
    cleanup_attempts: Vec<usize>,
    invalid_handles: BTreeSet<usize>,
    failed_adds_remaining: usize,
    cleanup_fails: bool,
    timestamp: i64,
    raw_value: i64,
    no_observation: BTreeSet<String>,
    expansions: BTreeMap<String, Vec<String>>,
}

struct InvalidHandlePdh(Rc<RefCell<InvalidHandleState>>);

impl PdhApi for InvalidHandlePdh {
    type Counter = usize;

    fn add(&mut self, path: &str, localized: bool) -> Result<Self::Counter, Error> {
        let mut state = self.0.borrow_mut();
        state.add_attempts.push((path.to_owned(), localized));
        if state.failed_adds_remaining > 0 {
            state.failed_adds_remaining -= 1;
            return Err(Error::Pdh {
                operation: if localized {
                    "PdhAddCounterW"
                } else {
                    "PdhAddEnglishCounterW"
                },
                path: path.to_owned(),
                status: 1,
            });
        }
        let handle = state.next_handle;
        state.next_handle += 1;
        let _ = state.paths.insert(handle, path.to_owned());
        let _ = state.active.insert(handle);
        state.max_active = state.max_active.max(state.active.len());
        Ok(handle)
    }

    fn remove(&mut self, counter: &Self::Counter, path: &str) -> Result<(), Error> {
        let mut state = self.0.borrow_mut();
        state.cleanup_attempts.push(*counter);
        if state.cleanup_fails {
            return Err(Error::Pdh {
                operation: "PdhRemoveCounter",
                path: path.to_owned(),
                status: 1,
            });
        }
        let _ = state.active.remove(counter);
        Ok(())
    }

    fn counter_type(&mut self, _counter: &Self::Counter, path: &str) -> Result<u32, Error> {
        if path.contains("Operations Delta") {
            Ok(0x0040_0500)
        } else if path.contains("Available Bytes") {
            Ok(0x0001_0100)
        } else {
            Ok(0x2041_0500)
        }
    }

    fn collect(&mut self) -> Result<bool, Error> {
        Ok(true)
    }

    fn raw(&mut self, counter: &Self::Counter, path: &str) -> Result<Option<RawSample>, Error> {
        let state = self.0.borrow();
        if state.invalid_handles.contains(counter) {
            return Err(Error::Pdh {
                operation: "PdhGetRawCounterValue",
                path: path.to_owned(),
                status: PDH_INVALID_HANDLE,
            });
        }
        if state.no_observation.contains(path) {
            return Ok(None);
        }
        Ok(Some(RawSample {
            second_value: state.raw_value,
            multi_count: 1,
        }))
    }

    fn formatted(
        &mut self,
        counter: &Self::Counter,
        path: &str,
        format: ValueFormat,
    ) -> Result<Option<Number>, Error> {
        let state = self.0.borrow();
        if state.invalid_handles.contains(counter) {
            return Err(Error::Pdh {
                operation: "PdhGetFormattedCounterValue",
                path: path.to_owned(),
                status: PDH_INVALID_HANDLE,
            });
        }
        if state.no_observation.contains(path) {
            return Ok(None);
        }
        Ok(Some(match format {
            ValueFormat::Integer => Number::Integer(state.raw_value),
            ValueFormat::Double => Number::Double(state.raw_value as f64),
        }))
    }

    fn timestamp(&mut self) -> Result<i64, Error> {
        Ok(self.0.borrow().timestamp)
    }

    fn expand(&mut self, path: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .0
            .borrow()
            .expansions
            .get(path)
            .cloned()
            .unwrap_or_default())
    }

    fn parse_instance(&mut self, path: &str) -> Result<InstanceIdentity, Error> {
        NativePdh::parse_instance_path(path)
    }
}

fn invalid_handle_query(
    configs: Vec<CounterConfig>,
    expansions: BTreeMap<String, Vec<String>>,
) -> (Rc<RefCell<InvalidHandleState>>, Query<InvalidHandlePdh>) {
    let state = Rc::new(RefCell::new(InvalidHandleState {
        next_handle: 0,
        paths: BTreeMap::new(),
        active: BTreeSet::new(),
        max_active: 0,
        add_attempts: Vec::new(),
        cleanup_attempts: Vec::new(),
        invalid_handles: BTreeSet::new(),
        failed_adds_remaining: 0,
        cleanup_fails: false,
        timestamp: 100,
        raw_value: 10,
        no_observation: BTreeSet::new(),
        expansions,
    }));
    let query = Query::open_with(
        InvalidHandlePdh(Rc::clone(&state)),
        configs,
        Duration::ZERO,
        256,
        4_096,
    )
    .unwrap();
    (state, query)
}

fn handle_for_path(state: &InvalidHandleState, path: &str) -> usize {
    state
        .paths
        .iter()
        .find_map(|(handle, candidate)| (candidate == path).then_some(*handle))
        .unwrap()
}

/// Scenario: One wildcard delta counter's native handle becomes invalid beside a healthy calculated counter.
/// Guarantees: Only the invalid handle retires, bounded re-add failures do not accumulate handles, localization is preserved, and replacement history starts fresh.
#[test]
fn invalid_handle_retries_one_counter_with_fresh_history() {
    let wildcard = r"\Process(worker*)\Operations Delta";
    let expanded = r"\Process(worker)\Operations Delta";
    let healthy = r"\Processor(_Total)\% Processor Time";
    let affected = CounterConfig {
        instance_config: InstanceConfig::Wildcard {
            excluded_aggregation: Some("_Total".to_owned()),
        },
        ..delta_sum(wildcard, "process.operations", false)
    };
    let (state, mut query) = invalid_handle_query(
        vec![affected, counter(healthy, "system.cpu.time")],
        BTreeMap::from([(wildcard.to_owned(), vec![expanded.to_owned()])]),
    );

    query.prime().unwrap();
    let old_handle = handle_for_path(&state.borrow(), expanded);
    {
        let mut state = state.borrow_mut();
        let _ = state.invalid_handles.insert(old_handle);
        state.timestamp = 110;
        state.raw_value = 20;
    }

    let retired = query.collect().unwrap();
    assert_eq!(retired.points.len(), 1);
    assert_eq!(retired.points[0].counter_index, 1);
    assert!(matches!(
        retired.points[0].value,
        SampleValue::Value(Number::Double(20.0))
    ));
    assert_eq!(retired.metadata.diagnostics.counter_read_failures, 1);
    assert_eq!(retired.metadata.failures[0].counter_index, 0);
    {
        let state = state.borrow();
        assert_eq!(state.cleanup_attempts, [old_handle]);
        assert_eq!(state.active.len(), 1);
        assert_eq!(state.max_active, 2);
    }

    state.borrow_mut().failed_adds_remaining = 2;
    for (timestamp, raw_value, expected_attempts) in [(120, 30, 3), (130, 40, 4)] {
        {
            let mut state = state.borrow_mut();
            state.timestamp = timestamp;
            state.raw_value = raw_value;
        }
        let retry = query.collect().unwrap();
        assert_eq!(retry.points.len(), 1);
        assert_eq!(retry.points[0].counter_index, 1);
        assert!(matches!(
            retry.points[0].value,
            SampleValue::Value(Number::Double(value)) if value == raw_value as f64
        ));
        assert_eq!(retry.metadata.diagnostics.retry_attempts, 1);
        assert_eq!(retry.metadata.diagnostics.counter_add_failures, 1);
        let state = state.borrow();
        assert_eq!(state.add_attempts.len(), expected_attempts);
        assert_eq!(state.active.len(), 1);
        assert_eq!(state.max_active, 2);
    }

    {
        let mut state = state.borrow_mut();
        state.timestamp = 140;
        state.raw_value = 50;
    }
    let warming = query.collect().unwrap();
    assert!(
        warming.points.iter().any(|point| {
            point.counter_index == 0 && matches!(point.value, SampleValue::Warming)
        })
    );
    assert!(warming.points.iter().any(|point| {
        point.counter_index == 1 && matches!(point.value, SampleValue::Value(Number::Double(50.0)))
    }));
    assert_eq!(warming.metadata.diagnostics.retry_attempts, 1);
    assert_eq!(warming.metadata.diagnostics.retry_recoveries, 1);
    {
        let state = state.borrow();
        assert_eq!(state.active.len(), 2);
        assert_eq!(state.max_active, 2);
        let affected_adds = state
            .add_attempts
            .iter()
            .filter(|(path, _)| path == expanded)
            .collect::<Vec<_>>();
        assert_eq!(affected_adds.len(), 4);
        assert!(affected_adds.iter().all(|(_, localized)| *localized));
    }

    {
        let mut state = state.borrow_mut();
        state.timestamp = 150;
        state.raw_value = 60;
    }
    let recovered = query.collect().unwrap();
    let affected = recovered
        .points
        .iter()
        .find(|point| point.counter_index == 0)
        .unwrap();
    assert_eq!(affected.start_time_unix_nano, Some(140));
    assert!(matches!(
        affected.value,
        SampleValue::Value(Number::Double(60.0))
    ));
}

/// Scenario: An exact direct counter returns PDH_INVALID_HANDLE from its formatted read.
/// Guarantees: Formatted reads use the same typed retirement path and preserve exact English-add behavior on replacement.
#[test]
fn formatted_invalid_handle_retries_exact_counter() {
    let path = r"\Memory\Available Bytes";
    let (state, mut query) = invalid_handle_query(
        vec![counter(path, "system.memory.available")],
        BTreeMap::new(),
    );
    let old_handle = handle_for_path(&state.borrow(), path);
    let _ = state.borrow_mut().invalid_handles.insert(old_handle);

    let retired = query.collect().unwrap();
    assert!(retired.points.is_empty());
    assert_eq!(retired.metadata.diagnostics.counter_read_failures, 1);
    {
        let state = state.borrow();
        assert_eq!(state.cleanup_attempts, [old_handle]);
        assert_eq!(state.active.len(), 0);
    }

    {
        let mut state = state.borrow_mut();
        state.timestamp = 110;
        state.raw_value = 42;
    }
    let recovered = query.collect().unwrap();
    assert!(matches!(
        recovered.points[0].value,
        SampleValue::Value(Number::Integer(42))
    ));
    assert_eq!(recovered.metadata.diagnostics.retry_attempts, 1);
    assert_eq!(recovered.metadata.diagnostics.retry_recoveries, 1);
    let state = state.borrow();
    assert_eq!(state.active.len(), 1);
    assert_eq!(state.max_active, 1);
    assert_eq!(state.add_attempts.len(), 2);
    assert!(state.add_attempts.iter().all(|(_, localized)| !localized));
}

/// Scenario: A counter read produces normal no-observation results without an invalid native handle.
/// Guarantees: No-observation resets readiness without removing, re-adding, or retrying the counter.
#[test]
fn no_observation_does_not_recover_counter_handle() {
    let path = r"\Provider\Operations Delta";
    let (state, mut query) = invalid_handle_query(
        vec![delta_sum(path, "provider.operations", false)],
        BTreeMap::new(),
    );
    let _ = state.borrow_mut().no_observation.insert(path.to_owned());

    for timestamp in [110, 120] {
        state.borrow_mut().timestamp = timestamp;
        let sample = query.collect().unwrap();
        assert!(matches!(sample.points[0].value, SampleValue::NoObservation));
        assert_eq!(sample.metadata.diagnostics.counter_read_failures, 0);
        assert_eq!(sample.metadata.diagnostics.retry_attempts, 0);
    }

    let state = state.borrow();
    assert_eq!(state.add_attempts.len(), 1);
    assert!(state.cleanup_attempts.is_empty());
    assert_eq!(state.active.len(), 1);
}

/// Scenario: Cleanup fails while retiring a counter whose native handle is invalid.
/// Guarantees: The handle remains tracked and the typed cleanup failure reaches the existing immediate query-rebuild path.
#[test]
fn invalid_handle_cleanup_failure_requires_query_rebuild() {
    let path = r"\Provider\Operations Delta";
    let (state, mut query) = invalid_handle_query(
        vec![delta_sum(path, "provider.operations", false)],
        BTreeMap::new(),
    );
    let handle = handle_for_path(&state.borrow(), path);
    {
        let mut state = state.borrow_mut();
        let _ = state.invalid_handles.insert(handle);
        state.cleanup_fails = true;
    }

    let error = query.collect().unwrap_err();
    assert!(matches!(
        error,
        Error::Pdh {
            operation: "PdhRemoveCounter",
            ..
        }
    ));
    assert!(error.requires_immediate_query_rebuild());
    let state = state.borrow();
    assert_eq!(state.cleanup_attempts, [handle]);
    assert_eq!(state.active, BTreeSet::from([handle]));
    assert_eq!(state.add_attempts.len(), 1);
}

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
    assert_eq!(sample.metadata.diagnostics.warmup_omissions, 2);
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
    assert_eq!(sample.metadata.failures.len(), 1);
    assert_eq!(sample.metadata.failures[0].counter_index, 0);
    assert_eq!(sample.metadata.diagnostics.counter_read_failures, 1);
    assert!(state.borrow().removed.is_empty());
}

/// Scenario: One exact configured path names a duplicate process instance.
/// Guarantees: Query opening preserves parsed instance identity for non-wildcard counters.
#[test]
fn exact_instance_path_preserves_identity() {
    let path = r"\Process(worker#1)\Private Bytes";
    let mut config = counter(path, "windows.process.private");
    config.instance_config = InstanceConfig::Exact {
        emit_attribute: true,
    };
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
    config.instance_config = InstanceConfig::Exact {
        emit_attribute: true,
    };
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
    assert_eq!(sample.metadata.failures.len(), 1);
    assert!(sample.metadata.failures[0].diagnostic_only);
    assert_eq!(sample.metadata.diagnostics.counter_add_failures, 0);
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
    assert_eq!(sample.metadata.failures.len(), 1);
    assert!(
        sample.metadata.failures[0].error.contains("unsupported"),
        "{}",
        sample.metadata.failures[0].error
    );
    assert!(
        sample.metadata.failures[0]
            .error
            .contains("PdhRemoveCounter")
    );
    assert_eq!(state.borrow().removed, vec![0]);
}

/// Scenario: An exact counter first fails transiently, then exposes an unsupported native type on its queued retry.
/// Guarantees: The permanent rejection is reported once, terminates the queued retry, and is not counted as recovery.
#[test]
fn queued_retry_stops_after_unsupported_type() {
    let path = r"\Object\Unsupported";
    let (state, mut query) = fake_query_with_refresh_interval(
        vec![counter(path, "windows.unsupported")],
        Vec::new(),
        Duration::ZERO,
    );
    state.borrow_mut().counters.push(FakeCounter {
        path: path.to_owned(),
        native_type: 0x4003_0401,
        raw: None,
        multi_count: 1,
        formatted: None,
        raw_error: false,
        formatted_error: false,
    });

    let rejected = query.collect().unwrap();
    assert_eq!(rejected.metadata.diagnostics.retry_attempts, 1);
    assert_eq!(rejected.metadata.diagnostics.retry_recoveries, 0);
    assert_eq!(rejected.metadata.diagnostics.counter_add_failures, 2);
    assert_eq!(rejected.metadata.failures.len(), 2);
    assert_eq!(rejected.metadata.failures[1].reason, "unsupported_type");
    assert_eq!(state.borrow().removed, [0]);

    state.borrow_mut().timestamp = 110;
    let settled = query.collect().unwrap();
    assert_eq!(settled.metadata.diagnostics.retry_attempts, 0);
    assert_eq!(settled.metadata.diagnostics.retry_recoveries, 0);
    assert_eq!(settled.metadata.diagnostics.counter_add_failures, 0);
    assert!(settled.metadata.failures.is_empty());
    assert_eq!(state.borrow().removed, [0]);
}

/// Scenario: An exact delta Sum first fails transiently, then its queued retry finds a direct native counter.
/// Guarantees: The invalid metric mapping is reported once, cleans up its native handle, and terminates the queued retry.
#[test]
fn queued_retry_stops_after_invalid_mapping() {
    let path = r"\Object\Operations";
    let (state, mut query) = fake_query_with_refresh_interval(
        vec![delta_sum(path, "object.operations", false)],
        Vec::new(),
        Duration::ZERO,
    );
    state.borrow_mut().counters.push(FakeCounter {
        path: path.to_owned(),
        native_type: 0x0001_0100,
        raw: None,
        multi_count: 1,
        formatted: Some(Number::Integer(42)),
        raw_error: false,
        formatted_error: false,
    });

    let rejected = query.collect().unwrap();
    assert_eq!(rejected.metadata.diagnostics.retry_attempts, 1);
    assert_eq!(rejected.metadata.diagnostics.retry_recoveries, 0);
    assert_eq!(rejected.metadata.diagnostics.counter_add_failures, 2);
    assert_eq!(rejected.metadata.failures.len(), 2);
    assert_eq!(
        rejected.metadata.failures[1].reason,
        "invalid_metric_mapping"
    );
    assert_eq!(state.borrow().removed, [0]);

    state.borrow_mut().timestamp = 110;
    let settled = query.collect().unwrap();
    assert_eq!(settled.metadata.diagnostics.retry_attempts, 0);
    assert_eq!(settled.metadata.diagnostics.retry_recoveries, 0);
    assert_eq!(settled.metadata.diagnostics.counter_add_failures, 0);
    assert!(settled.metadata.failures.is_empty());
    assert_eq!(state.borrow().removed, [0]);
}

/// Scenario: An exact delta Sum first fails transiently, then its queued retry finds an incompatible native type whose cleanup also fails.
/// Guarantees: The permanent CounterSetup wrapper retains the mapping and cleanup evidence while terminating the queued retry.
#[test]
fn queued_retry_stops_after_wrapped_invalid_mapping() {
    let path = r"\Object\Operations";
    let (state, mut query) = fake_query_with_refresh_interval(
        vec![delta_sum(path, "object.operations", false)],
        Vec::new(),
        Duration::ZERO,
    );
    {
        let mut state = state.borrow_mut();
        state.counters.push(FakeCounter {
            path: path.to_owned(),
            native_type: 0x0001_0100,
            raw: None,
            multi_count: 1,
            formatted: Some(Number::Integer(42)),
            raw_error: false,
            formatted_error: false,
        });
        state.remove_error = true;
    }

    let rejected = query.collect().unwrap();
    assert_eq!(rejected.metadata.diagnostics.retry_attempts, 1);
    assert_eq!(rejected.metadata.diagnostics.retry_recoveries, 0);
    assert_eq!(rejected.metadata.diagnostics.counter_add_failures, 2);
    assert_eq!(rejected.metadata.failures.len(), 2);
    assert_eq!(rejected.metadata.failures[1].reason, "counter_setup");
    assert!(
        rejected.metadata.failures[1]
            .error
            .contains("invalid metric mapping")
    );
    assert!(
        rejected.metadata.failures[1]
            .error
            .contains("PdhRemoveCounter")
    );
    assert_eq!(state.borrow().removed, [0]);

    state.borrow_mut().timestamp = 110;
    let settled = query.collect().unwrap();
    assert_eq!(settled.metadata.diagnostics.retry_attempts, 0);
    assert_eq!(settled.metadata.diagnostics.retry_recoveries, 0);
    assert_eq!(settled.metadata.diagnostics.counter_add_failures, 0);
    assert!(settled.metadata.failures.is_empty());
    assert_eq!(state.borrow().removed, [0]);
}

/// Scenario: An exact counter remains transiently unavailable for one queued retry and then becomes valid.
/// Guarantees: The transient failure remains queued through existing backoff state and successful re-add counts one recovery.
#[test]
fn queued_transient_retry_still_recovers() {
    let path = r"\Memory\Available Bytes";
    let (state, mut query) = fake_query_with_refresh_interval(
        vec![counter(path, "system.memory.available")],
        Vec::new(),
        Duration::ZERO,
    );

    let retrying = query.collect().unwrap();
    assert_eq!(retrying.metadata.diagnostics.retry_attempts, 1);
    assert_eq!(retrying.metadata.diagnostics.retry_recoveries, 0);
    assert_eq!(retrying.metadata.diagnostics.counter_add_failures, 2);
    assert_eq!(retrying.metadata.failures.len(), 2);

    {
        let mut state = state.borrow_mut();
        state.timestamp = 110;
        state.counters.push(FakeCounter {
            path: path.to_owned(),
            native_type: 0x0001_0100,
            raw: None,
            multi_count: 1,
            formatted: Some(Number::Integer(42)),
            raw_error: false,
            formatted_error: false,
        });
    }
    let recovered = query.collect().unwrap();
    assert_eq!(recovered.metadata.diagnostics.retry_attempts, 1);
    assert_eq!(recovered.metadata.diagnostics.retry_recoveries, 1);
    assert_eq!(recovered.metadata.diagnostics.counter_add_failures, 0);
    assert!(recovered.metadata.failures.is_empty());
    assert!(matches!(
        recovered.points[0].value,
        SampleValue::Value(Number::Integer(42))
    ));

    state.borrow_mut().timestamp = 120;
    let steady = query.collect().unwrap();
    assert_eq!(steady.metadata.diagnostics.retry_attempts, 0);
    assert_eq!(steady.metadata.diagnostics.retry_recoveries, 0);
    assert!(matches!(
        steady.points[0].value,
        SampleValue::Value(Number::Integer(42))
    ));
}

/// Scenario: A wildcard refresh repeatedly discovers an instance whose native type is permanently unsupported.
/// Guarantees: Each refresh records one bounded rejection without queuing retries or permanently blacklisting rediscovery.
#[test]
fn wildcard_refresh_retries_permanently_rejected_instance() {
    let template = r"\Process(*)\Unsupported";
    let expanded = r"\Process(worker)\Unsupported";
    let mut config = counter(template, "process.unsupported");
    config.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    let (state, mut query) = fake_query_with_refresh_interval(
        vec![config],
        vec![FakeCounter {
            path: expanded.to_owned(),
            native_type: 0x4003_0401,
            raw: None,
            multi_count: 1,
            formatted: None,
            raw_error: false,
            formatted_error: false,
        }],
        Duration::ZERO,
    );
    let _ = state
        .borrow_mut()
        .expansions
        .insert(template.to_owned(), vec![expanded.to_owned()]);

    for (timestamp, expected_attempts) in [(110, 1), (120, 2)] {
        state.borrow_mut().timestamp = timestamp;
        let rejected = query.collect().unwrap();
        assert_eq!(rejected.metadata.diagnostics.retry_attempts, 0);
        assert_eq!(rejected.metadata.diagnostics.retry_recoveries, 0);
        assert_eq!(rejected.metadata.diagnostics.counter_add_failures, 1);
        assert_eq!(rejected.metadata.failures.len(), 1);
        assert_eq!(rejected.metadata.failures[0].reason, "unsupported_type");
        let state = state.borrow();
        assert_eq!(state.added.len(), expected_attempts);
        assert_eq!(state.removed.len(), expected_attempts);
    }
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
    assert_eq!(
        omitted,
        ExpansionAllocation {
            retained: 2,
            per_wildcard_omitted: 1,
            total_omitted: 0,
        }
    );

    let omitted = limit_expanded_paths(&mut paths, 2, 1);
    assert_eq!(paths, ["alpha"]);
    assert_eq!(
        omitted,
        ExpansionAllocation {
            retained: 1,
            per_wildcard_omitted: 0,
            total_omitted: 1,
        }
    );
}

/// Scenario: An earlier configured wildcard succeeds while a later wildcard's discovery fails with retained identities.
/// Guarantees: Failed-discovery retention reserves global capacity before successful discoveries regardless of configuration order.
#[test]
fn failed_discovery_retention_reserves_global_capacity_first() {
    let total_limit = 4;
    let failed = expansion_allocation(3, 10, total_limit);
    let success = expansion_allocation(3, 10, total_limit - failed.retained);

    assert_eq!(
        failed,
        ExpansionAllocation {
            retained: 3,
            per_wildcard_omitted: 0,
            total_omitted: 0,
        }
    );
    assert_eq!(
        success,
        ExpansionAllocation {
            retained: 1,
            per_wildcard_omitted: 0,
            total_omitted: 2,
        }
    );
    assert_eq!(failed.retained + success.retained, total_limit);
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

/// Scenario: A retrying duplicate-name sibling disappears while its peer still has a live handle.
/// Guarantees: The live peer is recreated so calculated history cannot cross a possible PDH renumbering.
#[test]
fn disappearing_retrying_duplicate_resets_healthy_peer() {
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
    let wanted = [live_worker.clone()].into_iter().collect::<BTreeSet<_>>();
    let known_groups = instance_groups(
        [
            (&live_worker, &live_instance),
            (&retrying_worker, &retrying_instance),
        ]
        .into_iter(),
    );
    let wanted_groups = instance_groups([(&live_worker, &live_instance)].into_iter());

    let (removed, added) =
        refresh_changes_with_group_resets(&live, &retrying, &wanted, &known_groups, &wanted_groups);

    assert_eq!(removed.as_slice(), std::slice::from_ref(&live_worker));
    assert_eq!(added.as_slice(), std::slice::from_ref(&live_worker));
    assert!(!removed.contains(&retrying_worker));
    assert!(!added.contains(&retrying_worker));
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
    let allocation = limit_expanded_paths(&mut paths, 1, 1);
    assert_eq!(
        allocation,
        ExpansionAllocation {
            retained: 1,
            per_wildcard_omitted: 0,
            total_omitted: 0,
        }
    );
    assert_eq!(paths, [r"\Process(worker)\Private Bytes"]);
}

/// Scenario: A wildcard discovery returns only the configured aggregate instance.
/// Guarantees: The aggregate value is retained while its Go-compatible `instance` label is omitted.
#[test]
fn retains_wildcard_only_aggregation_without_instance_label() {
    let template = r"\Process(*)\Private Bytes";
    let aggregate = r"\Process(_Total)\Private Bytes";
    let mut config = counter(template, "windows.process.private");
    config.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: Some("_Total".to_owned()),
    };
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
