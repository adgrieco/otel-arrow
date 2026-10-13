// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::super::config::MetricKind;
use super::super::model::Number;
use super::super::{config::Config, otap_builder::into_otap};
use super::*;
use std::cell::RefCell;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::rc::Rc;

fn counter(path: &str, name: &str) -> CounterConfig {
    CounterConfig {
        path: path.to_owned(),
        name: name.to_owned(),
        unit: "By".to_owned(),
        description: Arc::from(format!("Description for {name}.")),
        metric_kind: MetricKind::Gauge,
        attributes: Arc::new(BTreeMap::new()),
        excluded_aggregation_instance: None,
        scale_power10: 0,
    }
}

fn monotonic_sum(path: &str, name: &str, unit: &str) -> CounterConfig {
    CounterConfig {
        metric_kind: MetricKind::Sum { monotonic: true },
        unit: unit.to_owned(),
        ..counter(path, name)
    }
}

fn query_counts() -> (usize, usize) {
    (
        QUERY_OPENS.load(Ordering::Relaxed),
        QUERY_CLOSES.load(Ordering::Relaxed),
    )
}

struct FakeCounter {
    path: String,
    native_type: u32,
    raw: Option<i64>,
    formatted: Option<Number>,
    raw_error: bool,
    formatted_error: bool,
}

struct FakeState {
    counters: Vec<FakeCounter>,
    added: Vec<String>,
    collect_has_data: bool,
    collect_error: bool,
    timestamp: i64,
    timestamp_error: bool,
    remove_error: bool,
    removed: Vec<usize>,
    expansions: BTreeMap<String, Vec<String>>,
}

struct FakePdh(Rc<RefCell<FakeState>>);

impl PdhApi for FakePdh {
    type Counter = usize;

    fn add(&mut self, path: &str, _localized: bool) -> Result<Self::Counter, Error> {
        let mut state = self.0.borrow_mut();
        let counter = state
            .counters
            .iter()
            .position(|counter| counter.path == path)
            .ok_or_else(|| Error::Pdh {
                operation: "PdhAddEnglishCounterW",
                path: path.to_owned(),
                status: 1,
            })?;
        state.added.push(path.to_owned());
        Ok(counter)
    }

    fn remove(&mut self, counter: &Self::Counter, _path: &str) -> Result<(), Error> {
        let mut state = self.0.borrow_mut();
        state.removed.push(*counter);
        if state.remove_error {
            Err(Error::Pdh {
                operation: "PdhRemoveCounter",
                path: state.counters[*counter].path.clone(),
                status: 1,
            })
        } else {
            Ok(())
        }
    }

    fn counter_type(&mut self, counter: &Self::Counter, _path: &str) -> Result<u32, Error> {
        Ok(self.0.borrow().counters[*counter].native_type)
    }

    fn collect(&mut self) -> Result<bool, Error> {
        let state = self.0.borrow();
        if state.collect_error {
            Err(Error::Pdh {
                operation: "PdhCollectQueryData",
                path: "<query>".to_owned(),
                status: 1,
            })
        } else {
            Ok(state.collect_has_data)
        }
    }

    fn raw(&mut self, counter: &Self::Counter, path: &str) -> Result<Option<i64>, Error> {
        let state = self.0.borrow();
        let counter = &state.counters[*counter];
        if counter.raw_error {
            Err(Error::Pdh {
                operation: "PdhGetRawCounterValue",
                path: path.to_owned(),
                status: 1,
            })
        } else {
            Ok(counter.raw)
        }
    }

    fn formatted(
        &mut self,
        counter: &Self::Counter,
        path: &str,
        format: ValueFormat,
    ) -> Result<Option<Number>, Error> {
        let state = self.0.borrow();
        let counter = &state.counters[*counter];
        if counter.formatted_error {
            return Err(Error::CounterData {
                operation: "counter CStatus",
                path: path.to_owned(),
                status: 1,
            });
        }
        if let Some(number) = counter.formatted {
            assert!(matches!(
                (format, number),
                (ValueFormat::Integer, Number::Integer(_))
                    | (ValueFormat::Double, Number::Double(_))
            ));
        }
        Ok(counter.formatted)
    }

    fn timestamp(&mut self) -> Result<i64, Error> {
        let state = self.0.borrow();
        if state.timestamp_error {
            Err(Error::InvalidSample("test timestamp failure"))
        } else {
            Ok(state.timestamp)
        }
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

fn fake_query(
    configs: Vec<CounterConfig>,
    counters: Vec<FakeCounter>,
) -> (Rc<RefCell<FakeState>>, Query<FakePdh>) {
    let state = Rc::new(RefCell::new(FakeState {
        counters,
        added: Vec::new(),
        collect_has_data: true,
        collect_error: false,
        timestamp: 100,
        timestamp_error: false,
        remove_error: false,
        removed: Vec::new(),
        expansions: BTreeMap::new(),
    }));
    let query = Query::open_with(
        FakePdh(Rc::clone(&state)),
        configs,
        Duration::from_secs(30),
        256,
        4_096,
    )
    .unwrap();
    (state, query)
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
        start_time_unix_nano: 1,
        timestamp_unix_nano: 2,
        points: Vec::new(),
        failures: Vec::new(),
        overflows: Vec::new(),
        diagnostics: SampleDiagnostics::default(),
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
                    let _accepted = AcceptedCollectGuard::new(Arc::clone(&worker_accepted));
                    if let Some((started, release, released)) = first.take() {
                        let _ = started.send(());
                        let _ = release.blocking_recv();
                        let _ = response.send(Ok(empty_sample()));
                        let _ = released.send(());
                    } else {
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

/// Scenario: A persistent worker collects two supported counters repeatedly and shuts down.
/// Guarantees: One query serves every scrape and normal shutdown closes it exactly once.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_persistent_worker_reuses_and_closes_query() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let before = query_counts();
    let counters = vec![
        counter(r"\Memory\Available Bytes", "windows.memory.available"),
        counter(r"\Memory\Committed Bytes", "windows.memory.committed"),
    ];
    let mut worker = Worker::start(
        counters,
        Duration::from_secs(30),
        256,
        4_096,
        "test".to_owned(),
    )
    .await
    .unwrap();
    for _ in 0..3 {
        let sample = worker.collect().await.unwrap();
        assert_eq!(sample.points.len(), 2);
        assert!(sample.points.iter().all(
            |point| matches!(point.value, SampleValue::Value(Number::Integer(value)) if value >= 0)
        ));
        assert_eq!(sample.points[0].counter_index, 0);
        assert_eq!(sample.points[0].path, r"\Memory\Available Bytes");
        assert_eq!(sample.points[1].counter_index, 1);
        assert_eq!(sample.points[1].path, r"\Memory\Committed Bytes");
    }
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    let after = query_counts();
    assert_eq!(after.0 - before.0, 1);
    assert_eq!(after.1 - before.1, 1);
}

/// Scenario: Two independently configured receiver workers collect in one process.
/// Guarantees: Each receiver owns a distinct query and both can collect and shut down cleanly.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_independent_workers_can_collect_concurrently() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let before = query_counts();
    let counters = vec![counter(
        r"\Memory\Available Bytes",
        "windows.memory.available",
    )];
    let mut first = Worker::start(
        counters.clone(),
        Duration::from_secs(30),
        256,
        4_096,
        "first".to_owned(),
    )
    .await
    .unwrap();
    let mut second = Worker::start(
        counters,
        Duration::from_secs(30),
        256,
        4_096,
        "second".to_owned(),
    )
    .await
    .unwrap();
    assert!(!first.collect().await.unwrap().points.is_empty());
    assert!(!second.collect().await.unwrap().points.is_empty());
    assert!(
        first
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    assert!(
        second
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    let after = query_counts();
    assert_eq!(after.0 - before.0, 2);
    assert_eq!(after.1 - before.1, 2);
}

/// Scenario: The first post-prime scrape has direct, raw-fraction, and two-sample values.
/// Guarantees: One-sample values emit immediately while only the two-sample timer is omitted.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_first_scrape_emits_one_sample_values_and_warms_two_sample() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let counters = vec![
        counter(r"\Memory\Available Bytes", "windows.memory.available"),
        counter(
            r"\Memory\% Committed Bytes In Use",
            "windows.memory.committed_percent",
        ),
        counter(
            r"\Processor(_Total)\% Processor Time",
            "windows.processor.time",
        ),
    ];
    let mut worker = Worker::start(
        counters,
        Duration::from_secs(30),
        256,
        4_096,
        "test".to_owned(),
    )
    .await
    .unwrap();
    let sample = worker.collect().await.unwrap();
    assert!(matches!(
        sample.points[0].value,
        SampleValue::Value(Number::Integer(_))
    ));
    assert!(
        matches!(sample.points[1].value, SampleValue::Value(Number::Double(value)) if value.is_finite())
    );
    assert_eq!(sample.points[2].value, SampleValue::Warming);
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
}

/// Scenario: A Windows direct count is mapped to a cumulative monotonic Sum.
/// Guarantees: The first scrape emits the formatted value with start time equal to point time.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_collects_first_monotonic_direct_value() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let mut worker = Worker::start(
        vec![monotonic_sum(
            r"\TCPv4\Connection Failures",
            "windows.tcp.connection_failures",
            "{failure}",
        )],
        Duration::from_secs(30),
        256,
        4_096,
        "test".to_owned(),
    )
    .await
    .unwrap();
    let sample = worker.collect().await.unwrap();
    assert_eq!(sample.points.len(), 1);
    assert!(matches!(
        sample.points[0].value,
        SampleValue::Value(Number::Integer(value)) if value >= 0
    ));
    assert_eq!(
        sample.points[0].start_time_unix_nano,
        Some(sample.timestamp_unix_nano)
    );
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
}

/// Scenario: Startup includes a healthy Gauge and an ineligible monotonic timer mapping.
/// Guarantees: The worker starts, emits the healthy point, and reports the mapping diagnostic.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_isolates_ineligible_monotonic_mapping_at_startup() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let mut worker = Worker::start(
        vec![
            counter(r"\Memory\Available Bytes", "windows.memory.available"),
            monotonic_sum(
                r"\Processor(_Total)\% Processor Time",
                "windows.processor.time",
                "%",
            ),
        ],
        Duration::from_secs(30),
        256,
        4_096,
        "test".to_owned(),
    )
    .await
    .unwrap();
    let sample = worker.collect().await.unwrap();
    assert_eq!(sample.points.len(), 1);
    assert_eq!(sample.points[0].counter_index, 0);
    assert!(sample.failures.iter().any(|failure| {
        failure.counter_index == 1 && failure.reason == "invalid_metric_mapping"
    }));
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
}

/// Scenario: The local host exposes one exact gauge and dynamically enumerated process gauges.
/// Guarantees: Native wildcard expansion returns concrete identity-keyed points without changing the exact point.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_collects_exact_and_wildcard_counters() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let counters = vec![
        counter(r"\Memory\Available Bytes", "windows.memory.available"),
        counter(r"\Process(*)\Private Bytes", "windows.process.private"),
    ];
    let mut worker = Worker::start(
        counters,
        Duration::from_secs(1),
        256,
        4_096,
        "test".to_owned(),
    )
    .await
    .unwrap();
    let sample = worker.collect().await.unwrap();
    let exact = sample
        .points
        .iter()
        .find(|point| point.counter_index == 0)
        .expect("exact Memory point");
    assert_eq!(exact.path, r"\Memory\Available Bytes");
    assert!(exact.instance.is_none());
    assert!(matches!(
        exact.value,
        SampleValue::Value(Number::Integer(value)) if value >= 0
    ));

    let wildcard = sample
        .points
        .iter()
        .filter(|point| point.counter_index == 1)
        .collect::<Vec<_>>();
    assert!(!wildcard.is_empty());
    let keys = wildcard
        .iter()
        .map(|point| CounterKey::new(point.counter_index, &point.path))
        .collect::<BTreeSet<_>>();
    assert_eq!(keys.len(), wildcard.len());
    assert!(wildcard.iter().all(|point| {
        point.instance.is_some()
            && point.path != r"\Process(*)\Private Bytes"
            && matches!(
                point.value,
                SampleValue::Value(Number::Integer(value)) if value >= 0
            )
    }));
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
}

/// Scenario: Live Windows PDH formats common disk queue-length and precision-timer counters.
/// Guarantees: Both additional two-sample families produce finite values from one query.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_collects_common_calculated_disk_counters() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let counters = vec![
        counter(
            r"\PhysicalDisk(_Total)\Avg. Disk Queue Length",
            "windows.disk.queue_length",
        ),
        counter(r"\PhysicalDisk(_Total)\% Disk Time", "windows.disk.time"),
    ];
    let mut worker = Worker::start(
        counters,
        Duration::from_secs(30),
        256,
        4_096,
        "test".to_owned(),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let warming = worker.collect().await.unwrap();
    assert!(warming.failures.is_empty(), "{:?}", warming.failures);
    assert_eq!(warming.points.len(), 2);
    assert!(
        warming
            .points
            .iter()
            .all(|point| point.value == SampleValue::Warming),
        "{:?}",
        warming.points
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    let sample = worker.collect().await.unwrap();
    assert!(sample.failures.is_empty(), "{:?}", sample.failures);
    assert_eq!(sample.points.len(), 2);
    assert!(
        sample.points.iter().all(|point| {
            matches!(
                point.value,
                SampleValue::Value(Number::Double(value)) if value.is_finite()
            )
        }),
        "{:?}",
        sample.points
    );
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
}

/// Scenario: Startup adds a healthy exact counter while another configured counter is absent.
/// Guarantees: The worker starts, emits the healthy point, and retains a visible retry failure.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_unavailable_exact_counter_retries_after_startup() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let before = query_counts();
    let counters = vec![
        counter(r"\Memory\Available Bytes", "windows.memory.available"),
        counter(
            r"\Missing Object\Missing Counter",
            "windows.missing.counter",
        ),
    ];
    let mut worker = Worker::start(
        counters,
        Duration::from_secs(30),
        256,
        4_096,
        "test".to_owned(),
    )
    .await
    .unwrap();
    let sample = worker.collect().await.unwrap();
    assert!(
        sample
            .points
            .iter()
            .any(|point| point.path == r"\Memory\Available Bytes")
    );
    assert!(sample.failures.iter().any(|failure| {
        failure.counter_index == 1 && failure.reason == "PdhAddEnglishCounterW"
    }));
    assert_eq!(sample.diagnostics.counter_add_failures, 1);
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    let after = query_counts();
    assert_eq!(after.0 - before.0, 1);
    assert_eq!(after.1 - before.1, 1);
}

/// Scenario: Every configured exact counter is unavailable when the worker starts.
/// Guarantees: Startup succeeds with an empty active set and reports retryable failures without points.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_entirely_unavailable_counter_set_starts_empty() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let mut worker = Worker::start(
        vec![counter(
            r"\Missing Object\Missing Counter",
            "windows.missing.counter",
        )],
        Duration::from_secs(30),
        256,
        4_096,
        "test".to_owned(),
    )
    .await
    .unwrap();
    let sample = worker.collect().await.unwrap();
    assert!(sample.points.is_empty());
    assert_eq!(sample.failures.len(), 1);
    assert_eq!(sample.diagnostics.counter_add_failures, 1);
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
}

fn open_live_query(configs: Vec<CounterConfig>, node: &str) -> Query {
    Query::open(
        configs,
        Duration::from_secs(30),
        256,
        4_096,
        node.to_owned(),
    )
    .unwrap()
}

/// Scenario: A removed counter token is reused after another counter is added to the same adapter.
/// Guarantees: Removed slots cannot alias new counters and every operation rejects the stale token.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_adapter_rejects_removed_tokens() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let path = r"\Memory\Available Bytes";
    let mut api = NativePdh::open("removed".to_owned()).unwrap();
    let removed = api.add(path, false).unwrap();
    api.remove(&removed, path).unwrap();
    let live = api.add(path, false).unwrap();
    for result in [
        api.remove(&removed, path),
        api.counter_type(&removed, path).map(|_| ()),
        api.raw(&removed, path).map(|_| ()),
        api.formatted(&removed, path, ValueFormat::Integer)
            .map(|_| ()),
    ] {
        let error = result.unwrap_err();
        assert!(matches!(&error, Error::InvalidCounterToken { path: failed } if failed == path));
    }
    assert!(api.collect().unwrap());
    assert!(api.counter_type(&live, path).is_ok());
}

struct CounterProcess(Child);

impl Drop for CounterProcess {
    fn drop(&mut self) {
        drop(self.0.stdin.take());
        let _ = self.0.wait();
    }
}

fn spawn_counter_process(executable: &Path) -> CounterProcess {
    let mut child = CounterProcess(
        ProcessCommand::new(executable)
            .args(["/d", "/q", "/c", "echo ready & set /p wait="])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut ready = String::new();
    let _ = BufReader::new(child.0.stdout.take().unwrap())
        .read_line(&mut ready)
        .unwrap();
    assert_eq!(ready.trim(), "ready");
    child
}

/// Scenario: Parsed configuration names duplicate live process instances using #0 and #1.
/// Guarantees: Normalized paths collect exactly the fixture process IDs without unrelated instances.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_collects_parsed_instance_paths() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("otel-arrow-pdh-{}-{nonce}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let instance = format!("otpd{:08x}", std::process::id());
    let executable = directory.join(format!("{instance}.exe"));
    let source = std::env::var_os("COMSPEC").expect("COMSPEC must identify cmd.exe");
    let _ = std::fs::copy(source, &executable).unwrap();
    let processes = [
        spawn_counter_process(&executable),
        spawn_counter_process(&executable),
    ];
    let config = Config::from_json(&serde_json::json!({
        "metrics": {
            "process.id": {
                "description": "Process identifier.",
                "unit": "1",
                "gauge": {}
            }
        },
        "perfcounters": [{
            "object": "Process",
            "instances": [format!("{instance}#0"), format!("{instance}#1")],
            "counters": [{"name": "ID Process", "metric": "process.id"}]
        }]
    }))
    .unwrap();
    assert_eq!(
        config.counters[0].path,
        format!(r"\Process({instance})\ID Process")
    );
    assert_eq!(
        config.counters[1].path,
        format!(r"\Process({instance}#1)\ID Process")
    );
    let counters = config.counters.clone();
    let mut query = open_live_query(counters.clone(), "parsed");
    let sample = query.collect().unwrap();
    assert!(sample.failures.is_empty(), "{:?}", sample.failures);
    assert_eq!(sample.points.len(), 2);
    let ids = sample
        .points
        .iter()
        .map(|point| match point.value {
            SampleValue::Value(Number::Integer(id)) if id > 0 => id,
            value => panic!("expected a positive process ID, got {value:?}"),
        })
        .collect::<BTreeSet<_>>();
    let expected = processes
        .iter()
        .map(|process| i64::from(process.0.id()))
        .collect::<BTreeSet<_>>();
    assert_eq!(ids, expected);
    assert!(into_otap(&counters, sample).unwrap().is_some());
    drop(query);
    drop(processes);
    std::fs::remove_file(executable).unwrap();
    std::fs::remove_dir(directory).unwrap();
}

/// Scenario: Two metric mappings open the same exact native counter in one query.
/// Guarantees: PDH accepts both handles and collection routes a ready point to each mapping.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_collects_one_path_for_multiple_mappings() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let path = r"\Memory\Available Bytes";
    let configs = vec![
        counter(path, "windows.memory.available"),
        counter(path, "windows.memory.available_copy"),
    ];
    let mut query = open_live_query(configs, "same-path-mappings");

    let sample = query.collect().unwrap();
    assert!(sample.failures.is_empty(), "{:?}", sample.failures);
    assert_eq!(
        sample
            .points
            .iter()
            .map(|point| point.counter_index)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert!(sample.points.iter().all(
        |point| matches!(point.value, SampleValue::Value(Number::Integer(value)) if value >= 0)
    ));
}

/// Scenario: Two native queries collect exact direct counters repeatedly and are dropped independently.
/// Guarantees: Integer points and cumulative start times remain valid across query lifetimes.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_direct_queries() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let configs = vec![counter(
        r"\Memory\Available Bytes",
        "windows.memory.available",
    )];
    let mut first = open_live_query(configs.clone(), "first");
    let mut second = open_live_query(configs.clone(), "second");
    let start = first.start_time_unix_nano;
    for _ in 0..3 {
        for query in [&mut first, &mut second] {
            let sample = query.collect().unwrap();
            assert!(sample.failures.is_empty(), "{:?}", sample.failures);
            assert!(
                matches!(sample.points[0].value, SampleValue::Value(Number::Integer(value)) if value >= 0)
            );
            assert!(into_otap(&configs, sample).unwrap().is_some());
        }
        assert_eq!(first.start_time_unix_nano, start);
    }
    drop(first);
    assert!(second.collect().unwrap().failures.is_empty());
}

/// Scenario: Live PDH reads exact rates, timers, queues, fractions, and averages.
/// Guarantees: Calculated types warm independently and later emit finite values or valid omissions.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_calculated_counters() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let configs = vec![
        counter(
            r"\System\Context Switches/sec",
            "windows.system.context_switches",
        ),
        counter(
            r"\Processor(_Total)\% Processor Time",
            "windows.processor.utilization",
        ),
        counter(
            r"\PhysicalDisk(_Total)\Avg. Disk Queue Length",
            "windows.disk.queue_length",
        ),
        counter(
            r"\Memory\% Committed Bytes In Use",
            "windows.memory.committed",
        ),
        counter(
            r"\PhysicalDisk(_Total)\Avg. Disk sec/Read",
            "windows.disk.read_latency",
        ),
    ];
    let mut query = open_live_query(configs.clone(), "calculated");
    let warming = query.collect().unwrap();
    assert!(warming.failures.is_empty(), "{:?}", warming.failures);
    for index in [0, 1, 2, 4] {
        assert_eq!(warming.points[index].value, SampleValue::Warming);
    }
    std::thread::sleep(Duration::from_millis(250));
    let sample = query.collect().unwrap();
    assert!(sample.failures.is_empty(), "{:?}", sample.failures);
    assert_eq!(sample.points.len(), configs.len());
    for point in &sample.points[..4] {
        assert!(matches!(
            point.value,
            SampleValue::Value(Number::Double(value)) if value.is_finite() && value >= 0.0
        ));
    }
    assert!(into_otap(&configs, sample).unwrap().is_some());
}

/// Scenario: An invalid exact path is configured beside a valid counter and then by itself.
/// Guarantees: Healthy collection continues, while an all-invalid set starts empty with diagnostics.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_invalid_counters_start_empty() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
    let configs = vec![
        counter(r"\Memory\Available Bytes", "windows.memory.available"),
        counter(
            r"\Memory\otel-arrow-nonexistent-counter",
            "windows.memory.invalid",
        ),
    ];
    let mut query = open_live_query(configs.clone(), "invalid");
    let sample = query.collect().unwrap();
    assert_eq!(sample.failures.len(), 1);
    assert_eq!(sample.failures[0].counter_index, 1);
    assert_eq!(sample.points.len(), 1);
    assert_eq!(sample.points[0].counter_index, 0);
    assert!(
        matches!(sample.points[0].value, SampleValue::Value(Number::Integer(value)) if value >= 0)
    );
    assert!(into_otap(&configs, sample).unwrap().is_some());

    let mut invalid = open_live_query(configs[1..].to_vec(), "all-invalid");
    let sample = invalid.collect().unwrap();
    assert!(sample.points.is_empty());
    assert_eq!(sample.failures.len(), 1);
    assert_eq!(sample.failures[0].counter_index, 0);
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

/// Scenario: The wall clock moves backward while a cumulative metric query remains open.
/// Guarantees: The next point begins a new cumulative sequence that later scrapes retain.
#[test]
fn cumulative_start_time_resets_after_clock_rollback() {
    let mut start = 10;
    let mut previous = 10;
    assert_eq!(cumulative_start_time(&mut start, &mut previous, 20), 10);
    assert_eq!(cumulative_start_time(&mut start, &mut previous, 15), 15);
    assert_eq!(cumulative_start_time(&mut start, &mut previous, 16), 15);
}

/// Scenario: A monotonic raw count advances, resets, and then advances from its new value.
/// Guarantees: Only a decrease or clock rollback starts a new cumulative sequence.
#[test]
fn monotonic_start_time_tracks_sum_resets() {
    let mut sequence = MonotonicSequence::default();
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, 10, 100).unwrap(),
        100
    );
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, 10, 110).unwrap(),
        100
    );
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, 15, 110).unwrap(),
        100
    );
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, 20, 120).unwrap(),
        100
    );
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, 30, 115).unwrap(),
        115
    );
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, 2, 130).unwrap(),
        130
    );
    assert!(monotonic_start_time("counter", &mut sequence, -1, 140).is_err());
    assert_eq!(sequence.start_time_unix_nano, Some(130));
    assert_eq!(sequence.previous_value, Some(2));
    assert_eq!(sequence.previous_timestamp_unix_nano, Some(130));
}

/// Scenario: Query rollback occurs while a monotonic Sum series is omitted.
/// Guarantees: Cleared sequence state remains empty until the next valid observation starts fresh.
#[test]
fn rollback_during_omission_starts_next_observation_fresh() {
    let mut sequence = MonotonicSequence::default();
    let mut query_start = 100;
    let mut previous_query_timestamp = 100;
    assert_eq!(
        advance_collection_time(
            &mut query_start,
            &mut previous_query_timestamp,
            120,
            std::iter::once(&mut sequence)
        ),
        100
    );
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, 20, 120).unwrap(),
        120
    );
    assert_eq!(
        advance_collection_time(
            &mut query_start,
            &mut previous_query_timestamp,
            115,
            std::iter::once(&mut sequence)
        ),
        115
    );
    assert_eq!(sequence.start_time_unix_nano, None);
    assert_eq!(sequence.previous_value, None);
    assert_eq!(sequence.previous_timestamp_unix_nano, None);
    assert_eq!(
        advance_collection_time(
            &mut query_start,
            &mut previous_query_timestamp,
            125,
            std::iter::once(&mut sequence)
        ),
        115
    );
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, 30, 125).unwrap(),
        125
    );
}

/// Scenario: A monotonic Sum succeeds, is omitted during query clock rollback, then succeeds later.
/// Guarantees: The full collection path retains the rollback reset until the next valid point.
#[test]
fn query_rollback_during_omission_resets_monotonic_sequence() {
    let path = r"\TCPv4\Connection Failures";
    let (state, mut query) = fake_query(
        vec![monotonic_sum(path, "windows.tcp.connection_failures", "1")],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0,
            raw: None,
            formatted: Some(Number::Integer(20)),
            raw_error: false,
            formatted_error: false,
        }],
    );

    state.borrow_mut().timestamp = 120;
    let sample = query.collect().unwrap();
    assert_eq!(sample.points[0].start_time_unix_nano, Some(120));

    {
        let mut state = state.borrow_mut();
        state.timestamp = 115;
        state.counters[0].formatted = None;
    }
    let sample = query.collect().unwrap();
    assert_eq!(sample.start_time_unix_nano, 115);
    assert_eq!(sample.points[0].value, SampleValue::NoObservation);

    {
        let mut state = state.borrow_mut();
        state.timestamp = 125;
        state.counters[0].formatted = Some(Number::Integer(30));
    }
    let sample = query.collect().unwrap();
    assert_eq!(sample.points[0].start_time_unix_nano, Some(125));
}

/// Scenario: A formatted transient interrupts a monotonic direct count before it resumes higher.
/// Guarantees: PDH readiness restarts without resetting the monotonic Sum sequence.
#[test]
fn formatted_transient_preserves_monotonic_sequence() {
    let path = r"\TCPv4\Connection Failures";
    let (state, mut query) = fake_query(
        vec![monotonic_sum(path, "windows.tcp.connection_failures", "1")],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0,
            raw: None,
            formatted: Some(Number::Integer(20)),
            raw_error: false,
            formatted_error: false,
        }],
    );

    state.borrow_mut().timestamp = 120;
    assert_eq!(
        query.collect().unwrap().points[0].start_time_unix_nano,
        Some(120)
    );
    {
        let mut state = state.borrow_mut();
        state.timestamp = 125;
        state.counters[0].formatted_error = true;
    }
    let sample = query.collect().unwrap();
    assert!(sample.points.is_empty());
    assert_eq!(sample.failures.len(), 1);

    {
        let mut state = state.borrow_mut();
        state.timestamp = 130;
        state.counters[0].formatted_error = false;
        state.counters[0].formatted = Some(Number::Integer(30));
    }
    assert_eq!(
        query.collect().unwrap().points[0].start_time_unix_nano,
        Some(120)
    );
}

/// Scenario: A calculated counter sees a formatted omission, a hard collect error, and a timestamp error.
/// Guarantees: Every failed sample clears the saved base so the next successful scrape re-warms.
#[test]
fn collection_failures_restart_calculated_readiness() {
    let path = r"\PhysicalDisk(_Total)\Avg. Disk sec/Transfer";
    let (state, mut query) = fake_query(
        vec![counter(path, "windows.disk.average_latency")],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x4002_0500,
            raw: Some(10),
            formatted: Some(Number::Double(4.0)),
            raw_error: false,
            formatted_error: false,
        }],
    );

    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::Warming
    );
    state.borrow_mut().counters[0].raw = Some(12);
    assert!(matches!(
        query.collect().unwrap().points[0].value,
        SampleValue::Value(Number::Double(4.0))
    ));

    {
        let mut state = state.borrow_mut();
        state.counters[0].raw = Some(14);
        state.counters[0].formatted = None;
    }
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::NoObservation
    );
    state.borrow_mut().counters[0].raw = Some(16);
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::Warming
    );

    {
        let mut state = state.borrow_mut();
        state.counters[0].raw = Some(18);
        state.collect_error = true;
    }
    assert!(query.collect().is_err());
    {
        let mut state = state.borrow_mut();
        state.collect_error = false;
        state.counters[0].raw = Some(20);
    }
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::Warming
    );

    {
        let mut state = state.borrow_mut();
        state.counters[0].raw = Some(22);
        state.timestamp_error = true;
    }
    assert!(query.collect().is_err());
    {
        let mut state = state.borrow_mut();
        state.timestamp_error = false;
        state.counters[0].raw = Some(24);
    }
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::Warming
    );
}

/// Scenario: A monotonic value decreases from a large value to a small value.
/// Guarantees: The decrease begins a new sequence without wrap compensation.
#[test]
fn monotonic_start_time_restarts_after_32_bit_wrap() {
    let mut sequence = MonotonicSequence::default();
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, i64::from(u32::MAX), 100).unwrap(),
        100
    );
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, 1, 110).unwrap(),
        110
    );
}

/// Scenario: Metric kinds are paired with direct and calculated native counter types.
/// Guarantees: Only monotonic Sums reject non-direct native types.
#[test]
fn validates_monotonic_sum_native_type() {
    assert!(
        validate_metric_mapping(
            "counter",
            MetricKind::Sum { monotonic: true },
            CounterKind::Direct
        )
        .is_ok()
    );
    assert!(
        validate_metric_mapping(
            "counter",
            MetricKind::Sum { monotonic: true },
            CounterKind::CalculatedTwoSample
        )
        .is_err()
    );
    assert!(
        validate_metric_mapping("gauge", MetricKind::Gauge, CounterKind::CalculatedTwoSample)
            .is_ok()
    );
    assert!(
        validate_metric_mapping(
            "sum",
            MetricKind::Sum { monotonic: false },
            CounterKind::CalculatedTwoSample
        )
        .is_ok()
    );
}

/// Scenario: An ineligible monotonic mapping is added beside a healthy counter during startup.
/// Guarantees: The mapping is permanent and diagnostic but does not require retry.
#[test]
fn isolates_invalid_monotonic_sum_mapping() {
    let error = validate_metric_mapping(
        "counter",
        MetricKind::Sum { monotonic: true },
        CounterKind::CalculatedTwoSample,
    )
    .unwrap_err();
    assert_eq!(
        error.bounded_detail(),
        "monotonic Sum requires a direct raw-count counter type \
             (PERF_COUNTER_RAWCOUNT / LARGE_RAWCOUNT)"
    );
    assert_eq!(
        exact_add_failure_disposition(&error),
        ExactAddFailureDisposition::Diagnostic
    );
    let retryable = Error::Pdh {
        operation: "PdhAddEnglishCounterW",
        path: "missing".to_owned(),
        status: 1,
    };
    assert_eq!(
        exact_add_failure_disposition(&retryable),
        ExactAddFailureDisposition::Retry
    );
    let unsupported = Error::UnsupportedType(UnsupportedNativeType {
        path: "unsupported".to_owned(),
        native_type: u32::MAX,
    });
    assert_eq!(
        exact_add_failure_disposition(&unsupported),
        ExactAddFailureDisposition::Diagnostic
    );
}

/// Scenario: Decimal scaling rounds two adjacent large integers to the same double.
/// Guarantees: A raw formatted-value decrease still resets the sequence before scaling.
#[test]
fn detects_value_drop_before_scaling() {
    let previous_value = i64::MAX;
    let current_value = i64::MAX - 1;
    assert_eq!(
        scale_integer(previous_value, -18).unwrap(),
        scale_integer(current_value, -18).unwrap()
    );
    let mut sequence = MonotonicSequence {
        start_time_unix_nano: Some(100),
        previous_value: Some(previous_value),
        previous_timestamp_unix_nano: Some(100),
    };
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, current_value, 110).unwrap(),
        110
    );
}

/// Scenario: Query metadata survives a failed collection and is merged with later recovery evidence.
/// Guarantees: Diagnostic counts, counter failures, and expansion overflows remain ordered and observable.
#[test]
fn sample_metadata_merge_preserves_pending_evidence() {
    let mut diagnostics = SampleDiagnostics {
        counter_add_failures: 1,
        ..Default::default()
    };
    let mut failures = vec![SampleFailure {
        counter_index: 0,
        reason: "initial",
        error: "initial failure".to_owned(),
    }];
    let mut overflows = vec![ExpansionOverflow {
        counter_index: 0,
        reason: "per_wildcard",
        discovered: 3,
        retained: 2,
        omitted: 1,
    }];

    merge_sample_metadata(
        &mut diagnostics,
        &mut failures,
        &mut overflows,
        SampleDiagnostics {
            retry_recoveries: 1,
            ..Default::default()
        },
        vec![SampleFailure {
            counter_index: 1,
            reason: "recovery",
            error: "recovery failure".to_owned(),
        }],
        vec![ExpansionOverflow {
            counter_index: 1,
            reason: "receiver_total",
            discovered: 4,
            retained: 3,
            omitted: 1,
        }],
    );

    assert_eq!(diagnostics.counter_add_failures, 1);
    assert_eq!(diagnostics.retry_recoveries, 1);
    assert_eq!(failures.len(), 2);
    assert_eq!(failures[0].reason, "initial");
    assert_eq!(failures[1].reason, "recovery");
    assert_eq!(overflows.len(), 2);
    assert_eq!(overflows[0].reason, "per_wildcard");
    assert_eq!(overflows[1].reason, "receiver_total");

    merge_sample_metadata(
        &mut diagnostics,
        &mut failures,
        &mut overflows,
        SampleDiagnostics::default(),
        vec![SampleFailure {
            counter_index: 0,
            reason: "initial",
            error: "latest failure".to_owned(),
        }],
        vec![ExpansionOverflow {
            counter_index: 0,
            reason: "per_wildcard",
            discovered: 5,
            retained: 2,
            omitted: 3,
        }],
    );

    assert_eq!(failures.len(), 2);
    assert_eq!(failures[0].error, "latest failure");
    assert_eq!(overflows.len(), 2);
    assert_eq!(overflows[0].discovered, 5);
}

/// Scenario: Worker recovery metadata precedes metadata from the successful scrape that releases it.
/// Guarantees: Bundling pending state preserves failure and overflow order while adding diagnostics.
#[test]
fn pending_metadata_bundle_preserves_worker_merge_order() {
    let mut pending = PendingMetadata {
        diagnostics: SampleDiagnostics {
            retry_attempts: 1,
            ..Default::default()
        },
        failures: vec![SampleFailure {
            counter_index: 0,
            reason: "pending",
            error: "pending failure".to_owned(),
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
        start_time_unix_nano: 1,
        timestamp_unix_nano: 2,
        points: Vec::new(),
        failures: vec![SampleFailure {
            counter_index: 1,
            reason: "current",
            error: "current failure".to_owned(),
        }],
        overflows: vec![ExpansionOverflow {
            counter_index: 1,
            reason: "receiver_total",
            discovered: 4,
            retained: 3,
            omitted: 1,
        }],
        diagnostics: SampleDiagnostics {
            retry_recoveries: 1,
            ..Default::default()
        },
    };

    pending.merge_into_sample(&mut sample);

    assert_eq!(sample.diagnostics.retry_attempts, 1);
    assert_eq!(sample.diagnostics.retry_recoveries, 1);
    assert_eq!(sample.failures[0].reason, "pending");
    assert_eq!(sample.failures[1].reason, "current");
    assert_eq!(sample.overflows[0].reason, "per_wildcard");
    assert_eq!(sample.overflows[1].reason, "receiver_total");
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

/// Scenario: The same failed counter is scheduled repeatedly before it recovers.
/// Guarantees: One stable identity owns one retry entry and repeated failures advance its bounded backoff.
#[test]
fn retry_state_is_unique_per_stable_identity() {
    let key = CounterKey::new(0, r"\Process(worker)\Private Bytes");
    let target = CounterTarget {
        config_index: 0,
        path: r"\Process(worker)\Private Bytes".to_owned(),
        localized: true,
        instance: Some(InstanceIdentity {
            name: "worker".to_owned(),
            parent: None,
            index: 0,
        }),
    };
    let now = Instant::now();
    let mut states = BTreeMap::new();

    schedule_retry_state(
        &mut states,
        key.clone(),
        target.clone(),
        now,
        Duration::from_secs(30),
    );
    schedule_retry_state(
        &mut states,
        key.clone(),
        target,
        now,
        Duration::from_secs(30),
    );

    assert_eq!(states.len(), 1);
    let state = states.get(&key).unwrap();
    assert_eq!(state.attempts, 2);
    assert_eq!(state.next_attempt, now + Duration::from_secs(2));
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

/// Scenario: A wildcard expansion contains aggregate and concrete provider instances.
/// Guarantees: Aggregate matching is case-insensitive and does not confuse concrete instances.
#[test]
fn identifies_provider_aggregation_instances() {
    assert!(is_aggregation_instance(
        r"\Process(_Total)\Private Bytes",
        "_Total"
    ));
    assert!(is_aggregation_instance(
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
    remove_aggregation_instances(&mut paths, Some("_Total"));
    let omitted = limit_expanded_paths(&mut paths, 1, 1);
    assert_eq!(omitted, (0, 0));
    assert_eq!(paths, [r"\Process(worker)\Private Bytes"]);
}

/// Scenario: A simulated PDH call remains blocked past the shutdown deadline.
/// Guarantees: Shutdown returns at the deadline, cleanup occurs later on the worker,
/// and worker-owned resources remain alive until that cleanup completes.
#[tokio::test(flavor = "current_thread")]
async fn blocked_worker_releases_resources_after_timeout() {
    let _serial = super::super::TEST_PDH_LOCK.lock().await;
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
