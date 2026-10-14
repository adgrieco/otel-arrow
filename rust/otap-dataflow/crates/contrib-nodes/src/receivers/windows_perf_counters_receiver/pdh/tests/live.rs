// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Scenario: A persistent worker collects two supported counters repeatedly and shuts down.
/// Guarantees: One query serves every scrape and normal shutdown closes it exactly once.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_persistent_worker_reuses_and_closes_query() {
    let _serial = TEST_PDH_LOCK.lock().await;
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

/// Scenario: One live calculated mapping opts into query recreation beside a persistent direct mapping.
/// Guarantees: Every scrape replaces and primes only the opted-in query, both mappings stay ready, and shutdown closes every opened query.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_recreates_and_primes_opted_in_query() {
    let _serial = TEST_PDH_LOCK.lock().await;
    let before = query_counts();
    let shared = counter(r"\Memory\Available Bytes", "windows.memory.available");
    let mut recreated = counter(
        r"\Processor(_Total)\% Processor Time",
        "windows.processor.time",
    );
    recreated.recreate_query = true;
    recreated.source_mapping_index = 1;
    let mut worker = Worker::start(
        vec![shared, recreated],
        Duration::from_secs(30),
        256,
        4_096,
        "test".to_owned(),
    )
    .await
    .unwrap();

    for _ in 0..2 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let sample = worker.collect().await.unwrap();
        assert_eq!(sample.points.len(), 2);
        assert!(matches!(
            sample.points[0].value,
            SampleValue::Value(Number::Integer(value)) if value >= 0
        ));
        assert!(
            matches!(sample.points[1].value, SampleValue::Value(Number::Double(value)) if value.is_finite())
        );
        assert_eq!(sample.diagnostics.query_recreation_attempts, 1);
        assert_eq!(sample.diagnostics.query_recreation_failures, 0);
    }
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
    let after = query_counts();
    assert_eq!(after.0 - before.0, 4);
    assert_eq!(after.1 - before.1, 4);
}

/// Scenario: Two independently configured receiver workers collect in one process.
/// Guarantees: Each receiver owns a distinct query and both can collect and shut down cleanly.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_independent_workers_can_collect_concurrently() {
    let _serial = TEST_PDH_LOCK.lock().await;
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

/// Scenario: Worker startup primes direct, raw-fraction, and two-sample counters before the first request.
/// Guarantees: The first requested scrape emits every valid family after sufficient elapsed time.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_startup_prime_enables_first_calculated_scrape() {
    let _serial = TEST_PDH_LOCK.lock().await;
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
    tokio::time::sleep(Duration::from_millis(250)).await;
    let sample = worker.collect().await.unwrap();
    assert!(matches!(
        sample.points[0].value,
        SampleValue::Value(Number::Integer(_))
    ));
    assert!(
        matches!(sample.points[1].value, SampleValue::Value(Number::Double(value)) if value.is_finite())
    );
    assert!(
        matches!(sample.points[2].value, SampleValue::Value(Number::Double(value)) if value.is_finite())
    );
    assert!(
        worker
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
    );
}

/// Scenario: Worker startup primes a Windows direct count mapped to a cumulative monotonic Sum.
/// Guarantees: The first scrape emits with a sequence start retained from the earlier prime.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_collects_first_monotonic_direct_value() {
    let _serial = TEST_PDH_LOCK.lock().await;
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
    tokio::time::sleep(Duration::from_millis(10)).await;
    let sample = worker.collect().await.unwrap();
    assert_eq!(sample.points.len(), 1);
    assert!(matches!(
        sample.points[0].value,
        SampleValue::Value(Number::Integer(value)) if value >= 0
    ));
    let start_time = sample.points[0].start_time_unix_nano.unwrap();
    assert!(start_time > 0);
    assert!(start_time < sample.timestamp_unix_nano);
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
    let _serial = TEST_PDH_LOCK.lock().await;
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
    let _serial = TEST_PDH_LOCK.lock().await;
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

/// Scenario: Worker startup primes common disk queue-length and precision-timer counters.
/// Guarantees: Both two-sample families produce finite values on the first requested scrape.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_collects_common_calculated_disk_counters() {
    let _serial = TEST_PDH_LOCK.lock().await;
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
    let sample = worker.collect().await.unwrap();
    assert!(sample.failures.is_empty(), "{:?}", sample.failures);
    assert_eq!(sample.points.len(), 2);
    assert!(
        sample.points.iter().all(|point| matches!(
            point.value,
            SampleValue::Value(Number::Double(value)) if value.is_finite()
        )),
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
    let _serial = TEST_PDH_LOCK.lock().await;
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
    let _serial = TEST_PDH_LOCK.lock().await;
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
    let _serial = TEST_PDH_LOCK.lock().await;
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
    let _serial = TEST_PDH_LOCK.lock().await;
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
    let _serial = TEST_PDH_LOCK.lock().await;
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
    let _serial = TEST_PDH_LOCK.lock().await;
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
    let _serial = TEST_PDH_LOCK.lock().await;
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

/// Scenario: Live Windows PDH reads system elapsed time and a processor multi-timer.
/// Guarantees: Elapsed time emits on the first collection while the multi-timer warms and later emits a finite value.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_elapsed_and_multi_timer_counters() {
    let _serial = TEST_PDH_LOCK.lock().await;
    let configs = vec![
        counter(r"\System\System Up Time", "system.uptime"),
        counter(
            r"\Processor Information(_Total)\% Processor Utility",
            "system.cpu.utility",
        ),
    ];
    let mut query = open_live_query(configs.clone(), "elapsed-multi");
    let first = query.collect().unwrap();
    assert!(first.failures.is_empty(), "{:?}", first.failures);
    assert!(matches!(
        first.points[0].value,
        SampleValue::Value(Number::Double(value)) if value.is_finite() && value >= 0.0
    ));
    assert_eq!(first.points[1].value, SampleValue::Warming);

    std::thread::sleep(Duration::from_millis(250));
    let second = query.collect().unwrap();
    assert!(second.failures.is_empty(), "{:?}", second.failures);
    assert!(second.points.iter().all(|point| {
        matches!(
            point.value,
            SampleValue::Value(Number::Double(value)) if value.is_finite() && value >= 0.0
        )
    }));
    assert!(into_otap(&configs, second).unwrap().is_some());
}

/// Scenario: An invalid exact path is configured beside a valid counter and then by itself.
/// Guarantees: Healthy collection continues, while an all-invalid set starts empty with diagnostics.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
async fn live_pdh_invalid_counters_start_empty() {
    let _serial = TEST_PDH_LOCK.lock().await;
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
