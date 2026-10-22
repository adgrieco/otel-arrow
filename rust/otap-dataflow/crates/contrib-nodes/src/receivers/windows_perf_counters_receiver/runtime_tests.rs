// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::receivers::windows_perf_counters_receiver::config::{
    CounterConfig, InstanceConfig, MetricKind,
};
use crate::receivers::windows_perf_counters_receiver::model::{
    Number, PendingMetadata, SampleDiagnostics, SamplePoint, SampleValue,
};
use arrow::array::TimestampNanosecondArray;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::capability::registry::Capabilities;
use otel_arrow_dfe_engine::context::ControllerContext;
use otel_arrow_dfe_engine::testing::receiver::TestRuntime;
use otel_arrow_dfe_engine::testing::{test_node, test_pipeline_ctx};
use otel_arrow_dfe_otap::OTAP_PIPELINE_FACTORY;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::any_value;
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::metric;
use otel_arrow_dfe_pdata::{OtlpProtoBytes, TryIntoWithOptions};
use otel_arrow_dfe_pdata_codec::OtapPayload;
use otel_arrow_dfe_pdata_codec::PayloadData;
use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

fn test_counter(attributes: BTreeMap<String, String>) -> CounterConfig {
    CounterConfig {
        path: r"\Memory\Available Bytes".to_owned(),
        name: Arc::from("windows.memory.available"),
        unit: Arc::from("By"),
        description: Arc::from("Available physical memory."),
        metric_kind: MetricKind::Gauge,
        attributes: Arc::new(attributes),
        instance_config: InstanceConfig::None,
        scale_power10: 0,
        source_mapping_index: 0,
    }
}

fn test_receiver(counters: Vec<CounterConfig>) -> WindowsPerfCountersReceiver {
    let (pipeline, _) = test_pipeline_ctx();
    WindowsPerfCountersReceiver {
        config: Config {
            counters,
            collection_interval: Duration::from_secs(1),
            initial_delay: Duration::ZERO,
            timeout: Some(Duration::from_secs(1)),
            wildcard_refresh_interval: None,
            max_instances_per_wildcard: 256,
            max_expanded_counters: 4_096,
            warnings: Vec::new(),
        },
        node: "windows_perf_counters".to_owned(),
        metrics: Rc::new(RefCell::new(WindowsPerfCountersReceiverMetrics::register(
            &pipeline,
        ))),
    }
}

fn sample_point(counter_index: usize, timestamp: i64, value: Number) -> SamplePoint {
    SamplePoint {
        counter_index,
        path: Arc::from(r"\Memory\Available Bytes"),
        instance: None,
        emit_instance_attribute: false,
        timestamp_unix_nano: timestamp,
        start_time_unix_nano: None,
        value: SampleValue::Value(value),
    }
}

fn collected_metadata() -> PendingMetadata {
    PendingMetadata {
        diagnostics: SampleDiagnostics {
            any_query_collected: true,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Scenario: A sample contains one value failure and one query-level log entry.
/// Guarantees: Only the actual counter-value omission contributes to failed-value metrics.
#[test]
fn diagnostic_only_failures_do_not_count_as_failed_values() {
    let failures = vec![
        SampleFailure {
            counter_index: 0,
            path: Arc::from(r"\Memory\Available Bytes"),
            reason: "counter CStatus",
            error: "PDH_INVALID_DATA".to_owned(),
            diagnostic_only: false,
        },
        SampleFailure {
            counter_index: 1,
            path: Arc::from(r"\Memory\Committed Bytes"),
            reason: "PdhCollectQueryData",
            error: "PDH status 0x00000001".to_owned(),
            diagnostic_only: true,
        },
    ];

    let mut failures = failures;
    let mut duplicate = failures[0].clone();
    duplicate.reason = "calculation";
    failures.push(duplicate);
    assert_eq!(failed_counter_value_count(&failures), 1);
}

/// Scenario: A sample contains a failed counter add beside a healthy value, then a recovery-only sample follows.
/// Guarantees: Healthy collection succeeds, recovery-only does not count as a failed native collection, and pending recovery is observable.
#[tokio::test]
async fn healthy_collection_and_recovery_only_have_distinct_accounting() {
    let mut unavailable = test_counter(BTreeMap::new());
    unavailable.path = r"\Unavailable\Value".to_owned();
    unavailable.name = Arc::from("windows.unavailable");
    let receiver = test_receiver(vec![test_counter(BTreeMap::new()), unavailable]);
    let mut failure_logs = FailureLogState::default();
    let sends = Rc::new(std::cell::Cell::new(0_usize));

    let failed_open = Sample {
        timestamp_unix_nano: 100,
        members: Vec::new(),
        points: vec![sample_point(0, 100, Number::Integer(42))],
        metadata: PendingMetadata {
            diagnostics: SampleDiagnostics {
                any_query_collected: true,
                counter_add_failures: 1,
                ..Default::default()
            },
            failures: vec![SampleFailure {
                counter_index: 1,
                path: Arc::from(r"\Unavailable\Value"),
                reason: "PdhAddEnglishCounterW",
                error: "PDH status 0x00000001".to_owned(),
                diagnostic_only: true,
            }],
            overflows: Vec::new(),
        },
    };
    let sent = Rc::clone(&sends);
    receiver
        .process_collected_sample(
            failed_open,
            Duration::from_millis(1),
            false,
            &mut failure_logs,
            move |_| {
                sent.set(sent.get() + 1);
                async { Ok(()) }
            },
        )
        .await
        .unwrap();

    let recovery_only = Sample {
        timestamp_unix_nano: 110,
        members: Vec::new(),
        points: Vec::new(),
        metadata: PendingMetadata {
            diagnostics: SampleDiagnostics {
                query_pending: true,
                ..Default::default()
            },
            failures: vec![SampleFailure {
                counter_index: 1,
                path: Arc::from(r"\Unavailable\Value"),
                reason: "query_recovery_pending",
                error: "recovery deferred".to_owned(),
                diagnostic_only: true,
            }],
            ..Default::default()
        },
    };
    receiver
        .process_collected_sample(
            recovery_only,
            Duration::from_millis(1),
            false,
            &mut failure_logs,
            |_| async { panic!("empty recovery sample must not be sent") },
        )
        .await
        .unwrap();

    let metrics = receiver.metrics.borrow();
    assert_eq!(sends.get(), 1);
    assert_eq!(metrics.scrape_attempts(Outcome::Success), 1);
    assert_eq!(metrics.scrape_attempts(Outcome::Failure), 1);
    assert_eq!(metrics.health.scrapes.get(), 1);
    assert_eq!(metrics.health.scrape_failures.get(), 0);
    assert_eq!(metrics.health.counter_add_failures.get(), 1);
    assert_eq!(metrics.health.query_pending.get(), 1);
    assert_eq!(metrics.health.counter_read_failures.get(), 0);
}

/// Scenario: Startup recovery, no-query backoff and failed replacement produce timestamped maintenance before a rebuilt query publishes.
/// Guarantees: The runtime projects producer-stamped maintenance without publication or collection failures and continues through successful recovery.
#[tokio::test(start_paused = true)]
async fn timestamped_query_maintenance_recovers_through_runtime() {
    let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    let mut samples = std::collections::VecDeque::new();
    for (timestamp, reason, rebuild_attempts) in [
        (70, "PdhOpenQueryW", 0),
        (80, "query_recovery_pending", 0),
        (90, "PdhOpenQueryW", 1),
    ] {
        samples.push_back(Sample {
            timestamp_unix_nano: timestamp,
            members: Vec::new(),
            points: Vec::new(),
            metadata: PendingMetadata {
                diagnostics: SampleDiagnostics {
                    query_pending: true,
                    query_rebuild_attempts: rebuild_attempts,
                    ..Default::default()
                },
                failures: vec![SampleFailure {
                    counter_index: 0,
                    path: Arc::from(r"\Memory\Available Bytes"),
                    reason,
                    error: "query unavailable".to_owned(),
                    diagnostic_only: true,
                }],
                ..Default::default()
            },
        });
    }
    samples.push_back(Sample {
        timestamp_unix_nano: 100,
        members: Vec::new(),
        points: Vec::new(),
        metadata: PendingMetadata {
            diagnostics: SampleDiagnostics {
                query_rebuild_attempts: 1,
                query_rebuild_recoveries: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    });
    samples.push_back(Sample {
        timestamp_unix_nano: 110,
        members: Vec::new(),
        points: vec![sample_point(0, 110, Number::Integer(42))],
        metadata: collected_metadata(),
    });
    let calls = std::cell::Cell::new(0);
    let sends = std::cell::Cell::new(0);
    let result = receiver.collect_and_send_with(
        || {
            calls.set(calls.get() + 1);
            std::future::ready(Ok(samples.pop_front().expect("unexpected extra scrape")))
        },
        |_| {
            sends.set(sends.get() + 1);
            std::future::pending::<Result<(), Error>>()
        },
    );
    tokio::pin!(result);
    tokio::select! {
        _ = &mut result => panic!("receiver terminated during query maintenance"),
        _ = tokio::time::sleep(Duration::from_secs(5)) => {}
    }
    assert_eq!(calls.get(), 5);
    assert_eq!(sends.get(), 1);
    let metrics = receiver.metrics.borrow();
    assert_eq!(metrics.scrape_attempts(Outcome::Failure), 4);
    assert_eq!(metrics.scrape_attempts(Outcome::Success), 1);
    assert_eq!(metrics.health.scrape_failures.get(), 0);
    assert_eq!(metrics.health.scrapes.get(), 1);
    assert_eq!(metrics.health.query_pending.get(), 0);
    assert_eq!(metrics.health.query_rebuild_attempts.get(), 2);
    assert_eq!(metrics.health.query_rebuild_recoveries.get(), 1);
}

/// Scenario: Collected and maintenance samples have a zero timestamp, or stamped maintenance has an invalid retained member index.
/// Guarantees: Every sample must satisfy strict timestamp and membership invariants, including empty no-query maintenance.
#[tokio::test]
async fn maintenance_handling_preserves_projection_invariants() {
    for (timestamp, collected, point, member) in [
        (0, true, false, false),
        (0, true, true, false),
        (0, false, true, false),
        (0, false, false, false),
        (100, false, false, true),
    ] {
        let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        let sample = Sample {
            timestamp_unix_nano: timestamp,
            members: if member {
                vec![super::super::model::SampleMember {
                    counter_index: 1,
                    path: Arc::from(r"\Memory\Available Bytes"),
                    instance: None,
                    emit_instance_attribute: false,
                }]
            } else {
                Vec::new()
            },
            points: if point {
                vec![sample_point(0, 10, Number::Integer(42))]
            } else {
                Vec::new()
            },
            metadata: PendingMetadata {
                diagnostics: SampleDiagnostics {
                    any_query_collected: collected,
                    query_pending: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        };
        let error = receiver
            .process_collected_sample(
                sample,
                Duration::ZERO,
                false,
                &mut FailureLogState::default(),
                |_| async { panic!("invalid sample must not publish") },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, CollectionLoopError::Projection(_)));
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.scrape_attempts(Outcome::Failure), 1);
        assert_eq!(metrics.health.scrape_failures.get(), 0);
    }
}

/// Scenario: A timeout warning is followed by a distinct native collection failure before any successful scrape.
/// Guarantees: Timeout suppression cannot hide the first real failure warning in the same unsuccessful episode.
#[test]
fn timeout_and_failure_warning_states_are_independent() {
    let mut state = FailureLogState::default();
    state.log_scrape_timeout("test", Duration::from_secs(1));
    assert!(state.scrape_timeout_active);
    assert!(!state.scrape_failure_active);

    state.log_scrape_failure(
        "test",
        &pdh::Error::Pdh {
            operation: pdh::PdhOperation::CollectQueryData,
            path: "<query>".to_owned(),
            status: 1,
        },
    );
    assert!(state.scrape_timeout_active);
    assert!(state.scrape_failure_active);
}

/// Scenario: A timeout warning is followed first by a late completion and then by an on-time failed completion.
/// Guarantees: Late completion keeps suppression armed, while an on-time completion re-arms the timeout independently of failure-warning suppression.
#[test]
fn timeout_warning_rearms_only_after_on_time_completion() {
    let mut state = FailureLogState::default();
    state.log_scrape_timeout("test", Duration::from_secs(1));

    state.record_completion(false);
    assert!(state.scrape_timeout_active);

    state.scrape_failure_active = true;
    state.record_completion(true);
    assert!(!state.scrape_timeout_active);
    assert!(state.scrape_failure_active);
}

/// Scenario: The initial PDH pass remains pending before it eventually succeeds.
/// Guarantees: Pipeline readiness stays blocked until native initialization completes, so downstream nodes cannot start against an unprimed receiver.
#[tokio::test]
async fn startup_readiness_waits_for_initial_pass() {
    let ready = Rc::new(std::cell::Cell::new(false));
    let signal = Rc::clone(&ready);
    let (release, pending) = tokio::sync::oneshot::channel::<()>();
    let startup = initialize_and_signal_ready(
        async move {
            pending.await.expect("release initial pass");
            Ok::<_, &'static str>(42)
        },
        move || signal.set(true),
    );
    tokio::pin!(startup);

    tokio::select! {
        biased;
        result = &mut startup => panic!("startup completed early: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
    assert!(!ready.get());

    release.send(()).expect("release startup");
    assert_eq!(startup.await.unwrap(), 42);
    assert!(ready.get());
}

/// Scenario: Native initialization fails before the worker can enter its collection loop.
/// Guarantees: A failed receiver never reports readiness, allowing the pipeline to surface startup failure instead of advertising a false healthy state.
#[tokio::test]
async fn startup_failure_does_not_signal_readiness() {
    let ready = Rc::new(std::cell::Cell::new(false));
    let signal = Rc::clone(&ready);
    let result = initialize_and_signal_ready(
        async { Err::<(), _>("initial PDH pass failed") },
        move || signal.set(true),
    )
    .await;

    assert_eq!(result.unwrap_err(), "initial PDH pass failed");
    assert!(!ready.get());
}

/// Scenario: A collected sample references a counter index outside the validated configuration.
/// Guarantees: A fatal invariant records a failed receiver attempt, not a query-collection failure, and publishes nothing.
#[tokio::test]
async fn runtime_terminates_on_projection_invariant() {
    let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    let sent = Rc::new(RefCell::new(Vec::new()));
    let capture = Rc::clone(&sent);
    let result = receiver
        .process_collected_sample(
            Sample {
                timestamp_unix_nano: 10,
                members: Vec::new(),
                points: vec![sample_point(1, 10, Number::Integer(42))],
                metadata: collected_metadata(),
            },
            Duration::ZERO,
            false,
            &mut FailureLogState::default(),
            move |pdata| async move {
                capture.borrow_mut().push(pdata);
                Ok(())
            },
        )
        .await;

    assert!(matches!(result, Err(CollectionLoopError::Projection(_))));
    assert!(sent.borrow().is_empty());
    let metrics = receiver.metrics.borrow();
    assert_eq!(metrics.scrape_attempts(Outcome::Success), 0);
    assert_eq!(metrics.scrape_attempts(Outcome::Failure), 1);
    assert_eq!(metrics.health.scrapes.get(), 0);
    assert_eq!(metrics.health.scrape_failures.get(), 0);
    assert_eq!(metrics.health.counter_read_failures.get(), 0);
}

/// Scenario: A scheduled timestamp read fails, then the next native scrape returns a valid point.
/// Guarantees: The receiver counts one failed collection, remains running, and publishes the next scrape.
#[tokio::test(start_paused = true)]
async fn timestamp_read_failure_recovers_on_next_scrape() {
    let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    let calls = std::cell::Cell::new(0);
    let sends = std::cell::Cell::new(0);
    let result = receiver.collect_and_send_with(
        || {
            calls.set(calls.get() + 1);
            let call = calls.get();
            async move {
                if call == 1 {
                    Err(pdh::Error::Timestamp("injected read failure"))
                } else {
                    Ok(Sample {
                        timestamp_unix_nano: 10,
                        members: Vec::new(),
                        points: vec![sample_point(0, 10, Number::Integer(42))],
                        metadata: collected_metadata(),
                    })
                }
            }
        },
        |_| {
            sends.set(sends.get() + 1);
            std::future::pending::<Result<(), Error>>()
        },
    );
    tokio::pin!(result);
    tokio::select! {
        _ = &mut result => panic!("receiver stopped before the next successful scrape"),
        _ = tokio::time::sleep(Duration::from_secs(2)) => {}
    }
    assert_eq!(calls.get(), 2);
    assert_eq!(sends.get(), 1);
    let metrics = receiver.metrics.borrow();
    assert_eq!(metrics.health.scrape_failures.get(), 1);
    assert_eq!(metrics.health.scrapes.get(), 1);
}

/// Scenario: Projection and downstream delivery occur after a two-second native collection.
/// Guarantees: The real collection loop records only collection and waits for a future tick after delayed delivery.
#[tokio::test(start_paused = true)]
async fn scrape_duration_excludes_projection_and_delivery() {
    let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    let mut receiver = receiver;
    receiver.config.timeout = Some(Duration::from_secs(30));
    let origin = tokio::time::Instant::now();
    let calls = std::cell::Cell::new(0);
    let result = receiver
        .collect_and_send_with(
            || {
                let call = calls.get();
                calls.set(call + 1);
                async move {
                    if call > 0 {
                        assert_eq!(origin.elapsed(), Duration::from_secs(13));
                        return Err(pdh::Error::WorkerStopped);
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    Ok(Sample {
                        timestamp_unix_nano: 10,
                        members: Vec::new(),
                        points: vec![sample_point(0, 10, Number::Integer(42))],
                        metadata: collected_metadata(),
                    })
                }
            },
            |_| async {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(())
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
    ));
    let distribution = receiver.metrics.borrow().health.scrape_duration.get();
    assert_eq!(distribution.summary(), (1, 2.0, 2.0, 2.0));
}

/// Scenario: Two collections each finish after the cadence while timeout is unset.
/// Guarantees: Late scrapes retain their native timestamps, count as successful overruns, and collection proceeds to the next scrape instead of stalling after the first.
#[tokio::test]
async fn no_timeout_delivers_late_scrapes_and_continues() {
    let mut receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    receiver.config.timeout = None;
    receiver.config.collection_interval = Duration::from_millis(10);
    let collect_calls = Rc::new(std::cell::Cell::new(0_usize));
    let collect_state = Rc::clone(&collect_calls);
    let collect = move || {
        let call = collect_state.get();
        collect_state.set(call + 1);
        async move {
            if call >= 2 {
                return std::future::pending::<Result<Sample, pdh::Error>>().await;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            let timestamp = if call == 0 { 100 } else { 200 };
            Ok(Sample {
                timestamp_unix_nano: timestamp,
                members: Vec::new(),
                points: vec![sample_point(0, timestamp, Number::Integer(call as i64))],
                metadata: collected_metadata(),
            })
        }
    };
    let sent = Rc::new(RefCell::new(Vec::new()));
    let capture = Rc::clone(&sent);
    let run = receiver.collect_and_send_with(collect, move |pdata| {
        let capture = Rc::clone(&capture);
        async move {
            capture.borrow_mut().push(pdata);
            Ok(())
        }
    });
    tokio::pin!(run);

    let wait_for_following_scrape = async {
        while collect_calls.get() < 3 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::select! {
        biased;
        _ = &mut run => panic!("collection loop stopped after a late scrape"),
        result = timeout(Duration::from_secs(1), wait_for_following_scrape) => {
            result.expect("following scrape did not start");
        }
    }

    assert_eq!(collect_calls.get(), 3);
    assert_eq!(sent.borrow().len(), 2);
    let timestamps = sent
        .borrow()
        .iter()
        .cloned()
        .map(|pdata| {
            let (_, payload) = pdata.into_parts();
            let PayloadData::OtapRecords(records) = payload.into_data() else {
                panic!("expected native OTAP records");
            };
            records
                .get(ArrowPayloadType::NumberDataPoints)
                .expect("number data points")
                .column_by_name("time_unix_nano")
                .expect("point timestamps")
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("nanosecond timestamp array")
                .value(0)
        })
        .collect::<Vec<_>>();
    assert_eq!(timestamps, vec![100, 200]);

    let metrics = receiver.metrics.borrow();
    assert_eq!(metrics.scrape_attempts(Outcome::Success), 2);
    assert_eq!(metrics.scrape_attempts(Outcome::Failure), 0);
    assert_eq!(metrics.health.scrapes.get(), 2);
    assert_eq!(metrics.health.scrape_failures.get(), 0);
    assert_eq!(metrics.health.scrape_overruns.get(), 2);
}

/// Scenario: A native collection request is rejected because the capacity-one worker queue is busy.
/// Guarantees: Worker backpressure counts as neither an overrun nor a failed scrape.
#[tokio::test]
async fn worker_busy_collection_is_not_an_overrun() {
    let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    let collect_calls = Rc::new(std::cell::Cell::new(0_usize));
    let collect_state = Rc::clone(&collect_calls);
    let result = receiver
        .collect_and_send_with(
            move || {
                let call = collect_state.get();
                collect_state.set(call + 1);
                async move {
                    if call == 0 {
                        Err(pdh::Error::WorkerBusy)
                    } else {
                        Err(pdh::Error::WorkerStopped)
                    }
                }
            },
            |_| async { Ok(()) },
        )
        .await;

    assert!(matches!(
        result,
        Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
    ));
    let metrics = receiver.metrics.borrow();
    assert_eq!(metrics.scrape_attempts(Outcome::Failure), 1);
    assert_eq!(metrics.health.scrape_failures.get(), 0);
    assert_eq!(metrics.health.scrape_overruns.get(), 0);
    assert_eq!(metrics.health.worker_busy.get(), 1);
}

/// Scenario: A scheduled native collection reports an invalid query handle.
/// Guarantees: The failed native attempt is counted once and runtime requests another scheduled collection instead of terminating on invalidation.
#[tokio::test(start_paused = true)]
async fn invalid_query_is_retryable_and_counted_once() {
    let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    let calls = std::cell::Cell::new(0);
    let result = receiver
        .collect_and_send_with(
            || {
                calls.set(calls.get() + 1);
                let call = calls.get();
                async move {
                    if call > 1 {
                        return Err(pdh::Error::WorkerStopped);
                    }
                    Err(pdh::Error::Pdh {
                        operation: pdh::PdhOperation::CollectQueryData,
                        path: "<query>".to_owned(),
                        status: windows_sys::Win32::System::Performance::PDH_INVALID_HANDLE,
                    })
                }
            },
            |_| async { panic!("invalid query must never publish") },
        )
        .await;
    assert!(matches!(
        result,
        Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
    ));
    assert_eq!(calls.get(), 2);
    let metrics = receiver.metrics.borrow();
    assert_eq!(metrics.scrape_attempts(Outcome::Failure), 2);
    assert_eq!(metrics.health.scrape_failures.get(), 1);
    assert_eq!(metrics.health.scrapes.get(), 0);
    assert_eq!(metrics.health.counter_read_failures.get(), 0);
}

/// Scenario: Native collection reports an impossible internal sample invariant.
/// Guarantees: Invariant corruption terminates the receiver instead of being retried as an ordinary scrape failure.
#[tokio::test]
async fn invalid_sample_collection_is_fatal() {
    let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    let result = receiver
        .collect_and_send_with(
            || async { Err(pdh::Error::InvalidSample("injected invariant")) },
            |_| async { Ok(()) },
        )
        .await;

    assert!(matches!(
        result,
        Err(CollectionLoopError::Native(pdh::Error::InvalidSample(
            "injected invariant"
        )))
    ));
    let metrics = receiver.metrics.borrow();
    assert_eq!(metrics.scrape_attempts(Outcome::Failure), 1);
    assert_eq!(metrics.health.scrape_failures.get(), 0);
}

/// Scenario: A scrape remains blocked after its configured timeout threshold and completes later.
/// Guarantees: Overrun telemetry increments at threshold crossing before completion or delivery, then remains single-counted.
#[tokio::test]
async fn timeout_records_overrun_before_late_scrape_completes() {
    let mut receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    receiver.config.timeout = Some(Duration::from_millis(10));
    receiver.config.collection_interval = Duration::from_secs(1);
    let release = Rc::new(std::cell::Cell::new(false));
    let collect_release = Rc::clone(&release);
    let collect_calls = Rc::new(std::cell::Cell::new(0_usize));
    let collect_state = Rc::clone(&collect_calls);
    let sent = Rc::new(RefCell::new(Vec::new()));
    let capture = Rc::clone(&sent);
    let run = receiver.collect_and_send_with(
        move || {
            let call = collect_state.get();
            collect_state.set(call + 1);
            let release = Rc::clone(&collect_release);
            async move {
                if call > 0 {
                    return std::future::pending::<Result<Sample, pdh::Error>>().await;
                }
                while !release.get() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Ok(Sample {
                    timestamp_unix_nano: 100,
                    members: Vec::new(),
                    points: vec![sample_point(0, 100, Number::Integer(1))],
                    metadata: collected_metadata(),
                })
            }
        },
        move |pdata| {
            let capture = Rc::clone(&capture);
            async move {
                capture.borrow_mut().push(pdata);
                Ok(())
            }
        },
    );
    tokio::pin!(run);

    let wait_for_overrun = async {
        while receiver.metrics.borrow().health.scrape_overruns.get() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::select! {
        biased;
        _ = &mut run => panic!("collection loop stopped before late scrape release"),
        result = timeout(Duration::from_secs(1), wait_for_overrun) => {
            result.expect("overrun was not recorded at the threshold");
        }
    }
    assert!(sent.borrow().is_empty());
    assert_eq!(receiver.metrics.borrow().health.scrapes.get(), 0);

    release.set(true);
    let wait_for_delivery = async {
        while sent.borrow().is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    tokio::select! {
        biased;
        _ = &mut run => panic!("collection loop stopped after late scrape release"),
        result = timeout(Duration::from_secs(1), wait_for_delivery) => {
            result.expect("late scrape was not delivered");
        }
    }
    let metrics = receiver.metrics.borrow();
    assert_eq!(metrics.health.scrapes.get(), 1);
    assert_eq!(metrics.health.scrape_overruns.get(), 1);
}

/// Scenario: A positive timeout is longer than the collection interval and the scrape completes between them.
/// Guarantees: The explicit timeout takes precedence, so the successful scrape is on time and does not count as an overrun.
#[tokio::test]
async fn positive_timeout_takes_precedence_over_collection_interval() {
    let mut receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    receiver.config.timeout = Some(Duration::from_millis(200));
    receiver.config.collection_interval = Duration::from_millis(5);
    let calls = Rc::new(std::cell::Cell::new(0_usize));
    let collect_calls = Rc::clone(&calls);
    let result = receiver
        .collect_and_send_with(
            move || {
                let call = collect_calls.get();
                collect_calls.set(call + 1);
                async move {
                    if call == 0 {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok(Sample {
                            timestamp_unix_nano: 100,
                            members: Vec::new(),
                            points: vec![sample_point(0, 100, Number::Integer(1))],
                            metadata: collected_metadata(),
                        })
                    } else {
                        Err(pdh::Error::WorkerStopped)
                    }
                }
            },
            |_| async { Ok(()) },
        )
        .await;

    assert!(matches!(
        result,
        Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
    ));
    let metrics = receiver.metrics.borrow();
    assert_eq!(metrics.health.scrapes.get(), 1);
    assert_eq!(metrics.health.scrape_overruns.get(), 0);
}

/// Scenario: A late native result and its timeout are both ready before the collection loop is polled again.
/// Guarantees: The completion-first branch still counts one overrun for an accepted result, but never for WorkerBusy.
#[tokio::test(start_paused = true)]
async fn ready_late_result_counts_overrun_unless_worker_busy() {
    for busy in [false, true] {
        let mut receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        receiver.config.timeout = Some(Duration::from_millis(10));
        receiver.config.collection_interval = Duration::from_secs(1);
        let calls = std::cell::Cell::new(0);
        let sent = std::cell::Cell::new(0);
        let run = receiver.collect_and_send_with(
            || {
                calls.set(calls.get() + 1);
                async {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    if busy {
                        Err(pdh::Error::WorkerBusy)
                    } else {
                        Ok(Sample {
                            timestamp_unix_nano: 100,
                            members: Vec::new(),
                            points: vec![sample_point(0, 100, Number::Integer(1))],
                            metadata: collected_metadata(),
                        })
                    }
                }
            },
            |_| {
                sent.set(sent.get() + 1);
                async { Ok(()) }
            },
        );
        tokio::pin!(run);
        std::future::poll_fn(|cx| {
            assert!(run.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(calls.get(), 1);
        tokio::time::advance(Duration::from_millis(20)).await;
        std::future::poll_fn(|cx| {
            assert!(run.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(calls.get(), 1);
        assert_eq!(sent.get(), usize::from(!busy));
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.health.scrape_overruns.get(), u64::from(!busy));
        assert_eq!(metrics.health.worker_busy.get(), u64::from(busy));
        assert_eq!(metrics.health.scrapes.get(), u64::from(!busy));
    }
}

/// Scenario: Native collection completes at the same instant as the configured timeout threshold.
/// Guarantees: Completion at the boundary is on time and does not increment overrun telemetry.
#[tokio::test(start_paused = true)]
async fn completion_at_timeout_threshold_is_on_time() {
    let mut receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    receiver.config.timeout = Some(Duration::from_millis(10));
    receiver.config.collection_interval = Duration::from_millis(10);
    let calls = Rc::new(std::cell::Cell::new(0_usize));
    let collect_calls = Rc::clone(&calls);
    let result = receiver
        .collect_and_send_with(
            move || {
                let call = collect_calls.get();
                collect_calls.set(call + 1);
                async move {
                    if call == 0 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Ok(Sample {
                            timestamp_unix_nano: 100,
                            members: Vec::new(),
                            points: vec![sample_point(0, 100, Number::Integer(1))],
                            metadata: collected_metadata(),
                        })
                    } else {
                        Err(pdh::Error::WorkerStopped)
                    }
                }
            },
            |_| async { Ok(()) },
        )
        .await;

    assert!(matches!(
        result,
        Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
    ));
    let metrics = receiver.metrics.borrow();
    assert_eq!(metrics.health.scrapes.get(), 1);
    assert_eq!(metrics.health.scrape_overruns.get(), 0);
}

/// Scenario: Collection has a nonzero initial delay before its first request.
/// Guarantees: The delay remains pending until the configured duration has fully elapsed.
#[tokio::test(start_paused = true)]
async fn initial_delay_gates_first_collection() {
    let delay = Duration::from_secs(10);
    let wait = wait_initial_delay(delay);
    tokio::pin!(wait);
    tokio::select! {
        biased;
        () = &mut wait => panic!("initial delay completed before time advanced"),
        () = tokio::task::yield_now() => {}
    }

    tokio::time::advance(delay - Duration::from_nanos(1)).await;
    tokio::select! {
        biased;
        () = &mut wait => panic!("initial delay completed before the full duration"),
        () = tokio::task::yield_now() => {}
    }
    tokio::time::advance(Duration::from_nanos(1)).await;
    wait.await;
}

/// Scenario: Native worker cleanup competes with receiver and pipeline teardown for one deadline.
/// Guarantees: Worker waiting is capped and preserves time for pipeline completion.
#[test]
fn worker_shutdown_preserves_pipeline_completion_budget() {
    let now = Instant::now();

    assert_eq!(
        worker_shutdown_deadline(now, now + Duration::from_secs(15)),
        now + WORKER_SHUTDOWN_MAX_WAIT
    );
    assert_eq!(
        worker_shutdown_deadline(now, now + Duration::from_millis(750)),
        now + Duration::from_millis(250)
    );
    assert_eq!(
        worker_shutdown_deadline(now, now + Duration::from_millis(250)),
        now
    );
}

/// Scenario: The factory is invoked for a host-wide source pipeline allocated two cores.
/// Guarantees: Configuration fails before receiver construction so one host is never collected twice.
#[test]
fn factory_rejects_multi_core_pipeline() {
    let test_runtime = TestRuntime::<OtapPdata>::new();
    let controller = ControllerContext::new(TelemetryRegistryHandle::new());
    let pipeline = controller
        .pipeline_context_with("test_group".into(), "test_pipeline".into(), 0, 2, 0)
        .with_node_context(
            "windows_perf_counters".into(),
            WINDOWSPERFCOUNTERS_RECEIVER_URN.into(),
            otel_arrow_dfe_config::node::NodeKind::Receiver,
            HashMap::new(),
        );
    let factory = OTAP_PIPELINE_FACTORY
        .get_receiver_factory_map()
        .get("urn:otel:receiver:windows_perf_counters")
        .expect("registered windows_perf_counters factory");
    let result = (factory.create)(
        pipeline,
        test_node(test_runtime.config().name.clone()),
        Arc::new(NodeUserConfig::new_receiver_config(
            WINDOWSPERFCOUNTERS_RECEIVER_URN,
        )),
        test_runtime.config(),
        &Capabilities::empty(),
    );
    let Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig { error }) = result else {
        panic!("multi-core factory construction must fail as invalid user configuration");
    };
    assert!(error.contains("requires a one-core source pipeline"));
}

/// Scenario: Public node shorthand is resolved and operational metrics are registered.
/// Guarantees: The executable node type, factory URN and both emitted metric-set names agree.
#[test]
fn public_receiver_names_match_registration_and_metrics() {
    let node: NodeUserConfig = serde_json::from_value(serde_json::json!({
        "type": "receiver:windows_perf_counters"
    }))
    .unwrap();
    assert_eq!(
        node.r#type.as_str(),
        "urn:otel:receiver:windows_perf_counters"
    );
    assert!(
        OTAP_PIPELINE_FACTORY
            .get_receiver_factory_map()
            .contains_key(node.r#type.as_str())
    );
    let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    let mut metrics = receiver.metrics.borrow_mut();
    metrics.record_scrape(Outcome::Success);
    let snapshots = metrics.terminal_snapshots();
    let names = snapshots
        .iter()
        .map(|snapshot| snapshot.descriptor().name)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        names,
        [
            "receiver.windows_perf_counters",
            "receiver.windows_perf_counters.scrapes"
        ]
        .into_iter()
        .collect()
    );
}

fn run_live_lifecycle(drain: bool) {
    let test_runtime = TestRuntime::<OtapPdata>::new();
    let (pipeline, _) = test_pipeline_ctx();
    let receiver = WindowsPerfCountersReceiver {
        config: Config {
            counters: vec![CounterConfig {
                path: r"\Memory\Available Bytes".to_owned(),
                name: Arc::from("windows.memory.available"),
                unit: Arc::from("By"),
                description: Arc::from("Available physical memory."),
                metric_kind: MetricKind::Gauge,
                attributes: Arc::new(BTreeMap::new()),
                instance_config: InstanceConfig::None,
                scale_power10: 0,
                source_mapping_index: 0,
            }],
            collection_interval: Duration::from_secs(1),
            initial_delay: Duration::ZERO,
            timeout: Some(Duration::from_secs(1)),
            wildcard_refresh_interval: None,
            max_instances_per_wildcard: 256,
            max_expanded_counters: 4_096,
            warnings: Vec::new(),
        },
        node: "windows_perf_counters".to_owned(),
        metrics: Rc::new(RefCell::new(WindowsPerfCountersReceiverMetrics::register(
            &pipeline,
        ))),
    };
    let receiver = ReceiverWrapper::local(
        receiver,
        test_node("windows_perf_counters"),
        Arc::new(NodeUserConfig::new_receiver_config(
            WINDOWSPERFCOUNTERS_RECEIVER_URN,
        )),
        test_runtime.config(),
    );

    test_runtime
        .set_receiver(receiver)
        .run_test(move |ctx| async move {
            ctx.sleep(Duration::from_secs(1)).await;
            let deadline = Instant::now() + Duration::from_secs(2);
            if drain {
                ctx.send_control_msg(NodeControlMsg::DrainIngress {
                    deadline,
                    reason: "test drain".to_owned(),
                })
                .await
                .unwrap();
            } else {
                ctx.send_shutdown(deadline, "test shutdown").await.unwrap();
            }
        })
        .run_validation(|_| async {});
}

/// Scenario: The registered factory starts a live receiver with exact, aggregate, wildcard, and unavailable peer mappings.
/// Guarantees: Two engine batches decode exact, aggregate, wildcard and fixed-start cumulative Sum points beside an omitted unavailable peer.
#[test]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
fn live_factory_collects_decodes_and_hands_off_metrics() {
    let test_runtime = TestRuntime::<OtapPdata>::new();
    let (pipeline, _) = test_pipeline_ctx();
    let factory = OTAP_PIPELINE_FACTORY
        .get_receiver_factory_map()
        .get(WINDOWSPERFCOUNTERS_RECEIVER_URN)
        .expect("registered windows_perf_counters factory");
    let mut node_config = NodeUserConfig::new_receiver_config(WINDOWSPERFCOUNTERS_RECEIVER_URN);
    node_config.config = serde_json::json!({
        "metrics": {
            "windows.memory.available": {
                "unit": "By",
                "gauge": {}
            },
            "windows.memory.available.sum": {
                "unit": "By",
                "sum": {"aggregation": "cumulative", "monotonic": false}
            },
            "windows.processor.time": {
                "unit": "%",
                "gauge": {}
            },
            "windows.process.id": {
                "unit": "1",
                "gauge": {}
            },
            "windows.memory.missing": {
                "unit": "By",
                "gauge": {}
            }
        },
        "perfcounters": [
            {
                "object": "Memory",
                "counters": [{
                    "name": "Available Bytes",
                    "metric": "windows.memory.available"
                }, {
                    "name": "Committed Bytes",
                    "metric": "windows.memory.available.sum"
                }]
            },
            {
                "object": "Processor",
                "instances": ["_Total"],
                "counters": [{
                    "name": "% Processor Time",
                    "metric": "windows.processor.time"
                }]
            },
            {
                "object": "Process",
                "instances": ["*"],
                "counters": [{
                    "name": "ID Process",
                    "metric": "windows.process.id"
                }]
            },
            {
                "object": "Memory",
                "counters": [{
                    "name": "otel-arrow-nonexistent-counter",
                    "metric": "windows.memory.missing"
                }]
            }
        ],
        "collection_interval": "1s",
        "initial_delay": "500ms",
        "timeout": "5s"
    });
    let receiver = (factory.create)(
        pipeline,
        test_node(test_runtime.config().name.clone()),
        Arc::new(node_config),
        test_runtime.config(),
        &Capabilities::empty(),
    )
    .expect("factory should create the live receiver");

    test_runtime
        .set_receiver(receiver)
        .run_test(|ctx| async move {
            ctx.sleep(Duration::from_millis(2_500)).await;
            ctx.send_shutdown(
                Instant::now() + Duration::from_secs(3),
                "live factory handoff complete",
            )
            .await
            .unwrap();
        })
        .run_validation(|mut ctx| async move {
            let mut previous_sum = None;
            for _ in 0..2 {
                let pdata = ctx
                    .recv()
                    .await
                    .expect("receiver should hand off one batch");
                let (_, payload) = pdata.into_parts();
                let PayloadData::OtapRecords(records) = payload.into_data() else {
                    panic!("factory handoff must contain decoded OTAP records");
                };
                let bytes: OtlpProtoBytes = OtapPayload::from_otap(records)
                    .try_into_with_default()
                    .expect("OTAP records should decode to OTLP");
                let request = ExportMetricsServiceRequest::decode(bytes.as_bytes())
                    .expect("decoded OTLP metrics request");
                let metrics = request
                    .resource_metrics
                    .iter()
                    .flat_map(|resource| &resource.scope_metrics)
                    .flat_map(|scope| &scope.metrics)
                    .collect::<Vec<_>>();
                let metric = |name: &str| metrics.iter().find(|metric| metric.name == name);

                let memory = metric("windows.memory.available").expect("Memory metric");
                let Some(metric::Data::Gauge(memory)) = &memory.data else {
                    panic!("Memory metric must be a Gauge");
                };
                assert_eq!(memory.data_points.len(), 1);

                let sum_metric = metric("windows.memory.available.sum").expect("Sum metric");
                let Some(metric::Data::Sum(sum)) = &sum_metric.data else {
                    panic!("cumulative Sum metric must be a Sum");
                };
                assert!(!sum.is_monotonic);
                assert_eq!(sum.aggregation_temporality, 2);
                assert_eq!(sum.data_points.len(), 1);
                let point = &sum.data_points[0];
                assert!(point.start_time_unix_nano > 0);
                assert!(point.start_time_unix_nano <= point.time_unix_nano);
                if let Some((start, time)) = previous_sum {
                    assert_eq!(point.start_time_unix_nano, start);
                    assert!(point.time_unix_nano > time);
                }
                previous_sum = Some((point.start_time_unix_nano, point.time_unix_nano));

                let processor = metric("windows.processor.time").expect("_Total metric");
                let Some(metric::Data::Gauge(processor)) = &processor.data else {
                    panic!("_Total metric must be a Gauge");
                };
                assert_eq!(processor.data_points.len(), 1);
                let aggregate = processor.data_points[0]
                    .attributes
                    .iter()
                    .find(|attribute| attribute.key == "windows.perf_counter.instance")
                    .expect("_Total instance identity");
                assert!(matches!(
                    aggregate.value.as_ref().and_then(|value| value.value.as_ref()),
                    Some(any_value::Value::StringValue(value)) if value == "_Total"
                ));

                let process = metric("windows.process.id").expect("expanded Process metric");
                let Some(metric::Data::Gauge(process)) = &process.data else {
                    panic!("expanded Process metric must be a Gauge");
                };
                assert!(!process.data_points.is_empty());
                assert!(process.data_points.iter().all(|point| {
                    point
                        .attributes
                        .iter()
                        .any(|attribute| attribute.key == "windows.perf_counter.instance")
                }));
                assert!(metric("windows.memory.missing").is_none());
            }
        });
}

/// Scenario: The engine requests ingress drain and shutdown from a live receiver.
/// Guarantees: Both lifecycle paths stop the PDH worker and complete before their deadlines.
#[test]
#[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
fn completes_drain_and_shutdown_through_engine_harness() {
    run_live_lifecycle(true);
    run_live_lifecycle(false);
}

async fn loop_collection_start_offsets(
    period: Duration,
    timeout: Option<Duration>,
    first_collection_duration: Duration,
) -> Vec<Duration> {
    let mut receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
    receiver.config.collection_interval = period;
    receiver.config.timeout = timeout;
    let origin = tokio::time::Instant::now();
    let starts = Rc::new(RefCell::new(Vec::new()));
    let collect_starts = Rc::clone(&starts);
    let result = receiver
        .collect_and_send_with(
            move || {
                let call = collect_starts.borrow().len();
                collect_starts
                    .borrow_mut()
                    .push(tokio::time::Instant::now().duration_since(origin));
                async move {
                    if call > 0 {
                        return Err(pdh::Error::WorkerStopped);
                    }
                    tokio::time::sleep(first_collection_duration).await;
                    Ok(Sample {
                        timestamp_unix_nano: 100,
                        members: Vec::new(),
                        points: vec![sample_point(0, 100, Number::Integer(1))],
                        metadata: PendingMetadata {
                            diagnostics: SampleDiagnostics {
                                any_query_collected: true,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    })
                }
            },
            |_| async { Ok(()) },
        )
        .await;
    assert!(matches!(
        result,
        Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
    ));
    starts.borrow().clone()
}

/// Scenario: Collection timing crosses zero or multiple cadence points with timeout omitted, longer, or shorter than the period.
/// Guarantees: Every case starts next at the first future original cadence point without catch-up or dropping an unmissed tick.
#[tokio::test(start_paused = true)]
async fn collection_loop_preserves_original_cadence_across_timeout_modes() {
    for (period_ms, timeout_ms, collection_ms, expected_ms) in [
        (10, None, 25, 30),
        (10, Some(50), 25, 30),
        (20, Some(5), 10, 20),
    ] {
        assert_eq!(
            loop_collection_start_offsets(
                Duration::from_millis(period_ms),
                timeout_ms.map(Duration::from_millis),
                Duration::from_millis(collection_ms),
            )
            .await,
            [Duration::ZERO, Duration::from_millis(expected_ms)]
        );
    }
}
