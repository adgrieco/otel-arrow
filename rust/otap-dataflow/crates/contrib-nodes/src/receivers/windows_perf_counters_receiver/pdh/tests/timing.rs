// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

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

/// Scenario: A valid monotonic observation is consumed by startup priming before two values are published.
/// Guarantees: Priming preserves reset detection without backdating the first exported cumulative sequence.
#[test]
fn monotonic_prime_defers_start_until_first_publication() {
    let mut sequence = MonotonicSequence::default();
    prime_monotonic_sequence("counter", &mut sequence, 10, 100).unwrap();
    assert_eq!(sequence.start_time_unix_nano, None);
    assert_eq!(sequence.previous_value, Some(10));
    assert_eq!(sequence.previous_timestamp_unix_nano, Some(100));

    assert_eq!(
        monotonic_start_time("counter", &mut sequence, 15, 110).unwrap(),
        110
    );
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, 20, 120).unwrap(),
        110
    );
}

/// Scenario: A primed monotonic query publishes, omits one value, resumes, and then observes a decrease.
/// Guarantees: Unexported and omitted observations never backdate output, while a real reset starts a new exported sequence.
#[test]
fn primed_monotonic_query_tracks_published_sequence_only() {
    let path = r"\TCPv4\Connection Failures";
    let (state, mut query) = fake_query(
        vec![monotonic_sum(path, "windows.tcp.connection_failures", "1")],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0,
            raw: None,
            multi_count: 1,
            formatted: Some(Number::Integer(10)),
            raw_error: false,
            formatted_error: false,
        }],
    );
    query.prime().unwrap();

    {
        let mut state = state.borrow_mut();
        state.timestamp = 110;
        state.counters[0].formatted = Some(Number::Integer(20));
    }
    let first = query.collect().unwrap();
    assert_eq!(first.points[0].start_time_unix_nano, Some(110));
    assert!(query.publish_monotonic_start(0, &Arc::from(path), 110));

    {
        let mut state = state.borrow_mut();
        state.timestamp = 120;
        state.counters[0].formatted = None;
    }
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::NoObservation
    );

    {
        let mut state = state.borrow_mut();
        state.timestamp = 130;
        state.counters[0].formatted = Some(Number::Integer(30));
    }
    assert_eq!(
        query.collect().unwrap().points[0].start_time_unix_nano,
        Some(110)
    );

    {
        let mut state = state.borrow_mut();
        state.timestamp = 140;
        state.counters[0].formatted = Some(Number::Integer(5));
    }
    assert_eq!(
        query.collect().unwrap().points[0].start_time_unix_nano,
        Some(140)
    );
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
            multi_count: 1,
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
    assert_eq!(sample.timestamp_unix_nano, 115);
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
            multi_count: 1,
            formatted: Some(Number::Integer(20)),
            raw_error: false,
            formatted_error: false,
        }],
    );

    state.borrow_mut().timestamp = 120;
    let first = query.collect().unwrap();
    assert_eq!(first.points[0].start_time_unix_nano, Some(120));
    assert!(query.publish_monotonic_start(0, &Arc::from(path), 120));
    {
        let mut state = state.borrow_mut();
        state.timestamp = 125;
        state.counters[0].formatted_error = true;
    }
    let sample = query.collect().unwrap();
    assert!(sample.points.is_empty());
    assert_eq!(sample.metadata.failures.len(), 1);

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
            multi_count: 1,
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

/// Scenario: A calculated counter's native base decreases and then advances from the smaller value.
/// Guarantees: The decrease is omitted while becoming the new baseline, so the next increase emits without another warm-up.
#[test]
fn calculated_base_decrease_preserves_new_baseline() {
    let path = r"\PhysicalDisk(_Total)\Avg. Disk sec/Transfer";
    let (state, mut query) = fake_query(
        vec![counter(path, "windows.disk.average_latency")],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x4002_0500,
            raw: Some(10),
            multi_count: 1,
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

    state.borrow_mut().counters[0].raw = Some(5);
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::NoObservation
    );

    state.borrow_mut().counters[0].raw = Some(6);
    assert!(matches!(
        query.collect().unwrap().points[0].value,
        SampleValue::Value(Number::Double(4.0))
    ));
}

/// Scenario: Elapsed-time, raw-fraction, and large raw-fraction counters each have one valid collected sample.
/// Guarantees: Every formatted single-sample formula emits immediately, accepts a zero native base through PDH formatting, and performs no raw read.
#[test]
fn elapsed_time_emits_from_first_sample() {
    let elapsed_path = r"\System\System Up Time";
    let fraction_path = r"\Memory\% Committed Bytes In Use";
    let large_fraction_path = r"\Memory\Large Fraction";
    let (state, mut query) = fake_query(
        vec![
            counter(elapsed_path, "system.uptime"),
            counter(fraction_path, "system.memory.committed"),
            counter(large_fraction_path, "system.memory.large_fraction"),
        ],
        vec![
            FakeCounter {
                path: elapsed_path.to_owned(),
                native_type: 0x3024_0500,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Double(42.5)),
                raw_error: false,
                formatted_error: false,
            },
            FakeCounter {
                path: fraction_path.to_owned(),
                native_type: 0x2002_0400,
                raw: Some(0),
                multi_count: 0,
                formatted: Some(Number::Double(50.0)),
                raw_error: true,
                formatted_error: false,
            },
            FakeCounter {
                path: large_fraction_path.to_owned(),
                native_type: 0x2002_0500,
                raw: Some(0),
                multi_count: 0,
                formatted: Some(Number::Double(25.0)),
                raw_error: true,
                formatted_error: false,
            },
        ],
    );

    let sample = query.collect().unwrap();
    assert_eq!(
        sample
            .points
            .iter()
            .map(|point| point.value)
            .collect::<Vec<_>>(),
        [
            SampleValue::Value(Number::Double(42.5)),
            SampleValue::Value(Number::Double(50.0)),
            SampleValue::Value(Number::Double(25.0))
        ]
    );
    assert_eq!(state.borrow().calls.raw, 0);
}

/// Scenario: Each multi-timer SDK family warms, receives a zero native MultiCount, then recovers.
/// Guarantees: The only receiver-side denominator guard covers every multi-timer and preserves the next valid interval.
#[test]
fn all_multi_timer_invalid_bases_preserve_readiness() {
    for native_type in [0x2241_0500, 0x2341_0500, 0x2251_0500, 0x2351_0500] {
        let path = format!(r"\Fixture\Multi Timer {native_type:08X}");
        let (state, mut query) = fake_query(
            vec![counter(&path, "system.cpu.utility")],
            vec![FakeCounter {
                path: path.clone(),
                native_type,
                raw: Some(10),
                multi_count: 4,
                formatted: Some(Number::Double(25.0)),
                raw_error: false,
                formatted_error: false,
            }],
        );

        assert_eq!(
            query.collect().unwrap().points[0].value,
            SampleValue::Warming
        );
        {
            let mut state = state.borrow_mut();
            state.counters[0].raw = Some(20);
            state.counters[0].multi_count = 0;
        }
        let sample = query.collect().unwrap();
        assert!(sample.points.is_empty());
        assert_eq!(sample.metadata.failures.len(), 1);
        assert!(
            sample.metadata.failures[0]
                .error
                .contains("multi-counter base denominator must be positive"),
            "native type 0x{native_type:08X}"
        );

        {
            let mut state = state.borrow_mut();
            state.counters[0].raw = Some(30);
            state.counters[0].multi_count = 4;
        }
        assert_eq!(
            query.collect().unwrap().points[0].value,
            SampleValue::Value(Number::Double(25.0)),
            "native type 0x{native_type:08X}"
        );
    }
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

/// Scenario: Metric kinds are paired with direct, calculated, and delta native counter types.
/// Guarantees: Cumulative monotonic and delta Sums enforce their distinct native eligibility.
#[test]
fn validates_sum_native_type() {
    assert!(
        validate_metric_mapping(
            "counter",
            MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: true
            },
            CounterKind::Direct
        )
        .is_ok()
    );
    assert!(
        validate_metric_mapping(
            "counter",
            MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: true
            },
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
            MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: false,
            },
            CounterKind::CalculatedTwoSample
        )
        .is_ok()
    );
    for monotonic in [false, true] {
        assert!(
            validate_metric_mapping(
                "delta",
                MetricKind::Sum {
                    aggregation: Aggregation::Delta,
                    monotonic,
                },
                CounterKind::Delta
            )
            .is_ok()
        );
        assert!(
            validate_metric_mapping(
                "delta",
                MetricKind::Sum {
                    aggregation: Aggregation::Delta,
                    monotonic,
                },
                CounterKind::CalculatedTwoSample
            )
            .is_err()
        );
    }
}

/// Scenario: An ineligible monotonic mapping is added beside a healthy counter during startup.
/// Guarantees: The mapping is permanent and diagnostic but does not require retry.
#[test]
fn isolates_invalid_monotonic_sum_mapping() {
    let error = validate_metric_mapping(
        "counter",
        MetricKind::Sum {
            aggregation: Aggregation::Cumulative,
            monotonic: true,
        },
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
        operation: PdhOperation::AddEnglishCounter,
        path: "missing".to_owned(),
        status: 1,
    };
    assert_eq!(
        exact_add_failure_disposition(&retryable),
        ExactAddFailureDisposition::Retry
    );
}

/// Scenario: An ineligible delta Sum mapping starts beside a healthy direct Gauge.
/// Guarantees: The invalid mapping is diagnostic and non-retryable while the healthy peer emits.
#[test]
fn isolates_invalid_delta_sum_mapping() {
    let direct_path = r"\Memory\Available Bytes";
    let invalid_path = r"\Processor(_Total)\% Processor Time";
    let configs = vec![
        counter(direct_path, "windows.memory.available"),
        delta_sum(invalid_path, "windows.processor.delta", true),
    ];
    let (_state, mut query) = fake_query(
        configs,
        vec![
            FakeCounter {
                path: direct_path.to_owned(),
                native_type: 0x0001_0100,
                raw: None,
                multi_count: 1,
                formatted: Some(Number::Integer(42)),
                raw_error: false,
                formatted_error: false,
            },
            FakeCounter {
                path: invalid_path.to_owned(),
                native_type: 0x2041_0500,
                raw: Some(10),
                multi_count: 1,
                formatted: Some(Number::Double(1.0)),
                raw_error: false,
                formatted_error: false,
            },
        ],
    );

    let sample = query.collect().unwrap();
    assert_eq!(sample.points.len(), 1);
    assert_eq!(sample.points[0].counter_index, 0);
    assert!(sample.metadata.failures.iter().any(|failure| {
        failure.counter_index == 1
            && failure.reason == "invalid_metric_mapping"
            && failure.error.contains("PERF_COUNTER_DELTA / LARGE_DELTA")
    }));
}

/// Scenario: A native delta count warms, emits, sees equal and rollback timestamps, and resumes.
/// Guarantees: Every emitted interval starts at the immediately preceding valid sample timestamp.
#[test]
fn delta_sum_tracks_previous_valid_sample_intervals() {
    let path = r"\Provider\Operations Delta";
    let (state, mut query) = fake_query(
        vec![delta_sum(path, "windows.operations", false)],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x0040_0500,
            raw: Some(10),
            multi_count: 1,
            formatted: Some(Number::Double(2.0)),
            raw_error: false,
            formatted_error: false,
        }],
    );

    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::Warming
    );
    {
        let mut state = state.borrow_mut();
        state.timestamp = 110;
        state.counters[0].raw = Some(20);
        state.counters[0].formatted = Some(Number::Double(3.0));
    }
    let sample = query.collect().unwrap();
    assert_eq!(sample.points[0].start_time_unix_nano, Some(100));
    assert_eq!(
        sample.points[0].value,
        SampleValue::Value(Number::Double(3.0))
    );

    {
        let mut state = state.borrow_mut();
        state.counters[0].raw = Some(30);
        state.counters[0].formatted = Some(Number::Double(4.0));
    }
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::NoObservation
    );

    {
        let mut state = state.borrow_mut();
        state.timestamp = 120;
        state.counters[0].raw = Some(40);
        state.counters[0].formatted = Some(Number::Double(5.0));
    }
    let sample = query.collect().unwrap();
    assert_eq!(sample.points[0].start_time_unix_nano, Some(110));
    assert_eq!(
        sample.points[0].value,
        SampleValue::Value(Number::Double(5.0))
    );

    {
        let mut state = state.borrow_mut();
        state.timestamp = 115;
        state.counters[0].raw = Some(50);
        state.counters[0].formatted = Some(Number::Double(6.0));
    }
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::NoObservation
    );

    {
        let mut state = state.borrow_mut();
        state.timestamp = 130;
        state.counters[0].raw = Some(60);
        state.counters[0].formatted = Some(Number::Double(7.0));
    }
    let sample = query.collect().unwrap();
    assert_eq!(sample.points[0].start_time_unix_nano, Some(115));
}

/// Scenario: A formatted transient interrupts a ready native delta count.
/// Guarantees: The interval baseline is invalidated and one valid sample re-warms before emission.
#[test]
fn delta_sum_transient_restarts_interval_readiness() {
    let path = r"\Provider\Operations Delta";
    let (state, mut query) = fake_query(
        vec![delta_sum(path, "windows.operations", false)],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x0040_0500,
            raw: Some(10),
            multi_count: 1,
            formatted: Some(Number::Double(2.0)),
            raw_error: false,
            formatted_error: false,
        }],
    );
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::Warming
    );

    {
        let mut state = state.borrow_mut();
        state.timestamp = 110;
        state.counters[0].raw = Some(20);
        state.counters[0].formatted = None;
    }
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::NoObservation
    );

    {
        let mut state = state.borrow_mut();
        state.timestamp = 120;
        state.counters[0].raw = Some(30);
        state.counters[0].formatted = Some(Number::Double(3.0));
    }
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::Warming
    );

    {
        let mut state = state.borrow_mut();
        state.timestamp = 130;
        state.counters[0].raw = Some(40);
        state.counters[0].formatted = Some(Number::Double(4.0));
    }
    let sample = query.collect().unwrap();
    assert_eq!(sample.points[0].start_time_unix_nano, Some(120));
}

/// Scenario: Query collection and timestamp failures interrupt a ready native delta count.
/// Guarantees: Each gap clears interval readiness so the next valid sample warms a fresh baseline.
#[test]
fn delta_sum_collection_gaps_restart_interval_readiness() {
    let path = r"\Provider\Operations Delta";
    let (state, mut query) = fake_query(
        vec![delta_sum(path, "windows.operations", false)],
        vec![FakeCounter {
            path: path.to_owned(),
            native_type: 0x0040_0500,
            raw: Some(10),
            multi_count: 1,
            formatted: Some(Number::Double(2.0)),
            raw_error: false,
            formatted_error: false,
        }],
    );
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::Warming
    );

    {
        let mut state = state.borrow_mut();
        state.collect_error = true;
    }
    assert!(query.collect().is_err());
    {
        let mut state = state.borrow_mut();
        state.collect_error = false;
        state.timestamp = 120;
        state.counters[0].raw = Some(20);
    }
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::Warming
    );

    {
        let mut state = state.borrow_mut();
        state.timestamp_error = true;
        state.counters[0].raw = Some(30);
    }
    assert!(query.collect().is_err());
    {
        let mut state = state.borrow_mut();
        state.timestamp_error = false;
        state.timestamp = 140;
        state.counters[0].raw = Some(40);
    }
    assert_eq!(
        query.collect().unwrap().points[0].value,
        SampleValue::Warming
    );

    {
        let mut state = state.borrow_mut();
        state.timestamp = 150;
        state.counters[0].raw = Some(50);
        state.counters[0].formatted = Some(Number::Double(5.0));
    }
    let sample = query.collect().unwrap();
    assert_eq!(sample.points[0].start_time_unix_nano, Some(140));
}

/// Scenario: Scaling would underflow a negative native delta difference to signed zero.
/// Guarantees: Monotonicity checks the native sign first; non-monotonic output preserves signed zero.
#[test]
fn delta_sum_negative_values_follow_monotonicity() {
    for monotonic in [false, true] {
        let path = r"\Provider\Operations Delta";
        let mut config = delta_sum(path, "windows.operations", monotonic);
        config.scale_power10 = -18;
        let (state, mut query) = fake_query(
            vec![config],
            vec![FakeCounter {
                path: path.to_owned(),
                native_type: 0x0040_0500,
                raw: Some(10),
                multi_count: 1,
                formatted: Some(Number::Double(1.0)),
                raw_error: false,
                formatted_error: false,
            }],
        );
        assert_eq!(
            query.collect().unwrap().points[0].value,
            SampleValue::Warming
        );

        {
            let mut state = state.borrow_mut();
            state.timestamp = 110;
            state.counters[0].raw = Some(20);
            state.counters[0].formatted = Some(Number::Double(-f64::MIN_POSITIVE));
        }
        let sample = query.collect().unwrap();
        if monotonic {
            assert_eq!(sample.points[0].value, SampleValue::NoObservation);
        } else {
            assert_eq!(sample.points[0].start_time_unix_nano, Some(100));
            let SampleValue::Value(Number::Double(value)) = sample.points[0].value else {
                panic!("expected a scaled non-monotonic delta value");
            };
            assert_eq!(value, 0.0);
            assert!(value.is_sign_negative());
        }

        {
            let mut state = state.borrow_mut();
            state.timestamp = 120;
            state.counters[0].raw = Some(30);
            state.counters[0].formatted = Some(Number::Double(3.0));
        }
        let sample = query.collect().unwrap();
        assert_eq!(sample.points[0].start_time_unix_nano, Some(110));
        assert!(matches!(
            sample.points[0].value,
            SampleValue::Value(Number::Double(value)) if value > 0.0
        ));
    }
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
        ..Default::default()
    };
    assert_eq!(
        monotonic_start_time("counter", &mut sequence, current_value, 110).unwrap(),
        110
    );
}
