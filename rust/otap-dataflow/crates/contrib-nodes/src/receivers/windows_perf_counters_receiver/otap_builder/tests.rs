// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::super::config::{Config, InstanceConfig};
use super::super::model::{InstanceIdentity, PendingMetadata, SampleFailure, SampleMember};
use super::*;
use arrow::array::{Array, Int64Array, UInt8Array};
use arrow::util::display::array_value_to_string;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{KeyValue, any_value};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::metric;
use otel_arrow_dfe_pdata::{OtlpProtoBytes, TryIntoWithOptions};
use otel_arrow_dfe_pdata_codec::OtapPayload;
use prost::Message;

pub(in super::super) fn into_otap(
    counters: &[CounterConfig],
    sample: Sample,
) -> Result<Option<OtapArrowRecords>, ArrowError> {
    project_sample(counters, &sample).map_err(|error| error.0)
}

fn counter(path: &str, name: &str, unit: &str) -> CounterConfig {
    CounterConfig {
        path: path.to_owned(),
        name: Arc::from(name),
        unit: Arc::from(unit),
        description: Arc::from(format!("Description for {name}.")),
        metric_kind: MetricKind::Gauge,
        attributes: Arc::new(BTreeMap::new()),
        instance_config: InstanceConfig::None,
        scale_power10: 0,
        source_mapping_index: 0,
    }
}

fn non_monotonic_sum(path: &str, name: &str, unit: &str) -> CounterConfig {
    CounterConfig {
        metric_kind: MetricKind::Sum,
        ..counter(path, name, unit)
    }
}

fn point(counter_index: usize, path: &str, value: SampleValue) -> SamplePoint {
    point_at(counter_index, path, 1, value)
}

fn point_at(
    counter_index: usize,
    path: &str,
    timestamp_unix_nano: i64,
    value: SampleValue,
) -> SamplePoint {
    SamplePoint {
        counter_index,
        path: Arc::from(path),
        instance: None,
        emit_instance_attribute: false,
        timestamp_unix_nano,
        start_time_unix_nano: None,
        value,
    }
}

fn string_attributes(records: &OtapArrowRecords) -> Vec<(String, String, String)> {
    let attrs = records.get(ArrowPayloadType::NumberDpAttrs).unwrap();
    let parent_ids = attrs.column_by_name("parent_id").unwrap();
    let keys = attrs.column_by_name("key").unwrap();
    let values = attrs.column_by_name("str").unwrap();
    (0..attrs.num_rows())
        .map(|index| {
            (
                array_value_to_string(parent_ids.as_ref(), index).unwrap(),
                array_value_to_string(keys.as_ref(), index).unwrap(),
                array_value_to_string(values.as_ref(), index).unwrap(),
            )
        })
        .collect()
}

fn decode(records: OtapArrowRecords) -> ExportMetricsServiceRequest {
    let bytes: OtlpProtoBytes = OtapPayload::from_otap(records)
        .try_into_with_default()
        .unwrap();
    ExportMetricsServiceRequest::decode(bytes.as_bytes()).unwrap()
}

fn decoded_attributes(attributes: &[KeyValue]) -> BTreeMap<&str, &str> {
    attributes
        .iter()
        .map(|attribute| {
            let Some(any_value::Value::StringValue(value)) = attribute
                .value
                .as_ref()
                .and_then(|value| value.value.as_ref())
            else {
                panic!("expected a string attribute");
            };
            (attribute.key.as_str(), value.as_str())
        })
        .collect()
}

fn sum_point(
    counter_index: usize,
    path: &str,
    start_time_unix_nano: i64,
    value: Number,
) -> SamplePoint {
    SamplePoint {
        timestamp_unix_nano: 10,
        start_time_unix_nano: Some(start_time_unix_nano),
        ..point(counter_index, path, SampleValue::Value(value))
    }
}

/// Scenario: An implicit path-named mapping matches a configured metric definition.
/// Guarantees: Decoded OTAP output uses the matching definition's name, description, unit, and kind.
#[test]
fn projects_implicit_path_named_metric_definition() {
    let config = Config::from_json(&serde_json::json!({
        "metrics": {
            r"\Memory\Available Bytes": {
                "description": "Available physical memory.",
                "unit": "By",
                "gauge": {}
            }
        },
        "perfcounters": [{
            "object": "Memory",
            "counters": [{"name": "Available Bytes"}]
        }]
    }))
    .unwrap();
    let records = into_otap(
        &config.counters,
        Sample {
            timestamp_unix_nano: 1,
            members: Vec::new(),
            points: vec![point(
                0,
                r"\Memory\Available Bytes",
                SampleValue::Value(Number::Integer(42)),
            )],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();

    let metrics = records.get(ArrowPayloadType::UnivariateMetrics).unwrap();
    assert_eq!(metrics.num_rows(), 1);
    assert_eq!(
        metrics
            .column_by_name("metric_type")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(0),
        MetricType::Gauge as u8
    );
    let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(metrics))
        .unwrap()
        .to_string();
    assert!(display.contains(r"\Memory\Available Bytes"));
    assert!(display.contains("Available physical memory."));
    assert!(display.contains("By"));
}

/// Scenario: An implicit path metric uses a configured instance index with leading zeros.
/// Guarantees: OTAP uses the configured metric name and preserves the same native path attribute spelling.
#[test]
fn projects_configured_instance_spelling_for_path_metric() {
    let configured_name = r"\Process(app#001)\Private Bytes";
    let native_path = configured_name;
    let config = Config::from_json(&serde_json::json!({
        "metrics": {
            configured_name: {
                "description": "Configured process memory.",
                "unit": "By",
                "gauge": {}
            }
        },
        "perfcounters": [{
            "object": "Process",
            "instances": "app#001",
            "counters": [{"name": "Private Bytes"}]
        }]
    }))
    .unwrap();
    let records = into_otap(
        &config.counters,
        Sample {
            timestamp_unix_nano: 1,
            members: Vec::new(),
            points: vec![point(
                0,
                native_path,
                SampleValue::Value(Number::Integer(42)),
            )],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();

    let metrics = records.get(ArrowPayloadType::UnivariateMetrics).unwrap();
    let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(metrics))
        .unwrap()
        .to_string();
    assert!(display.contains(configured_name));
    assert!(display.contains("Configured process memory."));
    assert!(display.contains("By"));
    assert!(
        string_attributes(&records)
            .iter()
            .any(|(_, key, value)| key == "windows.perf_counter.path" && value == native_path)
    );
}

/// Scenario: Explicit mappings use space-distinct, whitespace-only, control-character, and near-reserved static keys.
/// Guarantees: Decoded points preserve exact spelling and values, and a static instance attribute takes precedence over generated instance identity.
#[test]
fn projects_static_attribute_spelling_verbatim() {
    let expected = BTreeMap::from([
        ("state", "active"),
        ("State", "case-distinct"),
        (" state", " padded "),
        ("state ", ""),
        (" state ", "both"),
        (" \t\r\n\u{2003}", "whitespace"),
        ("control\0\t\n\u{7f}key", "control\0\t\n\u{7f}value"),
        (" windows.perf_counter.path", "not the native path"),
        ("Windows.perf_counter.path", "case-distinct prefix"),
        ("windows.perf_counter", "no trailing dot"),
        ("instance", "fixed"),
        (" instance ", "not an override"),
    ]);
    let config = Config::from_json(&serde_json::json!({
        "metrics": {"process.id": {"unit": "1"}},
        "perfcounters": [{
            "object": "Process",
            "instances": ["app", "worker"],
            "counters": [{
                "name": "ID Process",
                "metric": "process.id",
                "attributes": expected
            }]
        }]
    }))
    .unwrap();
    let points = ["app", "worker"]
        .into_iter()
        .enumerate()
        .map(|(index, instance)| SamplePoint {
            counter_index: index,
            path: Arc::from(config.counters[index].path.as_str()),
            instance: Some(Arc::new(InstanceIdentity {
                name: instance.to_owned(),
                parent: None,
                index: 0,
            })),
            emit_instance_attribute: true,
            timestamp_unix_nano: 10,
            start_time_unix_nano: None,
            value: SampleValue::Value(Number::Integer(42 + index as i64)),
        })
        .collect();
    let decoded = decode(
        into_otap(
            &config.counters,
            Sample {
                timestamp_unix_nano: 10,
                members: Vec::new(),
                points,
                metadata: Default::default(),
            },
        )
        .unwrap()
        .unwrap(),
    );
    let metrics = &decoded.resource_metrics[0].scope_metrics[0].metrics;
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].name, "process.id");
    let Some(metric::Data::Gauge(gauge)) = &metrics[0].data else {
        panic!("expected a Gauge");
    };
    assert_eq!(gauge.data_points.len(), 2);
    for (index, point) in gauge.data_points.iter().enumerate() {
        let mut expected = expected.clone();
        let _ = expected.insert(
            "windows.perf_counter.path",
            config.counters[index].path.as_str(),
        );
        let _ = expected.insert(
            "windows.perf_counter.instance",
            if index == 0 { "app" } else { "worker" },
        );
        let _ = expected.insert("windows.perf_counter.instance_index", "0");
        assert_eq!(point.attributes.len(), expected.len());
        assert_eq!(decoded_attributes(&point.attributes), expected);
    }
}

/// Scenario: An implicit configured path differs from a path-shaped definition only by case.
/// Guarantees: Exact lookup emits a default path-named Gauge instead of applying the mismatched definition.
#[test]
fn projects_case_distinct_path_name_fallback() {
    let configured_path = r"\memory\available bytes";
    let config = Config::from_json(&serde_json::json!({
        "metrics": {
            r"\Memory\Available Bytes": {
                "description": "Must not match.",
                "unit": "By",
                "sum": {"aggregation": "cumulative"}
            }
        },
        "perfcounters": [{
            "object": "memory",
            "counters": [{"name": "available bytes"}]
        }]
    }))
    .unwrap();
    let records = into_otap(
        &config.counters,
        Sample {
            timestamp_unix_nano: 1,
            members: Vec::new(),
            points: vec![point(
                0,
                configured_path,
                SampleValue::Value(Number::Integer(42)),
            )],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();

    let metrics = records.get(ArrowPayloadType::UnivariateMetrics).unwrap();
    assert_eq!(
        metrics
            .column_by_name("metric_type")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(0),
        MetricType::Gauge as u8
    );
    let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(metrics))
        .unwrap()
        .to_string();
    assert!(display.contains(configured_path));
    assert!(!display.contains("Must not match."));
    assert!(
        string_attributes(&records).iter().any(|(_, key, value)| {
            key == "windows.perf_counter.path" && value == configured_path
        })
    );
}

/// Scenario: Multiple configured readings include an integer beyond f64's exact range.
/// Guarantees: Gauge order, metadata, paths, timestamps, and exact i64 values are retained.
#[test]
fn projects_integer_gauges() {
    let timestamp = 1_788_500_000_123_456_789;
    let bytes = 9_007_199_254_740_993;
    let counters = [
        counter(r"\Memory\Available Bytes", "windows.memory.available", "By"),
        counter(r"\Memory\Committed Bytes", "windows.memory.committed", "By"),
    ];
    let records = into_otap(
        &counters,
        Sample {
            timestamp_unix_nano: timestamp,
            members: Vec::new(),
            points: vec![
                point_at(
                    0,
                    r"\Memory\Available Bytes",
                    timestamp,
                    SampleValue::Value(Number::Integer(bytes)),
                ),
                point_at(
                    1,
                    r"\Memory\Committed Bytes",
                    timestamp,
                    SampleValue::Value(Number::Integer(42)),
                ),
            ],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();
    let metrics = records.get(ArrowPayloadType::UnivariateMetrics).unwrap();
    let points = records.get(ArrowPayloadType::NumberDataPoints).unwrap();
    assert_eq!(metrics.num_rows(), 2);
    assert_eq!(points.num_rows(), 2);
    assert_eq!(
        metrics
            .column_by_name("metric_type")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(0),
        MetricType::Gauge as u8
    );
    assert_eq!(
        points
            .column_by_name("int_value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        bytes
    );
    assert!(
        points
            .column_by_name("double_value")
            .is_none_or(|column| column.is_null(0))
    );
    assert!(
        points
            .column_by_name("start_time_unix_nano")
            .is_none_or(|column| column.is_null(0))
    );
    // Pretty-printing the record batches also exercises the dictionary-aware Arrow display.
    let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(metrics))
        .unwrap()
        .to_string();
    assert!(display.contains("windows.memory.available"));
    assert!(display.contains("windows.memory.committed"));
    assert!(display.contains("Description for windows.memory.available."));
    assert!(display.contains("By"));
    let attrs = records.get(ArrowPayloadType::NumberDpAttrs).unwrap();
    let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(attrs))
        .unwrap()
        .to_string();
    assert!(display.contains(r"\Memory\Available Bytes"));
    assert!(display.contains(r"\Memory\Committed Bytes"));
    assert!(display.contains("windows.perf_counter.path"));
    let time = points
        .column_by_name("time_unix_nano")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(time.value(0), timestamp);
}

/// Scenario: A cumulative point has an observation time earlier than its containing sample.
/// Guarantees: OTAP preserves the point's own start and observation timestamps.
#[test]
fn projects_sum_with_its_observation_timestamp() {
    let counter = non_monotonic_sum(
        r"\Provider\Operations Delta",
        "provider.operations",
        "{operation}",
    );
    let mut point = sum_point(0, r"\Provider\Operations Delta", 100, Number::Double(2.0));
    point.timestamp_unix_nano = 110;
    let records = into_otap(
        &[counter],
        Sample {
            timestamp_unix_nano: 115,
            members: Vec::new(),
            points: vec![point],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();
    let points = records.get(ArrowPayloadType::NumberDataPoints).unwrap();
    let start = points
        .column_by_name("start_time_unix_nano")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .unwrap();
    let end = points
        .column_by_name("time_unix_nano")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .unwrap();

    assert_eq!(start.value(0), 100);
    assert_eq!(end.value(0), 110);
}

/// Scenario: Process memory is configured as a non-monotonic Sum with a current integer value.
/// Guarantees: OTAP emits a cumulative non-monotonic Sum and preserves the query start time.
#[test]
fn projects_integer_non_monotonic_sum() {
    let mut counter = non_monotonic_sum(
        r"\Process(*)\Private Bytes",
        "windows.process.private",
        "By",
    );
    counter.description = Arc::from("Committed private memory for each process instance.");
    let counters = [counter];
    let records = into_otap(
        &counters,
        Sample {
            timestamp_unix_nano: 10,
            members: Vec::new(),
            points: vec![sum_point(
                0,
                r"\Process(worker)\Private Bytes",
                5,
                Number::Integer(42),
            )],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();
    let metrics = records.get(ArrowPayloadType::UnivariateMetrics).unwrap();
    assert_eq!(
        metrics
            .column_by_name("metric_type")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(0),
        MetricType::Sum as u8
    );
    assert_eq!(
        array_value_to_string(
            metrics
                .column_by_name("aggregation_temporality")
                .unwrap()
                .as_ref(),
            0
        )
        .unwrap(),
        AGGREGATION_TEMPORALITY_CUMULATIVE.to_string()
    );
    assert!(metrics.column_by_name("is_monotonic").is_none());
    let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(metrics))
        .unwrap()
        .to_string();
    assert!(display.contains("Committed private memory for each process instance."));
    let points = records.get(ArrowPayloadType::NumberDataPoints).unwrap();
    assert_eq!(
        points
            .column_by_name("start_time_unix_nano")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap()
            .value(0),
        5
    );
}

/// Scenario: A ready Sum point reaches projection without its required cumulative or delta interval start.
/// Guarantees: Projection reports an internal invariant error instead of emitting a Sum with an absent start time.
#[test]
fn rejects_sum_without_start_time() {
    let counters = [non_monotonic_sum(
        r"\Process(*)\Private Bytes",
        "windows.process.private",
        "By",
    )];
    let error = into_otap(
        &counters,
        Sample {
            timestamp_unix_nano: 10,
            members: Vec::new(),
            points: vec![point(
                0,
                r"\Process(worker)\Private Bytes",
                SampleValue::Value(Number::Integer(42)),
            )],
            metadata: Default::default(),
        },
    )
    .unwrap_err();

    assert!(matches!(
        error,
        ArrowError::InvalidArgumentError(message)
            if message == "Sum windows.process.private (\\Process(worker)\\Private Bytes) has no start time"
    ));
}

/// Scenario: A ready cumulative Sum carries a non-positive start or a start after its observation.
/// Guarantees: Invalid starts fail projection explicitly instead of producing malformed Sum points.
#[test]
fn rejects_sum_with_invalid_start_time() {
    let counters = [non_monotonic_sum(
        r"\Memory\Available Bytes",
        "memory",
        "By",
    )];
    for start in [-1, 0, 11] {
        let mut observation = point(
            0,
            r"\Memory\Available Bytes",
            SampleValue::Value(Number::Integer(42)),
        );
        observation.timestamp_unix_nano = 10;
        observation.start_time_unix_nano = Some(start);
        let error = into_otap(
            &counters,
            Sample {
                timestamp_unix_nano: 10,
                members: Vec::new(),
                points: vec![observation],
                metadata: Default::default(),
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("0 < start <= observation time"));
    }
}

/// Scenario: A Gauge sample carries an unused start timestamp later than its collection timestamp.
/// Guarantees: Gauge projection ignores cumulative start-time state and preserves existing output.
#[test]
fn gauge_ignores_irrelevant_start_time() {
    let counters = [counter(
        r"\Memory\Available Bytes",
        "windows.memory.available",
        "By",
    )];
    let mut observation = point(
        0,
        r"\Memory\Available Bytes",
        SampleValue::Value(Number::Integer(42)),
    );
    observation.start_time_unix_nano = Some(2);
    assert!(
        into_otap(
            &counters,
            Sample {
                timestamp_unix_nano: 1,
                members: Vec::new(),
                points: vec![observation],
                metadata: Default::default(),
            }
        )
        .unwrap()
        .is_some()
    );
}

/// Scenario: The immediate first scrape has a ready direct gauge and a warming calculated gauge.
/// Guarantees: The first batch contains only the direct gauge, without delaying or zero-filling it.
#[test]
fn first_scrape_emits_ready_direct_gauge() {
    let counters = [
        counter(r"\Memory\Available Bytes", "windows.memory.available", "By"),
        counter(
            r"\Processor(_Total)\% Processor Time",
            "windows.processor.time",
            "%",
        ),
    ];
    let records = into_otap(
        &counters,
        Sample {
            timestamp_unix_nano: 1,
            members: Vec::new(),
            points: vec![
                point(
                    0,
                    r"\Memory\Available Bytes",
                    SampleValue::Value(Number::Integer(42)),
                ),
                point(
                    1,
                    r"\Processor(_Total)\% Processor Time",
                    SampleValue::Warming,
                ),
            ],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();
    let metrics = records.get(ArrowPayloadType::UnivariateMetrics).unwrap();
    let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(metrics))
        .unwrap()
        .to_string();
    assert_eq!(metrics.num_rows(), 1);
    assert!(display.contains("windows.memory.available"));
    assert!(!display.contains("windows.processor.time"));
}

/// Scenario: Every configured calculated counter is still warming.
/// Guarantees: The receiver can skip the scrape without constructing an empty metric batch.
#[test]
fn suppresses_all_warming_scrape() {
    let counters = [counter(
        r"\Processor(_Total)\% Processor Time",
        "windows.processor.time",
        "%",
    )];
    assert!(
        into_otap(
            &counters,
            Sample {
                timestamp_unix_nano: 1,
                members: Vec::new(),
                points: vec![point(
                    0,
                    r"\Processor(_Total)\% Processor Time",
                    SampleValue::Warming,
                )],
                metadata: Default::default(),
            }
        )
        .unwrap()
        .is_none()
    );
}

/// Scenario: An idle average has no observations while Memory and CPU have ready values.
/// Guarantees: Only the idle average is omitted and ready gauges remain in the batch.
#[test]
fn omits_idle_average_without_dropping_ready_values() {
    let counters = [
        counter(r"\Memory\Available Bytes", "windows.memory.available", "By"),
        counter(
            r"\Processor(_Total)\% Processor Time",
            "windows.processor.time",
            "%",
        ),
        counter(
            r"\PhysicalDisk(_Total)\Avg. Disk Bytes/Read",
            "windows.disk.read.average",
            "By",
        ),
    ];
    let records = into_otap(
        &counters,
        Sample {
            timestamp_unix_nano: 1,
            members: Vec::new(),
            points: vec![
                point(
                    0,
                    r"\Memory\Available Bytes",
                    SampleValue::Value(Number::Integer(42)),
                ),
                point(
                    1,
                    r"\Processor(_Total)\% Processor Time",
                    SampleValue::Value(Number::Double(12.5)),
                ),
                point(
                    2,
                    r"\PhysicalDisk(_Total)\Avg. Disk Bytes/Read",
                    SampleValue::NoObservation,
                ),
            ],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();
    let metrics = records.get(ArrowPayloadType::UnivariateMetrics).unwrap();
    let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(metrics))
        .unwrap()
        .to_string();
    assert_eq!(metrics.num_rows(), 2);
    assert!(display.contains("windows.memory.available"));
    assert!(display.contains("windows.processor.time"));
    assert!(!display.contains("windows.disk.read.average"));
}

/// Scenario: A disappeared wildcard instance fails while an exact Memory point remains valid.
/// Guarantees: Counter-local failure metadata does not suppress projection of healthy points.
#[test]
fn projects_healthy_points_when_a_peer_failed() {
    let counters = [
        counter(r"\Memory\Available Bytes", "windows.memory.available", "By"),
        counter(
            r"\Process(*)\Private Bytes",
            "windows.process.private",
            "By",
        ),
    ];
    let records = into_otap(
        &counters,
        Sample {
            timestamp_unix_nano: 1,
            members: Vec::new(),
            points: vec![point(
                0,
                r"\Memory\Available Bytes",
                SampleValue::Value(Number::Integer(42)),
            )],
            metadata: PendingMetadata {
                failures: vec![SampleFailure {
                    counter_index: 1,
                    path: Arc::from(r"\Memory\Committed Bytes"),
                    reason: "PdhGetFormattedCounterValue",
                    error: "PDH_INVALID_DATA".to_owned(),
                    diagnostic_only: false,
                }],
                ..Default::default()
            },
        },
    )
    .unwrap()
    .unwrap();
    let metrics = records.get(ArrowPayloadType::UnivariateMetrics).unwrap();
    let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(metrics))
        .unwrap()
        .to_string();
    assert_eq!(metrics.num_rows(), 1);
    assert!(display.contains("windows.memory.available"));
    assert!(!display.contains("windows.process.private"));
}

/// Scenario: A calculated counter produces a finite floating-point value.
/// Guarantees: OTAP uses the double column and does not synthesize an integer value.
#[test]
fn projects_calculated_double_gauge() {
    let counters = [counter(
        r"\Processor(_Total)\% Processor Time",
        "windows.processor.time",
        "%",
    )];
    let records = into_otap(
        &counters,
        Sample {
            timestamp_unix_nano: 1,
            members: Vec::new(),
            points: vec![point(
                0,
                r"\Processor(_Total)\% Processor Time",
                SampleValue::Value(Number::Double(12.5)),
            )],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();
    let points = records.get(ArrowPayloadType::NumberDataPoints).unwrap();
    let doubles = points
        .column_by_name("double_value")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(doubles.value(0), 12.5);
    assert!(
        points
            .column_by_name("int_value")
            .is_none_or(|column| column.is_null(0))
    );
}

/// Scenario: A sample references a configured-counter index that does not exist.
/// Guarantees: Internal contract corruption cannot masquerade as recoverable point loss.
#[test]
fn rejects_missing_counter_index_as_invariant() {
    let counters = [counter(
        r"\Memory\Available Bytes",
        "windows.memory.available",
        "By",
    )];
    let error = project_sample(
        &counters,
        &Sample {
            timestamp_unix_nano: 1,
            members: Vec::new(),
            points: vec![point(
                1,
                r"\Memory\Available Bytes",
                SampleValue::Value(Number::Integer(42)),
            )],
            metadata: Default::default(),
        },
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "Invalid argument error: sample references missing configured counter 1"
    );
}

/// Scenario: A point references a concrete source absent from the worker's admitted membership snapshot.
/// Guarantees: Corrupt membership cannot silently masquerade as a suppressed output collision.
#[test]
fn rejects_point_absent_from_admitted_membership() {
    let counters = [counter(
        r"\Process(*)\Private Bytes",
        "process.private",
        "By",
    )];
    let error = project_sample(
        &counters,
        &Sample {
            timestamp_unix_nano: 1,
            members: vec![SampleMember {
                counter_index: 0,
                path: Arc::from(r"\Process(owner)\Private Bytes"),
                instance: None,
                emit_instance_attribute: false,
            }],
            points: vec![point(
                0,
                r"\Process(untracked)\Private Bytes",
                SampleValue::Value(Number::Integer(42)),
            )],
            metadata: Default::default(),
        },
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        r"Invalid argument error: sample point 0 (\Process(untracked)\Private Bytes) is absent from admitted membership"
    );
}

/// Scenario: Two returned instances map to one configured pure question-mark wildcard metric.
/// Guarantees: Output uses concrete returned names and duplicate identities rather than the configured pattern.
#[test]
fn projects_wildcard_instance_identity() {
    let mut wildcard = counter(
        r"\Process(svc?ost)\Private Bytes",
        "windows.process.private",
        "By",
    );
    wildcard.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    let counters = [wildcard];
    let records = into_otap(
        &counters,
        Sample {
            timestamp_unix_nano: 1,
            members: Vec::new(),
            points: vec![
                SamplePoint {
                    counter_index: 0,
                    path: Arc::from(r"\Process(svchost)\Private Bytes"),
                    instance: Some(Arc::new(InstanceIdentity {
                        name: "svchost".to_owned(),
                        parent: None,
                        index: 0,
                    })),
                    emit_instance_attribute: true,
                    timestamp_unix_nano: 1,
                    start_time_unix_nano: None,
                    value: SampleValue::Value(Number::Integer(10)),
                },
                SamplePoint {
                    counter_index: 0,
                    path: Arc::from(r"\Process(svchost#1)\Private Bytes"),
                    instance: Some(Arc::new(InstanceIdentity {
                        name: "svchost".to_owned(),
                        parent: Some("service".to_owned()),
                        index: 1,
                    })),
                    emit_instance_attribute: true,
                    timestamp_unix_nano: 1,
                    start_time_unix_nano: None,
                    value: SampleValue::Value(Number::Integer(20)),
                },
            ],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();
    let metrics = records.get(ArrowPayloadType::UnivariateMetrics).unwrap();
    assert_eq!(metrics.num_rows(), 1);
    let points = records.get(ArrowPayloadType::NumberDataPoints).unwrap();
    assert_eq!(points.num_rows(), 2);
    let attrs = records.get(ArrowPayloadType::NumberDpAttrs).unwrap();
    let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(attrs))
        .unwrap()
        .to_string();
    assert!(display.contains(r"\Process(svchost)\Private Bytes"));
    assert!(display.contains(r"\Process(svchost#1)\Private Bytes"));
    assert!(display.contains(r"\Process(svc?ost)\Private Bytes"));
    assert!(display.contains("windows.perf_counter.instance"));
    assert!(display.contains("windows.perf_counter.instance_index"));
    assert!(display.contains("windows.perf_counter.parent_instance"));
    assert!(display.contains("svchost"));
    assert!(display.contains("service"));
    assert_eq!(
        string_attributes(&records)
            .into_iter()
            .filter(|(_, key, _)| key == "instance")
            .collect::<Vec<_>>(),
        [
            ("0".to_owned(), "instance".to_owned(), "svchost".to_owned()),
            (
                "1".to_owned(),
                "instance".to_owned(),
                "svchost#1".to_owned()
            )
        ]
    );
}

/// Scenario: An aggregate is collected alone or explicitly included with other instances.
/// Guarantees: Go's `instance` attribute is omitted only for the aggregate-only mapping.
#[test]
fn projects_aggregate_instance_selection() {
    let mut aggregate_alone = counter(r"\Processor(_Total)\Counter", "aggregate.alone", "1");
    aggregate_alone.instance_config = InstanceConfig::Exact {
        emit_attribute: false,
    };
    let mut explicitly_included = counter(r"\Processor(*)\Counter", "aggregate.included", "1");
    explicitly_included.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    let aggregate = Arc::new(InstanceIdentity {
        name: "_Total".to_owned(),
        parent: None,
        index: 0,
    });
    let records = into_otap(
        &[aggregate_alone, explicitly_included],
        Sample {
            timestamp_unix_nano: 1,
            members: Vec::new(),
            points: vec![
                SamplePoint {
                    counter_index: 0,
                    path: Arc::from(r"\Processor(_Total)\Counter"),
                    instance: Some(aggregate.clone()),
                    emit_instance_attribute: false,
                    timestamp_unix_nano: 1,
                    start_time_unix_nano: None,
                    value: SampleValue::Value(Number::Integer(10)),
                },
                SamplePoint {
                    counter_index: 1,
                    path: Arc::from(r"\Processor(_Total)\Counter"),
                    instance: Some(aggregate),
                    emit_instance_attribute: true,
                    timestamp_unix_nano: 1,
                    start_time_unix_nano: None,
                    value: SampleValue::Value(Number::Integer(20)),
                },
            ],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();

    let attrs = string_attributes(&records);
    assert_eq!(
        attrs
            .iter()
            .filter(|(_, key, _)| key == "instance")
            .cloned()
            .collect::<Vec<_>>(),
        [("1".to_owned(), "instance".to_owned(), "_Total".to_owned())]
    );
    assert_eq!(
        attrs
            .iter()
            .filter(|(_, key, _)| key == "windows.perf_counter.instance")
            .count(),
        2
    );
}

/// Scenario: Two native instances in one mapping share a fixed user `instance` attribute.
/// Guarantees: Both decoded points retain the static override and distinct native path/instance provenance without suppression.
#[test]
fn static_instance_override_preserves_both_native_sources() {
    let mut wildcard = counter(r"\Process(*)\Private Bytes", "process.private", "By");
    wildcard.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    wildcard.attributes = Arc::new(BTreeMap::from([(
        "instance".to_owned(),
        "fixed".to_owned(),
    )]));
    let outcome = project_sample(
        &[wildcard],
        &Sample {
            timestamp_unix_nano: 1,
            members: ["worker", "service"]
                .into_iter()
                .map(|name| SampleMember {
                    counter_index: 0,
                    path: Arc::from(format!(r"\Process({name})\Private Bytes")),
                    instance: Some(Arc::new(InstanceIdentity {
                        name: name.to_owned(),
                        parent: None,
                        index: 0,
                    })),
                    emit_instance_attribute: true,
                })
                .collect(),
            points: vec![
                SamplePoint {
                    counter_index: 0,
                    path: Arc::from(r"\Process(worker)\Private Bytes"),
                    instance: Some(Arc::new(InstanceIdentity {
                        name: "worker".to_owned(),
                        parent: None,
                        index: 0,
                    })),
                    emit_instance_attribute: true,
                    timestamp_unix_nano: 1,
                    start_time_unix_nano: None,
                    value: SampleValue::Value(Number::Integer(10)),
                },
                SamplePoint {
                    counter_index: 0,
                    path: Arc::from(r"\Process(service)\Private Bytes"),
                    instance: Some(Arc::new(InstanceIdentity {
                        name: "service".to_owned(),
                        parent: None,
                        index: 0,
                    })),
                    emit_instance_attribute: true,
                    timestamp_unix_nano: 1,
                    start_time_unix_nano: None,
                    value: SampleValue::Value(Number::Integer(20)),
                },
            ],
            metadata: Default::default(),
        },
    )
    .unwrap();
    let request = decode(outcome.unwrap());
    let metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];
    let Some(metric::Data::Gauge(gauge)) = &metric.data else {
        panic!("expected Gauge");
    };
    assert_eq!(gauge.data_points.len(), 2);
    for (point, name) in gauge.data_points.iter().zip(["worker", "service"]) {
        let attrs = decoded_attributes(&point.attributes);
        assert_eq!(attrs["instance"], "fixed");
        assert_eq!(attrs["windows.perf_counter.instance"], name);
        assert_eq!(
            attrs["windows.perf_counter.path"],
            format!(r"\Process({name})\Private Bytes")
        );
        assert_eq!(
            attrs["windows.perf_counter.path_template"],
            r"\Process(*)\Private Bytes"
        );
    }
}

/// Scenario: Distinct configured patterns overlap one concrete path while the first mapping initially warms.
/// Guarantees: Decoded output publishes each valid mapping independently and preserves both template-qualified points when ready.
#[test]
fn overlapping_patterns_publish_each_valid_mapping() {
    let concrete = r"\Process(svchost)\Private Bytes";
    let instance = Arc::new(InstanceIdentity {
        name: "svchost".to_owned(),
        parent: None,
        index: 0,
    });
    let mut first = counter(r"\Process(svc*)\Private Bytes", "process.private", "By");
    first.source_mapping_index = 0;
    first.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    let mut second = counter(r"\Process(*host)\Private Bytes", "process.private", "By");
    second.source_mapping_index = 1;
    second.instance_config = InstanceConfig::Wildcard {
        excluded_aggregation: None,
    };
    let members = [0, 1]
        .into_iter()
        .map(|counter_index| SampleMember {
            counter_index,
            path: Arc::from(concrete),
            instance: Some(Arc::clone(&instance)),
            emit_instance_attribute: true,
        })
        .collect();
    let mut sample = Sample {
        timestamp_unix_nano: 1,
        members,
        points: vec![
            SamplePoint {
                counter_index: 0,
                path: Arc::from(concrete),
                instance: Some(Arc::clone(&instance)),
                emit_instance_attribute: true,
                timestamp_unix_nano: 1,
                start_time_unix_nano: None,
                value: SampleValue::Warming,
            },
            SamplePoint {
                counter_index: 1,
                path: Arc::from(concrete),
                instance: Some(instance),
                emit_instance_attribute: true,
                timestamp_unix_nano: 1,
                start_time_unix_nano: None,
                value: SampleValue::Value(Number::Integer(20)),
            },
        ],
        metadata: Default::default(),
    };

    let first_request = decode(
        project_sample(&[first.clone(), second.clone()], &sample)
            .unwrap()
            .unwrap(),
    );
    let Some(metric::Data::Gauge(gauge)) =
        &first_request.resource_metrics[0].scope_metrics[0].metrics[0].data
    else {
        panic!("expected Gauge");
    };
    assert_eq!(gauge.data_points.len(), 1);
    assert_eq!(
        decoded_attributes(&gauge.data_points[0].attributes)["windows.perf_counter.path_template"],
        second.path
    );

    sample.points[0].value = SampleValue::Value(Number::Integer(10));
    let recovered = project_sample(&[first, second], &sample).unwrap();
    let request = decode(recovered.unwrap());
    let Some(metric::Data::Gauge(gauge)) =
        &request.resource_metrics[0].scope_metrics[0].metrics[0].data
    else {
        panic!("expected Gauge");
    };
    assert_eq!(gauge.data_points.len(), 2);
    for (point, template) in gauge.data_points.iter().zip([
        r"\Process(svc*)\Private Bytes",
        r"\Process(*host)\Private Bytes",
    ]) {
        let attrs = decoded_attributes(&point.attributes);
        assert_eq!(attrs["windows.perf_counter.path_template"], template);
        assert_eq!(attrs["windows.perf_counter.path"], concrete);
        assert_eq!(attrs["instance"], "svchost");
    }
}

/// Scenario: Two different exact native paths feed one metric without static disambiguation.
/// Guarantees: Both decoded Gauge points survive under their retained concrete-path attributes.
#[test]
fn distinct_exact_paths_publish_both_points_of_shared_metric() {
    let config = Config::from_json(&serde_json::json!({
        "metrics": {"memory.value": {"unit": "By", "gauge": {}}},
        "perfcounters": [{"object": "Memory", "counters": [
            {"name": "Available Bytes", "metric": "memory.value"},
            {"name": "Committed Bytes", "metric": "memory.value"}
        ]}]
    }))
    .unwrap();
    let points = config
        .counters
        .iter()
        .enumerate()
        .map(|(index, counter)| {
            point(
                index,
                &counter.path,
                SampleValue::Value(Number::Integer(10 + index as i64)),
            )
        })
        .collect::<Vec<_>>();
    let members = points
        .iter()
        .map(|point| SampleMember {
            counter_index: point.counter_index,
            path: Arc::clone(&point.path),
            instance: None,
            emit_instance_attribute: false,
        })
        .collect();
    let request = decode(
        into_otap(
            &config.counters,
            Sample {
                timestamp_unix_nano: 1,
                members,
                points,
                metadata: Default::default(),
            },
        )
        .unwrap()
        .unwrap(),
    );
    let metrics = &request.resource_metrics[0].scope_metrics[0].metrics;
    assert_eq!(metrics.len(), 1);
    let Some(metric::Data::Gauge(gauge)) = &metrics[0].data else {
        panic!("expected Gauge");
    };
    assert_eq!(gauge.data_points.len(), 2);
    let paths = gauge
        .data_points
        .iter()
        .map(|point| decoded_attributes(&point.attributes)["windows.perf_counter.path"])
        .collect::<BTreeSet<_>>();
    assert_eq!(
        paths,
        BTreeSet::from([r"\Memory\Available Bytes", r"\Memory\Committed Bytes"])
    );
}

/// Scenario: Two counters map to one metric and use custom attributes to distinguish their points.
/// Guarantees: OTAP emits one metric row and preserves both attribute-qualified gauge points.
#[test]
fn projects_shared_metric_with_custom_attributes() {
    let mut active = counter(
        r"\Processor(_Total)\% Processor Time",
        "windows.processor.time",
        "%",
    );
    active.attributes = Arc::new(BTreeMap::from([("state".to_owned(), "active".to_owned())]));
    let mut idle = counter(
        r"\Processor(_Total)\% Idle Time",
        "windows.processor.time",
        "%",
    );
    idle.attributes = Arc::new(BTreeMap::from([("state".to_owned(), "idle".to_owned())]));
    let records = into_otap(
        &[active, idle],
        Sample {
            timestamp_unix_nano: 1,
            members: Vec::new(),
            points: vec![
                point(
                    0,
                    r"\Processor(_Total)\% Processor Time",
                    SampleValue::Value(Number::Double(60.0)),
                ),
                point(
                    1,
                    r"\Processor(_Total)\% Idle Time",
                    SampleValue::Value(Number::Double(40.0)),
                ),
            ],
            metadata: Default::default(),
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        records
            .get(ArrowPayloadType::UnivariateMetrics)
            .unwrap()
            .num_rows(),
        1
    );
    let attrs = records.get(ArrowPayloadType::NumberDpAttrs).unwrap();
    let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(attrs))
        .unwrap()
        .to_string();
    assert!(display.contains("state"));
    assert!(display.contains("active"));
    assert!(display.contains("idle"));
}
