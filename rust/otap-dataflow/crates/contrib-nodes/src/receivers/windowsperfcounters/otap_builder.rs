// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
use super::SamplePoint;
use super::{CounterConfig, Number, Sample, SampleValue};
use arrow::error::ArrowError;
use otel_arrow_dfe_pdata::encode::record::attributes::StrKeysAttributesRecordBatchBuilder;
use otel_arrow_dfe_pdata::encode::record::metrics::{
    MetricsRecordBatchBuilder, NumberDataPointsRecordBatchBuilder,
};
use otel_arrow_dfe_pdata::otap::{Metrics, OtapArrowRecords};
use otel_arrow_dfe_pdata::otlp::metrics::MetricType;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use std::collections::BTreeMap;

/// Project the ready values from one scrape directly into OTAP gauges.
pub fn into_otap(
    counters: &[CounterConfig],
    sample: Sample,
) -> Result<Option<OtapArrowRecords>, ArrowError> {
    if sample.timestamp_unix_nano <= 0 {
        return Err(ArrowError::InvalidArgumentError(
            "performance-counter sample requires a positive Unix timestamp".to_owned(),
        ));
    }
    let mut metrics = MetricsRecordBatchBuilder::new();
    let mut points = NumberDataPointsRecordBatchBuilder::new();
    let mut attrs = StrKeysAttributesRecordBatchBuilder::<u32>::new();
    let mut metric_ids = BTreeMap::new();
    let ready = sample
        .points
        .into_iter()
        .filter_map(|point| match point.value {
            SampleValue::Value(value) => Some((point, value)),
            SampleValue::Warming | SampleValue::NoObservation => None,
        });
    let mut point_count = 0;
    for (index, (point, value)) in ready.enumerate() {
        let counter = counters.get(point.counter_index).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "sample references missing configured counter {}",
                point.counter_index
            ))
        })?;
        let metric_id = if let Some(metric_id) = metric_ids.get(&counter.name) {
            *metric_id
        } else {
            let metric_id = u16::try_from(metric_ids.len()).map_err(|_| {
                ArrowError::InvalidArgumentError("too many configured metrics".to_owned())
            })?;
            metrics.append_id(metric_id);
            metrics.append_metric_type(MetricType::Gauge as u8);
            metrics.append_name(counter.name.as_bytes());
            metrics.append_description(counter.description.as_bytes());
            metrics.append_unit(counter.unit.as_bytes());
            metrics.append_aggregation_temporality(None);
            metrics.append_is_monotonic(None);
            let _ = metric_ids.insert(counter.name.clone(), metric_id);
            metric_id
        };
        let point_id = u32::try_from(index)
            .map_err(|_| ArrowError::InvalidArgumentError("too many counter values".to_owned()))?;

        points.append_id(point_id);
        points.append_parent_id(metric_id);
        points.append_start_time_unix_nano(None);
        points.append_time_unix_nano(sample.timestamp_unix_nano);
        match value {
            Number::Integer(value) => {
                points.append_int_value(Some(value));
                points.append_double_value(None);
            }
            Number::Double(value) if value.is_finite() => {
                points.append_int_value(None);
                points.append_double_value(Some(value));
            }
            Number::Double(_) => {
                return Err(ArrowError::InvalidArgumentError(
                    "performance-counter double value must be finite".to_owned(),
                ));
            }
        }
        points.append_flags(0);

        for (key, value) in &counter.attributes {
            attrs.append_parent_id(&point_id);
            attrs.append_key(key);
            attrs.any_values_builder.append_str(value.as_bytes());
        }
        attrs.append_parent_id(&point_id);
        attrs.append_key("windows.perf_counter.path");
        attrs.any_values_builder.append_str(point.path.as_bytes());
        if point.path != counter.path {
            attrs.append_parent_id(&point_id);
            attrs.append_key("windows.perf_counter.path_template");
            attrs.any_values_builder.append_str(counter.path.as_bytes());
        }
        if let Some(instance) = point.instance {
            attrs.append_parent_id(&point_id);
            attrs.append_key("windows.perf_counter.instance");
            attrs
                .any_values_builder
                .append_str(instance.name.as_bytes());
            if let Some(parent) = instance.parent {
                attrs.append_parent_id(&point_id);
                attrs.append_key("windows.perf_counter.parent_instance");
                attrs.any_values_builder.append_str(parent.as_bytes());
            }
            attrs.append_parent_id(&point_id);
            attrs.append_key("windows.perf_counter.instance_index");
            attrs
                .any_values_builder
                .append_str(instance.index.to_string().as_bytes());
        }
        point_count += 1;
    }
    if point_count == 0 {
        return Ok(None);
    }
    let metric_count = metric_ids.len();
    metrics.resource.append_id_n(0, metric_count);
    metrics.resource.append_schema_url_n(None, metric_count);
    metrics
        .resource
        .append_dropped_attributes_count_n(0, metric_count);
    metrics.scope.append_id_n(0, metric_count);
    metrics.scope.append_name_n(
        Some(b"otel-arrow-dfe-contrib-nodes/windowsperfcounters"),
        metric_count,
    );
    metrics
        .scope
        .append_version_n(Some(env!("CARGO_PKG_VERSION").as_bytes()), metric_count);
    metrics
        .scope
        .append_dropped_attributes_count_n(0, metric_count);
    metrics.append_scope_schema_url_n(b"", metric_count);

    let mut resource = StrKeysAttributesRecordBatchBuilder::<u16>::new();
    resource.append_parent_id(&0);
    resource.append_key("os.type");
    resource.any_values_builder.append_str(b"windows");

    let mut records = OtapArrowRecords::Metrics(Metrics::default());
    for (kind, batch) in [
        (ArrowPayloadType::UnivariateMetrics, metrics.finish()?),
        (ArrowPayloadType::NumberDataPoints, points.finish()?),
        (ArrowPayloadType::NumberDpAttrs, attrs.finish()?),
        (ArrowPayloadType::ResourceAttrs, resource.finish()?),
    ] {
        records
            .set(kind, batch)
            .map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
    }
    Ok(Some(records))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, UInt8Array};

    fn counter(path: &str, name: &str, unit: &str) -> CounterConfig {
        CounterConfig {
            path: path.to_owned(),
            name: name.to_owned(),
            unit: unit.to_owned(),
            description: format!("Description for {name}."),
            attributes: BTreeMap::new(),
            scale_power10: 0,
        }
    }

    fn point(counter_index: usize, path: &str, value: SampleValue) -> SamplePoint {
        SamplePoint {
            counter_index,
            path: path.to_owned(),
            instance: None,
            value,
        }
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
                points: vec![
                    point(
                        0,
                        r"\Memory\Available Bytes",
                        SampleValue::Value(Number::Integer(bytes)),
                    ),
                    point(
                        1,
                        r"\Memory\Committed Bytes",
                        SampleValue::Value(Number::Integer(42)),
                    ),
                ],
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
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
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
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
                    points: vec![point(
                        0,
                        r"\Processor(_Total)\% Processor Time",
                        SampleValue::Warming,
                    )],
                    failures: Vec::new(),
                    overflows: Vec::new(),
                    diagnostics: Default::default(),
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
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
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
                points: vec![point(
                    0,
                    r"\Memory\Available Bytes",
                    SampleValue::Value(Number::Integer(42)),
                )],
                failures: vec![super::super::SampleFailure {
                    counter_index: 1,
                    reason: "PdhGetFormattedCounterValue",
                    error: "PDH_INVALID_DATA".to_owned(),
                }],
                overflows: Vec::new(),
                diagnostics: Default::default(),
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
                points: vec![point(
                    0,
                    r"\Processor(_Total)\% Processor Time",
                    SampleValue::Value(Number::Double(12.5)),
                )],
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
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

    /// Scenario: A sample has an invalid timestamp or references a missing configured counter.
    /// Guarantees: Misidentified or untimed values never become plausible output gauges.
    #[test]
    fn rejects_invalid_sample() {
        let counters = [counter(
            r"\Memory\Available Bytes",
            "windows.memory.available",
            "By",
        )];
        assert!(
            into_otap(
                &counters,
                Sample {
                    timestamp_unix_nano: 1,
                    points: vec![point(
                        1,
                        r"\Memory\Available Bytes",
                        SampleValue::Value(Number::Integer(1)),
                    )],
                    failures: Vec::new(),
                    overflows: Vec::new(),
                    diagnostics: Default::default(),
                }
            )
            .is_err()
        );
        assert!(
            into_otap(
                &counters,
                Sample {
                    timestamp_unix_nano: 0,
                    points: vec![point(
                        0,
                        r"\Memory\Available Bytes",
                        SampleValue::Value(Number::Integer(1)),
                    )],
                    failures: Vec::new(),
                    overflows: Vec::new(),
                    diagnostics: Default::default(),
                }
            )
            .is_err()
        );
        assert!(
            into_otap(
                &counters,
                Sample {
                    timestamp_unix_nano: 1,
                    points: vec![point(
                        0,
                        r"\Memory\Available Bytes",
                        SampleValue::Value(Number::Integer(-1)),
                    )],
                    failures: Vec::new(),
                    overflows: Vec::new(),
                    diagnostics: Default::default(),
                }
            )
            .is_ok()
        );
        assert!(
            into_otap(
                &counters,
                Sample {
                    timestamp_unix_nano: 1,
                    points: vec![point(
                        0,
                        r"\Memory\Available Bytes",
                        SampleValue::Value(Number::Double(f64::INFINITY)),
                    )],
                    failures: Vec::new(),
                    overflows: Vec::new(),
                    diagnostics: Default::default(),
                }
            )
            .is_err()
        );
    }

    /// Scenario: Two expanded instances map to one configured wildcard metric.
    /// Guarantees: Concrete paths and parsed duplicate identities are emitted without positional joining.
    #[test]
    fn projects_wildcard_instance_identity() {
        let counters = [counter(
            r"\Process(*)\Private Bytes",
            "windows.process.private",
            "By",
        )];
        let records = into_otap(
            &counters,
            Sample {
                timestamp_unix_nano: 1,
                points: vec![
                    SamplePoint {
                        counter_index: 0,
                        path: r"\Process(worker)\Private Bytes".to_owned(),
                        instance: Some(super::super::InstanceIdentity {
                            name: "worker".to_owned(),
                            parent: None,
                            index: 0,
                        }),
                        value: SampleValue::Value(Number::Integer(10)),
                    },
                    SamplePoint {
                        counter_index: 0,
                        path: r"\Process(worker#1)\Private Bytes".to_owned(),
                        instance: Some(super::super::InstanceIdentity {
                            name: "worker".to_owned(),
                            parent: Some("service".to_owned()),
                            index: 1,
                        }),
                        value: SampleValue::Value(Number::Integer(20)),
                    },
                ],
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
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
        assert!(display.contains(r"\Process(worker)\Private Bytes"));
        assert!(display.contains(r"\Process(worker#1)\Private Bytes"));
        assert!(display.contains(r"\Process(*)\Private Bytes"));
        assert!(display.contains("windows.perf_counter.instance"));
        assert!(display.contains("windows.perf_counter.instance_index"));
        assert!(display.contains("windows.perf_counter.parent_instance"));
        assert!(display.contains("worker"));
        assert!(display.contains("service"));
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
        let _ = active
            .attributes
            .insert("state".to_owned(), "active".to_owned());
        let mut idle = counter(
            r"\Processor(_Total)\% Idle Time",
            "windows.processor.time",
            "%",
        );
        let _ = idle
            .attributes
            .insert("state".to_owned(), "idle".to_owned());
        let records = into_otap(
            &[active, idle],
            Sample {
                timestamp_unix_nano: 1,
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
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
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
}
