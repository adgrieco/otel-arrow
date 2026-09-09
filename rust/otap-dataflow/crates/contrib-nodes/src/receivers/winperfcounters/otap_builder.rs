// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::{CounterConfig, Number, Sample, SampleValue};
use arrow::error::ArrowError;
use otel_arrow_dfe_pdata::encode::record::attributes::StrKeysAttributesRecordBatchBuilder;
use otel_arrow_dfe_pdata::encode::record::metrics::{
    MetricsRecordBatchBuilder, NumberDataPointsRecordBatchBuilder,
};
use otel_arrow_dfe_pdata::otap::{Metrics, OtapArrowRecords};
use otel_arrow_dfe_pdata::otlp::metrics::MetricType;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

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
    if counters.len() != sample.values.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "configured counter count {} does not match sample value count {}",
            counters.len(),
            sample.values.len()
        )));
    }
    let mut metrics = MetricsRecordBatchBuilder::new();
    let mut points = NumberDataPointsRecordBatchBuilder::new();
    let mut attrs = StrKeysAttributesRecordBatchBuilder::<u32>::new();
    let ready = counters
        .iter()
        .zip(sample.values)
        .filter_map(|(counter, value)| match value {
            SampleValue::Value(value) => Some((counter, value)),
            SampleValue::Warming | SampleValue::NoObservation => None,
        });
    let mut count = 0;
    for (index, (counter, value)) in ready.enumerate() {
        let metric_id = u16::try_from(index).map_err(|_| {
            ArrowError::InvalidArgumentError("too many configured counters".to_owned())
        })?;
        let point_id = u32::try_from(index)
            .map_err(|_| ArrowError::InvalidArgumentError("too many counter values".to_owned()))?;
        metrics.append_id(metric_id);
        metrics.append_metric_type(MetricType::Gauge as u8);
        metrics.append_name(counter.name.as_bytes());
        metrics.append_description(counter.description.as_bytes());
        metrics.append_unit(counter.unit.as_bytes());
        metrics.append_aggregation_temporality(None);
        metrics.append_is_monotonic(None);

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

        attrs.append_parent_id(&point_id);
        attrs.append_key("windows.perf_counter.path");
        attrs.any_values_builder.append_str(counter.path.as_bytes());
        count += 1;
    }
    if count == 0 {
        return Ok(None);
    }
    metrics.resource.append_id_n(0, count);
    metrics.resource.append_schema_url_n(None, count);
    metrics.resource.append_dropped_attributes_count_n(0, count);
    metrics.scope.append_id_n(0, count);
    metrics
        .scope
        .append_name_n(Some(b"otel-arrow-dfe-contrib-nodes/winperfcounters"), count);
    metrics
        .scope
        .append_version_n(Some(env!("CARGO_PKG_VERSION").as_bytes()), count);
    metrics.scope.append_dropped_attributes_count_n(0, count);
    metrics.append_scope_schema_url_n(b"", count);

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
            scale_power10: 0,
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
                values: vec![
                    SampleValue::Value(Number::Integer(bytes)),
                    SampleValue::Value(Number::Integer(42)),
                ],
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
                values: vec![
                    SampleValue::Value(Number::Integer(42)),
                    SampleValue::Warming,
                ],
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
                    values: vec![SampleValue::Warming],
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
                values: vec![
                    SampleValue::Value(Number::Integer(42)),
                    SampleValue::Value(Number::Double(12.5)),
                    SampleValue::NoObservation,
                ],
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
                values: vec![SampleValue::Value(Number::Double(12.5))],
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

    /// Scenario: A sample has an invalid timestamp or does not match configured counter count.
    /// Guarantees: Misaligned or untimed values never become plausible output gauges.
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
                    values: vec![]
                }
            )
            .is_err()
        );
        assert!(
            into_otap(
                &counters,
                Sample {
                    timestamp_unix_nano: 0,
                    values: vec![SampleValue::Value(Number::Integer(1))]
                }
            )
            .is_err()
        );
        assert!(
            into_otap(
                &counters,
                Sample {
                    timestamp_unix_nano: 1,
                    values: vec![SampleValue::Value(Number::Integer(-1))]
                }
            )
            .is_ok()
        );
        assert!(
            into_otap(
                &counters,
                Sample {
                    timestamp_unix_nano: 1,
                    values: vec![SampleValue::Value(Number::Double(f64::INFINITY))]
                }
            )
            .is_err()
        );
    }
}
