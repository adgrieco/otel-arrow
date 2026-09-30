// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Projection of normalized PDH samples into OTAP Arrow metric records.

use super::config::{CounterConfig, MetricKind};
use super::model::{Number, Sample, SampleValue};
use arrow::error::ArrowError;
use otel_arrow_dfe_pdata::encode::record::attributes::StrKeysAttributesRecordBatchBuilder;
use otel_arrow_dfe_pdata::encode::record::metrics::{
    MetricsRecordBatchBuilder, NumberDataPointsRecordBatchBuilder,
};
use otel_arrow_dfe_pdata::otap::{Metrics, OtapArrowRecords};
use otel_arrow_dfe_pdata::otlp::metrics::MetricType;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use std::collections::BTreeMap;

const AGGREGATION_TEMPORALITY_CUMULATIVE: i32 = 2;

/// Project the ready values from one scrape into OTAP metrics.
pub(super) fn into_otap(
    counters: &[CounterConfig],
    sample: Sample,
) -> Result<Option<OtapArrowRecords>, ArrowError> {
    if sample.timestamp_unix_nano <= 0 || sample.start_time_unix_nano <= 0 {
        return Err(ArrowError::InvalidArgumentError(
            "performance-counter sample requires positive Unix timestamps".to_owned(),
        ));
    }
    let mut metrics = MetricsRecordBatchBuilder::new();
    let mut points = NumberDataPointsRecordBatchBuilder::new();
    let mut attrs = StrKeysAttributesRecordBatchBuilder::<u32>::new();
    let mut metric_ids = BTreeMap::new();
    let mut point_count = 0_u32;

    for point in sample.points {
        let SampleValue::Value(value) = point.value else {
            continue;
        };
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
            match counter.kind {
                MetricKind::Gauge => {
                    metrics.append_metric_type(MetricType::Gauge as u8);
                    metrics.append_aggregation_temporality(None);
                    metrics.append_is_monotonic(None);
                }
                MetricKind::UpDownCounter => {
                    metrics.append_metric_type(MetricType::Sum as u8);
                    metrics
                        .append_aggregation_temporality(Some(AGGREGATION_TEMPORALITY_CUMULATIVE));
                    metrics.append_is_monotonic(Some(false));
                }
            }
            metrics.append_name(counter.name.as_bytes());
            metrics.append_description(counter.description.as_bytes());
            metrics.append_unit(counter.unit.as_bytes());
            let _ = metric_ids.insert(counter.name.clone(), metric_id);
            metric_id
        };

        points.append_id(point_count);
        points.append_parent_id(metric_id);
        points.append_start_time_unix_nano(match counter.kind {
            MetricKind::Gauge => None,
            MetricKind::UpDownCounter => Some(sample.start_time_unix_nano),
        });
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
            attrs.append_parent_id(&point_count);
            attrs.append_key(key);
            attrs.any_values_builder.append_str(value.as_bytes());
        }
        attrs.append_parent_id(&point_count);
        attrs.append_key("windows.perf_counter.path");
        attrs.any_values_builder.append_str(counter.path.as_bytes());
        point_count = point_count.checked_add(1).ok_or_else(|| {
            ArrowError::InvalidArgumentError("too many counter values".to_owned())
        })?;
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
    use super::super::model::{SamplePoint, SampleValue};
    use super::*;
    use arrow::array::{
        Array, DictionaryArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
        UInt8Array,
    };
    use arrow::datatypes::{UInt8Type, UInt16Type};

    fn counter(name: &str, kind: MetricKind) -> CounterConfig {
        CounterConfig {
            path: format!(r"\Object\{name}"),
            name: name.to_owned(),
            unit: "By".to_owned(),
            description: format!("Description for {name}."),
            kind,
            attributes: BTreeMap::new(),
            scale_power10: 0,
        }
    }

    /// Scenario: One scrape contains a Gauge and an UpDownCounter.
    /// Guarantees: OTAP retains integer values and encodes cumulative non-monotonic Sum semantics.
    #[test]
    fn projects_metric_kinds() {
        let start = 1_788_400_000_000_000_000;
        let timestamp = start + 1_000_000_000;
        let counters = [
            counter("gauge", MetricKind::Gauge),
            counter("updown", MetricKind::UpDownCounter),
        ];
        let records = into_otap(
            &counters,
            Sample {
                start_time_unix_nano: start,
                timestamp_unix_nano: timestamp,
                points: vec![
                    SamplePoint {
                        counter_index: 0,
                        value: SampleValue::Value(Number::Integer(7)),
                    },
                    SamplePoint {
                        counter_index: 1,
                        value: SampleValue::Value(Number::Integer(9)),
                    },
                ],
                failures: Vec::new(),
            },
        )
        .unwrap()
        .unwrap();
        let metric_batch = records.get(ArrowPayloadType::UnivariateMetrics).unwrap();
        let metric_types = metric_batch
            .column_by_name("metric_type")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(metric_types.value(0), MetricType::Gauge as u8);
        assert_eq!(metric_types.value(1), MetricType::Sum as u8);
        let point_batch = records.get(ArrowPayloadType::NumberDataPoints).unwrap();
        let values = point_batch
            .column_by_name("int_value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[7, 9]);
        let starts = point_batch
            .column_by_name("start_time_unix_nano")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert!(starts.is_null(0));
        assert_eq!(starts.value(1), start);
    }

    /// Scenario: Multiple counters share one metric while carrying attributes, doubles, and omissions.
    /// Guarantees: The builder emits one metric, retains point attributes and paths, and omits no-observation points.
    #[test]
    fn projects_shared_metric_points_and_attributes() {
        let start = 1_788_400_000_000_000_000;
        let timestamp = start + 1_000_000_000;
        let mut counters = [
            counter("active", MetricKind::Gauge),
            counter("idle", MetricKind::Gauge),
            counter("omitted", MetricKind::Gauge),
        ];
        for counter in &mut counters {
            counter.name = "windows.processor.time".to_owned();
            counter.unit = "%".to_owned();
        }
        let _ = counters[0]
            .attributes
            .insert("state".to_owned(), "active".to_owned());
        let _ = counters[1]
            .attributes
            .insert("state".to_owned(), "idle".to_owned());

        let records = into_otap(
            &counters,
            Sample {
                start_time_unix_nano: start,
                timestamp_unix_nano: timestamp,
                points: vec![
                    SamplePoint {
                        counter_index: 0,
                        value: SampleValue::Value(Number::Double(12.5)),
                    },
                    SamplePoint {
                        counter_index: 1,
                        value: SampleValue::Value(Number::Double(87.5)),
                    },
                    SamplePoint {
                        counter_index: 2,
                        value: SampleValue::NoObservation,
                    },
                ],
                failures: Vec::new(),
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
        let point_batch = records.get(ArrowPayloadType::NumberDataPoints).unwrap();
        assert_eq!(point_batch.num_rows(), 2);
        let values = point_batch
            .column_by_name("double_value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.values(), &[12.5, 87.5]);

        let attrs = records.get(ArrowPayloadType::NumberDpAttrs).unwrap();
        assert_eq!(attrs.num_rows(), 4);
        let keys = attrs
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<UInt8Type>>()
            .unwrap();
        let key_values = keys
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            key_values.iter().flatten().collect::<Vec<_>>(),
            vec!["state", "windows.perf_counter.path"]
        );
        let strings = attrs
            .column_by_name("str")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<UInt16Type>>()
            .unwrap();
        let string_values = strings
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            string_values.iter().flatten().collect::<Vec<_>>(),
            vec!["active", r"\Object\active", "idle", r"\Object\idle"]
        );
    }

    /// Scenario: A successful scrape contains only no-observation values.
    /// Guarantees: The builder returns no OTAP batch instead of emitting empty metric records.
    #[test]
    fn omits_empty_sample() {
        let counters = [counter("average", MetricKind::Gauge)];
        let records = into_otap(
            &counters,
            Sample {
                start_time_unix_nano: 1,
                timestamp_unix_nano: 2,
                points: vec![SamplePoint {
                    counter_index: 0,
                    value: SampleValue::NoObservation,
                }],
                failures: Vec::new(),
            },
        )
        .unwrap();
        assert!(records.is_none());
    }
}
