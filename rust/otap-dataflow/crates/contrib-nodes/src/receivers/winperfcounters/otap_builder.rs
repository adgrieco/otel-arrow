// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::{COUNTER_PATH, METRIC_NAME, Sample};
use arrow::error::ArrowError;
use otel_arrow_dfe_pdata::encode::record::attributes::StrKeysAttributesRecordBatchBuilder;
use otel_arrow_dfe_pdata::encode::record::metrics::{
    MetricsRecordBatchBuilder, NumberDataPointsRecordBatchBuilder,
};
use otel_arrow_dfe_pdata::otap::{Metrics, OtapArrowRecords};
use otel_arrow_dfe_pdata::otlp::metrics::MetricType;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

/// Project one valid reading directly into an integer OTAP gauge with unit `By`.
pub fn into_otap(sample: Sample) -> Result<OtapArrowRecords, ArrowError> {
    if sample.timestamp_unix_nano <= 0 || sample.available_bytes < 0 {
        return Err(ArrowError::InvalidArgumentError(
            "memory sample requires a positive Unix timestamp and nonnegative bytes".to_owned(),
        ));
    }
    let mut metrics = MetricsRecordBatchBuilder::new();
    metrics.append_id(0);
    metrics.append_metric_type(MetricType::Gauge as u8);
    metrics.append_name(METRIC_NAME.as_bytes());
    metrics.append_description(b"Physical memory immediately available for allocation.");
    metrics.append_unit(b"By");
    metrics.append_aggregation_temporality(None);
    metrics.append_is_monotonic(None);
    metrics.resource.append_id_n(0, 1);
    metrics.resource.append_schema_url_n(None, 1);
    metrics.resource.append_dropped_attributes_count_n(0, 1);
    metrics.scope.append_id_n(0, 1);
    metrics
        .scope
        .append_name_n(Some(b"otel-arrow-dfe-contrib-nodes/winperfcounters"), 1);
    metrics
        .scope
        .append_version_n(Some(env!("CARGO_PKG_VERSION").as_bytes()), 1);
    metrics.scope.append_dropped_attributes_count_n(0, 1);
    metrics.append_scope_schema_url_n(b"", 1);

    let mut points = NumberDataPointsRecordBatchBuilder::new();
    points.append_id(0);
    points.append_parent_id(0);
    points.append_start_time_unix_nano(None);
    points.append_time_unix_nano(sample.timestamp_unix_nano);
    points.append_int_value(Some(sample.available_bytes));
    points.append_double_value(None);
    points.append_flags(0);

    let mut attrs = StrKeysAttributesRecordBatchBuilder::<u32>::new();
    attrs.append_parent_id(&0);
    attrs.append_key("windows.perf_counter.path");
    attrs.any_values_builder.append_str(COUNTER_PATH.as_bytes());

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
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, UInt8Array};

    /// Scenario: A byte reading beyond f64's exact integer range is projected.
    /// Guarantees: The gauge retains exact i64 bytes, timestamp, unit and input identity.
    #[test]
    fn projects_integer_gauge() {
        let timestamp = 1_788_500_000_123_456_789;
        let bytes = 9_007_199_254_740_993;
        let records = into_otap(Sample {
            timestamp_unix_nano: timestamp,
            available_bytes: bytes,
        })
        .unwrap();
        let metrics = records.get(ArrowPayloadType::UnivariateMetrics).unwrap();
        let points = records.get(ArrowPayloadType::NumberDataPoints).unwrap();
        assert_eq!(metrics.num_rows(), 1);
        assert_eq!(points.num_rows(), 1);
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
        assert!(display.contains(METRIC_NAME));
        assert!(display.contains("By"));
        let attrs = records.get(ArrowPayloadType::NumberDpAttrs).unwrap();
        let display = arrow::util::pretty::pretty_format_batches(std::slice::from_ref(attrs))
            .unwrap()
            .to_string();
        assert!(display.contains(COUNTER_PATH));
        assert!(display.contains("windows.perf_counter.path"));
        let time = points
            .column_by_name("time_unix_nano")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(time.value(0), timestamp);
    }

    /// Scenario: A malformed source reading reaches the portable projection boundary.
    /// Guarantees: Negative bytes and missing timestamps never become plausible output gauges.
    #[test]
    fn rejects_invalid_sample() {
        assert!(
            into_otap(Sample {
                timestamp_unix_nano: 1,
                available_bytes: -1
            })
            .is_err()
        );
        assert!(
            into_otap(Sample {
                timestamp_unix_nano: 0,
                available_bytes: 1
            })
            .is_err()
        );
        assert!(
            into_otap(Sample {
                timestamp_unix_nano: 1,
                available_bytes: 0
            })
            .is_ok()
        );
    }
}
