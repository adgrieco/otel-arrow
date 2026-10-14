// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::config::{Aggregation, CounterConfig, MetricKind};
use super::model::{InstanceIdentity, Number, Sample, SamplePoint, SampleValue};
use arrow::error::ArrowError;
use otel_arrow_dfe_pdata::encode::record::attributes::StrKeysAttributesRecordBatchBuilder;
use otel_arrow_dfe_pdata::encode::record::metrics::{
    MetricsRecordBatchBuilder, NumberDataPointsRecordBatchBuilder,
};
use otel_arrow_dfe_pdata::otap::{Metrics, OtapArrowRecords};
use otel_arrow_dfe_pdata::otlp::metrics::MetricType;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use std::collections::BTreeMap;
#[cfg(test)]
use std::sync::Arc;

const AGGREGATION_TEMPORALITY_DELTA: i32 = 1;
const AGGREGATION_TEMPORALITY_CUMULATIVE: i32 = 2;

fn sum_kind(aggregation: Aggregation, monotonic: bool) -> &'static str {
    match (aggregation, monotonic) {
        (Aggregation::Cumulative, true) => "monotonic cumulative Sum",
        (Aggregation::Cumulative, false) => "non-monotonic cumulative Sum",
        (Aggregation::Delta, true) => "monotonic delta Sum",
        (Aggregation::Delta, false) => "non-monotonic delta Sum",
    }
}

fn instance_attribute_value(instance: &InstanceIdentity) -> String {
    if instance.index == 0 {
        instance.name.clone()
    } else {
        format!("{}#{}", instance.name, instance.index)
    }
}

struct ProjectionBuilder {
    metrics: MetricsRecordBatchBuilder,
    points: NumberDataPointsRecordBatchBuilder,
    attributes: StrKeysAttributesRecordBatchBuilder<u32>,
    metric_ids: BTreeMap<String, u16>,
}

impl ProjectionBuilder {
    fn new() -> Self {
        Self {
            metrics: MetricsRecordBatchBuilder::new(),
            points: NumberDataPointsRecordBatchBuilder::new(),
            attributes: StrKeysAttributesRecordBatchBuilder::<u32>::new(),
            metric_ids: BTreeMap::new(),
        }
    }

    fn append_point(
        &mut self,
        counters: &[CounterConfig],
        point_id: u32,
        point: SamplePoint,
        value: Number,
    ) -> Result<(), ArrowError> {
        validate_point_timestamp(&point)?;
        let counter = counters.get(point.counter_index).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "sample references missing configured counter {}",
                point.counter_index
            ))
        })?;
        let start_time = point_start_time(counter, &point)?;
        let metric_id = self.metric_id(counter)?;
        self.points.append_id(point_id);
        self.points.append_parent_id(metric_id);
        self.points.append_start_time_unix_nano(start_time);
        self.points.append_time_unix_nano(point.timestamp_unix_nano);
        append_number(&mut self.points, &point.path, value)?;
        self.points.append_flags(0);
        append_attributes(&mut self.attributes, point_id, counter, &point);
        Ok(())
    }

    fn metric_id(&mut self, counter: &CounterConfig) -> Result<u16, ArrowError> {
        if let Some(metric_id) = self.metric_ids.get(&counter.name) {
            return Ok(*metric_id);
        }
        let metric_id = u16::try_from(self.metric_ids.len()).map_err(|_| {
            ArrowError::InvalidArgumentError("too many configured metrics".to_owned())
        })?;
        self.metrics.append_id(metric_id);
        self.metrics.append_metric_type(match counter.metric_kind {
            MetricKind::Gauge => MetricType::Gauge as u8,
            MetricKind::Sum { .. } => MetricType::Sum as u8,
        });
        self.metrics.append_name(counter.name.as_bytes());
        self.metrics
            .append_description(counter.description.as_bytes());
        self.metrics.append_unit(counter.unit.as_bytes());
        match counter.metric_kind {
            MetricKind::Gauge => {
                self.metrics.append_aggregation_temporality(None);
                self.metrics.append_is_monotonic(None);
            }
            MetricKind::Sum {
                aggregation,
                monotonic,
            } => {
                self.metrics
                    .append_aggregation_temporality(Some(match aggregation {
                        Aggregation::Cumulative => AGGREGATION_TEMPORALITY_CUMULATIVE,
                        Aggregation::Delta => AGGREGATION_TEMPORALITY_DELTA,
                    }));
                self.metrics.append_is_monotonic(Some(monotonic));
            }
        }
        let _ = self.metric_ids.insert(counter.name.clone(), metric_id);
        Ok(metric_id)
    }

    fn finish(mut self) -> Result<OtapArrowRecords, ArrowError> {
        let metric_count = self.metric_ids.len();
        self.metrics.resource.append_id_n(0, metric_count);
        self.metrics
            .resource
            .append_schema_url_n(None, metric_count);
        self.metrics
            .resource
            .append_dropped_attributes_count_n(0, metric_count);
        self.metrics.scope.append_id_n(0, metric_count);
        self.metrics.scope.append_name_n(
            Some(b"otel-arrow-dfe-contrib-nodes/windows_perf_counters"),
            metric_count,
        );
        self.metrics
            .scope
            .append_version_n(Some(env!("CARGO_PKG_VERSION").as_bytes()), metric_count);
        self.metrics
            .scope
            .append_dropped_attributes_count_n(0, metric_count);
        self.metrics.append_scope_schema_url_n(b"", metric_count);

        let mut resource = StrKeysAttributesRecordBatchBuilder::<u16>::new();
        resource.append_parent_id(&0);
        resource.append_key("os.type");
        resource.any_values_builder.append_str(b"windows");

        let mut records = OtapArrowRecords::Metrics(Metrics::default());
        for (kind, batch) in [
            (ArrowPayloadType::UnivariateMetrics, self.metrics.finish()?),
            (ArrowPayloadType::NumberDataPoints, self.points.finish()?),
            (ArrowPayloadType::NumberDpAttrs, self.attributes.finish()?),
            (ArrowPayloadType::ResourceAttrs, resource.finish()?),
        ] {
            records
                .set(kind, batch)
                .map_err(|err| ArrowError::ExternalError(Box::new(err)))?;
        }
        Ok(records)
    }
}

fn validate_point_timestamp(point: &SamplePoint) -> Result<(), ArrowError> {
    if point.timestamp_unix_nano <= 0 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "counter {} has non-positive timestamp {}",
            point.path, point.timestamp_unix_nano
        )));
    }
    Ok(())
}

fn point_start_time(
    counter: &CounterConfig,
    point: &SamplePoint,
) -> Result<Option<i64>, ArrowError> {
    let MetricKind::Sum {
        aggregation,
        monotonic,
    } = counter.metric_kind
    else {
        return Ok(None);
    };
    let start_time = point.start_time_unix_nano.ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "{} {} ({}) is missing its start time",
            sum_kind(aggregation, monotonic),
            counter.name,
            point.path
        ))
    })?;
    if start_time <= 0
        || start_time > point.timestamp_unix_nano
        || aggregation == Aggregation::Delta && start_time == point.timestamp_unix_nano
    {
        return Err(ArrowError::InvalidArgumentError(format!(
            "{} {} ({}) has invalid start time {} for timestamp {}",
            sum_kind(aggregation, monotonic),
            counter.name,
            point.path,
            start_time,
            point.timestamp_unix_nano
        )));
    }
    Ok(Some(start_time))
}

fn append_number(
    points: &mut NumberDataPointsRecordBatchBuilder,
    path: &str,
    value: Number,
) -> Result<(), ArrowError> {
    match value {
        Number::Integer(value) => {
            points.append_int_value(Some(value));
            points.append_double_value(None);
        }
        Number::Double(value) if value.is_finite() => {
            points.append_int_value(None);
            points.append_double_value(Some(value));
        }
        Number::Double(value) => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "counter {path} has non-finite double value {value}"
            )));
        }
    }
    Ok(())
}

fn append_attributes(
    attributes: &mut StrKeysAttributesRecordBatchBuilder<u32>,
    point_id: u32,
    counter: &CounterConfig,
    point: &SamplePoint,
) {
    let mut append = |key: &str, value: &str| {
        attributes.append_parent_id(&point_id);
        attributes.append_key(key);
        attributes.any_values_builder.append_str(value.as_bytes());
    };
    for (key, value) in counter.attributes.iter() {
        append(key, value);
    }
    append("windows.perf_counter.path", &point.path);
    if point.path != counter.path {
        append("windows.perf_counter.path_template", &counter.path);
    }
    if let Some(instance) = &point.instance {
        if point.emit_instance_attribute && !counter.attributes.contains_key("instance") {
            append("instance", &instance_attribute_value(instance));
        }
        append("windows.perf_counter.instance", &instance.name);
        if let Some(parent) = &instance.parent {
            append("windows.perf_counter.parent_instance", parent);
        }
        append(
            "windows.perf_counter.instance_index",
            &instance.index.to_string(),
        );
    }
}

/// Project the ready values from one scrape directly into OTAP metric streams.
pub(super) fn into_otap(
    counters: &[CounterConfig],
    sample: Sample,
) -> Result<Option<OtapArrowRecords>, ArrowError> {
    if sample.timestamp_unix_nano <= 0 {
        return Err(ArrowError::InvalidArgumentError(format!(
            "sample has non-positive timestamp {}",
            sample.timestamp_unix_nano
        )));
    }
    let mut builder = ProjectionBuilder::new();
    let ready = sample
        .points
        .into_iter()
        .filter_map(|point| match point.value {
            SampleValue::Value(value) => Some((point, value)),
            SampleValue::Warming | SampleValue::NoObservation => None,
        });
    for (index, (point, value)) in ready.enumerate() {
        let point_id = u32::try_from(index)
            .map_err(|_| ArrowError::InvalidArgumentError("too many counter values".to_owned()))?;
        builder.append_point(counters, point_id, point, value)?;
    }
    if builder.metric_ids.is_empty() {
        return Ok(None);
    }
    Ok(Some(builder.finish()?))
}

#[cfg(test)]
mod tests {
    use super::super::config::Config;
    use super::super::model::{InstanceIdentity, SampleFailure};
    use super::*;
    use arrow::array::{Array, Int64Array, UInt8Array};
    use arrow::util::display::array_value_to_string;

    fn counter(path: &str, name: &str, unit: &str) -> CounterConfig {
        CounterConfig {
            path: path.to_owned(),
            name: name.to_owned(),
            unit: unit.to_owned(),
            description: Arc::from(format!("Description for {name}.")),
            metric_kind: MetricKind::Gauge,
            attributes: Arc::new(BTreeMap::new()),
            has_instance: false,
            emit_instance_attribute: false,
            excluded_aggregation_instance: None,
            scale_power10: 0,
            recreate_query: false,
            source_mapping_index: 0,
        }
    }

    fn non_monotonic_sum(path: &str, name: &str, unit: &str) -> CounterConfig {
        CounterConfig {
            metric_kind: MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: false,
            },
            ..counter(path, name, unit)
        }
    }

    fn monotonic_sum(path: &str, name: &str, unit: &str) -> CounterConfig {
        CounterConfig {
            metric_kind: MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: true,
            },
            ..counter(path, name, unit)
        }
    }

    fn delta_sum(path: &str, name: &str, unit: &str, monotonic: bool) -> CounterConfig {
        CounterConfig {
            metric_kind: MetricKind::Sum {
                aggregation: Aggregation::Delta,
                monotonic,
            },
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
            path: path.to_owned(),
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
                points: vec![point(
                    0,
                    r"\Memory\Available Bytes",
                    SampleValue::Value(Number::Integer(42)),
                )],
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
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
    /// Guarantees: OTAP uses the configured metric name while retaining the canonical native path attribute.
    #[test]
    fn projects_configured_instance_spelling_for_path_metric() {
        let configured_name = r"\Process(app#001)\Private Bytes";
        let native_path = r"\Process(app#1)\Private Bytes";
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
                points: vec![point(
                    0,
                    native_path,
                    SampleValue::Value(Number::Integer(42)),
                )],
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
        assert!(display.contains(configured_name));
        assert!(display.contains("Configured process memory."));
        assert!(display.contains("By"));
        assert!(
            string_attributes(&records)
                .iter()
                .any(|(_, key, value)| key == "windows.perf_counter.path" && value == native_path)
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

    /// Scenario: A delta point from an earlier query is merged into a sample with a later peer timestamp.
    /// Guarantees: OTAP uses the point's own interval end instead of stretching it to the merged sample timestamp.
    #[test]
    fn projects_delta_with_its_query_timestamp() {
        let counter = delta_sum(
            r"\Provider\Operations Delta",
            "provider.operations",
            "{operation}",
            false,
        );
        let mut point = sum_point(0, r"\Provider\Operations Delta", 100, Number::Double(2.0));
        point.timestamp_unix_nano = 110;
        let records = into_otap(
            &[counter],
            Sample {
                timestamp_unix_nano: 115,
                points: vec![point],
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
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
                points: vec![sum_point(
                    0,
                    r"\Process(worker)\Private Bytes",
                    5,
                    Number::Integer(42),
                )],
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
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

    /// Scenario: The first running-count observation is configured as a monotonic Sum.
    /// Guarantees: OTAP emits cumulative monotonic metadata with start time equal to point time.
    #[test]
    fn projects_integer_monotonic_sum() {
        let counters = [monotonic_sum(
            r"\Custom Provider\Operations",
            "windows.operations",
            "{operation}",
        )];
        let records = into_otap(
            &counters,
            Sample {
                timestamp_unix_nano: 10,
                points: vec![sum_point(
                    0,
                    r"\Custom Provider\Operations",
                    10,
                    Number::Integer(42),
                )],
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
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
        assert_eq!(
            array_value_to_string(metrics.column_by_name("is_monotonic").unwrap().as_ref(), 0)
                .unwrap(),
            "true"
        );
        let points = records.get(ArrowPayloadType::NumberDataPoints).unwrap();
        assert_eq!(
            points
                .column_by_name("start_time_unix_nano")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .unwrap()
                .value(0),
            10
        );
    }

    /// Scenario: A monotonic Sum point omits its sequence start or has an invalid start timestamp.
    /// Guarantees: Projection rejects the point with its metric and concrete PDH counter identity.
    #[test]
    fn validates_monotonic_sum_start_time() {
        let counters = [monotonic_sum(
            r"\Custom Provider\Operations",
            "windows.operations",
            "{operation}",
        )];
        let missing = into_otap(
            &counters,
            Sample {
                timestamp_unix_nano: 10,
                points: vec![point(
                    0,
                    r"\Custom Provider\Operations",
                    SampleValue::Value(Number::Integer(42)),
                )],
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
            },
        );
        assert!(matches!(
            missing,
            Err(ArrowError::InvalidArgumentError(message))
                if message == "monotonic cumulative Sum windows.operations (\\Custom Provider\\Operations) is missing its start time"
        ));
        for start in [i64::MIN, -1, 0, 11, i64::MAX] {
            assert!(matches!(
                into_otap(
                    &counters,
                    Sample {
                        timestamp_unix_nano: 10,
                        points: vec![sum_point(
                            0,
                            r"\Custom Provider\Operations",
                            start,
                            Number::Integer(42),
                        )],
                        failures: Vec::new(),
                        overflows: Vec::new(),
                        diagnostics: Default::default(),
                    },
                ),
                Err(ArrowError::InvalidArgumentError(message))
                    if message == format!(
                        "monotonic cumulative Sum windows.operations (\\Custom Provider\\Operations) \
                         has invalid start time {start} for timestamp 10"
                    )
            ));
        }
    }

    /// Scenario: An emitted non-monotonic Sum has an invalid or boundary cumulative start time.
    /// Guarantees: Only 0 < start <= observation time is accepted and errors identify the PDH counter and both times.
    #[test]
    fn validates_non_monotonic_sum_start_time() {
        let counters = [non_monotonic_sum(
            r"\Process(*)\Private Bytes",
            "windows.process.private",
            "By",
        )];
        for start in [i64::MIN, -1, 0, 11, i64::MAX] {
            assert!(matches!(
                into_otap(
                    &counters,
                    Sample {
                        timestamp_unix_nano: 10,
                        points: vec![sum_point(
                            0,
                            r"\Process(worker)\Private Bytes",
                            start,
                            Number::Integer(42),
                        )],
                        failures: Vec::new(),
                        overflows: Vec::new(),
                        diagnostics: Default::default(),
                    },
                ),
                Err(ArrowError::InvalidArgumentError(message))
                    if message == format!(
                        "non-monotonic cumulative Sum windows.process.private (\\Process(worker)\\Private Bytes) \
                         has invalid start time {start} for timestamp 10"
                    )
            ));
        }
        for start in [1, 10] {
            assert!(
                into_otap(
                    &counters,
                    Sample {
                        timestamp_unix_nano: 10,
                        points: vec![sum_point(
                            0,
                            r"\Process(worker)\Private Bytes",
                            start,
                            Number::Integer(42),
                        )],
                        failures: Vec::new(),
                        overflows: Vec::new(),
                        diagnostics: Default::default(),
                    },
                )
                .unwrap()
                .is_some()
            );
        }
    }

    /// Scenario: Two native delta-count mappings share one metric with different valid intervals.
    /// Guarantees: Projection emits delta Sum metadata and preserves each point's strict interval start.
    #[test]
    fn projects_delta_sum_intervals() {
        let counters = [
            delta_sum(
                r"\Provider\Delta A",
                "windows.operations",
                "{operation}",
                true,
            ),
            delta_sum(
                r"\Provider\Delta B",
                "windows.operations",
                "{operation}",
                true,
            ),
        ];
        let records = into_otap(
            &counters,
            Sample {
                timestamp_unix_nano: 10,
                points: vec![
                    sum_point(0, r"\Provider\Delta A", 5, Number::Double(2.0)),
                    sum_point(1, r"\Provider\Delta B", 7, Number::Double(3.0)),
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
        assert_eq!(
            array_value_to_string(
                metrics
                    .column_by_name("aggregation_temporality")
                    .unwrap()
                    .as_ref(),
                0
            )
            .unwrap(),
            AGGREGATION_TEMPORALITY_DELTA.to_string()
        );
        assert_eq!(
            array_value_to_string(metrics.column_by_name("is_monotonic").unwrap().as_ref(), 0)
                .unwrap(),
            "true"
        );
        let points = records.get(ArrowPayloadType::NumberDataPoints).unwrap();
        let starts = points
            .column_by_name("start_time_unix_nano")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();
        assert_eq!([starts.value(0), starts.value(1)], [5, 7]);
    }

    /// Scenario: A delta Sum interval has a missing, non-positive, or non-increasing start.
    /// Guarantees: Projection requires every delta point to satisfy 0 < start < observation time.
    #[test]
    fn validates_delta_sum_start_time() {
        let counters = [delta_sum(
            r"\Provider\Operations Delta",
            "windows.operations",
            "{operation}",
            false,
        )];
        let missing = into_otap(
            &counters,
            Sample {
                timestamp_unix_nano: 10,
                points: vec![point(
                    0,
                    r"\Provider\Operations Delta",
                    SampleValue::Value(Number::Double(2.0)),
                )],
                failures: Vec::new(),
                overflows: Vec::new(),
                diagnostics: Default::default(),
            },
        );
        assert!(matches!(
            missing,
            Err(ArrowError::InvalidArgumentError(message))
                if message == "non-monotonic delta Sum windows.operations (\\Provider\\Operations Delta) is missing its start time"
        ));
        for start in [i64::MIN, -1, 0, 10, 11, i64::MAX] {
            assert!(matches!(
                into_otap(
                    &counters,
                    Sample {
                        timestamp_unix_nano: 10,
                        points: vec![sum_point(
                            0,
                            r"\Provider\Operations Delta",
                            start,
                            Number::Double(2.0),
                        )],
                        failures: Vec::new(),
                        overflows: Vec::new(),
                        diagnostics: Default::default(),
                    },
                ),
                Err(ArrowError::InvalidArgumentError(message))
                    if message == format!(
                        "non-monotonic delta Sum windows.operations (\\Provider\\Operations Delta) \
                         has invalid start time {start} for timestamp 10"
                    )
            ));
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
        assert!(
            into_otap(
                &counters,
                Sample {
                    timestamp_unix_nano: 1,
                    points: vec![point(
                        0,
                        r"\Memory\Available Bytes",
                        SampleValue::Value(Number::Integer(42)),
                    )],
                    failures: Vec::new(),
                    overflows: Vec::new(),
                    diagnostics: Default::default(),
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
                failures: vec![SampleFailure {
                    counter_index: 1,
                    reason: "PdhGetFormattedCounterValue",
                    error: "PDH_INVALID_DATA".to_owned(),
                    diagnostic_only: false,
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

    /// Scenario: A sample has an invalid timestamp, counter reference, or numeric value.
    /// Guarantees: Invalid observations are rejected with the timestamp, path, or value needed for diagnosis.
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
        for timestamp in [i64::MIN, -1, 0] {
            assert!(matches!(
                into_otap(
                    &counters,
                    Sample {
                        timestamp_unix_nano: timestamp,
                        points: vec![point(
                            0,
                            r"\Memory\Available Bytes",
                            SampleValue::Value(Number::Integer(1)),
                        )],
                        failures: Vec::new(),
                        overflows: Vec::new(),
                        diagnostics: Default::default(),
                    }
                ),
                Err(ArrowError::InvalidArgumentError(message))
                    if message == format!("sample has non-positive timestamp {timestamp}")
            ));
        }
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
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(matches!(
                into_otap(
                    &counters,
                    Sample {
                        timestamp_unix_nano: 1,
                        points: vec![point(
                            0,
                            r"\Memory\Available Bytes",
                            SampleValue::Value(Number::Double(value)),
                        )],
                        failures: Vec::new(),
                        overflows: Vec::new(),
                        diagnostics: Default::default(),
                    }
                ),
                Err(ArrowError::InvalidArgumentError(message))
                    if message == format!(
                        "counter \\Memory\\Available Bytes has non-finite double value {value}"
                    )
            ));
        }
    }

    /// Scenario: Two expanded instances map to one configured wildcard metric.
    /// Guarantees: Concrete paths and parsed duplicate identities are emitted without positional joining.
    #[test]
    fn projects_wildcard_instance_identity() {
        let mut wildcard = counter(
            r"\Process(*)\Private Bytes",
            "windows.process.private",
            "By",
        );
        wildcard.has_instance = true;
        wildcard.emit_instance_attribute = true;
        let counters = [wildcard];
        let records = into_otap(
            &counters,
            Sample {
                timestamp_unix_nano: 1,
                points: vec![
                    SamplePoint {
                        counter_index: 0,
                        path: r"\Process(worker)\Private Bytes".to_owned(),
                        instance: Some(InstanceIdentity {
                            name: "worker".to_owned(),
                            parent: None,
                            index: 0,
                        }),
                        emit_instance_attribute: true,
                        timestamp_unix_nano: 1,
                        start_time_unix_nano: None,
                        value: SampleValue::Value(Number::Integer(10)),
                    },
                    SamplePoint {
                        counter_index: 0,
                        path: r"\Process(worker#1)\Private Bytes".to_owned(),
                        instance: Some(InstanceIdentity {
                            name: "worker".to_owned(),
                            parent: Some("service".to_owned()),
                            index: 1,
                        }),
                        emit_instance_attribute: true,
                        timestamp_unix_nano: 1,
                        start_time_unix_nano: None,
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
        assert_eq!(
            string_attributes(&records)
                .into_iter()
                .filter(|(_, key, _)| key == "instance")
                .collect::<Vec<_>>(),
            [
                ("0".to_owned(), "instance".to_owned(), "worker".to_owned()),
                ("1".to_owned(), "instance".to_owned(), "worker#1".to_owned())
            ]
        );
    }

    /// Scenario: An aggregate is collected alone or explicitly included with other instances.
    /// Guarantees: Go's `instance` attribute is omitted only for the aggregate-only mapping.
    #[test]
    fn projects_aggregate_instance_selection() {
        let mut aggregate_alone = counter(r"\Processor(_Total)\Counter", "aggregate.alone", "1");
        aggregate_alone.has_instance = true;
        aggregate_alone.emit_instance_attribute = false;
        let mut explicitly_included = counter(r"\Processor(*)\Counter", "aggregate.included", "1");
        explicitly_included.has_instance = true;
        explicitly_included.emit_instance_attribute = true;
        let aggregate = InstanceIdentity {
            name: "_Total".to_owned(),
            parent: None,
            index: 0,
        };
        let records = into_otap(
            &[aggregate_alone, explicitly_included],
            Sample {
                timestamp_unix_nano: 1,
                points: vec![
                    SamplePoint {
                        counter_index: 0,
                        path: r"\Processor(_Total)\Counter".to_owned(),
                        instance: Some(aggregate.clone()),
                        emit_instance_attribute: false,
                        timestamp_unix_nano: 1,
                        start_time_unix_nano: None,
                        value: SampleValue::Value(Number::Integer(10)),
                    },
                    SamplePoint {
                        counter_index: 1,
                        path: r"\Processor(_Total)\Counter".to_owned(),
                        instance: Some(aggregate),
                        emit_instance_attribute: true,
                        timestamp_unix_nano: 1,
                        start_time_unix_nano: None,
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

    /// Scenario: Two native instances share one fixed user `instance` attribute.
    /// Guarantees: User precedence can collide emitted series without merging or dropping either point.
    #[test]
    fn static_instance_override_preserves_distinct_points() {
        let mut wildcard = counter(r"\Process(*)\Private Bytes", "process.private", "By");
        wildcard.has_instance = true;
        wildcard.emit_instance_attribute = true;
        wildcard.attributes = Arc::new(BTreeMap::from([(
            "instance".to_owned(),
            "fixed".to_owned(),
        )]));
        let records = into_otap(
            &[wildcard],
            Sample {
                timestamp_unix_nano: 1,
                points: vec![
                    SamplePoint {
                        counter_index: 0,
                        path: r"\Process(worker)\Private Bytes".to_owned(),
                        instance: Some(InstanceIdentity {
                            name: "worker".to_owned(),
                            parent: None,
                            index: 0,
                        }),
                        emit_instance_attribute: true,
                        timestamp_unix_nano: 1,
                        start_time_unix_nano: None,
                        value: SampleValue::Value(Number::Integer(10)),
                    },
                    SamplePoint {
                        counter_index: 0,
                        path: r"\Process(service)\Private Bytes".to_owned(),
                        instance: Some(InstanceIdentity {
                            name: "service".to_owned(),
                            parent: None,
                            index: 0,
                        }),
                        emit_instance_attribute: true,
                        timestamp_unix_nano: 1,
                        start_time_unix_nano: None,
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

        assert_eq!(
            records
                .get(ArrowPayloadType::NumberDataPoints)
                .unwrap()
                .num_rows(),
            2
        );
        assert_eq!(
            string_attributes(&records)
                .into_iter()
                .filter(|(_, key, _)| key == "instance")
                .collect::<Vec<_>>(),
            [
                ("0".to_owned(), "instance".to_owned(), "fixed".to_owned()),
                ("1".to_owned(), "instance".to_owned(), "fixed".to_owned())
            ]
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
