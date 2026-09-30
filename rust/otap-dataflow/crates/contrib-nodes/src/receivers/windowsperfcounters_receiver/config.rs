// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Public configuration parsing, validation, and exact-path normalization.

use otel_arrow_dfe_config::error::Error;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

const MAX_COUNTER_PATH_LEN: usize = 2_047;
const MIN_SCALE_POWER10: i32 = -18;
const MAX_SCALE_POWER10: i32 = 18;
const MIN_COLLECTION_INTERVAL: Duration = Duration::from_secs(1);
const MAX_COLLECTION_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_INITIAL_DELAY: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_METRICS: usize = u16::MAX as usize + 1;
const RECEIVER_ATTRIBUTE_PREFIX: &str = "windows.perf_counter.";

/// OTel metric kind used to project a performance counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum MetricKind {
    /// A point-in-time Gauge.
    Gauge,
    /// A cumulative, non-monotonic Sum representing an UpDownCounter.
    UpDownCounter,
}

/// One normalized exact counter consumed by the PDH worker and OTAP builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CounterConfig {
    /// Exact configured PDH path.
    pub(super) path: String,
    /// OTel metric name.
    pub(super) name: String,
    /// OTel metric unit.
    pub(super) unit: String,
    /// OTel metric description.
    pub(super) description: String,
    /// OTel metric kind.
    pub(super) kind: MetricKind,
    /// Static attributes added to every point from this counter.
    pub(super) attributes: BTreeMap<String, String>,
    /// Base-10 scaling applied after PDH calculates the native value.
    pub(super) scale_power10: i32,
}

/// Configuration normalized for exact Windows performance-counter collection.
#[derive(Debug, Clone)]
pub(super) struct RuntimeConfig {
    /// Exact counters to collect.
    pub(super) counters: Vec<CounterConfig>,
    /// Time between collections; defaults to 30 seconds.
    pub(super) collection_interval: Duration,
    /// Delay before the first collection request; defaults to one second.
    pub(super) initial_delay: Duration,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    metrics: BTreeMap<String, MetricConfig>,
    perfcounters: Vec<ObjectConfig>,
    #[serde(default = "default_interval", with = "humantime_serde")]
    collection_interval: Duration,
    #[serde(default = "default_initial_delay", with = "humantime_serde")]
    initial_delay: Duration,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricConfig {
    description: String,
    unit: String,
    #[serde(default)]
    gauge: Option<EmptyConfig>,
    #[serde(default)]
    up_down_counter: Option<EmptyConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyConfig {}

impl MetricConfig {
    fn kind(&self, name: &str) -> Result<MetricKind, Error> {
        match (self.gauge.is_some(), self.up_down_counter.is_some()) {
            (true, false) => Ok(MetricKind::Gauge),
            (false, true) => Ok(MetricKind::UpDownCounter),
            _ => Err(invalid(format!(
                "metrics.{name} must contain exactly one of gauge or up_down_counter"
            ))),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectConfig {
    object: String,
    #[serde(default)]
    instances: Option<OneOrMany>,
    counters: Vec<CounterMapping>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

impl OneOrMany {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CounterMapping {
    name: String,
    metric: String,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    #[serde(default)]
    scale_power10: i32,
}

fn default_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_initial_delay() -> Duration {
    Duration::from_secs(1)
}

fn invalid(error: impl Into<String>) -> Error {
    Error::InvalidUserConfig {
        error: error.into(),
    }
}

fn require_name(field: &str, value: &str) -> Result<(), Error> {
    if value.trim().is_empty() {
        return Err(invalid(format!("{field} must not be empty")));
    }
    Ok(())
}

fn validate_object_or_instance(field: &str, value: &str) -> Result<(), Error> {
    require_name(field, value)?;
    if value.contains(['\\', '(', ')', '*', '?']) {
        return Err(invalid(format!(
            "{field} contains a reserved performance-counter path character"
        )));
    }
    Ok(())
}

fn validate_counter_name(field: &str, value: &str) -> Result<(), Error> {
    require_name(field, value)?;
    if value.contains(['\\', '*', '?']) {
        return Err(invalid(format!(
            "{field} contains a reserved performance-counter path character"
        )));
    }
    Ok(())
}

fn validate_metric_count(count: usize) -> Result<(), Error> {
    if count > MAX_METRICS {
        return Err(invalid(format!(
            "metrics must contain at most {MAX_METRICS} entries"
        )));
    }
    Ok(())
}

fn validate_counter(counter: &CounterConfig) -> Result<(), Error> {
    if counter.path.encode_utf16().count() > MAX_COUNTER_PATH_LEN {
        return Err(invalid(format!(
            "counter path {:?} must contain at most {MAX_COUNTER_PATH_LEN} UTF-16 code units",
            counter.path
        )));
    }
    if !(MIN_SCALE_POWER10..=MAX_SCALE_POWER10).contains(&counter.scale_power10) {
        return Err(invalid(format!(
            "counter path {:?} scale_power10 must be between {MIN_SCALE_POWER10} and {MAX_SCALE_POWER10}",
            counter.path
        )));
    }
    for key in counter.attributes.keys() {
        require_name(
            &format!("counter path {:?} attribute key", counter.path),
            key,
        )?;
        if key.starts_with(RECEIVER_ATTRIBUTE_PREFIX) {
            return Err(invalid(format!(
                "counter path {:?} attribute {key:?} conflicts with receiver-generated attributes",
                counter.path
            )));
        }
    }
    Ok(())
}

impl RuntimeConfig {
    /// Parse, validate, and normalize the public configuration contract.
    pub(super) fn from_json(value: &serde_json::Value) -> Result<Self, Error> {
        let user: Config =
            serde_json::from_value(value.clone()).map_err(|err| invalid(err.to_string()))?;
        validate_metric_count(user.metrics.len())?;
        if !(MIN_COLLECTION_INTERVAL..=MAX_COLLECTION_INTERVAL).contains(&user.collection_interval)
        {
            return Err(invalid(format!(
                "collection_interval must be between {} and {}",
                humantime::format_duration(MIN_COLLECTION_INTERVAL),
                humantime::format_duration(MAX_COLLECTION_INTERVAL)
            )));
        }
        if user.initial_delay > MAX_INITIAL_DELAY {
            return Err(invalid(format!(
                "initial_delay must be between {} and {}",
                humantime::format_duration(Duration::ZERO),
                humantime::format_duration(MAX_INITIAL_DELAY)
            )));
        }
        if user.metrics.is_empty() {
            return Err(invalid("metrics must contain at least one entry"));
        }
        if user.perfcounters.is_empty() {
            return Err(invalid("perfcounters must contain at least one entry"));
        }

        for (name, metric) in &user.metrics {
            require_name("metric name", name)?;
            require_name(&format!("metrics.{name}.description"), &metric.description)?;
            require_name(&format!("metrics.{name}.unit"), &metric.unit)?;
            let _ = metric.kind(name)?;
        }

        let mut counters = Vec::new();
        let mut referenced_metrics = HashSet::new();
        for (object_index, object) in user.perfcounters.into_iter().enumerate() {
            let object_field = format!("perfcounters[{object_index}]");
            validate_object_or_instance(&format!("{object_field}.object"), &object.object)?;
            if object.counters.is_empty() {
                return Err(invalid(format!(
                    "{object_field}.counters must contain at least one entry"
                )));
            }
            let instances = match object.instances {
                None => vec![None],
                Some(instances) => {
                    let instances = instances.into_vec();
                    if instances.is_empty() {
                        return Err(invalid(format!(
                            "{object_field}.instances must not be empty"
                        )));
                    }
                    let mut unique = HashSet::new();
                    instances
                        .into_iter()
                        .map(|instance| {
                            validate_object_or_instance(
                                &format!("{object_field}.instances"),
                                &instance,
                            )?;
                            if !unique.insert(instance.to_lowercase()) {
                                return Err(invalid(format!(
                                    "{object_field}.instances contains duplicate {instance:?}"
                                )));
                            }
                            Ok(Some(instance))
                        })
                        .collect::<Result<Vec<_>, Error>>()?
                }
            };
            for (counter_index, mapping) in object.counters.into_iter().enumerate() {
                let counter_field = format!("{object_field}.counters[{counter_index}]");
                validate_counter_name(&format!("{counter_field}.name"), &mapping.name)?;
                let metric = user.metrics.get(&mapping.metric).ok_or_else(|| {
                    invalid(format!(
                        "{counter_field}.metric references undefined metric {:?}",
                        mapping.metric
                    ))
                })?;
                let _ = referenced_metrics.insert(mapping.metric.clone());
                for instance in &instances {
                    let path = match instance {
                        Some(instance) => {
                            format!(r"\{}({})\{}", object.object, instance, mapping.name)
                        }
                        None => format!(r"\{}\{}", object.object, mapping.name),
                    };
                    counters.push(CounterConfig {
                        path,
                        name: mapping.metric.clone(),
                        unit: metric.unit.clone(),
                        description: metric.description.clone(),
                        kind: metric.kind(&mapping.metric)?,
                        attributes: mapping.attributes.clone(),
                        scale_power10: mapping.scale_power10,
                    });
                }
            }
        }
        let mut paths = HashSet::with_capacity(counters.len());
        for counter in &counters {
            validate_counter(counter)?;
            if !paths.insert(counter.path.to_lowercase()) {
                return Err(invalid(format!("duplicate counter path: {}", counter.path)));
            }
        }
        let unused = user
            .metrics
            .keys()
            .filter(|name| !referenced_metrics.contains(*name))
            .cloned()
            .collect::<Vec<_>>();
        if !unused.is_empty() {
            return Err(invalid(format!(
                "unreferenced metric definitions: {}",
                unused.join(", ")
            )));
        }
        Ok(Self {
            counters,
            collection_interval: user.collection_interval,
            initial_delay: user.initial_delay,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn metrics() -> serde_json::Value {
        json!({
            "available": {
                "description": "Available physical memory.",
                "unit": "By",
                "gauge": {}
            },
            "private": {
                "description": "Committed private memory.",
                "unit": "By",
                "up_down_counter": {}
            }
        })
    }

    fn config_with_timing(collection_interval: &str, initial_delay: &str) -> serde_json::Value {
        json!({
            "metrics": {
                "available": {
                    "description": "Available physical memory.",
                    "unit": "By",
                    "gauge": {}
                }
            },
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": "available"}]
            }],
            "collection_interval": collection_interval,
            "initial_delay": initial_delay
        })
    }

    fn assert_config_error(value: serde_json::Value, expected: &str) {
        let error = RuntimeConfig::from_json(&value).unwrap_err().to_string();
        assert!(error.contains(expected), "unexpected error: {error}");
    }

    /// Scenario: Exact object counters use no instance or explicitly named instances.
    /// Guarantees: Structured configuration normalizes paths and retains both metric kinds.
    #[test]
    fn normalizes_exact_configuration() {
        let config = RuntimeConfig::from_json(&json!({
            "metrics": metrics(),
            "perfcounters": [
                {
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "available"}]
                },
                {
                    "object": "Process",
                    "instances": ["app", "worker"],
                    "counters": [{"name": "Private Bytes", "metric": "private"}]
                }
            ]
        }))
        .unwrap();
        assert_eq!(config.counters.len(), 3);
        assert_eq!(config.counters[0].path, r"\Memory\Available Bytes");
        assert_eq!(config.counters[0].kind, MetricKind::Gauge);
        assert_eq!(config.counters[1].path, r"\Process(app)\Private Bytes");
        assert_eq!(config.counters[1].kind, MetricKind::UpDownCounter);
        assert_eq!(config.initial_delay, Duration::from_secs(1));
    }

    /// Scenario: An object specifies one exact instance as a scalar string.
    /// Guarantees: The shorthand normalizes to the same exact instance path as a one-item list.
    #[test]
    fn normalizes_single_instance_string() {
        let config = RuntimeConfig::from_json(&json!({
            "metrics": {"private": {
                "description": "Committed private memory.",
                "unit": "By",
                "up_down_counter": {}
            }},
            "perfcounters": [{
                "object": "Process",
                "instances": "app",
                "counters": [{"name": "Private Bytes", "metric": "private"}]
            }]
        }))
        .unwrap();
        assert_eq!(config.counters[0].path, r"\Process(app)\Private Bytes");
    }

    /// Scenario: Collection timing is configured at, below, and above its supported boundaries.
    /// Guarantees: Exact boundary values succeed while out-of-range values identify their field.
    #[test]
    fn enforces_collection_timing_bounds() {
        let _config = RuntimeConfig::from_json(&config_with_timing("24h", "24h")).unwrap();

        assert_config_error(config_with_timing("500ms", "0s"), "collection_interval");
        assert_config_error(config_with_timing("86401s", "0s"), "collection_interval");
        assert_config_error(config_with_timing("1s", "86401s"), "initial_delay");
    }

    /// Scenario: A configured instance uses a wildcard in this exact-path receiver.
    /// Guarantees: Validation rejects wildcard expansion until that behavior is supported.
    #[test]
    fn rejects_wildcard_instances() {
        assert_config_error(
            json!({
                "metrics": metrics(),
                "perfcounters": [{
                    "object": "Process",
                    "instances": "*",
                    "counters": [{"name": "Private Bytes", "metric": "private"}]
                }]
            }),
            "contains a reserved performance-counter path character",
        );
    }

    /// Scenario: A metric definition selects both supported metric kinds.
    /// Guarantees: Validation rejects ambiguous projection semantics.
    #[test]
    fn rejects_ambiguous_metric_kind() {
        assert_config_error(
            json!({
                "metrics": {"value": {
                    "description": "Value.",
                    "unit": "1",
                    "gauge": {},
                    "up_down_counter": {}
                }},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "value"}]
                }]
            }),
            "metrics.value must contain exactly one of gauge or up_down_counter",
        );
    }

    /// Scenario: A metric definition is not referenced by any configured counter.
    /// Guarantees: Validation rejects metadata that cannot produce telemetry.
    #[test]
    fn rejects_unreferenced_metric_definition() {
        assert_config_error(
            json!({
                "metrics": metrics(),
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "available"}]
                }]
            }),
            "unreferenced metric definitions: private",
        );
    }

    /// Scenario: A counter mapping names a metric that is not defined.
    /// Guarantees: Validation identifies the public YAML field that contains the bad reference.
    #[test]
    fn rejects_undefined_metric_reference() {
        assert_config_error(
            json!({
                "metrics": {"available": {
                    "description": "Available physical memory.",
                    "unit": "By",
                    "gauge": {}
                }},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "missing"}]
                }]
            }),
            "perfcounters[0].counters[0].metric references undefined metric \"missing\"",
        );
    }

    /// Scenario: A configured data-point attribute uses the receiver-owned namespace.
    /// Guarantees: Validation preserves ownership of generated performance-counter attributes.
    #[test]
    fn rejects_reserved_attribute_prefix() {
        assert_config_error(
            json!({
                "metrics": {"available": {
                    "description": "Available physical memory.",
                    "unit": "By",
                    "gauge": {}
                }},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{
                        "name": "Available Bytes",
                        "metric": "available",
                        "attributes": {"windows.perf_counter.custom": "value"}
                    }]
                }]
            }),
            "counter path \"\\\\Memory\\\\Available Bytes\" attribute",
        );
    }

    /// Scenario: Decimal scaling exceeds the receiver's exact integer-power range.
    /// Guarantees: Validation rejects both lower and upper out-of-range values with the path.
    #[test]
    fn rejects_out_of_range_scaling() {
        for scale_power10 in [-19, 19] {
            assert_config_error(
                json!({
                    "metrics": {"available": {
                        "description": "Available physical memory.",
                        "unit": "By",
                        "gauge": {}
                    }},
                    "perfcounters": [{
                        "object": "Memory",
                        "counters": [{
                            "name": "Available Bytes",
                            "metric": "available",
                            "scale_power10": scale_power10
                        }]
                    }]
                }),
                "counter path \"\\\\Memory\\\\Available Bytes\" scale_power10",
            );
        }
    }

    /// Scenario: Required metric and performance-counter collections are empty.
    /// Guarantees: Validation rejects configurations that cannot emit any telemetry.
    #[test]
    fn rejects_empty_required_collections() {
        assert_config_error(
            json!({"metrics": {}, "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": "available"}]
            }]}),
            "metrics must contain at least one entry",
        );
        assert_config_error(
            json!({"metrics": {"available": {
                "description": "Available physical memory.",
                "unit": "By",
                "gauge": {}
            }}, "perfcounters": []}),
            "perfcounters must contain at least one entry",
        );
    }

    /// Scenario: Object and counter fields contain path delimiters or wildcard characters.
    /// Guarantees: Structural characters are rejected while parentheses remain valid in counter names.
    #[test]
    fn validates_reserved_path_characters_by_field_role() {
        assert_config_error(
            json!({
                "metrics": {"available": {
                    "description": "Available physical memory.",
                    "unit": "By",
                    "gauge": {}
                }},
                "perfcounters": [{
                    "object": "Memory(test)",
                    "counters": [{"name": "Available Bytes", "metric": "available"}]
                }]
            }),
            "perfcounters[0].object contains a reserved",
        );
        assert_config_error(
            json!({
                "metrics": {"available": {
                    "description": "Available physical memory.",
                    "unit": "By",
                    "gauge": {}
                }},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available *", "metric": "available"}]
                }]
            }),
            "perfcounters[0].counters[0].name contains a reserved",
        );
        let config = RuntimeConfig::from_json(&json!({
            "metrics": {"available": {
                "description": "Available physical memory.",
                "unit": "By",
                "gauge": {}
            }},
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available (Bytes)", "metric": "available"}]
            }]
        }))
        .unwrap();
        assert_eq!(config.counters[0].path, r"\Memory\Available (Bytes)");
    }

    /// Scenario: Configuration contains a field not defined by the receiver contract.
    /// Guarantees: Deserialization rejects misspelled or unsupported fields instead of ignoring them.
    #[test]
    fn rejects_unknown_fields() {
        let mut value = config_with_timing("30s", "1s");
        value["unknown"] = json!(true);
        assert_config_error(value, "unknown field `unknown`");
    }

    /// Scenario: The number of metric definitions exceeds the OTAP metric identifier domain.
    /// Guarantees: Validation rejects the count before a receiver can fail on every scrape.
    #[test]
    fn rejects_too_many_metric_definitions() {
        assert!(validate_metric_count(MAX_METRICS).is_ok());
        assert!(validate_metric_count(MAX_METRICS + 1).is_err());
    }

    /// Scenario: Explicit instance names differ only by letter casing.
    /// Guarantees: Validation rejects duplicate instance paths before opening a PDH query.
    #[test]
    fn rejects_duplicate_instances_case_insensitively() {
        assert_config_error(
            json!({
                "metrics": {"available": {
                    "description": "Available physical memory.",
                    "unit": "By",
                    "gauge": {}
                }},
                "perfcounters": [{
                    "object": "Process",
                    "instances": ["app", "APP"],
                    "counters": [{"name": "Private Bytes", "metric": "available"}]
                }]
            }),
            "perfcounters[0].instances contains duplicate \"APP\"",
        );
    }

    /// Scenario: Object and counter names normalize to the same path with different casing.
    /// Guarantees: Validation rejects duplicate native counter handles.
    #[test]
    fn rejects_duplicate_counter_paths_case_insensitively() {
        assert_config_error(
            json!({
                "metrics": {"available": {
                    "description": "Available physical memory.",
                    "unit": "By",
                    "gauge": {}
                }},
                "perfcounters": [
                    {
                        "object": "Memory",
                        "counters": [{"name": "Available Bytes", "metric": "available"}]
                    },
                    {
                        "object": "memory",
                        "counters": [{"name": "available bytes", "metric": "available"}]
                    }
                ]
            }),
            "duplicate counter path: \\memory\\available bytes",
        );
    }
}
