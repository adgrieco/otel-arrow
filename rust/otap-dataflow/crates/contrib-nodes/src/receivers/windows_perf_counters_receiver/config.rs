// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_config::error::Error;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

pub(super) const MIN_SCALE_POWER10: i32 = -18;
pub(super) const MAX_SCALE_POWER10: i32 = 18;

const MAX_COUNTERS: usize = 256;
const MAX_METRIC_NAME_LEN: usize = 255;
const MAX_METRIC_UNIT_LEN: usize = 63;
const MAX_INSTANCE_LIMIT: usize = 16_384;
const MAX_COUNTER_PATH_LEN: usize = 2_047;
const DEFAULT_MAX_INSTANCES_PER_WILDCARD: usize = 256;
const DEFAULT_MAX_EXPANDED_COUNTERS: usize = 4_096;
const DEFAULT_AGGREGATION_NAME: &str = "_Total";
const RECEIVER_ATTRIBUTE_PREFIX: &str = "windows.perf_counter.";

/// One normalized counter consumed by the existing PDH worker and OTAP builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CounterConfig {
    /// PDH path, optionally wildcarding only the instance segment.
    pub(super) path: String,
    /// OTel metric name.
    pub(super) name: String,
    /// OTel metric unit.
    pub(super) unit: String,
    /// OTel metric description, shared by every counter mapped to this metric.
    pub(super) description: Arc<str>,
    /// OTel metric stream kind.
    pub(super) metric_kind: MetricKind,
    /// Static attributes shared by paths expanded from one counter mapping.
    pub(super) attributes: Arc<BTreeMap<String, String>>,
    /// Provider aggregation instance omitted from this wildcard, when any.
    pub(super) excluded_aggregation_instance: Option<String>,
    /// Base-10 scaling applied after PDH calculates the native value.
    pub(super) scale_power10: i32,
}

/// Supported OTel metric stream kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetricKind {
    /// A point-in-time Gauge.
    Gauge,
    /// A cumulative non-monotonic Sum produced by an UpDownCounter.
    UpDownCounter,
}

/// Configuration normalized for Windows performance-counter collection.
#[derive(Debug, Clone)]
pub(super) struct Config {
    /// Exact or instance-wildcard counters to collect.
    pub(super) counters: Vec<CounterConfig>,
    /// Time between collections; defaults to 30 seconds.
    pub(super) collection_interval: Duration,
    /// Delay before the first collection request; defaults to one second.
    pub(super) initial_delay: Duration,
    /// Time between wildcard discovery refreshes; defaults to the collection interval.
    pub(super) wildcard_refresh_interval: Option<Duration>,
    /// Maximum expanded instances retained for one wildcard path.
    pub(super) max_instances_per_wildcard: usize,
    /// Maximum expanded wildcard counters retained by this receiver.
    pub(super) max_expanded_counters: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UserConfig {
    metrics: BTreeMap<String, MetricConfig>,
    perfcounters: Vec<ObjectConfig>,
    #[serde(default = "default_interval", with = "humantime_serde")]
    collection_interval: Duration,
    #[serde(default = "default_initial_delay", with = "humantime_serde")]
    initial_delay: Duration,
    #[serde(default, with = "humantime_serde")]
    wildcard_refresh_interval: Option<Duration>,
    #[serde(default = "default_max_instances_per_wildcard")]
    max_instances_per_wildcard: usize,
    #[serde(default = "default_max_expanded_counters")]
    max_expanded_counters: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricConfig {
    description: String,
    unit: String,
    #[serde(default, deserialize_with = "present_marker")]
    gauge: Option<EmptyConfig>,
    #[serde(default, deserialize_with = "present_marker")]
    up_down_counter: Option<EmptyConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyConfig {}

fn present_marker<'de, D>(deserializer: D) -> Result<Option<EmptyConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Some(
        Option::<EmptyConfig>::deserialize(deserializer)?.unwrap_or_default(),
    ))
}

impl MetricConfig {
    fn kind(&self, name: &str) -> Result<MetricKind, Error> {
        match (&self.gauge, &self.up_down_counter) {
            (Some(_), None) => Ok(MetricKind::Gauge),
            (None, Some(_)) => Ok(MetricKind::UpDownCounter),
            (None, None) => Err(invalid(format!(
                "metrics.{name} must configure gauge or up_down_counter"
            ))),
            (Some(_), Some(_)) => Err(invalid(format!(
                "metrics.{name} must configure exactly one of gauge or up_down_counter"
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
    #[serde(default)]
    aggregation_name: Option<String>,
    counters: Vec<CounterMapping>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

impl OneOrMany {
    fn as_slice(&self) -> &[String] {
        match self {
            Self::One(value) => std::slice::from_ref(value),
            Self::Many(values) => values,
        }
    }

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

fn default_max_instances_per_wildcard() -> usize {
    DEFAULT_MAX_INSTANCES_PER_WILDCARD
}

fn default_max_expanded_counters() -> usize {
    DEFAULT_MAX_EXPANDED_COUNTERS
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

fn validate_path_element<'a>(
    field: &str,
    value: &'a str,
    allow_star: bool,
) -> Result<&'a str, Error> {
    let value = value.trim();
    require_name(field, value)?;
    if value.contains('\\')
        || value.contains('(')
        || value.contains(')')
        || value.contains('?')
        || value.contains('\0')
        || (!allow_star && value.contains('*'))
    {
        return Err(invalid(format!(
            "{field} contains a reserved performance-counter path character"
        )));
    }
    Ok(value)
}

/// Canonicalizes PDH's decimal instance index, where the first occurrence omits `#0`.
fn normalize_instance(field: &str, value: &str) -> Result<String, Error> {
    let value = validate_path_element(field, value, false)?;
    let Some((name, index)) = value.rsplit_once('#') else {
        return Ok(value.to_owned());
    };
    if index.is_empty() || !index.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(value.to_owned());
    }
    if name.is_empty() {
        return Err(invalid(format!(
            "{field} must include an instance name before its index"
        )));
    }
    let index = index.trim_start_matches('0');
    if index.is_empty() {
        Ok(name.to_owned())
    } else {
        Ok(format!("{name}#{index}"))
    }
}

fn validate_counter_name<'a>(field: &str, value: &'a str) -> Result<&'a str, Error> {
    let value = value.trim();
    require_name(field, value)?;
    if value.contains(['\\', '*', '?', '\0']) {
        return Err(invalid(format!(
            "{field} contains a reserved performance-counter path character"
        )));
    }
    Ok(value)
}

fn validate_metric_name(name: &str) -> Result<&str, Error> {
    let name = name.trim();
    require_name("metric name", name)?;
    let mut chars = name.chars();
    let valid = name.len() <= MAX_METRIC_NAME_LEN
        && chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-' | '/'));
    if !valid {
        return Err(invalid(format!(
            "metric name {name:?} must start with an ASCII letter, contain only ASCII \
             letters, digits, '_', '.', '-', or '/', and be at most {MAX_METRIC_NAME_LEN} characters"
        )));
    }
    Ok(name)
}

fn validate_metric_unit<'a>(name: &str, unit: &'a str) -> Result<&'a str, Error> {
    let unit = unit.trim();
    let field = format!("metrics.{name}.unit");
    require_name(&field, unit)?;
    if unit.len() > MAX_METRIC_UNIT_LEN || !unit.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
    {
        return Err(invalid(format!(
            "{field} must be printable ASCII and at most {MAX_METRIC_UNIT_LEN} characters"
        )));
    }
    Ok(unit)
}

fn normalize_attributes(
    field: &str,
    attributes: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, Error> {
    let mut normalized = BTreeMap::new();
    for (key, value) in attributes {
        let key = key.trim();
        require_name(&format!("{field} attribute key"), key)?;
        if key.starts_with(RECEIVER_ATTRIBUTE_PREFIX) {
            return Err(invalid(format!(
                "{field} attribute {key:?} conflicts with receiver-generated attributes"
            )));
        }
        if normalized.insert(key.to_owned(), value.clone()).is_some() {
            return Err(invalid(format!(
                "{field} contains duplicate attribute key {key:?}"
            )));
        }
    }
    Ok(normalized)
}

fn expanded_counter_count(objects: &[ObjectConfig]) -> Result<usize, Error> {
    let too_many = || {
        invalid(format!(
            "perfcounters must normalize to at most {MAX_COUNTERS} counter paths"
        ))
    };
    let mut total = 0usize;
    for object in objects {
        let paths_per_counter = match &object.instances {
            None => 1,
            Some(instances)
                if instances
                    .as_slice()
                    .iter()
                    .any(|instance| instance.trim() == "*") =>
            {
                1
            }
            Some(instances) => instances.as_slice().len(),
        };
        let paths = paths_per_counter
            .checked_mul(object.counters.len())
            .ok_or_else(too_many)?;
        total = total.checked_add(paths).ok_or_else(too_many)?;
        if total > MAX_COUNTERS {
            return Err(too_many());
        }
    }
    Ok(total)
}

struct NormalizedMetric {
    description: Arc<str>,
    unit: String,
    kind: MetricKind,
}

fn validate_counter(counter: &CounterConfig, index: usize) -> Result<(), Error> {
    if counter.path.encode_utf16().count() > MAX_COUNTER_PATH_LEN {
        return Err(invalid(format!(
            "normalized counter {index} path must contain at most {MAX_COUNTER_PATH_LEN} UTF-16 code units"
        )));
    }
    if !(MIN_SCALE_POWER10..=MAX_SCALE_POWER10).contains(&counter.scale_power10) {
        return Err(invalid(format!(
            "normalized counter {index} scale_power10 must be between {MIN_SCALE_POWER10} and {MAX_SCALE_POWER10}"
        )));
    }
    Ok(())
}

impl Config {
    /// Effective wildcard refresh interval.
    #[must_use]
    pub(super) fn wildcard_refresh_interval(&self) -> Duration {
        self.wildcard_refresh_interval
            .unwrap_or(self.collection_interval)
    }

    /// Parse, validate, and normalize the public configuration contract.
    pub(super) fn from_json(value: &serde_json::Value) -> Result<Self, Error> {
        let user: UserConfig =
            serde_json::from_value(value.clone()).map_err(|err| invalid(err.to_string()))?;
        if !(Duration::from_secs(1)..=Duration::from_secs(86400))
            .contains(&user.collection_interval)
        {
            return Err(invalid("collection_interval must be between 1s and 24h"));
        }
        if user.initial_delay > Duration::from_secs(86400) {
            return Err(invalid("initial_delay must be between 0s and 24h"));
        }
        if let Some(refresh_interval) = user.wildcard_refresh_interval
            && (!(Duration::from_secs(1)..=Duration::from_secs(86400)).contains(&refresh_interval)
                || refresh_interval < user.collection_interval)
        {
            return Err(invalid(
                "wildcard_refresh_interval must be between collection_interval and 24h",
            ));
        }
        if user.metrics.is_empty() {
            return Err(invalid("metrics must contain at least one entry"));
        }
        if user.metrics.len() > MAX_COUNTERS {
            return Err(invalid(format!(
                "metrics must contain at most {MAX_COUNTERS} entries"
            )));
        }
        if user.perfcounters.is_empty() {
            return Err(invalid("perfcounters must contain at least one entry"));
        }
        if !(1..=MAX_INSTANCE_LIMIT).contains(&user.max_instances_per_wildcard) {
            return Err(invalid(format!(
                "max_instances_per_wildcard must be between 1 and {MAX_INSTANCE_LIMIT}"
            )));
        }
        if !(1..=MAX_INSTANCE_LIMIT).contains(&user.max_expanded_counters) {
            return Err(invalid(format!(
                "max_expanded_counters must be between 1 and {MAX_INSTANCE_LIMIT}"
            )));
        }
        if user.max_instances_per_wildcard > user.max_expanded_counters {
            return Err(invalid(
                "max_instances_per_wildcard must not exceed max_expanded_counters",
            ));
        }

        let mut metrics = BTreeMap::new();
        let mut metric_identities = HashSet::new();
        for (name, metric) in &user.metrics {
            let name = validate_metric_name(name)?;
            if !metric_identities.insert(name.to_ascii_lowercase()) {
                return Err(invalid(format!("metrics contains duplicate {name:?}")));
            }
            let description = metric.description.trim();
            require_name(&format!("metrics.{name}.description"), description)?;
            let unit = validate_metric_unit(name, &metric.unit)?;
            let _ = metrics.insert(
                name.to_owned(),
                NormalizedMetric {
                    description: Arc::from(description),
                    unit: unit.to_owned(),
                    kind: metric.kind(name)?,
                },
            );
        }

        let expanded_count = expanded_counter_count(&user.perfcounters)?;
        let mut counters = Vec::with_capacity(expanded_count);
        let mut referenced_metrics = HashSet::new();
        for (object_index, object) in user.perfcounters.into_iter().enumerate() {
            let object_field = format!("perfcounters[{object_index}]");
            let object_name =
                validate_path_element(&format!("{object_field}.object"), &object.object, false)?;
            if object.counters.is_empty() {
                return Err(invalid(format!(
                    "{object_field}.counters must contain at least one entry"
                )));
            }
            let aggregation_name = object
                .aggregation_name
                .unwrap_or_else(|| DEFAULT_AGGREGATION_NAME.to_owned());
            let aggregation_name = normalize_instance(
                &format!("{object_field}.aggregation_name"),
                &aggregation_name,
            )?;
            let instances = match object.instances.map(OneOrMany::into_vec) {
                None => None,
                Some(instances) if instances.is_empty() => {
                    return Err(invalid(format!(
                        "{object_field}.instances must not be empty"
                    )));
                }
                Some(instances) => {
                    let field = format!("{object_field}.instances");
                    let mut unique = HashSet::new();
                    let mut normalized = Vec::with_capacity(instances.len());
                    for instance in instances {
                        let instance = if instance.trim() == "*" {
                            validate_path_element(&field, &instance, true)?.to_owned()
                        } else {
                            normalize_instance(&field, &instance)?
                        };
                        if !unique.insert(instance.to_lowercase()) {
                            return Err(invalid(format!(
                                "{object_field}.instances contains duplicate {instance:?}"
                            )));
                        }
                        normalized.push(instance);
                    }
                    if normalized.iter().any(|instance| instance == "*")
                        && normalized.iter().any(|instance| {
                            instance != "*" && !instance.eq_ignore_ascii_case(&aggregation_name)
                        })
                    {
                        return Err(invalid(format!(
                            "{object_field}.instances may combine \"*\" only with aggregation_name"
                        )));
                    }
                    Some(normalized)
                }
            };

            for (counter_index, mapping) in object.counters.into_iter().enumerate() {
                let counter_field = format!("{object_field}.counters[{counter_index}]");
                let counter_name =
                    validate_counter_name(&format!("{counter_field}.name"), &mapping.name)?;
                let metric_name = mapping.metric.trim();
                let metric = metrics.get(metric_name).ok_or_else(|| {
                    invalid(format!(
                        "{counter_field}.metric references undefined metric {:?}",
                        metric_name
                    ))
                })?;
                let _ = referenced_metrics.insert(metric_name.to_owned());
                let attributes =
                    Arc::new(normalize_attributes(&counter_field, &mapping.attributes)?);
                let paths = match &instances {
                    None => vec![(format!(r"\{object_name}\{counter_name}"), None)],
                    Some(instances) if instances.iter().any(|instance| instance == "*") => {
                        let include_aggregation = instances
                            .iter()
                            .any(|instance| instance.eq_ignore_ascii_case(&aggregation_name));
                        vec![(
                            format!(r"\{object_name}(*)\{counter_name}"),
                            (!include_aggregation).then(|| aggregation_name.clone()),
                        )]
                    }
                    Some(instances) => instances
                        .iter()
                        .map(|instance| {
                            (format!(r"\{object_name}({instance})\{counter_name}"), None)
                        })
                        .collect(),
                };
                for (path, excluded_aggregation_instance) in paths {
                    counters.push(CounterConfig {
                        path,
                        name: metric_name.to_owned(),
                        unit: metric.unit.clone(),
                        description: Arc::clone(&metric.description),
                        metric_kind: metric.kind,
                        attributes: Arc::clone(&attributes),
                        excluded_aggregation_instance,
                        scale_power10: mapping.scale_power10,
                    });
                }
            }
        }
        let unused = metrics
            .keys()
            .filter(|name| !referenced_metrics.contains(*name))
            .cloned()
            .collect::<Vec<_>>();
        if !unused.is_empty() {
            return Err(invalid(format!(
                "metrics are not referenced by any counter: {}",
                unused.join(", ")
            )));
        }
        let mut paths = HashSet::with_capacity(counters.len());
        for (index, counter) in counters.iter().enumerate() {
            validate_counter(counter, index)?;
            if !paths.insert(counter.path.to_lowercase()) {
                return Err(invalid(format!("duplicate counter path: {}", counter.path)));
            }
        }

        Ok(Self {
            counters,
            collection_interval: user.collection_interval,
            initial_delay: user.initial_delay,
            wildcard_refresh_interval: user.wildcard_refresh_interval,
            max_instances_per_wildcard: user.max_instances_per_wildcard,
            max_expanded_counters: user.max_expanded_counters,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn metric() -> serde_json::Value {
        json!({
            "description": "Test counter.",
            "unit": "By",
            "gauge": {}
        })
    }

    fn up_down_counter_metric() -> serde_json::Value {
        json!({
            "description": "Committed private memory for each process instance.",
            "unit": "By",
            "up_down_counter": {}
        })
    }

    fn assert_config_error(value: serde_json::Value, expected: &str) {
        let error = Config::from_json(&value).unwrap_err().to_string();
        assert!(
            error.contains(expected),
            "expected {error:?} to contain {expected:?}"
        );
    }

    /// Scenario: Objects with no instances, named instances, and wildcards share metric metadata.
    /// Guarantees: Public configuration normalizes to exact and wildcard paths without changing metadata.
    #[test]
    fn normalizes_structured_configuration() {
        let config = Config::from_json(&json!({
            "metrics": {
                "windows.memory.available": metric(),
                "windows.process.time": {
                    "description": "Process time.",
                    "unit": "%",
                    "gauge": {}
                }
            },
            "perfcounters": [
                {
                    "object": "Memory",
                    "counters": [{
                        "name": "Available Bytes",
                        "metric": "windows.memory.available"
                    }]
                },
                {
                    "object": "Process",
                    "instances": ["*", "_Total"],
                    "counters": [
                        {
                            "name": "% Processor Time",
                            "metric": "windows.process.time",
                            "attributes": {"state": "active"}
                        },
                        {
                            "name": "% Idle Time",
                            "metric": "windows.process.time",
                            "attributes": {"state": "idle"}
                        }
                    ]
                }
            ]
        }))
        .unwrap();
        assert_eq!(config.counters.len(), 3);
        assert_eq!(config.counters[0].path, r"\Memory\Available Bytes");
        assert_eq!(config.counters[1].path, r"\Process(*)\% Processor Time");
        assert_eq!(config.counters[1].name, "windows.process.time");
        assert_eq!(config.counters[1].metric_kind, MetricKind::Gauge);
        assert_eq!(config.counters[1].attributes["state"], "active");
        assert_eq!(config.counters[2].attributes["state"], "idle");
        assert_eq!(config.counters[1].excluded_aggregation_instance, None);
        assert_eq!(config.collection_interval, Duration::from_secs(30));
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.wildcard_refresh_interval(), Duration::from_secs(30));
    }

    /// Scenario: A current additive value is configured as an UpDownCounter.
    /// Guarantees: Configuration preserves the requested non-monotonic Sum kind on every normalized path.
    #[test]
    fn normalizes_up_down_counter_configuration() {
        let config = Config::from_json(&json!({
            "metrics": {"windows.process.private": up_down_counter_metric()},
            "perfcounters": [{
                "object": "Process",
                "instances": "*",
                "counters": [{
                    "name": "Private Bytes",
                    "metric": "windows.process.private"
                }]
            }]
        }))
        .unwrap();
        assert_eq!(config.counters[0].name, "windows.process.private");
        assert_eq!(
            &*config.counters[0].description,
            "Committed private memory for each process instance."
        );
        assert_eq!(config.counters[0].metric_kind, MetricKind::UpDownCounter);
    }

    /// Scenario: A wildcard omits the default or custom aggregation instance unless selected.
    /// Guarantees: Normalization records filtering only when the aggregate was not explicitly included.
    #[test]
    fn normalizes_aggregation_selection() {
        let excluded = Config::from_json(&json!({
            "metrics": {"test": metric()},
            "perfcounters": [{
                "object": "Custom Object",
                "instances": "*",
                "aggregation_name": "_Global_",
                "counters": [{"name": "Counter", "metric": "test"}]
            }]
        }))
        .unwrap();
        assert_eq!(
            excluded.counters[0]
                .excluded_aggregation_instance
                .as_deref(),
            Some("_Global_")
        );
        let included = Config::from_json(&json!({
            "metrics": {"test": metric()},
            "perfcounters": [{
                "object": "Custom Object",
                "instances": ["*", "_Global_"],
                "aggregation_name": "_Global_",
                "counters": [{"name": "Counter", "metric": "test"}]
            }]
        }))
        .unwrap();
        assert_eq!(included.counters[0].excluded_aggregation_instance, None);
    }

    /// Scenario: Multiple named instances are selected without a wildcard.
    /// Guarantees: Each instance becomes one exact path while retaining shared metric identity.
    #[test]
    fn normalizes_named_instances() {
        let config = Config::from_json(&json!({
            "metrics": {"test": metric()},
            "perfcounters": [{
                "object": "Processor",
                "instances": ["0", "1"],
                "counters": [{"name": "% Processor Time", "metric": "test"}]
            }]
        }))
        .unwrap();
        assert_eq!(
            config
                .counters
                .iter()
                .map(|counter| counter.path.as_str())
                .collect::<Vec<_>>(),
            [
                r"\Processor(0)\% Processor Time",
                r"\Processor(1)\% Processor Time"
            ]
        );
        assert!(config.counters.iter().all(|counter| counter.name == "test"));
    }

    /// Scenario: A canonical Windows counter name contains parentheses.
    /// Guarantees: Counter-name punctuation remains valid while path separators and wildcards are rejected.
    #[test]
    fn accepts_parentheses_in_counter_names() {
        let config = Config::from_json(&json!({
            "metrics": {"test": metric()},
            "perfcounters": [{
                "object": "Object",
                "counters": [{"name": "Counter (value)", "metric": "test"}]
            }]
        }))
        .unwrap();
        assert_eq!(config.counters[0].path, r"\Object\Counter (value)");
    }

    /// Scenario: Metric definitions and path segments contain surrounding whitespace.
    /// Guarantees: Normalization trims identities consistently while preserving attribute values.
    #[test]
    fn trims_metric_metadata_attributes_and_paths() {
        let config = Config::from_json(&json!({
            "metrics": {" windows.memory.available ": {
                "description": " Available memory. ",
                "unit": " By ",
                "gauge": {}
            }},
            "perfcounters": [{
                "object": " Memory ",
                "counters": [{
                    "name": " Available Bytes ",
                    "metric": "windows.memory.available ",
                    "attributes": {" state ": " free "}
                }]
            }]
        }))
        .unwrap();
        let counter = &config.counters[0];
        assert_eq!(counter.path, r"\Memory\Available Bytes");
        assert_eq!(counter.name, "windows.memory.available");
        assert_eq!(&*counter.description, "Available memory.");
        assert_eq!(counter.unit, "By");
        assert_eq!(
            *counter.attributes,
            BTreeMap::from([("state".to_owned(), " free ".to_owned())])
        );
    }

    /// Scenario: OTel metric names or units violate their public syntax constraints.
    /// Guarantees: Invalid identities are rejected before constructing PDH paths.
    #[test]
    fn validates_metric_names_and_units() {
        let config = |name: &str, unit: &str| {
            json!({
                "metrics": {name: {"description": "Value.", "unit": unit, "gauge": {}}},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": name}]
                }]
            })
        };
        let long_name = format!("a{}", "b".repeat(MAX_METRIC_NAME_LEN));
        for name in ["1value", "value space", "\u{e9}", long_name.as_str()] {
            assert_config_error(config(name, "By"), "must start with an ASCII letter");
        }
        let long_unit = "a".repeat(MAX_METRIC_UNIT_LEN + 1);
        for unit in ["\u{b5}s", "B\ty", long_unit.as_str()] {
            assert_config_error(config("value", unit), "must be printable ASCII");
        }
        assert!(Config::from_json(&config("system.memory_usage-1/s", "{item}/s")).is_ok());
    }

    /// Scenario: More metric definitions are configured than normalized counters can reference.
    /// Guarantees: The bounded metric count fails before producing a large unused-metric error.
    #[test]
    fn bounds_metric_definitions() {
        let metrics = (0..=MAX_COUNTERS)
            .map(|index| (format!("m{index}"), metric()))
            .collect::<serde_json::Map<_, _>>();
        assert_config_error(
            json!({
                "metrics": metrics,
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "m0"}]
                }]
            }),
            "metrics must contain at most 256 entries",
        );
    }

    /// Scenario: Normalized metric names or attribute keys collide.
    /// Guarantees: Case-folded and post-trim duplicates fail instead of silently merging.
    #[test]
    fn rejects_normalized_identity_collisions() {
        let definition = json!({"description": "Value.", "unit": "By", "gauge": {}});
        assert_config_error(
            json!({
                "metrics": {"cpu": definition.clone(), " CPU": definition},
                "perfcounters": [{
                    "object": "Processor",
                    "counters": [{"name": "% Processor Time", "metric": "cpu"}]
                }]
            }),
            "metrics contains duplicate",
        );
        assert_config_error(
            json!({
                "metrics": {"test": metric()},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{
                        "name": "Available Bytes",
                        "metric": "test",
                        "attributes": {"state": "a", " state": "b"}
                    }]
                }]
            }),
            "contains duplicate attribute key",
        );
    }

    /// Scenario: A path segment contains an embedded NUL character.
    /// Guarantees: Native string truncation cannot change the validated PDH path.
    #[test]
    fn rejects_nul_in_path_segments() {
        for (field, value) in [
            ("object", "Mem\0ory"),
            ("instance", "app\0worker"),
            ("counter", "Available\0Bytes"),
        ] {
            let object = match field {
                "object" => json!({
                    "object": value,
                    "counters": [{"name": "Available Bytes", "metric": "test"}]
                }),
                "instance" => json!({
                    "object": "Process",
                    "instances": value,
                    "counters": [{"name": "Private Bytes", "metric": "test"}]
                }),
                _ => json!({
                    "object": "Memory",
                    "counters": [{"name": value, "metric": "test"}]
                }),
            };
            assert_config_error(
                json!({"metrics": {"test": metric()}, "perfcounters": [object]}),
                "contains a reserved performance-counter path character",
            );
        }
    }

    /// Scenario: Instance and counter lists multiply beyond the normalized path cap.
    /// Guarantees: The total is rejected before allocating the expanded path vector.
    #[test]
    fn bounds_expanded_counter_paths_before_materialization() {
        let object = |instances: usize, counters: usize| {
            json!({
                "object": "Process",
                "instances": (0..instances).map(|i| format!("i{i}")).collect::<Vec<_>>(),
                "counters": (0..counters)
                    .map(|i| json!({"name": format!("C{i}"), "metric": "test"}))
                    .collect::<Vec<_>>()
            })
        };
        let config =
            |perfcounters| json!({"metrics": {"test": metric()}, "perfcounters": perfcounters});
        assert_eq!(
            Config::from_json(&config(json!([object(16, 16)])))
                .unwrap()
                .counters
                .len(),
            MAX_COUNTERS
        );
        assert_config_error(
            config(json!([object(17, 16)])),
            "must normalize to at most 256 counter paths",
        );
        assert_config_error(
            config(json!([object(1, 129), object(1, 128)])),
            "must normalize to at most 256 counter paths",
        );
    }

    /// Scenario: One counter mapping expands across multiple explicit instances.
    /// Guarantees: Repeated description and attribute metadata is shared across paths.
    #[test]
    fn shares_expanded_counter_metadata() {
        let config = Config::from_json(&json!({
            "metrics": {"test": metric()},
            "perfcounters": [{
                "object": "Process",
                "instances": ["a", "b"],
                "counters": [{
                    "name": "Private Bytes",
                    "metric": "test",
                    "attributes": {"state": "used"}
                }]
            }]
        }))
        .unwrap();
        let [first, second] = config.counters.as_slice() else {
            panic!("expected two expanded counters");
        };
        assert!(Arc::ptr_eq(&first.description, &second.description));
        assert!(Arc::ptr_eq(&first.attributes, &second.attributes));
    }

    /// Scenario: Trimmed path segments create duplicate instance or counter paths.
    /// Guarantees: Duplicate detection operates on normalized, case-insensitive paths.
    #[test]
    fn rejects_duplicates_after_path_normalization() {
        assert_config_error(
            json!({
                "metrics": {"test": metric()},
                "perfcounters": [{
                    "object": "Process",
                    "instances": ["app", " APP "],
                    "counters": [{"name": "Private Bytes", "metric": "test"}]
                }]
            }),
            "instances contains duplicate",
        );
        assert_config_error(
            json!({
                "metrics": {"test": metric()},
                "perfcounters": [
                    {
                        "object": "Memory",
                        "counters": [{"name": "Available Bytes", "metric": "test"}]
                    },
                    {
                        "object": " memory ",
                        "counters": [{"name": " available bytes ", "metric": "test"}]
                    }
                ]
            }),
            "duplicate counter path",
        );
    }

    /// Scenario: Explicit instance indexes use omitted, zero, or leading-zero spellings.
    /// Guarantees: Equivalent PDH indexes normalize before duplicate and path checks.
    #[test]
    fn canonicalizes_explicit_instance_indexes() {
        for instances in [["app", "app#0"], ["app#1", "app#01"]] {
            assert_config_error(
                json!({
                    "metrics": {"test": metric()},
                    "perfcounters": [{
                        "object": "Process",
                        "instances": instances,
                        "counters": [{"name": "Private Bytes", "metric": "test"}]
                    }]
                }),
                "instances contains duplicate",
            );
        }

        let config = Config::from_json(&json!({
            "metrics": {"test": metric()},
            "perfcounters": [{
                "object": "Process",
                "instances": "app#001",
                "counters": [{"name": "Private Bytes", "metric": "test"}]
            }]
        }))
        .unwrap();
        assert_eq!(config.counters[0].path, r"\Process(app#1)\Private Bytes");

        for instance in ["#0", "#1"] {
            assert_config_error(
                json!({
                    "metrics": {"test": metric()},
                    "perfcounters": [{
                        "object": "Process",
                        "instances": instance,
                        "counters": [{"name": "Private Bytes", "metric": "test"}]
                    }]
                }),
                "must include an instance name before its index",
            );
        }
    }

    /// Scenario: YAML shorthand selects a metric kind with a null value.
    /// Guarantees: A present `gauge:` or `up_down_counter:` key remains selected.
    #[test]
    fn accepts_null_metric_kind_shorthand() {
        for kind in ["gauge", "up_down_counter"] {
            let mut definition = json!({"description": "Value.", "unit": "1"});
            definition[kind] = serde_json::Value::Null;
            let config = Config::from_json(&json!({
                "metrics": {"test": definition},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "test"}]
                }]
            }))
            .unwrap();
            assert_eq!(
                config.counters[0].metric_kind,
                if kind == "gauge" {
                    MetricKind::Gauge
                } else {
                    MetricKind::UpDownCounter
                }
            );
        }
    }

    /// Scenario: Invalid metadata, references, path elements, attributes, limits, or metric types are used.
    /// Guarantees: Invalid configuration fails before any PDH handles are opened.
    #[test]
    fn rejects_invalid_configuration() {
        let valid = json!({
            "metrics": {"test": metric()},
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": "test"}]
            }]
        });
        for value in [
            json!({}),
            json!({"metrics": {}, "perfcounters": []}),
            json!({
                "metrics": {"test": metric()},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "missing"}]
                }]
            }),
            json!({
                "metrics": {"test": {"description": "x", "unit": "1", "sum": {}}},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "test"}]
                }]
            }),
            json!({
                "metrics": {"test": {"description": "x", "unit": "1"}},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "test"}]
                }]
            }),
            json!({
                "metrics": {
                    "test": {
                        "description": "x",
                        "unit": "1",
                        "gauge": {},
                        "up_down_counter": {}
                    }
                },
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "test"}]
                }]
            }),
            json!({
                "metrics": {"test": metric()},
                "perfcounters": [{
                    "object": "Process",
                    "instances": ["*", "worker"],
                    "counters": [{"name": "Private Bytes", "metric": "test"}]
                }]
            }),
            json!({
                "metrics": {"test": metric()},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{
                        "name": "Available Bytes",
                        "metric": "test",
                        "attributes": {"windows.perf_counter.path": "override"}
                    }]
                }]
            }),
            json!({
                "metrics": {"test": metric()},
                "perfcounters": [{
                    "object": "Mem\\ory",
                    "counters": [{"name": "Available Bytes", "metric": "test"}]
                }]
            }),
            json!({
                "metrics": {"test": metric()},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{
                        "name": "Available Bytes",
                        "metric": "test",
                        "scale_power10": 19
                    }]
                }]
            }),
            json!({
                "metrics": {"unused": metric(), "test": metric()},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "test"}]
                }]
            }),
        ] {
            assert!(Config::from_json(&value).is_err(), "{value}");
        }
        for (key, value) in [
            ("collection_interval", json!("0s")),
            ("initial_delay", json!("25h")),
            ("wildcard_refresh_interval", json!("25h")),
            ("max_instances_per_wildcard", json!(0)),
            ("max_expanded_counters", json!(16_385)),
        ] {
            let mut invalid = valid.clone();
            invalid[key] = value;
            assert!(Config::from_json(&invalid).is_err(), "{invalid}");
        }
    }
}
