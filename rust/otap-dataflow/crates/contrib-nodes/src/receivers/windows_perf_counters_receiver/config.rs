// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_config::error::Error;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

pub(super) const MIN_SCALE_POWER10: i32 = -18;
pub(super) const MAX_SCALE_POWER10: i32 = 18;

const MAX_CONFIGURED_COUNTER_PATHS: usize = 256;
const MAX_INSTANCE_LIMIT: usize = 16_384;
const MAX_COUNTER_PATH_LEN: usize = 2_047;
const DEFAULT_MAX_INSTANCES_PER_WILDCARD: usize = 256;
const DEFAULT_MAX_EXPANDED_COUNTERS: usize = 4_096;
const DEFAULT_AGGREGATION_NAME: &str = "_Total";
const RECEIVER_ATTRIBUTE_PREFIX: &str = "windows.perf_counter.";

/// Configuration normalized for Windows performance-counter collection.
#[derive(Debug, Clone)]
pub(super) struct Config {
    /// Exact or instance-wildcard counters to collect.
    pub(super) counters: Vec<CounterConfig>,
    /// Time between collections; defaults to one minute.
    pub(super) collection_interval: Duration,
    /// Delay before the first collection request; defaults to one second.
    pub(super) initial_delay: Duration,
    /// Maximum wait for one scrape; `None` disables the deadline.
    pub(super) timeout: Option<Duration>,
    /// Time between wildcard discovery refreshes; defaults to the collection interval.
    pub(super) wildcard_refresh_interval: Option<Duration>,
    /// Maximum expanded instances retained for one wildcard path.
    pub(super) max_instances_per_wildcard: usize,
    /// Maximum expanded wildcard counters retained by this receiver.
    pub(super) max_expanded_counters: usize,
}

fn should_warn_instance_override(
    instances: Option<&[String]>,
    attributes: &BTreeMap<String, String>,
) -> bool {
    attributes.contains_key("instance")
        && instances.is_some_and(|instances| {
            instances.len() > 1 || instances.iter().any(|instance| instance.contains('*'))
        })
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
        if user.collection_interval.is_zero() {
            return Err(invalid("collection_interval must be greater than zero"));
        }
        if let Some(refresh_interval) = user.wildcard_refresh_interval
            && refresh_interval < user.collection_interval
        {
            return Err(invalid(
                "wildcard_refresh_interval must be at least collection_interval",
            ));
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
            if !metric_identities.insert(name.to_owned()) {
                return Err(invalid(format!("metrics contains duplicate {name:?}")));
            }
            let description = metric.description.trim();
            let unit = if metric.unit.trim().is_empty() {
                "1"
            } else {
                metric.unit.trim()
            };
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
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_AGGREGATION_NAME.to_owned());
            let aggregation_name = normalize_instance(
                &format!("{object_field}.aggregation_name"),
                &aggregation_name,
                false,
            )?;
            let instances = match object.instances.map(OneOrMany::into_vec) {
                None => None,
                Some(instances) if instances.is_empty() => None,
                Some(instances) => {
                    let field = format!("{object_field}.instances");
                    let mut unique = HashSet::new();
                    let mut normalized = Vec::with_capacity(instances.len());
                    for instance in instances {
                        let instance = if instance.trim() == "*" {
                            validate_path_element(&field, &instance, true)?.to_owned()
                        } else {
                            normalize_instance(&field, &instance, instance.contains('*'))?
                        };
                        if !unique.insert(instance.to_lowercase()) {
                            return Err(invalid(format!(
                                "{object_field}.instances contains duplicate {instance:?}"
                            )));
                        }
                        normalized.push(instance);
                    }
                    Some(normalized)
                }
            };

            for (counter_index, mapping) in object.counters.into_iter().enumerate() {
                let counter_field = format!("{object_field}.counters[{counter_index}]");
                let counter_name =
                    validate_counter_name(&format!("{counter_field}.name"), &mapping.name)?;
                if mapping.recreate_query {
                    return Err(invalid(format!(
                        "{counter_field}.recreate_query is not supported yet; omit it or set it to false"
                    )));
                }
                let metric_name = mapping
                    .metric
                    .as_deref()
                    .map(str::trim)
                    .filter(|name| !name.is_empty());
                let explicit_metric = if let Some(metric_name) = metric_name {
                    let metric = metrics.get(metric_name).ok_or_else(|| {
                        invalid(format!(
                            "{counter_field}.metric references undefined metric {:?}",
                            metric_name
                        ))
                    })?;
                    let attributes = normalize_attributes(&counter_field, &mapping.attributes)?;
                    if should_warn_instance_override(instances.as_deref(), &attributes) {
                        otel_arrow_dfe_telemetry::otel_warn!(
                            "otelcol.node.windowsperfcounters.config.instance_override",
                            message = "a static instance attribute overrides generated instance \
                                       identity for multiple or wildcard instances and may merge \
                                       distinct series",
                            object_index = object_index,
                            counter_index = counter_index
                        );
                    }
                    Some((
                        metric_name.to_owned(),
                        metric.unit.clone(),
                        Arc::clone(&metric.description),
                        metric.kind,
                        Arc::new(attributes),
                    ))
                } else {
                    None
                };
                let paths = match &instances {
                    None => vec![(
                        format!(r"\{object_name}\{counter_name}"),
                        None,
                        false,
                        false,
                    )],
                    Some(instances) if instances.iter().any(|instance| instance == "*") => {
                        let include_aggregation = instances
                            .iter()
                            .any(|instance| instance.eq_ignore_ascii_case(&aggregation_name));
                        vec![(
                            format!(r"\{object_name}(*)\{counter_name}"),
                            (!include_aggregation).then(|| aggregation_name.clone()),
                            true,
                            true,
                        )]
                    }
                    Some(instances) => instances
                        .iter()
                        .map(|instance| {
                            let is_exact_aggregation = !instance.contains('*')
                                && instance.eq_ignore_ascii_case(&aggregation_name);
                            (
                                format!(r"\{object_name}({instance})\{counter_name}"),
                                instance.contains('*').then(|| aggregation_name.clone()),
                                true,
                                !is_exact_aggregation,
                            )
                        })
                        .collect(),
                };
                for (path, excluded_aggregation_instance, has_instance, emit_instance_attribute) in
                    paths
                {
                    let (name, unit, description, metric_kind, attributes) =
                        if let Some((name, unit, description, metric_kind, attributes)) =
                            &explicit_metric
                        {
                            (
                                name.clone(),
                                unit.clone(),
                                Arc::clone(description),
                                *metric_kind,
                                Arc::clone(attributes),
                            )
                        } else {
                            if metrics.contains_key(&path) {
                                return Err(invalid(format!(
                                    "{counter_field} implicit path-named Gauge {path:?} conflicts \
                                     with an explicitly defined metric of the same name"
                                )));
                            }
                            (
                                path.clone(),
                                "1".to_owned(),
                                Arc::from(""),
                                MetricKind::Gauge,
                                Arc::new(BTreeMap::new()),
                            )
                        };
                    counters.push(CounterConfig {
                        path,
                        name,
                        unit,
                        description,
                        metric_kind,
                        attributes,
                        has_instance,
                        emit_instance_attribute,
                        excluded_aggregation_instance,
                        scale_power10: mapping.scale_power10,
                    });
                }
            }
        }
        let mut output_identities = HashSet::with_capacity(counters.len());
        for (index, counter) in counters.iter().enumerate() {
            validate_counter(counter, index)?;
            let identity = (
                counter.path.to_lowercase(),
                counter.name.clone(),
                counter
                    .attributes
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<Vec<_>>(),
            );
            if !output_identities.insert(identity) {
                return Err(invalid(format!(
                    "duplicate counter output mapping for path {} and metric {}",
                    counter.path, counter.name
                )));
            }
        }

        Ok(Self {
            counters,
            collection_interval: user.collection_interval,
            initial_delay: user.initial_delay,
            timeout: match user.timeout {
                Some(Duration::ZERO) => None,
                Some(timeout) => Some(timeout),
                None => Some(user.collection_interval),
            },
            wildcard_refresh_interval: user.wildcard_refresh_interval,
            max_instances_per_wildcard: user.max_instances_per_wildcard,
            max_expanded_counters: user.max_expanded_counters,
        })
    }
}

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
    /// Whether the configured PDH path contains an instance segment.
    pub(super) has_instance: bool,
    /// Whether to emit Go's `instance` attribute for parsed instance identity.
    pub(super) emit_instance_attribute: bool,
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
    /// A Sum with configured aggregation temporality and monotonicity.
    Sum {
        aggregation: Aggregation,
        monotonic: bool,
    },
}

/// OTel Sum aggregation temporality accepted by the configuration parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Aggregation {
    Cumulative,
    Delta,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UserConfig {
    #[serde(default)]
    metrics: BTreeMap<String, MetricConfig>,
    perfcounters: Vec<ObjectConfig>,
    #[serde(default = "default_interval", with = "humantime_serde")]
    collection_interval: Duration,
    #[serde(
        default = "default_initial_delay",
        deserialize_with = "deserialize_initial_delay"
    )]
    initial_delay: Duration,
    #[serde(default, with = "humantime_serde")]
    timeout: Option<Duration>,
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
    #[serde(default)]
    description: String,
    #[serde(default)]
    unit: String,
    #[serde(default, deserialize_with = "present_marker")]
    gauge: Option<EmptyConfig>,
    #[serde(default, deserialize_with = "present_sum")]
    sum: Option<SumConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyConfig {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SumConfig {
    aggregation: Aggregation,
    #[serde(default)]
    monotonic: bool,
}

fn present_sum<'de, D>(deserializer: D) -> Result<Option<SumConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<SumConfig>::deserialize(deserializer)?
        .ok_or_else(|| {
            serde::de::Error::custom(
                "sum must be a mapping with explicit aggregation: cumulative or delta; null Sum is invalid",
            )
        })
        .map(Some)
}

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
        if self.gauge.is_some() && self.sum.is_some() {
            return Err(invalid(format!(
                "metrics.{name} must not contain both gauge and sum"
            )));
        }
        if let Some(sum) = &self.sum {
            Ok(MetricKind::Sum {
                aggregation: sum.aggregation,
                monotonic: sum.monotonic,
            })
        } else {
            Ok(MetricKind::Gauge)
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
    #[serde(default)]
    metric: Option<String>,
    #[serde(default)]
    recreate_query: bool,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    #[serde(default)]
    scale_power10: i32,
}

fn default_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_initial_delay() -> Duration {
    Duration::from_secs(1)
}

fn deserialize_initial_delay<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    let value = value.trim();
    if let Some(magnitude) = value.strip_prefix('-') {
        let _ = humantime::parse_duration(magnitude).map_err(serde::de::Error::custom)?;
        Ok(Duration::ZERO)
    } else {
        humantime::parse_duration(value).map_err(serde::de::Error::custom)
    }
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
fn normalize_instance(field: &str, value: &str, allow_star: bool) -> Result<String, Error> {
    let value = validate_path_element(field, value, allow_star)?;
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
    Ok(name)
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
            "perfcounters must normalize to at most {MAX_CONFIGURED_COUNTER_PATHS} counter paths"
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
        if total > MAX_CONFIGURED_COUNTER_PATHS {
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

    fn non_monotonic_sum_metric() -> serde_json::Value {
        json!({
            "description": "Committed private memory for each process instance.",
            "unit": "By",
            "sum": {
                "aggregation": "cumulative"
            }
        })
    }

    fn monotonic_sum_metric() -> serde_json::Value {
        json!({
            "description": "Cumulative operations since the provider sequence began.",
            "unit": "{operation}",
            "sum": {
                "aggregation": "cumulative",
                "monotonic": true
            }
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
        assert!(!config.counters[0].has_instance);
        assert!(!config.counters[0].emit_instance_attribute);
        assert!(config.counters[1].has_instance);
        assert!(config.counters[1].emit_instance_attribute);
        assert_eq!(config.counters[1].excluded_aggregation_instance, None);
        assert_eq!(config.collection_interval, Duration::from_secs(60));
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.timeout, Some(Duration::from_secs(60)));
        assert_eq!(config.wildcard_refresh_interval(), Duration::from_secs(60));
    }

    /// Scenario: Timeout is omitted, positive, zero, or negative.
    /// Guarantees: Omission follows the interval, positive values are retained, zero disables it, and negatives fail.
    #[test]
    fn normalizes_scrape_timeout() {
        let base = json!({
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes"}]
            }],
            "collection_interval": "45s"
        });

        let omitted = Config::from_json(&base).unwrap();
        assert_eq!(omitted.timeout, Some(Duration::from_secs(45)));

        let mut positive = base.clone();
        positive["timeout"] = json!("10s");
        assert_eq!(
            Config::from_json(&positive).unwrap().timeout,
            Some(Duration::from_secs(10))
        );

        let mut disabled = base.clone();
        disabled["timeout"] = json!("0s");
        assert_eq!(Config::from_json(&disabled).unwrap().timeout, None);

        let mut negative = base;
        negative["timeout"] = json!("-1s");
        assert_config_error(negative, "invalid value");
    }

    /// Scenario: Timing uses sub-second, multi-day, zero, and negative configured durations.
    /// Guarantees: Collection accepts any positive interval and non-positive initial delay is immediate.
    #[test]
    fn accepts_go_timing_range() {
        let config = |collection_interval: &str, initial_delay: &str, refresh_interval: &str| {
            Config::from_json(&json!({
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes"}]
                }],
                "collection_interval": collection_interval,
                "initial_delay": initial_delay,
                "wildcard_refresh_interval": refresh_interval
            }))
        };

        let fast = config("1ms", "-5s", "2ms").unwrap();
        assert_eq!(fast.collection_interval, Duration::from_millis(1));
        assert_eq!(fast.initial_delay, Duration::ZERO);
        assert_eq!(fast.wildcard_refresh_interval(), Duration::from_millis(2));

        let slow = config("48h", "72h", "96h").unwrap();
        assert_eq!(slow.collection_interval, Duration::from_secs(48 * 60 * 60));
        assert_eq!(slow.initial_delay, Duration::from_secs(72 * 60 * 60));
        assert_eq!(
            slow.wildcard_refresh_interval(),
            Duration::from_secs(96 * 60 * 60)
        );

        assert_config_error(
            json!({
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes"}]
                }],
                "collection_interval": "2s",
                "wildcard_refresh_interval": "1s"
            }),
            "must be at least collection_interval",
        );
    }

    /// Scenario: A cumulative Sum omits its optional monotonic setting.
    /// Guarantees: Configuration defaults the Sum to non-monotonic on every normalized path.
    #[test]
    fn defaults_sum_to_non_monotonic() {
        let config = Config::from_json(&json!({
            "metrics": {"windows.process.private": non_monotonic_sum_metric()},
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
        assert_eq!(
            config.counters[0].metric_kind,
            MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: false
            }
        );
    }

    /// Scenario: A cumulative additive value is configured as a monotonic Sum.
    /// Guarantees: Configuration preserves the requested cumulative monotonic Sum kind.
    #[test]
    fn normalizes_monotonic_sum_configuration() {
        let config = Config::from_json(&json!({
            "metrics": {"windows.operations": monotonic_sum_metric()},
            "perfcounters": [{
                "object": "Custom Provider",
                "counters": [{
                    "name": "Operations",
                    "metric": "windows.operations"
                }]
            }]
        }))
        .unwrap();
        assert_eq!(config.counters[0].name, "windows.operations");
        assert_eq!(
            config.counters[0].metric_kind,
            MetricKind::Sum {
                aggregation: Aggregation::Cumulative,
                monotonic: true
            }
        );
    }

    /// Scenario: A metric omits both `gauge` and `sum`.
    /// Guarantees: Configuration defaults the metric to Gauge.
    #[test]
    fn defaults_missing_kind_to_gauge() {
        let config = Config::from_json(&json!({
            "metrics": {
                "windows.memory.available": {
                    "description": "Available memory.",
                    "unit": "By"
                }
            },
            "perfcounters": [{
                "object": "Memory",
                "counters": [{
                    "name": "Available Bytes",
                    "metric": "windows.memory.available"
                }]
            }]
        }))
        .unwrap();
        assert_eq!(config.counters[0].metric_kind, MetricKind::Gauge);
    }

    /// Scenario: Metrics and a counter's metric reference are omitted.
    /// Guarantees: The full configured path names a default Gauge and mapping attributes are ignored.
    #[test]
    fn defaults_metricless_mapping_to_path_named_gauge() {
        let config = Config::from_json(&json!({
            "perfcounters": [{
                "object": "Process",
                "instances": "*",
                "counters": [{
                    "name": "Private Bytes",
                    "attributes": {
                        "state": "ignored",
                        "windows.perf_counter.path": "also ignored"
                    }
                }]
            }]
        }))
        .unwrap();

        let counter = &config.counters[0];
        assert_eq!(counter.path, r"\Process(*)\Private Bytes");
        assert_eq!(counter.name, counter.path);
        assert_eq!(counter.unit, "1");
        assert!(counter.description.is_empty());
        assert_eq!(counter.metric_kind, MetricKind::Gauge);
        assert!(counter.attributes.is_empty());
    }

    /// Scenario: Metric metadata fields are omitted and another definition is unused.
    /// Guarantees: Referenced metadata receives Go-compatible defaults and unused definitions are allowed.
    #[test]
    fn defaults_optional_metric_metadata_and_allows_unused_definitions() {
        let config = Config::from_json(&json!({
            "metrics": {
                "used": {"gauge": {}},
                "unused": {}
            },
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": "used"}]
            }]
        }))
        .unwrap();

        let counter = &config.counters[0];
        assert_eq!(counter.name, "used");
        assert_eq!(counter.unit, "1");
        assert!(counter.description.is_empty());
        assert_eq!(counter.metric_kind, MetricKind::Gauge);
    }

    /// Scenario: A counter supplies an empty metric name.
    /// Guarantees: Go's zero-value mapping behavior treats it as an implicit path-named Gauge.
    #[test]
    fn treats_empty_metric_name_as_omitted() {
        let config = Config::from_json(&json!({
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": "  "}]
            }]
        }))
        .unwrap();

        assert_eq!(config.counters[0].name, r"\Memory\Available Bytes");
    }

    /// Scenario: A mapping explicitly disables or enables query recreation.
    /// Guarantees: False is accepted while true fails instead of being silently ignored.
    #[test]
    fn rejects_unimplemented_query_recreation() {
        let config = |recreate_query| {
            json!({
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{
                        "name": "Available Bytes",
                        "recreate_query": recreate_query
                    }]
                }]
            })
        };

        let _ = Config::from_json(&config(false)).unwrap();
        assert_config_error(
            config(true),
            "recreate_query is not supported yet; omit it or set it to false",
        );
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
        assert!(included.counters[0].emit_instance_attribute);

        let aggregate_alone = Config::from_json(&json!({
            "metrics": {"test": metric()},
            "perfcounters": [{
                "object": "Custom Object",
                "instances": "_Global_",
                "aggregation_name": "_Global_",
                "counters": [{"name": "Counter", "metric": "test"}]
            }]
        }))
        .unwrap();
        assert!(aggregate_alone.counters[0].has_instance);
        assert!(!aggregate_alone.counters[0].emit_instance_attribute);

        let aggregate_with_peer = Config::from_json(&json!({
            "metrics": {"test": metric()},
            "perfcounters": [{
                "object": "Processor",
                "instances": ["_Total", "worker"],
                "counters": [{"name": "Counter", "metric": "test"}]
            }]
        }))
        .unwrap();
        assert_eq!(
            aggregate_with_peer
                .counters
                .iter()
                .map(|counter| (counter.path.as_str(), counter.emit_instance_attribute))
                .collect::<Vec<_>>(),
            [
                (r"\Processor(_Total)\Counter", false),
                (r"\Processor(worker)\Counter", true)
            ]
        );
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
        assert!(config.counters.iter().all(|counter| counter.has_instance));
        assert!(
            config
                .counters
                .iter()
                .all(|counter| counter.emit_instance_attribute)
        );
    }

    /// Scenario: Static `instance` attributes are configured for exact, multiple, wildcard, or ignored mappings.
    /// Guarantees: Only effective overrides that can collapse multiple selected instances request a warning.
    #[test]
    fn identifies_risky_instance_overrides() {
        let override_attributes = BTreeMap::from([("instance".to_owned(), "fixed".to_owned())]);
        let ignored_attributes = BTreeMap::new();

        assert!(!should_warn_instance_override(
            Some(&["worker".to_owned()]),
            &override_attributes
        ));
        assert!(should_warn_instance_override(
            Some(&["worker".to_owned(), "service".to_owned()]),
            &override_attributes
        ));
        assert!(should_warn_instance_override(
            Some(&["worker*".to_owned()]),
            &override_attributes
        ));
        assert!(!should_warn_instance_override(
            Some(&["*".to_owned()]),
            &ignored_attributes
        ));
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

    /// Scenario: Metric names use PDH paths or Unicode and units exceed SDK instrument guidance.
    /// Guarantees: OTLP stream identity accepts arbitrary non-empty names and unit strings.
    #[test]
    fn accepts_otlp_metric_names_and_units() {
        let config = |name: &str, unit: &str| {
            json!({
                "metrics": {name: {"description": "Value.", "unit": unit, "gauge": {}}},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": name}]
                }]
            })
        };
        let long_name = format!("metric {}", "x".repeat(512));
        let long_unit = format!("{{{}}}", "u".repeat(128));
        for (name, unit) in [
            (r"\Memory\Available Bytes", "By"),
            ("1 value", "\u{b5}s"),
            (long_name.as_str(), long_unit.as_str()),
        ] {
            let normalized = Config::from_json(&config(name, unit)).unwrap();
            assert_eq!(normalized.counters[0].name, name);
            assert_eq!(normalized.counters[0].unit, unit);
        }
        assert_config_error(config(" ", "By"), "metric name must not be empty");
    }

    /// Scenario: More than 256 metric definitions are configured but only one is referenced.
    /// Guarantees: Unused metadata definitions do not consume the configured PDH path budget.
    #[test]
    fn allows_metric_definitions_beyond_counter_path_limit() {
        let metrics = (0..=MAX_CONFIGURED_COUNTER_PATHS)
            .map(|index| (format!("m{index}"), metric()))
            .collect::<serde_json::Map<_, _>>();
        let config = Config::from_json(&json!({
            "metrics": metrics,
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": "m0"}]
            }]
        }))
        .unwrap();
        assert_eq!(config.counters.len(), 1);
    }

    /// Scenario: Normalized metric names or attribute keys collide.
    /// Guarantees: Exact post-trim duplicates fail while case-distinct OTel stream names remain distinct.
    #[test]
    fn rejects_normalized_identity_collisions() {
        let definition = json!({"description": "Value.", "unit": "By", "gauge": {}});
        assert_config_error(
            json!({
                "metrics": {"cpu": definition.clone(), " cpu ": definition},
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
        assert_config_error(
            json!({
                "metrics": {
                    r"\Memory\Available Bytes": {"unit": "By"}
                },
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes"}]
                }]
            }),
            "implicit path-named Gauge",
        );
    }

    /// Scenario: Metric names differ only by ASCII case and are referenced independently.
    /// Guarantees: OTel's case-sensitive stream identity is preserved during normalization.
    #[test]
    fn accepts_case_distinct_metric_names() {
        let config = Config::from_json(&json!({
            "metrics": {
                "cpu": {"unit": "1"},
                "CPU": {"unit": "%"}
            },
            "perfcounters": [{
                "object": "Processor",
                "counters": [
                    {"name": "% Processor Time", "metric": "cpu"},
                    {"name": "% Idle Time", "metric": "CPU"}
                ]
            }]
        }))
        .unwrap();

        assert_eq!(config.counters[0].name, "cpu");
        assert_eq!(config.counters[1].name, "CPU");
    }

    /// Scenario: Instances are empty, use a partial wildcard, or combine full wildcard with explicit names.
    /// Guarantees: Empty means no instance, partial patterns are preserved, and full wildcard follows Go selection.
    #[test]
    fn accepts_go_instance_selection_forms() {
        let empty = Config::from_json(&json!({
            "perfcounters": [{
                "object": "Memory",
                "instances": [],
                "aggregation_name": "",
                "counters": [{"name": "Available Bytes"}]
            }]
        }))
        .unwrap();
        assert_eq!(empty.counters[0].path, r"\Memory\Available Bytes");

        let partial = Config::from_json(&json!({
            "perfcounters": [{
                "object": "Process",
                "instances": ["worker*", "service"],
                "counters": [{"name": "Private Bytes"}]
            }]
        }))
        .unwrap();
        assert_eq!(
            partial
                .counters
                .iter()
                .map(|counter| counter.path.as_str())
                .collect::<Vec<_>>(),
            [
                r"\Process(worker*)\Private Bytes",
                r"\Process(service)\Private Bytes"
            ]
        );
        assert_eq!(
            partial.counters[0].excluded_aggregation_instance.as_deref(),
            Some("_Total")
        );

        let full = Config::from_json(&json!({
            "perfcounters": [{
                "object": "Process",
                "instances": ["*", "worker"],
                "counters": [{"name": "Private Bytes"}]
            }]
        }))
        .unwrap();
        assert_eq!(full.counters.len(), 1);
        assert_eq!(full.counters[0].path, r"\Process(*)\Private Bytes");
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
            MAX_CONFIGURED_COUNTER_PATHS
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

    /// Scenario: Trimmed path segments create duplicate instance or output identities.
    /// Guarantees: Duplicate detection operates on normalized, case-insensitive PDH paths.
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
            "duplicate counter output mapping",
        );
    }

    /// Scenario: One normalized PDH path feeds different metric mappings.
    /// Guarantees: Distinct output identities are retained while duplicate series mappings are rejected.
    #[test]
    fn allows_one_path_to_feed_multiple_mappings() {
        let config = Config::from_json(&json!({
            "metrics": {
                "available.bytes": {"unit": "By"},
                "available.scaled": {"unit": "KiBy"}
            },
            "perfcounters": [{
                "object": "Memory",
                "counters": [
                    {"name": "Available Bytes", "metric": "available.bytes"},
                    {
                        "name": "Available Bytes",
                        "metric": "available.scaled",
                        "scale_power10": -3
                    }
                ]
            }]
        }))
        .unwrap();

        assert_eq!(config.counters.len(), 2);
        assert_eq!(config.counters[0].path, config.counters[1].path);
        assert_eq!(config.counters[0].name, "available.bytes");
        assert_eq!(config.counters[1].name, "available.scaled");

        assert_config_error(
            json!({
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [
                        {"name": "Available Bytes"},
                        {"name": "Available Bytes"}
                    ]
                }]
            }),
            "duplicate counter output mapping",
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

    /// Scenario: YAML shorthand selects an empty Gauge with a null value.
    /// Guarantees: A present `gauge:` key remains selected.
    #[test]
    fn accepts_null_metric_kind_shorthand() {
        let config = Config::from_json(&json!({
            "metrics": {
                "test": {"description": "Value.", "unit": "1", "gauge": null}
            },
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": "test"}]
            }]
        }))
        .unwrap();
        assert_eq!(config.counters[0].metric_kind, MetricKind::Gauge);
    }

    /// Scenario: Sum is null, empty, lacks aggregation or selects an invalid aggregation.
    /// Guarantees: Sum never silently becomes Gauge or defaults its aggregation.
    #[test]
    fn rejects_missing_or_invalid_sum_aggregation() {
        for (sum, expected) in [
            (json!(null), "null Sum is invalid"),
            (json!({}), "missing field `aggregation`"),
            (json!({"monotonic": false}), "missing field `aggregation`"),
            (json!({"aggregation": null}), "expected string or map"),
            (json!({"aggregation": ""}), "unknown variant"),
            (json!({"aggregation": "invalid"}), "unknown variant"),
        ] {
            assert_config_error(
                json!({
                    "metrics": {"test": {
                        "description": "Value.",
                        "unit": "1",
                        "sum": sum
                    }},
                    "perfcounters": [{
                        "object": "Memory",
                        "counters": [{"name": "Available Bytes", "metric": "test"}]
                    }]
                }),
                expected,
            );
        }
    }

    /// Scenario: Sum selects delta temporality with either monotonicity setting.
    /// Guarantees: Configuration preserves delta aggregation for native eligibility validation.
    #[test]
    fn normalizes_delta_sum_configuration() {
        for monotonic in [false, true] {
            let config = Config::from_json(&json!({
                "metrics": {"test": {
                    "description": "Value.",
                    "unit": "1",
                    "sum": {"aggregation": "delta", "monotonic": monotonic}
                }},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "test"}]
                }]
            }))
            .unwrap();
            assert_eq!(
                config.counters[0].metric_kind,
                MetricKind::Sum {
                    aggregation: Aggregation::Delta,
                    monotonic,
                }
            );
        }
    }

    /// Scenario: A metric definition selects both Gauge and cumulative Sum.
    /// Guarantees: Conflicting metric kinds are rejected with the PR A diagnostic.
    #[test]
    fn rejects_conflicting_metric_kinds() {
        assert_config_error(
            json!({
                "metrics": {"test": {
                    "description": "Value.",
                    "unit": "1",
                    "gauge": {},
                    "sum": {"aggregation": "cumulative"}
                }},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{"name": "Available Bytes", "metric": "test"}]
                }]
            }),
            "metrics.test must not contain both gauge and sum",
        );
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
                "metrics": {"test": {"description": "x", "unit": "1", "sum": null}},
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
                        "up_down_counter": {}
                    }
                },
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
                        "sum": {
                            "aggregation": "cumulative",
                            "monotonic": true
                        }
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
        ] {
            assert!(Config::from_json(&value).is_err(), "{value}");
        }
        for (key, value) in [
            ("collection_interval", json!("0s")),
            ("max_instances_per_wildcard", json!(0)),
            ("max_expanded_counters", json!(16_385)),
        ] {
            let mut invalid = valid.clone();
            invalid[key] = value;
            assert!(Config::from_json(&invalid).is_err(), "{invalid}");
        }
    }
}
