// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_config::error::Error;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

const MAX_COUNTERS: usize = 256;
const MAX_INSTANCE_LIMIT: usize = 16_384;
const MAX_COUNTER_PATH_LEN: usize = 2_047;
const DEFAULT_MAX_INSTANCES_PER_WILDCARD: usize = 256;
const DEFAULT_MAX_EXPANDED_COUNTERS: usize = 4_096;
const DEFAULT_AGGREGATION_NAME: &str = "_Total";
const MIN_SCALE_POWER10: i32 = -18;
const MAX_SCALE_POWER10: i32 = 18;
const RECEIVER_ATTRIBUTE_PREFIX: &str = "windows.perf_counter.";

/// One normalized counter consumed by the existing PDH worker and OTAP builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CounterConfig {
    /// PDH path, optionally wildcarding only the instance segment.
    pub path: String,
    /// OTel metric name.
    pub name: String,
    /// OTel metric unit.
    pub unit: String,
    /// OTel metric description.
    pub description: String,
    /// Static attributes added to every point from this counter.
    pub attributes: BTreeMap<String, String>,
    /// Provider aggregation instance omitted from this wildcard, when any.
    pub excluded_aggregation_instance: Option<String>,
    /// Base-10 scaling applied after PDH calculates the native value.
    pub scale_power10: i32,
}

/// Configuration normalized for Windows performance-counter collection.
#[derive(Debug, Clone)]
pub struct Config {
    /// Exact or instance-wildcard counters to collect.
    pub counters: Vec<CounterConfig>,
    /// Time between collections; defaults to 30 seconds.
    pub collection_interval: Duration,
    /// Delay before the first collection request; defaults to one second.
    pub initial_delay: Duration,
    /// Time between wildcard discovery refreshes; defaults to the collection interval.
    pub wildcard_refresh_interval: Option<Duration>,
    /// Maximum expanded instances retained for one wildcard path.
    pub max_instances_per_wildcard: usize,
    /// Maximum expanded wildcard counters retained by this receiver.
    pub max_expanded_counters: usize,
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
    gauge: GaugeConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GaugeConfig {}

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

fn validate_path_element(field: &str, value: &str, allow_star: bool) -> Result<(), Error> {
    require_name(field, value)?;
    if value.contains('\\')
        || value.contains('(')
        || value.contains(')')
        || value.contains('?')
        || (!allow_star && value.contains('*'))
    {
        return Err(invalid(format!(
            "{field} contains a reserved performance-counter path character"
        )));
    }
    Ok(())
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
    for key in counter.attributes.keys() {
        require_name(&format!("normalized counter {index} attribute key"), key)?;
        if key.starts_with(RECEIVER_ATTRIBUTE_PREFIX) {
            return Err(invalid(format!(
                "attribute {key:?} conflicts with receiver-generated attributes"
            )));
        }
    }
    Ok(())
}

impl Config {
    /// Effective wildcard refresh interval.
    #[must_use]
    pub fn wildcard_refresh_interval(&self) -> Duration {
        self.wildcard_refresh_interval
            .unwrap_or(self.collection_interval)
    }

    /// Parse, validate, and normalize the public configuration contract.
    pub fn from_json(value: &serde_json::Value) -> Result<Self, Error> {
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

        for (name, metric) in &user.metrics {
            require_name("metric name", name)?;
            require_name(&format!("metrics.{name}.description"), &metric.description)?;
            require_name(&format!("metrics.{name}.unit"), &metric.unit)?;
            let _ = &metric.gauge;
        }

        let mut counters = Vec::new();
        let mut referenced_metrics = HashSet::new();
        for (object_index, object) in user.perfcounters.into_iter().enumerate() {
            let object_field = format!("perfcounters[{object_index}]");
            validate_path_element(&format!("{object_field}.object"), &object.object, false)?;
            if object.counters.is_empty() {
                return Err(invalid(format!(
                    "{object_field}.counters must contain at least one entry"
                )));
            }
            let aggregation_name = object
                .aggregation_name
                .unwrap_or_else(|| DEFAULT_AGGREGATION_NAME.to_owned());
            validate_path_element(
                &format!("{object_field}.aggregation_name"),
                &aggregation_name,
                false,
            )?;
            let instances = object.instances.map(OneOrMany::into_vec);
            if instances.as_ref().is_some_and(Vec::is_empty) {
                return Err(invalid(format!(
                    "{object_field}.instances must not be empty"
                )));
            }
            if let Some(instances) = &instances {
                let mut unique = HashSet::new();
                for instance in instances {
                    validate_path_element(
                        &format!("{object_field}.instances"),
                        instance,
                        instance == "*",
                    )?;
                    if !unique.insert(instance.to_lowercase()) {
                        return Err(invalid(format!(
                            "{object_field}.instances contains duplicate {instance:?}"
                        )));
                    }
                }
                if instances.iter().any(|instance| instance == "*")
                    && instances.iter().any(|instance| {
                        instance != "*" && !instance.eq_ignore_ascii_case(&aggregation_name)
                    })
                {
                    return Err(invalid(format!(
                        "{object_field}.instances may combine \"*\" only with aggregation_name"
                    )));
                }
            }

            for (counter_index, mapping) in object.counters.into_iter().enumerate() {
                let counter_field = format!("{object_field}.counters[{counter_index}]");
                validate_path_element(&format!("{counter_field}.name"), &mapping.name, false)?;
                let metric = user.metrics.get(&mapping.metric).ok_or_else(|| {
                    invalid(format!(
                        "{counter_field}.metric references undefined metric {:?}",
                        mapping.metric
                    ))
                })?;
                let _ = referenced_metrics.insert(mapping.metric.clone());
                let paths = match &instances {
                    None => vec![(format!(r"\{}\{}", object.object, mapping.name), None)],
                    Some(instances) if instances.iter().any(|instance| instance == "*") => {
                        let include_aggregation = instances
                            .iter()
                            .any(|instance| instance.eq_ignore_ascii_case(&aggregation_name));
                        vec![(
                            format!(r"\{}(*)\{}", object.object, mapping.name),
                            (!include_aggregation).then(|| aggregation_name.clone()),
                        )]
                    }
                    Some(instances) => instances
                        .iter()
                        .map(|instance| {
                            (
                                format!(r"\{}({})\{}", object.object, instance, mapping.name),
                                None,
                            )
                        })
                        .collect(),
                };
                for (path, excluded_aggregation_instance) in paths {
                    counters.push(CounterConfig {
                        path,
                        name: mapping.metric.clone(),
                        unit: metric.unit.clone(),
                        description: metric.description.clone(),
                        attributes: mapping.attributes.clone(),
                        excluded_aggregation_instance,
                        scale_power10: mapping.scale_power10,
                    });
                }
            }
        }
        if !(1..=MAX_COUNTERS).contains(&counters.len()) {
            return Err(invalid(format!(
                "normalized counters must contain between 1 and {MAX_COUNTERS} entries"
            )));
        }
        let unused = user
            .metrics
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
        assert_eq!(config.counters[1].attributes["state"], "active");
        assert_eq!(config.counters[2].attributes["state"], "idle");
        assert_eq!(config.counters[1].excluded_aggregation_instance, None);
        assert_eq!(config.collection_interval, Duration::from_secs(30));
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.wildcard_refresh_interval(), Duration::from_secs(30));
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
