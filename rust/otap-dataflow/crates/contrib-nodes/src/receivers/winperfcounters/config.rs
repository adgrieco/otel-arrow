// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_config::error::Error;
use serde::Deserialize;
use std::collections::HashSet;
use std::time::Duration;

const MAX_COUNTERS: usize = 256;
const MAX_INSTANCE_LIMIT: usize = 16_384;
const MAX_COUNTER_PATH_LEN: usize = 2_047;
const DEFAULT_MAX_INSTANCES_PER_WILDCARD: usize = 256;
const DEFAULT_MAX_EXPANDED_COUNTERS: usize = 4_096;
const MIN_SCALE_POWER10: i32 = -18;
const MAX_SCALE_POWER10: i32 = 18;

fn has_valid_wildcard_placement(path: &str) -> bool {
    if !path.contains('*') {
        return true;
    }
    let Some(counter_separator) = path.rfind('\\') else {
        return false;
    };
    let object_and_instance = &path[..counter_separator];
    let Some(instance_start) = object_and_instance.rfind('(') else {
        return false;
    };
    let Some(instance_end) = object_and_instance.rfind(')') else {
        return false;
    };
    instance_start < instance_end
        && !path[..instance_start].contains('*')
        && !path[instance_end + 1..].contains('*')
}

/// One Windows performance counter and its OTel metric identity.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CounterConfig {
    /// English PDH path, optionally wildcarding only the instance segment.
    pub path: String,
    /// OTel metric name.
    pub name: String,
    /// OTel metric unit.
    pub unit: String,
    /// OTel metric description.
    pub description: String,
    /// Base-10 scaling applied after PDH calculates the native value.
    #[serde(default)]
    pub scale_power10: i32,
}

/// Configuration for Windows performance-counter collection.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Exact or instance-wildcard counters to collect.
    pub counters: Vec<CounterConfig>,
    /// Time between collections; defaults to 30 seconds.
    #[serde(default = "default_interval", with = "humantime_serde")]
    pub collection_interval: Duration,
    /// Time between wildcard discovery refreshes; defaults to the collection interval.
    #[serde(default, with = "humantime_serde")]
    pub wildcard_refresh_interval: Option<Duration>,
    /// Maximum expanded instances retained for one wildcard path.
    #[serde(default = "default_max_instances_per_wildcard")]
    pub max_instances_per_wildcard: usize,
    /// Maximum expanded wildcard counters retained by this receiver.
    #[serde(default = "default_max_expanded_counters")]
    pub max_expanded_counters: usize,
}

fn default_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_max_instances_per_wildcard() -> usize {
    DEFAULT_MAX_INSTANCES_PER_WILDCARD
}

fn default_max_expanded_counters() -> usize {
    DEFAULT_MAX_EXPANDED_COUNTERS
}

impl Config {
    /// Effective wildcard refresh interval.
    #[must_use]
    pub fn wildcard_refresh_interval(&self) -> Duration {
        self.wildcard_refresh_interval
            .unwrap_or(self.collection_interval)
    }

    /// Parse and validate the portable configuration contract.
    pub fn from_json(value: &serde_json::Value) -> Result<Self, Error> {
        let config: Self =
            serde_json::from_value(value.clone()).map_err(|err| Error::InvalidUserConfig {
                error: err.to_string(),
            })?;
        if !(Duration::from_secs(1)..=Duration::from_secs(86400))
            .contains(&config.collection_interval)
        {
            return Err(Error::InvalidUserConfig {
                error: "collection_interval must be between 1s and 24h".to_owned(),
            });
        }
        if let Some(refresh_interval) = config.wildcard_refresh_interval
            && (!(Duration::from_secs(1)..=Duration::from_secs(86400)).contains(&refresh_interval)
                || refresh_interval < config.collection_interval)
        {
            return Err(Error::InvalidUserConfig {
                error: "wildcard_refresh_interval must be between collection_interval and 24h"
                    .to_owned(),
            });
        }
        if !(1..=MAX_COUNTERS).contains(&config.counters.len()) {
            return Err(Error::InvalidUserConfig {
                error: format!("counters must contain between 1 and {MAX_COUNTERS} entries"),
            });
        }
        if !(1..=MAX_INSTANCE_LIMIT).contains(&config.max_instances_per_wildcard) {
            return Err(Error::InvalidUserConfig {
                error: format!(
                    "max_instances_per_wildcard must be between 1 and {MAX_INSTANCE_LIMIT}"
                ),
            });
        }
        if !(1..=MAX_INSTANCE_LIMIT).contains(&config.max_expanded_counters) {
            return Err(Error::InvalidUserConfig {
                error: format!("max_expanded_counters must be between 1 and {MAX_INSTANCE_LIMIT}"),
            });
        }
        if config.max_instances_per_wildcard > config.max_expanded_counters {
            return Err(Error::InvalidUserConfig {
                error: "max_instances_per_wildcard must not exceed max_expanded_counters"
                    .to_owned(),
            });
        }

        let mut paths = HashSet::with_capacity(config.counters.len());
        let mut names = HashSet::with_capacity(config.counters.len());
        for (index, counter) in config.counters.iter().enumerate() {
            let field = |name| format!("counters[{index}].{name}");
            for (name, value) in [
                ("path", counter.path.as_str()),
                ("name", counter.name.as_str()),
                ("unit", counter.unit.as_str()),
                ("description", counter.description.as_str()),
            ] {
                if value.trim().is_empty() {
                    return Err(Error::InvalidUserConfig {
                        error: format!("{} must not be empty", field(name)),
                    });
                }
            }
            if counter.path.contains('?') {
                return Err(Error::InvalidUserConfig {
                    error: format!("{} does not support the '?' wildcard", field("path")),
                });
            }
            if counter.path.encode_utf16().count() > MAX_COUNTER_PATH_LEN {
                return Err(Error::InvalidUserConfig {
                    error: format!(
                        "{} must contain at most {MAX_COUNTER_PATH_LEN} UTF-16 code units",
                        field("path")
                    ),
                });
            }
            if !has_valid_wildcard_placement(&counter.path) {
                return Err(Error::InvalidUserConfig {
                    error: format!(
                        "{} may contain '*' only in the instance segment",
                        field("path")
                    ),
                });
            }
            if !(MIN_SCALE_POWER10..=MAX_SCALE_POWER10).contains(&counter.scale_power10) {
                return Err(Error::InvalidUserConfig {
                    error: format!(
                        "{} must be between {MIN_SCALE_POWER10} and {MAX_SCALE_POWER10}",
                        field("scale_power10")
                    ),
                });
            }
            if !paths.insert(counter.path.to_lowercase()) {
                return Err(Error::InvalidUserConfig {
                    error: format!("duplicate counter path: {}", counter.path),
                });
            }
            if !names.insert(counter.name.as_str()) {
                return Err(Error::InvalidUserConfig {
                    error: format!("duplicate metric name: {}", counter.name),
                });
            }
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn counter(path: &str, name: &str) -> serde_json::Value {
        json!({
            "path": path,
            "name": name,
            "unit": "By",
            "description": "Test counter."
        })
    }

    /// Scenario: Multiple exact counters are configured with an omitted or explicit interval.
    /// Guarantees: Metadata and order are preserved, and only interval omission defaults to 30s.
    #[test]
    fn valid_config() {
        let config = Config::from_json(&json!({
            "counters": [
                counter(r"\Memory\Available Bytes", "windows.memory.available"),
                counter(r"\Memory\Committed Bytes", "windows.memory.committed")
            ]
        }))
        .unwrap();
        assert_eq!(config.counters.len(), 2);
        assert_eq!(config.counters[0].path, r"\Memory\Available Bytes");
        assert_eq!(config.counters[0].scale_power10, 0);
        assert_eq!(config.collection_interval, Duration::from_secs(30));
        assert_eq!(config.wildcard_refresh_interval(), Duration::from_secs(30));
        assert_eq!(config.max_instances_per_wildcard, 256);
        assert_eq!(config.max_expanded_counters, 4_096);
        let config = Config::from_json(&json!({
            "counters": [counter(r"\Memory\Available Bytes", "windows.memory.available")],
            "collection_interval": "2s",
            "wildcard_refresh_interval": "10s",
            "max_instances_per_wildcard": 10,
            "max_expanded_counters": 20
        }))
        .unwrap();
        assert_eq!(config.collection_interval, Duration::from_secs(2));
        assert_eq!(config.wildcard_refresh_interval(), Duration::from_secs(10));
        assert_eq!(config.max_instances_per_wildcard, 10);
        assert_eq!(config.max_expanded_counters, 20);
    }

    /// Scenario: Empty fields, invalid wildcards, duplicates, unknown options, or invalid limits are used.
    /// Guarantees: Invalid configuration fails explicitly before any PDH handles are opened.
    #[test]
    fn rejects_invalid_config() {
        for value in [
            json!({}),
            json!({"counters": []}),
            json!({"counters": [counter(r"\Process(?)\Private Bytes", "process.private")]}),
            json!({"counters": [counter(r"\Proc*ess(foo)\Private Bytes", "process.private")]}),
            json!({"counters": [counter(r"\Process(foo)\Private *", "process.private")]}),
            json!({"counters": [counter(r"\Memory\Available *", "windows.memory.available")]}),
            json!({"counters": [counter("", "windows.memory.available")]}),
            json!({"counters": [counter(r"\Memory\Available Bytes", "")]}),
            json!({"counters": [counter(r"\Memory\Available Bytes", "windows.memory.available")],
                   "collection_interval": "0s"}),
            json!({"counters": [counter(r"\Memory\Available Bytes", "windows.memory.available")],
                   "collection_interval": "1ms"}),
            json!({"counters": [counter(r"\Memory\Available Bytes", "windows.memory.available")],
                   "collection_interval": "25h"}),
            json!({"counters": [counter(r"\Process(*)\Private Bytes", "process.private")],
                   "collection_interval": "10s",
                   "wildcard_refresh_interval": "5s"}),
            json!({"counters": [counter(r"\Process(*)\Private Bytes", "process.private")],
                   "wildcard_refresh_interval": "25h"}),
            json!({"counters": [counter(r"\Process(*)\Private Bytes", "process.private")],
                   "max_instances_per_wildcard": 0}),
            json!({"counters": [counter(r"\Process(*)\Private Bytes", "process.private")],
                   "max_expanded_counters": 16385}),
            json!({"counters": [counter(r"\Process(*)\Private Bytes", "process.private")],
                   "max_instances_per_wildcard": 10,
                   "max_expanded_counters": 5}),
            json!({"counters": [counter(r"\Memory\Available Bytes", "windows.memory.available")],
                   "collection_interval": "bad"}),
            json!({"counters": [counter(
                    &format!(r"\Process({})\Private Bytes", "x".repeat(2048)),
                    "process.private"
            )]}),
            json!({"counters": [counter(r"\Memory\Available Bytes", "windows.memory.available")],
                   "extra": true}),
            json!({"counters": [{
                    "path": r"\Memory\Available Bytes",
                    "name": "windows.memory.available",
                    "unit": "By",
                    "description": "Test counter.",
                    "scale_power10": 19
            }]}),
        ] {
            assert!(Config::from_json(&value).is_err(), "{value}");
        }
    }

    /// Scenario: A counter uses a full or partial wildcard inside its instance segment.
    /// Guarantees: Instance discovery patterns are accepted without allowing object or counter wildcards.
    #[test]
    fn accepts_instance_wildcards() {
        for path in [
            r"\Process(*)\Private Bytes",
            r"\Process(dotnet*)\Private Bytes",
            r"\Thread(*)\% Processor Time",
            r"\\host\Process(parent/*#0)\Private Bytes",
        ] {
            let config = Config::from_json(&json!({
                "counters": [counter(path, "process.private")]
            }))
            .unwrap();
            assert_eq!(config.counters[0].path, path);
        }
    }

    /// Scenario: Paths differ only by case or metric names are repeated exactly.
    /// Guarantees: One PDH path is never sampled twice and metric identities never compete.
    #[test]
    fn rejects_duplicate_paths_and_names() {
        for value in [
            json!({"counters": [
                counter(r"\Memory\Available Bytes", "windows.memory.available"),
                counter(r"\memory\available bytes", "windows.memory.other")
            ]}),
            json!({"counters": [
                counter(r"\Memory\Available Bytes", "windows.memory.available"),
                counter(r"\Memory\Committed Bytes", "windows.memory.available")
            ]}),
        ] {
            assert!(Config::from_json(&value).is_err(), "{value}");
        }
    }
}
