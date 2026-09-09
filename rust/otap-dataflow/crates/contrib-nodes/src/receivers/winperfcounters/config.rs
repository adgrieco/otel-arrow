// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_config::error::Error;
use serde::Deserialize;
use std::collections::HashSet;
use std::time::Duration;

const MAX_COUNTERS: usize = 256;
const MIN_SCALE_POWER10: i32 = -18;
const MAX_SCALE_POWER10: i32 = 18;

/// One exact Windows performance counter and its OTel metric identity.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CounterConfig {
    /// Exact English PDH path. Wildcards are not supported.
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
    /// Exact counters to collect.
    pub counters: Vec<CounterConfig>,
    /// Time between collections; defaults to 30 seconds.
    #[serde(default = "default_interval", with = "humantime_serde")]
    pub collection_interval: Duration,
}

fn default_interval() -> Duration {
    Duration::from_secs(30)
}

impl Config {
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
        if !(1..=MAX_COUNTERS).contains(&config.counters.len()) {
            return Err(Error::InvalidUserConfig {
                error: format!("counters must contain between 1 and {MAX_COUNTERS} entries"),
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
            if counter.path.contains(['*', '?']) {
                return Err(Error::InvalidUserConfig {
                    error: format!("{} must be an exact path without wildcards", field("path")),
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
            if !paths.insert(counter.path.to_ascii_lowercase()) {
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
        let config = Config::from_json(&json!({
            "counters": [counter(r"\Memory\Available Bytes", "windows.memory.available")],
            "collection_interval": "2s"
        }))
        .unwrap();
        assert_eq!(config.collection_interval, Duration::from_secs(2));
    }

    /// Scenario: Empty fields, wildcards, duplicates, unknown options, or invalid limits are used.
    /// Guarantees: Invalid configuration fails explicitly before any PDH handles are opened.
    #[test]
    fn rejects_invalid_config() {
        for value in [
            json!({}),
            json!({"counters": []}),
            json!({"counters": [counter(r"\Process(*)\Private Bytes", "process.private")]}),
            json!({"counters": [counter(r"\Process(?)\Private Bytes", "process.private")]}),
            json!({"counters": [counter("", "windows.memory.available")]}),
            json!({"counters": [counter(r"\Memory\Available Bytes", "")]}),
            json!({"counters": [counter(r"\Memory\Available Bytes", "windows.memory.available")],
                   "collection_interval": "0s"}),
            json!({"counters": [counter(r"\Memory\Available Bytes", "windows.memory.available")],
                   "collection_interval": "1ms"}),
            json!({"counters": [counter(r"\Memory\Available Bytes", "windows.memory.available")],
                   "collection_interval": "25h"}),
            json!({"counters": [counter(r"\Memory\Available Bytes", "windows.memory.available")],
                   "collection_interval": "bad"}),
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
