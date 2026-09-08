// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_config::error::Error;
use serde::Deserialize;
use std::time::Duration;

/// Deliberately closed subset: arbitrary paths, wildcards and rates are unsupported.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
pub enum Counter {
    /// The direct, single-sample physical-memory gauge.
    #[serde(rename = "\\Memory\\Available Bytes")]
    MemoryAvailableBytes,
}

/// Configuration for the first Windows performance-counter vertical slice.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Required explicit selection of the one supported development counter.
    pub counter: Counter,
    /// Time between collections; defaults to 30 seconds.
    #[serde(default = "default_interval", with = "humantime_serde")]
    pub collection_interval: Duration,
}

fn default_interval() -> Duration {
    Duration::from_secs(30)
}

impl Config {
    /// Parse strictly and reject intervals outside the POC's 1s..=24h range.
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
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::winperfcounters::COUNTER_PATH;
    use serde_json::json;

    /// Scenario: The fixed counter is selected with an omitted or explicit interval.
    /// Guarantees: Only omission defaults to 30s; a valid explicit duration is preserved.
    #[test]
    fn valid_config() {
        let config = Config::from_json(&json!({"counter": COUNTER_PATH})).unwrap();
        assert_eq!(config.counter, Counter::MemoryAvailableBytes);
        assert_eq!(config.collection_interval, Duration::from_secs(30));
        let config =
            Config::from_json(&json!({"counter": COUNTER_PATH, "collection_interval": "2s"}))
                .unwrap();
        assert_eq!(config.collection_interval, Duration::from_secs(2));
    }

    /// Scenario: Unsupported counters, unknown options or invalid intervals are supplied.
    /// Guarantees: Configuration fails explicitly instead of falling back or ignoring fields.
    #[test]
    fn rejects_unsupported_config() {
        for value in [
            json!({}),
            json!({"counter": "\\Processor(_Total)\\% Processor Time"}),
            json!({"counter": "\\Process(*)\\Private Bytes"}),
            json!({"counter": COUNTER_PATH, "counters": []}),
            json!({"counter": COUNTER_PATH, "collection_interval": "0s"}),
            json!({"counter": COUNTER_PATH, "collection_interval": "1ms"}),
            json!({"counter": COUNTER_PATH, "collection_interval": "25h"}),
            json!({"counter": COUNTER_PATH, "collection_interval": "bad"}),
            json!({"counter": COUNTER_PATH, "collection_interval": null}),
        ] {
            assert!(Config::from_json(&value).is_err(), "{value}");
        }
    }
}
