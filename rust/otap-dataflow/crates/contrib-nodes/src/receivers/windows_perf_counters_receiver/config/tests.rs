// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

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
    assert_eq!(config.counters[1].name.as_ref(), "windows.process.time");
    assert_eq!(config.counters[1].metric_kind, MetricKind::Gauge);
    assert_eq!(config.counters[1].attributes["state"], "active");
    assert_eq!(config.counters[2].attributes["state"], "idle");
    assert!(!config.counters[0].instance_config.has_instance());
    assert!(!config.counters[0].instance_config.emit_attribute());
    assert!(config.counters[1].instance_config.has_instance());
    assert!(config.counters[1].instance_config.emit_attribute());
    assert_eq!(
        config.counters[1].instance_config.excluded_aggregation(),
        None
    );
    assert_eq!(config.collection_interval, Duration::from_secs(60));
    assert_eq!(config.initial_delay, Duration::from_secs(1));
    assert_eq!(config.timeout, None);
    assert_eq!(config.wildcard_refresh_interval(), Duration::from_secs(60));
    assert_eq!(config.query_retry_cap(), Duration::from_secs(60));
}

/// Scenario: Timeout is omitted, positive, zero, or negative.
/// Guarantees: Late data is retained by default, explicit positive deadlines opt into discard, and invalid negatives fail.
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
    assert_eq!(omitted.timeout, None);

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

/// Scenario: Wildcard refresh is omitted for short, default, and long collection intervals or set explicitly.
/// Guarantees: Membership scans avoid high-frequency defaults while query recovery remains capped by scrape cadence.
#[test]
fn defaults_refresh_independently_of_query_retry() {
    let config = |collection_interval: &str, refresh_interval: Option<&str>| {
        let mut value = json!({
            "perfcounters": [{
                "object": "Process",
                "instances": ["*"],
                "counters": [{"name": "Private Bytes"}]
            }],
            "collection_interval": collection_interval
        });
        if let Some(refresh_interval) = refresh_interval {
            value["wildcard_refresh_interval"] = json!(refresh_interval);
        }
        Config::from_json(&value).unwrap()
    };

    let short = config("5s", None);
    assert_eq!(short.wildcard_refresh_interval(), Duration::from_secs(60));
    assert_eq!(short.query_retry_cap(), Duration::from_secs(5));

    let default = config("60s", None);
    assert_eq!(default.wildcard_refresh_interval(), Duration::from_secs(60));
    assert_eq!(default.query_retry_cap(), Duration::from_secs(60));

    let long = config("2m", None);
    assert_eq!(long.wildcard_refresh_interval(), Duration::from_secs(120));
    assert_eq!(long.query_retry_cap(), Duration::from_secs(120));

    let explicit = config("5s", Some("90s"));
    assert_eq!(
        explicit.wildcard_refresh_interval(),
        Duration::from_secs(90)
    );
    assert_eq!(explicit.query_retry_cap(), Duration::from_secs(5));
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
    assert_eq!(config.counters[0].name.as_ref(), "windows.process.private");
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
    assert_eq!(config.counters[0].name.as_ref(), "windows.operations");
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
            "instances": "app#001",
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
    assert_eq!(counter.path, r"\Process(app#1)\Private Bytes");
    assert_eq!(counter.name.as_ref(), r"\Process(app#001)\Private Bytes");
    assert_eq!(counter.unit.as_ref(), "1");
    assert!(counter.description.is_empty());
    assert_eq!(counter.metric_kind, MetricKind::Gauge);
    assert!(counter.attributes.is_empty());
}

/// Scenario: Metric definitions are omitted or empty and references are omitted or blank.
/// Guarantees: Every form produces path-named Gauges with configured spelling and canonical native paths.
#[test]
fn defaults_path_metrics_without_definitions_or_references() {
    for definitions in [None, Some(json!({}))] {
        let mut value = json!({
            "perfcounters": [{
                "object": "Process",
                "instances": ["app#00", "worker#001"],
                "counters": [{"name": "Private Bytes", "scale_power10": -3}]
            }]
        });
        if let Some(definitions) = definitions {
            value["metrics"] = definitions;
        }
        for reference in [None, Some(""), Some(" \t ")] {
            if let Some(reference) = reference {
                value["perfcounters"][0]["counters"][0]["metric"] = json!(reference);
            }
            let config = Config::from_json(&value).unwrap();
            for (counter, name, path) in [
                (
                    &config.counters[0],
                    r"\Process(app#00)\Private Bytes",
                    r"\Process(app)\Private Bytes",
                ),
                (
                    &config.counters[1],
                    r"\Process(worker#001)\Private Bytes",
                    r"\Process(worker#1)\Private Bytes",
                ),
            ] {
                assert_eq!(counter.name.as_ref(), name);
                assert_eq!(counter.path, path);
                assert_eq!(counter.metric_kind, MetricKind::Gauge);
                assert_eq!(counter.unit.as_ref(), "1");
                assert!(counter.description.is_empty());
                assert!(counter.attributes.is_empty());
                assert_eq!(counter.scale_power10, -3);
            }
        }
    }
}

/// Scenario: An implicit path name exactly matches a configured metric definition.
/// Guarantees: The mapping uses that definition's metadata and kind while ignoring mapping attributes.
#[test]
fn resolves_implicit_path_name_to_metric_definition() {
    let config = Config::from_json(&json!({
        "metrics": {
            r"\Memory\Available Bytes": {
                "description": "Available physical memory.",
                "unit": "By",
                "sum": {
                    "aggregation": "cumulative",
                    "monotonic": false
                }
            }
        },
        "perfcounters": [{
            "object": "Memory",
            "counters": [{
                "name": "Available Bytes",
                "attributes": {"state": "ignored"}
            }]
        }]
    }))
    .unwrap();

    let counter = &config.counters[0];
    assert_eq!(counter.name.as_ref(), r"\Memory\Available Bytes");
    assert_eq!(counter.unit.as_ref(), "By");
    assert_eq!(counter.description.as_ref(), "Available physical memory.");
    assert_eq!(
        counter.metric_kind,
        MetricKind::Sum {
            aggregation: Aggregation::Cumulative,
            monotonic: false,
        }
    );
    assert!(counter.attributes.is_empty());
}

/// Scenario: Implicit exact and wildcard paths match definitions using configured spelling.
/// Guarantees: Metric lookup preserves configured instance indexes while native exact paths are canonical.
#[test]
fn resolves_configured_path_spelling_to_metric_definitions() {
    let config = Config::from_json(&json!({
        "metrics": {
            r"\Process(app#001)\Private Bytes": {"unit": "By"},
            r"\Process(worker*)\Working Set": {"unit": "KiBy"}
        },
        "perfcounters": [{
            "object": "Process",
            "instances": ["app#001", "worker*"],
            "counters": [
                {"name": "Private Bytes"},
                {"name": "Working Set"}
            ]
        }]
    }))
    .unwrap();

    assert_eq!(config.counters[0].path, r"\Process(app#1)\Private Bytes");
    assert_eq!(
        config.counters[0].name.as_ref(),
        r"\Process(app#001)\Private Bytes"
    );
    assert_eq!(config.counters[0].unit.as_ref(), "By");
    assert_eq!(
        config.counters[3].name.as_ref(),
        r"\Process(worker*)\Working Set"
    );
    assert_eq!(config.counters[3].unit.as_ref(), "KiBy");
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
    assert_eq!(counter.name.as_ref(), "used");
    assert_eq!(counter.unit.as_ref(), "1");
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

    assert_eq!(config.counters[0].name.as_ref(), r"\Memory\Available Bytes");
}

/// Scenario: Null metric references appear at each object/counter position or replace the definitions map.
/// Guarantees: Every null reference reports its exact location and a null definitions map is rejected.
#[test]
fn rejects_null_metric_forms_at_each_location() {
    for (object_index, counter_index) in [(0, 0), (0, 1), (1, 0), (1, 1)] {
        let mut value = json!({
            "perfcounters": [
                {
                    "object": "Memory",
                    "counters": [
                        {"name": "Available Bytes"},
                        {"name": "Committed Bytes"}
                    ]
                },
                {
                    "object": "Process",
                    "instances": "worker",
                    "counters": [
                        {"name": "Private Bytes"},
                        {"name": "Working Set"}
                    ]
                }
            ]
        });
        value["perfcounters"][object_index]["counters"][counter_index]["metric"] = json!(null);
        assert_config_error(
            value,
            &format!(
                "perfcounters[{object_index}].counters[{counter_index}].metric must be a string \
                 when present; null metric is invalid"
            ),
        );
    }

    assert_config_error(
        json!({
            "metrics": null,
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes"}]
            }]
        }),
        "expected a map",
    );
}

/// Scenario: Mappings explicitly disable or enable query recreation across expanded paths.
/// Guarantees: The option is retained and every path from one source mapping shares its identity.
#[test]
fn retains_query_recreation_by_source_mapping() {
    let config = |recreate_query| {
        json!({
            "perfcounters": [{
                "object": "Memory",
                "instances": ["first", "second"],
                "counters": [
                    {
                        "name": "Available Bytes",
                        "recreate_query": recreate_query
                    },
                    {"name": "Committed Bytes"}
                ]
            }]
        })
    };

    let disabled = Config::from_json(&config(false)).unwrap();
    assert!(!disabled.counters[0].recreate_query);
    assert_eq!(disabled.counters[0].source_mapping_index, 0);
    assert_eq!(disabled.counters[1].source_mapping_index, 0);

    let enabled = Config::from_json(&config(true)).unwrap();
    assert!(enabled.counters[0].recreate_query);
    assert!(enabled.counters[1].recreate_query);
    assert_eq!(enabled.counters[0].source_mapping_index, 0);
    assert_eq!(enabled.counters[1].source_mapping_index, 0);
    assert!(!enabled.counters[2].recreate_query);
    assert_eq!(enabled.counters[2].source_mapping_index, 1);
    assert_eq!(enabled.counters[3].source_mapping_index, 1);
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
        excluded.counters[0].instance_config.excluded_aggregation(),
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
    assert_eq!(
        included.counters[0].instance_config.excluded_aggregation(),
        None
    );
    assert!(included.counters[0].instance_config.emit_attribute());

    let case_distinct = Config::from_json(&json!({
        "metrics": {"test": metric()},
        "perfcounters": [{
            "object": "Custom Object",
            "instances": ["*", "_GLOBAL_"],
            "aggregation_name": "_Global_",
            "counters": [{"name": "Counter", "metric": "test"}]
        }]
    }))
    .unwrap();
    assert_eq!(
        case_distinct.counters[0]
            .instance_config
            .excluded_aggregation(),
        Some("_Global_")
    );

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
    assert!(aggregate_alone.counters[0].instance_config.has_instance());
    assert!(!aggregate_alone.counters[0].instance_config.emit_attribute());

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
            .map(|counter| {
                (
                    counter.path.as_str(),
                    counter.instance_config.emit_attribute(),
                )
            })
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
    assert!(
        config
            .counters
            .iter()
            .all(|counter| counter.name.as_ref() == "test")
    );
    assert!(
        config
            .counters
            .iter()
            .all(|counter| counter.instance_config.has_instance())
    );
    assert!(
        config
            .counters
            .iter()
            .all(|counter| counter.instance_config.emit_attribute())
    );
}

/// Scenario: Static `instance` attributes are configured for exact, multiple, wildcard, or ignored mappings.
/// Guarantees: Only effective overrides that can collapse multiple selected instances request a warning.
#[test]
fn identifies_risky_instance_overrides() {
    let override_attributes = BTreeMap::from([("instance".to_owned(), "fixed".to_owned())]);
    let ignored_attributes = BTreeMap::new();

    assert!(!should_warn_instance_override(false, &override_attributes));
    assert!(should_warn_instance_override(true, &override_attributes));
    assert!(should_warn_instance_override(true, &override_attributes));
    assert!(!should_warn_instance_override(true, &ignored_attributes));
}

/// Scenario: Native object, instance, and counter components contain PDH-significant delimiters.
/// Guarantees: Configuration preserves parentheses, backslashes, question marks, and asterisks for PDH to interpret.
#[test]
fn preserves_native_component_delimiters() {
    let config = Config::from_json(&json!({
        "metrics": {"test": metric()},
        "perfcounters": [{
            "object": "Object (IPv4)",
            "instances": r"C:\pagefile.sys(parent)?",
            "counters": [{"name": r"Counter (value)\*?", "metric": "test"}]
        }]
    }))
    .unwrap();
    assert_eq!(
        config.counters[0].path,
        r"\Object (IPv4)(C:\pagefile.sys(parent)?)\Counter (value)\*?"
    );
}

/// Scenario: Ordinary metadata and native path components contain surrounding whitespace.
/// Guarantees: Ordinary labels are trimmed while nonblank object and counter spelling remains native identity.
#[test]
fn trims_metadata_but_preserves_native_component_spelling() {
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
    assert_eq!(counter.path, r"\ Memory \ Available Bytes ");
    assert_eq!(counter.name.as_ref(), "windows.memory.available");
    assert_eq!(&*counter.description, "Available memory.");
    assert_eq!(counter.unit.as_ref(), "By");
    assert_eq!(
        *counter.attributes,
        BTreeMap::from([("state".to_owned(), " free ".to_owned())])
    );

    let instances = Config::from_json(&json!({
        "perfcounters": [{
            "object": "Process",
            "instances": [" worker ", "*"],
            "aggregation_name": " _Total ",
            "counters": [{"name": "Private Bytes"}]
        }]
    }))
    .unwrap();
    assert_eq!(instances.counters[0].path, r"\Process(*)\Private Bytes");
    assert_eq!(
        instances.counters[0].instance_config.excluded_aggregation(),
        Some(" _Total ")
    );
}

/// Scenario: An exact nonblank instance contains surrounding whitespace.
/// Guarantees: Its configured/native spelling remains distinct and supplies the implicit metric name unchanged.
#[test]
fn preserves_significant_instance_spaces() {
    let config = Config::from_json(&json!({
        "perfcounters": [{
            "object": "Process",
            "instances": " worker ",
            "counters": [{"name": "Private Bytes"}]
        }]
    }))
    .unwrap();
    let counter = &config.counters[0];
    assert_eq!(counter.path, r"\Process( worker )\Private Bytes");
    assert_eq!(counter.name.as_ref(), r"\Process( worker )\Private Bytes");
}

/// Scenario: A leading-padded metric key and reference are ordinary labels before normalization.
/// Guarantees: Both trim once and resolve exactly without being reclassified as raw path-shaped input.
#[test]
fn resolves_leading_padded_ordinary_reference() {
    let configured = r" \Memory\Available Bytes";
    let config = Config::from_json(&json!({
        "metrics": {
            configured: {"description": "Selected.", "unit": "By"}
        },
        "perfcounters": [{
            "object": "Memory",
            "counters": [{
                "name": "Available Bytes",
                "metric": configured
            }]
        }]
    }))
    .unwrap();
    let counter = &config.counters[0];
    assert_eq!(counter.name.as_ref(), r"\Memory\Available Bytes");
    assert_eq!(counter.unit.as_ref(), "By");
    assert_eq!(counter.description.as_ref(), "Selected.");
}

/// Scenario: A segment-padded path definition is selected by an exact path-shaped reference.
/// Guarantees: Exact lookup resolves the definition without segment or native-path validation.
#[test]
fn resolves_segment_padded_path_reference() {
    let padded = r"\Memory \Available Bytes ";
    let config = Config::from_json(&json!({
        "metrics": {
            padded: {"description": "Selected.", "unit": "By"}
        },
        "perfcounters": [{
            "object": "Memory",
            "counters": [{"name": "Available Bytes", "metric": padded}]
        }]
    }))
    .unwrap();
    assert_eq!(config.counters[0].name.as_ref(), padded);
    assert_eq!(config.counters[0].description.as_ref(), "Selected.");
}

/// Scenario: A padded path-shaped reference has no exact definition.
/// Guarantees: It reports an undefined metric instead of a native-path syntax or padding error.
#[test]
fn rejects_undefined_padded_path_reference() {
    assert_config_error(
        json!({
            "metrics": {r"\Memory\Available Bytes": metric()},
            "perfcounters": [{
                "object": "Memory",
                "counters": [{
                    "name": "Available Bytes",
                    "metric": r"\Memory\Available Bytes "
                }]
            }]
        }),
        "references undefined metric",
    );
}

/// Scenario: Metric names and units use unrestricted printable OTLP text or path-shaped names.
/// Guarantees: Every non-empty printable value survives normalization without SDK-style limits.
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
    let overlong_path_name = format!(r"\Memory\{}", "x".repeat(MAX_COUNTER_PATH_LEN));
    for (name, unit) in [
        (r"\Memory\Available Bytes", "By"),
        ("1 value", "\u{b5}s"),
        ("\u{e9}", "B y"),
        (long_name.as_str(), long_unit.as_str()),
        (overlong_path_name.as_str(), "1"),
    ] {
        let normalized = Config::from_json(&config(name, unit)).unwrap();
        assert_eq!(normalized.counters[0].name.as_ref(), name);
        assert_eq!(normalized.counters[0].unit.as_ref(), unit);
    }
    assert_config_error(config(" ", "By"), "metric name must not be empty");
}

/// Scenario: Normalized metric names, units, references, or generated path names retain controls.
/// Guarantees: Remaining controls are rejected, while controls removed by trimming keep existing defaults.
#[test]
fn rejects_controls_remaining_after_metric_normalization() {
    let config = |metrics, metric: &str| {
        json!({
            "metrics": metrics,
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": metric}]
            }]
        })
    };

    assert_config_error(
        config(json!({"metric\nname": {"unit": "By"}}), "metric\nname"),
        "metric name must not contain control characters",
    );
    assert_config_error(
        config(json!({"selected": {"unit": "B\ty"}}), "selected"),
        "metric unit must not contain control characters",
    );
    assert_config_error(
        config(json!({"selected": metric()}), "selected\nname"),
        "metric name must not contain control characters",
    );
    assert_config_error(
        config(json!({"selected": metric()}), "\\Memory\\Available Bytes\t"),
        "metric name must not contain control characters",
    );
    assert_config_error(
        config(
            json!({
                "selected": metric(),
                "\\Memory\\Available Bytes\t": {"unit": "By"}
            }),
            "selected",
        ),
        "metric name must not contain control characters",
    );
    assert_config_error(
        json!({
            "perfcounters": [{
                "object": "Mem\nory",
                "counters": [{"name": "Available Bytes"}]
            }]
        }),
        "perfcounters[0].object must not contain control characters",
    );

    let normalized = Config::from_json(&config(
        json!({" selected\t": {"unit": " \t "}}),
        " selected\t",
    ))
    .unwrap();
    assert_eq!(normalized.counters[0].name.as_ref(), "selected");
    assert_eq!(normalized.counters[0].unit.as_ref(), "1");
}

/// Scenario: Path-shaped definitions are selected implicitly, by a wildcard template, or by an explicit alias.
/// Guarantees: Only raw path-shaped definitions unused by every mapping produce one usage-warning candidate.
#[test]
fn identifies_unused_path_metric_definitions() {
    let config = Config::from_json(&json!({
        "metrics": {
            r"\Memory\Available Bytes": {"unit": "By"},
            r"\Process(worker*)\Private Bytes": {"unit": "By"},
            r"\alias": {"unit": "By"},
            r"\unused": {"unit": "1"},
            r" \ordinary-after-trim": {"unit": "1"},
            "ordinary": {"unit": "1"}
        },
        "perfcounters": [
            {
                "object": "Memory",
                "counters": [
                    {"name": "Available Bytes"},
                    {"name": "Cache Bytes", "metric": r"\alias"}
                ]
            },
            {
                "object": "Process",
                "instances": "worker*",
                "counters": [{"name": "Private Bytes"}]
            }
        ]
    }))
    .unwrap();
    let definitions = [
        (r"\Memory\Available Bytes", true),
        (r"\Process(worker*)\Private Bytes", true),
        (r"\alias", true),
        (r"\unused", true),
        (r"\ordinary-after-trim", false),
        ("ordinary", false),
    ];
    assert_eq!(
        unused_path_metric_definitions(
            definitions,
            config.counters.iter().map(|counter| counter.name.as_ref())
        ),
        [r"\unused"]
    );
}

/// Scenario: Malformed and overlong path-shaped definitions are selected by exact references.
/// Guarantees: Metric identity lookup does not impose native segment or generated-path length validation.
#[test]
fn resolves_exact_malformed_and_overlong_path_references() {
    let overlong = format!(r"\Memory\{}", "x".repeat(MAX_COUNTER_PATH_LEN));
    for name in [r"\Memory", r"\Memory(*)\X\Y", overlong.as_str()] {
        let config = Config::from_json(&json!({
            "metrics": {name: {"description": "Selected.", "unit": "By"}},
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": name}]
            }]
        }))
        .unwrap();
        assert_eq!(config.counters[0].name.as_ref(), name);
        assert_eq!(config.counters[0].description.as_ref(), "Selected.");
    }
}

/// Scenario: Metric units are blank or contain printable OTLP unit text.
/// Guarantees: Blank defaults to "1"; non-empty printable values are trimmed but unrestricted.
#[test]
fn normalizes_unrestricted_metric_units() {
    let config = |unit: &str| {
        json!({
            "metrics": {"test": {"unit": unit}},
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": "test"}]
            }]
        })
    };
    for unit in ["", " \t "] {
        assert_eq!(
            Config::from_json(&config(unit)).unwrap().counters[0]
                .unit
                .as_ref(),
            "1"
        );
    }
    let long_unit = format!("{{{}}}", "u".repeat(128));
    for unit in ["By", "{packets}/s", "\u{b5}s", "B y", long_unit.as_str()] {
        assert_eq!(
            Config::from_json(&config(unit)).unwrap().counters[0]
                .unit
                .as_ref(),
            unit
        );
    }
}

/// Scenario: More metric definitions are configured than the default native-counter budget.
/// Guarantees: Unused metadata cannot prevent an otherwise bounded receiver configuration from starting.
#[test]
fn allows_metric_definitions_beyond_native_counter_limit() {
    let metrics = (0..=DEFAULT_MAX_EXPANDED_COUNTERS)
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
/// Guarantees: Metric errors identify both configured keys and normalized identities never merge.
#[test]
fn rejects_normalized_identity_collisions() {
    let definition = json!({"description": "Value.", "unit": "By", "gauge": {}});
    let error = Config::from_json(&json!({
        "metrics": {"cpu": definition.clone(), " cpu ": definition},
        "perfcounters": [{
            "object": "Processor",
            "counters": [{"name": "% Processor Time", "metric": "cpu"}]
        }]
    }))
    .unwrap_err()
    .to_string();
    assert!(error.contains(r#"" cpu ""#), "{error:?}");
    assert!(error.contains(r#""cpu""#), "{error:?}");
    assert!(error.contains(r#"duplicate "cpu""#), "{error:?}");
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

/// Scenario: Metric names differ only by ASCII case.
/// Guarantees: Exact references preserve both case-sensitive OTel stream identities.
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

    assert_eq!(config.counters[0].name.as_ref(), "cpu");
    assert_eq!(config.counters[0].unit.as_ref(), "1");
    assert_eq!(config.counters[1].name.as_ref(), "CPU");
    assert_eq!(config.counters[1].unit.as_ref(), "%");
}

/// Scenario: Ordinary and path-shaped metric definitions differ only by Unicode case.
/// Guarantees: Operators see every conflicting spelling once per group instead of an incomplete or noisy warning.
#[test]
fn warns_once_for_case_distinct_metric_definitions() {
    let names = [
        "cpu".to_owned(),
        "CPU".to_owned(),
        "Cpu".to_owned(),
        r"\Memory\Available Bytes".to_owned(),
        r"\memory\available bytes".to_owned(),
        r"\MEMORY\AVAILABLE BYTES".to_owned(),
        r"\Malformed".to_owned(),
        r"\MALFORMED".to_owned(),
        "\u{00c4}vailable".to_owned(),
        "\u{00e4}vailable".to_owned(),
    ];

    assert_eq!(
        case_distinct_metric_definitions(names.iter().map(String::as_str)),
        BTreeMap::from([
            (
                "cpu".to_owned(),
                vec![names[0].as_str(), names[1].as_str(), names[2].as_str()]
            ),
            (
                r"\malformed".to_owned(),
                vec![names[6].as_str(), names[7].as_str()]
            ),
            (
                r"\memory\available bytes".to_owned(),
                vec![names[3].as_str(), names[4].as_str(), names[5].as_str()]
            ),
            (
                "\u{00e4}vailable".to_owned(),
                vec![names[8].as_str(), names[9].as_str()]
            )
        ])
    );
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
        partial.counters[0].instance_config.excluded_aggregation(),
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
            "must not contain control characters",
        );
    }
}

/// Scenario: Native object, instance, and counter components are blank, or the optional aggregation name is blank.
/// Guarantees: Required blanks fail without trimming nonblank spelling, while blank aggregation retains its documented default.
#[test]
fn handles_blank_native_components_and_aggregation() {
    for value in [
        json!({
            "perfcounters": [{
                "object": " ",
                "counters": [{"name": "Available Bytes"}]
            }]
        }),
        json!({
            "perfcounters": [{
                "object": "Process",
                "instances": " ",
                "counters": [{"name": "Private Bytes"}]
            }]
        }),
        json!({
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": " "}]
            }]
        }),
        json!({
            "perfcounters": [{
                "object": "Process",
                "instances": "*",
                "aggregation_name": " ",
                "counters": [{"name": "Private Bytes"}]
            }]
        }),
    ] {
        if value["perfcounters"][0]["aggregation_name"].is_string() {
            let config = Config::from_json(&value).unwrap();
            assert_eq!(
                config.counters[0].instance_config.excluded_aggregation(),
                Some("_Total")
            );
        } else {
            assert_config_error(value, "must not be empty");
        }
    }
}

/// Scenario: Explicit instances and counter mappings produce more exact paths than the configured native-entry budget.
/// Guarantees: Exact handles cannot bypass the receiver-wide bound that also limits wildcard expansion.
#[test]
fn bounds_exact_paths_by_native_counter_budget() {
    let object = |instances: usize, counters: usize| {
        json!({
            "object": "Process",
            "instances": (0..instances).map(|i| format!("i{i}")).collect::<Vec<_>>(),
            "counters": (0..counters)
                .map(|i| json!({"name": format!("C{i}"), "metric": "test"}))
                .collect::<Vec<_>>()
        })
    };
    let config = |perfcounters, limit| {
        json!({
            "metrics": {"test": metric()},
            "perfcounters": perfcounters,
            "max_expanded_counters": limit
        })
    };
    assert_eq!(
        Config::from_json(&config(json!([object(16, 16)]), 256))
            .unwrap()
            .counters
            .len(),
        256
    );
    assert_config_error(
        config(json!([object(17, 16)]), 271),
        "normalize to 272 exact counter paths, exceeding max_expanded_counters 271",
    );
    assert_config_error(
        config(json!([object(1, 129), object(1, 128)]), 256),
        "normalize to 257 exact counter paths, exceeding max_expanded_counters 256",
    );
}

/// Scenario: An empty instance list accompanies 257 exact counter mappings and an equally sized native budget.
/// Guarantees: Larger bounded deployments are not rejected by an unrelated legacy template cap.
#[test]
fn allows_exact_paths_beyond_removed_template_cap() {
    let object = |counters: usize| {
        json!({
            "object": "Memory",
            "instances": [],
            "counters": (0..counters)
                .map(|index| json!({"name": format!("Counter {index}")}))
                .collect::<Vec<_>>()
        })
    };
    assert_eq!(
        Config::from_json(&json!({
            "perfcounters": [object(257)],
            "max_expanded_counters": 257
        }))
        .unwrap()
        .counters
        .len(),
        257
    );
}

/// Scenario: Exact paths exhaust the native budget while an additional wildcard template is configured.
/// Guarantees: Templates remain configurable without pre-consuming entries, while their concrete expansions cannot exceed the runtime budget.
#[test]
fn wildcard_templates_do_not_consume_exact_counter_budget() {
    let counters = |prefix: &str, count: usize| {
        (0..count)
            .map(|index| json!({"name": format!("{prefix} {index}")}))
            .collect::<Vec<_>>()
    };
    let omitted = json!({
        "object": "Memory",
        "counters": counters("Memory", 100)
    });
    let empty = json!({
        "object": "System",
        "instances": [],
        "counters": counters("System", 100)
    });
    let selected = json!({
        "object": "Process",
        "instances": ["worker", "service"],
        "counters": counters("Process", 28)
    });
    let wildcard = json!({
        "object": "Processor",
        "instances": ["*"],
        "counters": [{"name": "Wildcard"}]
    });

    let config = Config::from_json(&json!({
        "perfcounters": [omitted, empty, selected, wildcard],
        "max_expanded_counters": 256
    }))
    .unwrap();
    assert_eq!(config.counters.len(), 257);
    assert!(
        config
            .counters
            .last()
            .unwrap()
            .instance_config
            .is_wildcard()
    );
}

/// Scenario: One definition is expanded, referenced again, and the normalized config is cloned.
/// Guarantees: Name, unit, description, and per-mapping attributes retain their shared backing.
#[test]
fn shares_expanded_counter_metadata() {
    let config = Config::from_json(&json!({
        "metrics": {"test": metric()},
        "perfcounters": [
            {
                "object": "Process",
                "instances": ["a", "b"],
                "counters": [{
                    "name": "Private Bytes",
                    "metric": "test",
                    "attributes": {"state": "used"}
                }]
            },
            {
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": "test"}]
            }
        ]
    }))
    .unwrap();
    let [first, second, repeated] = config.counters.as_slice() else {
        panic!("expected two expanded counters and one repeated reference");
    };
    assert!(Arc::ptr_eq(&first.name, &second.name));
    assert!(Arc::ptr_eq(&first.unit, &second.unit));
    assert!(Arc::ptr_eq(&first.description, &second.description));
    assert!(Arc::ptr_eq(&first.attributes, &second.attributes));
    assert!(Arc::ptr_eq(&first.name, &repeated.name));
    assert!(Arc::ptr_eq(&first.unit, &repeated.unit));
    assert!(Arc::ptr_eq(&first.description, &repeated.description));

    let cloned = config.clone();
    assert!(Arc::ptr_eq(&first.name, &cloned.counters[0].name));
    assert!(Arc::ptr_eq(&first.unit, &cloned.counters[0].unit));
    assert!(Arc::ptr_eq(
        &first.description,
        &cloned.counters[0].description
    ));
    assert!(Arc::ptr_eq(
        &first.attributes,
        &cloned.counters[0].attributes
    ));
}

/// Scenario: Separate implicit mappings derive distinct path-shaped metric names.
/// Guarantees: Distinct generated names do not accidentally share name backing.
#[test]
fn keeps_distinct_implicit_metric_name_backing() {
    let config = Config::from_json(&json!({
        "perfcounters": [{
            "object": "Memory",
            "counters": [
                {"name": "Available Bytes"},
                {"name": "Cache Bytes"}
            ]
        }]
    }))
    .unwrap();
    let [first, second] = config.counters.as_slice() else {
        panic!("expected two implicit counters");
    };
    assert_ne!(first.name, second.name);
    assert!(!Arc::ptr_eq(&first.name, &second.name));
    assert!(Arc::ptr_eq(&first.unit, &second.unit));
}

/// Scenario: Native components differ only by significant surrounding spaces.
/// Guarantees: Exact spelling remains distinct while case-only duplicates still use native identity comparison.
#[test]
fn preserves_space_distinct_native_paths() {
    let instances = Config::from_json(&json!({
        "metrics": {"test": metric()},
        "perfcounters": [{
            "object": "Process",
            "instances": ["app", " APP "],
            "counters": [{"name": "Private Bytes", "metric": "test"}]
        }]
    }))
    .unwrap();
    assert_eq!(instances.counters[0].path, r"\Process(app)\Private Bytes");
    assert_eq!(instances.counters[1].path, r"\Process( APP )\Private Bytes");

    let paths = Config::from_json(&json!({
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
    }))
    .unwrap();
    assert_eq!(paths.counters[0].path, r"\Memory\Available Bytes");
    assert_eq!(paths.counters[1].path, r"\ memory \ available bytes ");

    assert_config_error(
        json!({
            "metrics": {"test": metric()},
            "perfcounters": [{
                "object": "Process",
                "instances": ["app", "APP"],
                "counters": [{"name": "Private Bytes", "metric": "test"}]
            }]
        }),
        "instances contains duplicate",
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
    assert_eq!(config.counters[0].name.as_ref(), "available.bytes");
    assert_eq!(config.counters[1].name.as_ref(), "available.scaled");

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
