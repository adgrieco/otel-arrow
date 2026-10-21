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
/// Guarantees: Omitted and zero values use the collection interval, a positive timeout takes precedence, and invalid negatives fail.
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
    assert_eq!(omitted.scrape_timeout_threshold(), Duration::from_secs(45));

    let mut positive = base.clone();
    positive["timeout"] = json!("10s");
    let positive = Config::from_json(&positive).unwrap();
    assert_eq!(positive.timeout, Some(Duration::from_secs(10)));
    assert_eq!(positive.scrape_timeout_threshold(), Duration::from_secs(10));

    let mut disabled = base.clone();
    disabled["timeout"] = json!("0s");
    let zero = Config::from_json(&disabled).unwrap();
    assert_eq!(zero.timeout, None);
    assert_eq!(zero.scrape_timeout_threshold(), Duration::from_secs(45));

    let mut negative = base;
    negative["timeout"] = json!("-1s");
    assert_config_error(negative, "invalid value");
}

/// Scenario: Timing uses sub-second, multi-day, zero, and negative configured durations.
/// Guarantees: Collection accepts any positive interval and non-positive initial delay is immediate.
#[test]
fn accepts_go_timing_range() {
    let config = |collection_interval: &str, initial_delay: &str| {
        Config::from_json(&json!({
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes"}]
            }],
            "collection_interval": collection_interval,
            "initial_delay": initial_delay
        }))
    };

    let fast = config("1ms", "-5s").unwrap();
    assert_eq!(fast.collection_interval, Duration::from_millis(1));
    assert_eq!(fast.initial_delay, Duration::ZERO);

    let slow = config("48h", "72h").unwrap();
    assert_eq!(slow.collection_interval, Duration::from_secs(48 * 60 * 60));
    assert_eq!(slow.initial_delay, Duration::from_secs(72 * 60 * 60));

    assert_config_error(
        json!({
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes"}]
            }],
            "collection_interval": "2s",
            "wildcard_refresh_interval": "1s"
        }),
        "wildcard_refresh_interval is not supported",
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
        Config::from_json(&value)
    };

    let short = config("5s", None).unwrap();
    assert_eq!(short.wildcard_refresh_interval(), Duration::from_secs(60));
    assert_eq!(short.query_retry_cap(), Duration::from_secs(5));

    let default = config("60s", None).unwrap();
    assert_eq!(default.wildcard_refresh_interval(), Duration::from_secs(60));
    assert_eq!(default.query_retry_cap(), Duration::from_secs(60));

    let long = config("2m", None).unwrap();
    assert_eq!(long.wildcard_refresh_interval(), Duration::from_secs(120));
    assert_eq!(long.query_retry_cap(), Duration::from_secs(120));

    assert!(config("5s", Some("90s")).is_err());
}

/// Scenario: Deferred options are explicitly requested, including their default values or null.
/// Guarantees: Presence is rejected before startup, while absent defaults keep fixed bounds and indexed recreation errors.
#[test]
fn rejects_explicit_deferred_capabilities() {
    let base = json!({
        "perfcounters": [
            {"object": "Memory", "counters": [{"name": "Available Bytes"}]},
            {"object": "System", "counters": [{"name": "Processes"}, {"name": "Threads"}]}
        ]
    });
    for (key, value) in [
        ("wildcard_refresh_interval", json!("60s")),
        ("wildcard_refresh_interval", json!(null)),
        ("max_instances_per_wildcard", json!(256)),
        ("max_instances_per_wildcard", json!(null)),
        ("max_expanded_counters", json!(4096)),
        ("max_expanded_counters", json!(null)),
    ] {
        let mut value_with_option = base.clone();
        value_with_option[key] = value;
        assert_config_error(value_with_option, &format!("{key} is not supported"));
    }
    let config = Config::from_json(&base).unwrap();
    assert_eq!(config.max_instances_per_wildcard, 256);
    assert_eq!(config.max_expanded_counters, 4096);
    let mut recreation = base;
    recreation["perfcounters"][1]["counters"][1]["recreate_query"] = json!(true);
    assert_config_error(
        recreation,
        "perfcounters[1].counters[1].recreate_query: true",
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
    assert_eq!(config.counters[0].name.as_ref(), "windows.process.private");
    assert_eq!(
        &*config.counters[0].description,
        "Committed private memory for each process instance."
    );
    assert_eq!(config.counters[0].metric_kind, MetricKind::Sum);
}

/// Scenario: A cumulative additive value is configured as a monotonic Sum.
/// Guarantees: Monotonic Sum definitions remain rejected, including when unused.
#[test]
fn normalizes_monotonic_sum_configuration() {
    let value = json!({
        "metrics": {"windows.operations": monotonic_sum_metric()},
        "perfcounters": [{
            "object": "Custom Provider",
            "counters": [{
                "name": "Operations",
                "metric": "windows.operations"
            }]
        }]
    });
    assert_config_error(
        value.clone(),
        "metrics.windows.operations.sum supports only cumulative, non-monotonic",
    );
    let mut unused = value;
    let _ = unused["perfcounters"][0]["counters"][0]
        .as_object_mut()
        .unwrap()
        .remove("metric");
    assert_config_error(
        unused,
        "metrics.windows.operations.sum supports only cumulative, non-monotonic",
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
    assert_eq!(counter.path, r"\Process(app#001)\Private Bytes");
    assert_eq!(counter.name.as_ref(), r"\Process(app#001)\Private Bytes");
    assert_eq!(counter.unit.as_ref(), "1");
    assert!(counter.description.is_empty());
    assert_eq!(counter.metric_kind, MetricKind::Gauge);
    assert!(counter.attributes.is_empty());
}

/// Scenario: Metric definitions are omitted or empty and references are omitted or literal empty strings.
/// Guarantees: Every implicit form produces path-named Gauges with configured spelling preserved in native paths.
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
        for reference in [None, Some("")] {
            if let Some(reference) = reference {
                value["perfcounters"][0]["counters"][0]["metric"] = json!(reference);
            }
            let config = Config::from_json(&value).unwrap();
            for (counter, name, path) in [
                (
                    &config.counters[0],
                    r"\Process(app#00)\Private Bytes",
                    r"\Process(app#00)\Private Bytes",
                ),
                (
                    &config.counters[1],
                    r"\Process(worker#001)\Private Bytes",
                    r"\Process(worker#001)\Private Bytes",
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
    assert_eq!(counter.metric_kind, MetricKind::Sum);
    assert!(counter.attributes.is_empty());
}

/// Scenario: Implicit exact and wildcard paths match definitions using configured spelling.
/// Guarantees: Metric lookup and native exact paths preserve configured instance indexes.
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

    assert_eq!(config.counters[0].path, r"\Process(app#001)\Private Bytes");
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

/// Scenario: A counter supplies a whitespace-only metric reference.
/// Guarantees: Every nonempty spelling uses exact lookup and reports the unmatched reference.
#[test]
fn rejects_whitespace_only_metric_reference() {
    assert_config_error(
        json!({
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": "  "}]
            }]
        }),
        "references undefined metric \"  \"",
    );
}

/// Scenario: Null metric references appear at each object/counter position.
/// Guarantees: Every null reference reports its exact location.
#[test]
fn rejects_null_metric_references_at_each_location() {
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
}

/// Scenario: Metric definitions are omitted, explicitly null, or an empty map.
/// Guarantees: Every form decodes as no definitions and leaves unreferenced counters on default Gauge metadata.
#[test]
fn accepts_null_metrics_as_empty_definitions() {
    for metrics in [None, Some(json!(null)), Some(json!({}))] {
        let mut value = json!({
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes"}]
            }]
        });
        if let Some(metrics) = metrics {
            value["metrics"] = metrics;
        }

        let config = Config::from_json(&value).unwrap();
        assert_eq!(config.counters.len(), 1);
        assert_eq!(config.counters[0].path, r"\Memory\Available Bytes");
        assert_eq!(config.counters[0].metric_kind, MetricKind::Gauge);
        assert_eq!(config.counters[0].unit.as_ref(), "1");
    }
}

/// Scenario: Mappings explicitly disable or enable query recreation across expanded paths.
/// Guarantees: Explicit false remains supported, while true fails with indexed context.
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

    assert_config_error(
        config(true),
        "perfcounters[0].counters[0].recreate_query: true is not supported",
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

/// Scenario: Metric identities, metadata, and attribute keys contain whitespace-only or control-character spellings.
/// Guarantees: Every nonempty metric name, unit, description, attribute key, and value remains verbatim.
#[test]
fn preserves_metric_identities_and_metadata() {
    let config = Config::from_json(&json!({
        "metrics": {
            " ": {
                "description": "\n",
                "unit": " ",
                "gauge": {}
            },
            "control\0name": {
                "description": " Available\tmemory.\n",
                "unit": "B\0y",
                "gauge": {}
            }
        },
        "perfcounters": [{
            "object": "Memory",
            "counters": [
                {
                    "name": "Available Bytes",
                    "metric": " ",
                    "attributes": {" ": " whitespace "}
                },
                {
                    "name": "Committed Bytes",
                    "metric": "control\0name",
                    "attributes": {"control\0key": "control\0value"}
                }
            ]
        }]
    }))
    .unwrap();

    let whitespace = &config.counters[0];
    assert_eq!(whitespace.name.as_ref(), " ");
    assert_eq!(whitespace.description.as_ref(), "\n");
    assert_eq!(whitespace.unit.as_ref(), " ");
    assert_eq!(
        *whitespace.attributes,
        BTreeMap::from([(" ".to_owned(), " whitespace ".to_owned())])
    );

    let controlled = &config.counters[1];
    assert_eq!(controlled.name.as_ref(), "control\0name");
    assert_eq!(controlled.description.as_ref(), " Available\tmemory.\n");
    assert_eq!(controlled.unit.as_ref(), "B\0y");
    assert_eq!(
        *controlled.attributes,
        BTreeMap::from([("control\0key".to_owned(), "control\0value".to_owned())])
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

/// Scenario: A leading-padded ordinary metric key is selected by the same exact reference.
/// Guarantees: Padding remains part of the case-sensitive metric identity and does not trigger path diagnostics.
#[test]
fn preserves_leading_padded_ordinary_reference() {
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
    assert_eq!(counter.name.as_ref(), configured);
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

/// Scenario: Metric names and units use unrestricted nonempty OTLP text or path-shaped names.
/// Guarantees: Every nonempty value survives normalization without SDK-style limits or character guards.
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
        (" ", " "),
        ("metric\nname", "B\ty"),
        (long_name.as_str(), long_unit.as_str()),
        (overlong_path_name.as_str(), "1"),
    ] {
        let normalized = Config::from_json(&config(name, unit)).unwrap();
        assert_eq!(normalized.counters[0].name.as_ref(), name);
        assert_eq!(normalized.counters[0].unit.as_ref(), unit);
    }
    assert_config_error(config("", "By"), "metric name must not be empty");
}

/// Scenario: Metric units are omitted, empty, whitespace-only, or contain control characters.
/// Guarantees: Only the literal empty unit defaults to `1`; every other configured unit remains verbatim.
#[test]
fn defaults_only_empty_metric_units() {
    let config = Config::from_json(&json!({
        "metrics": {
            "empty": {"unit": ""},
            "whitespace": {"unit": " \t "},
            "control": {"unit": "B\ny"}
        },
        "perfcounters": [{
            "object": "Memory",
            "counters": [
                {"name": "Available Bytes", "metric": "empty"},
                {"name": "Committed Bytes", "metric": "whitespace"},
                {"name": "Cache Bytes", "metric": "control"}
            ]
        }]
    }))
    .unwrap();

    assert_eq!(config.counters[0].unit.as_ref(), "1");
    assert_eq!(config.counters[1].unit.as_ref(), " \t ");
    assert_eq!(config.counters[2].unit.as_ref(), "B\ny");
}

/// Scenario: Path-shaped definitions are selected implicitly, by a wildcard template, or by an explicit alias.
/// Guarantees: Only raw path-shaped definitions unused by every mapping produce one usage-warning candidate.
#[test]
fn identifies_unused_path_metric_definitions() {
    let value = json!({
        "metrics": {
            r"\Memory\Available Bytes": {"unit": "By"},
            r"\Process(worker*)\Private Bytes": {"unit": "By"},
            r"\alias": {"unit": "By"},
            r"\unused": {"unit": "1"},
            r" \ordinary-leading-space": {"unit": "1"},
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
    });
    let user: UserConfig = serde_json::from_value(value.clone()).unwrap();
    let metrics = normalize_metrics(&user.metrics).unwrap();
    let config = Config::from_json(&value).unwrap();
    assert_eq!(
        unused_path_metric_definitions(
            metrics
                .iter()
                .map(|(name, metric)| (name.as_ref(), metric.path_shaped)),
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

/// Scenario: Metric units are empty or contain unrestricted OTLP unit text.
/// Guarantees: Literal empty defaults to "1"; every nonempty value remains verbatim.
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
    assert_eq!(
        Config::from_json(&config("")).unwrap().counters[0]
            .unit
            .as_ref(),
        "1"
    );
    let long_unit = format!("{{{}}}", "u".repeat(128));
    for unit in [
        "By",
        "{packets}/s",
        "\u{b5}s",
        "B y",
        " \t ",
        "B\ny",
        long_unit.as_str(),
    ] {
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

/// Scenario: Metric names and attribute keys differ only by surrounding spaces.
/// Guarantees: Exact configured identities coexist and references resolve only the matching spelling.
#[test]
fn preserves_space_distinct_metric_and_attribute_keys() {
    let definition = json!({"description": "Value.", "unit": "By", "gauge": {}});
    let config = Config::from_json(&json!({
        "metrics": {"cpu": definition.clone(), " cpu ": definition},
        "perfcounters": [{
            "object": "Processor",
            "counters": [
                {
                    "name": "% Processor Time",
                    "metric": "cpu",
                    "attributes": {"host": "a", " host": " b ", " ": "blank"}
                },
                {"name": "% Idle Time", "metric": " cpu "}
            ]
        }]
    }))
    .unwrap();
    assert_eq!(config.counters[0].name.as_ref(), "cpu");
    assert_eq!(config.counters[1].name.as_ref(), " cpu ");
    assert_eq!(
        *config.counters[0].attributes,
        BTreeMap::from([
            (" ".to_owned(), "blank".to_owned()),
            (" host".to_owned(), " b ".to_owned()),
            ("host".to_owned(), "a".to_owned()),
        ])
    );

    assert_config_error(
        json!({
            "metrics": {"cpu": metric()},
            "perfcounters": [{
                "object": "Processor",
                "counters": [{
                    "name": "% Processor Time",
                    "metric": " cpu "
                }]
            }]
        }),
        "references undefined metric",
    );
    assert_config_error(
        json!({
            "metrics": {"cpu": metric()},
            "perfcounters": [{
                "object": "Processor",
                "counters": [{
                    "name": "% Processor Time",
                    "metric": "cpu",
                    "attributes": {"": "invalid"}
                }]
            }]
        }),
        "attribute key must not be empty",
    );
}

/// Scenario: A configured data-point attribute uses the receiver-owned namespace.
/// Guarantees: Validation rejects the reserved prefix and variants that remain exact prefix matches.
#[test]
fn rejects_reserved_attribute_prefix() {
    for key in [
        "windows.perf_counter.",
        "windows.perf_counter.custom",
        "windows.perf_counter.path ",
        "windows.perf_counter.\0",
    ] {
        assert_config_error(
            json!({
                "metrics": {"available": {"gauge": {}}},
                "perfcounters": [{
                    "object": "Memory",
                    "counters": [{
                        "name": "Available Bytes",
                        "metric": "available",
                        "attributes": {(key): "value"}
                    }]
                }]
            }),
            &format!("perfcounters[0].counters[0] attribute {key:?} conflicts"),
        );
    }
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

/// Scenario: A valid configuration has a case collision, an unused path definition, and a multi-instance static override.
/// Guarantees: Parsing collects each warning exactly once for emission only after full validation succeeds.
#[test]
fn collects_config_warnings_after_validation() {
    let config = Config::from_json(&json!({
        "metrics": {
            "cpu": {"unit": "1"},
            "CPU": {"unit": "%"},
            r"\Memory\Unused": {"unit": "By"}
        },
        "perfcounters": [{
            "object": "Processor",
            "instances": "*",
            "counters": [{
                "name": "% Processor Time",
                "metric": "cpu",
                "attributes": {"instance": "fixed"}
            }]
        }]
    }))
    .unwrap();

    assert_eq!(config.warnings.len(), 3);
    assert!(matches!(
        &config.warnings[0],
        ConfigWarning::CaseDistinctMetricDefinitions(names)
            if names == &vec!["CPU".to_owned(), "cpu".to_owned()]
    ));
    assert_eq!(
        config.warnings[1],
        ConfigWarning::InstanceOverride {
            object_index: 0,
            counter_index: 0,
        }
    );
    assert_eq!(
        config.warnings[2],
        ConfigWarning::UnusedPathMetricDefinition(r"\Memory\Unused".to_owned())
    );
}

/// Scenario: Instances are empty, use `*` or `?` partial wildcards, or combine full wildcard with explicit names.
/// Guarantees: Empty means no instance, partial patterns remain wildcard templates, and only literal `*` selects all instances.
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

    let question = Config::from_json(&json!({
        "perfcounters": [{
            "object": "Process",
            "instances": ["svc?ost"],
            "counters": [{"name": "ID Process"}]
        }]
    }))
    .unwrap();
    assert_eq!(question.counters[0].path, r"\Process(svc?ost)\ID Process");
    assert!(question.counters[0].instance_config.is_wildcard());
    assert_eq!(
        question.counters[0].instance_config.excluded_aggregation(),
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
        ("aggregation", "_To\0tal"),
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
            "aggregation" => json!({
                "object": "Process",
                "instances": "*",
                "aggregation_name": value,
                "counters": [{"name": "Private Bytes", "metric": "test"}]
            }),
            _ => json!({
                "object": "Memory",
                "counters": [{"name": value, "metric": "test"}]
            }),
        };
        assert_config_error(
            json!({"metrics": {"test": metric()}, "perfcounters": [object]}),
            "must not contain NUL",
        );
    }
}

/// Scenario: Native object, instance, and counter components are empty, or aggregation names are empty or whitespace-only.
/// Guarantees: Required literal empties fail, only an empty aggregation defaults, and whitespace-only spellings remain configured identity.
#[test]
fn handles_empty_native_components_and_aggregation() {
    for value in [
        json!({
            "perfcounters": [{
                "object": "",
                "counters": [{"name": "Available Bytes"}]
            }]
        }),
        json!({
            "perfcounters": [{
                "object": "Process",
                "instances": "",
                "counters": [{"name": "Private Bytes"}]
            }]
        }),
        json!({
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": ""}]
            }]
        }),
    ] {
        assert_config_error(value, "must not be empty");
    }

    let defaulted = Config::from_json(&json!({
        "perfcounters": [{
            "object": "Process",
            "instances": "*",
            "aggregation_name": "",
            "counters": [{"name": "Private Bytes"}]
        }]
    }))
    .unwrap();
    assert_eq!(
        defaulted.counters[0].instance_config.excluded_aggregation(),
        Some("_Total")
    );

    let whitespace = Config::from_json(&json!({
        "perfcounters": [{
            "object": " ",
            "instances": "*",
            "aggregation_name": " ",
            "counters": [{"name": " "}]
        }]
    }))
    .unwrap();
    assert_eq!(whitespace.counters[0].path, r"\ (*)\ ");
    assert_eq!(
        whitespace.counters[0]
            .instance_config
            .excluded_aggregation(),
        Some(" ")
    );
}

/// Scenario: Native object, instance, and counter components contain whitespace or non-NUL control characters.
/// Guarantees: Every nonempty native component reaches the constructed PDH path without receiver rewriting or rejection.
#[test]
fn preserves_non_nul_native_component_controls() {
    let config = Config::from_json(&json!({
        "perfcounters": [{
            "object": "Mem\tory",
            "instances": "work\ner",
            "counters": [{"name": "Private\rBytes"}]
        }]
    }))
    .unwrap();

    assert_eq!(
        config.counters[0].path,
        "\\Mem\tory(work\ner)\\Private\rBytes"
    );
    assert_eq!(
        config.counters[0].name.as_ref(),
        "\\Mem\tory(work\ner)\\Private\rBytes"
    );
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
    let config = |perfcounters| {
        json!({
            "metrics": {"test": metric()},
            "perfcounters": perfcounters
        })
    };
    assert_eq!(
        Config::from_json(&config(json!([object(16, 16)])))
            .unwrap()
            .counters
            .len(),
        256
    );
    assert_config_error(
        config(json!([object(257, 16)])),
        "normalize to 4097 exact counter paths, exceeding max_expanded_counters 4096",
    );
    assert_config_error(
        config(json!([object(1, 2049), object(1, 2048)])),
        "normalize to 4097 exact counter paths, exceeding max_expanded_counters 4096",
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
            "perfcounters": [object(257)]
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
        "instances": ["svc?ost"],
        "counters": [{"name": "Wildcard"}]
    });

    let config = Config::from_json(&json!({
        "perfcounters": [omitted, empty, selected, wildcard]
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

/// Scenario: One normalized PDH path is configured repeatedly with different output metadata.
/// Guarantees: MVP rejects duplicate paths regardless of metric, scaling or static attributes before starting a worker.
#[test]
fn rejects_duplicate_paths_across_output_mappings() {
    let error = Config::from_json(&json!({
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
    .unwrap_err();
    assert!(error.to_string().contains("duplicate counter path"));

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
        "duplicate counter path",
    );
    assert_config_error(
        json!({
            "metrics": {"available.bytes": {"unit": "By"}},
            "perfcounters": [{
                "object": "Memory",
                "counters": [
                    {"name": "Available Bytes", "metric": "available.bytes"},
                    {
                        "name": "Available Bytes",
                        "metric": "available.bytes",
                        "scale_power10": -3,
                        "attributes": {"source": "second"}
                    }
                ]
            }]
        }),
        "duplicate counter path",
    );
}

/// Scenario: Explicit instances and the aggregation name contain zero or leading-zero numeric suffixes.
/// Guarantees: Every configured suffix remains distinct and reaches the native path or wildcard filter unchanged.
#[test]
fn preserves_explicit_instance_indexes() {
    let config = Config::from_json(&json!({
        "metrics": {"test": metric()},
        "perfcounters": [{
            "object": "Process",
            "instances": ["app", "app#0", "app#1", "app#01", "worker*"],
            "aggregation_name": "_Total#00",
            "counters": [{"name": "Private Bytes", "metric": "test"}]
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
            r"\Process(app)\Private Bytes",
            r"\Process(app#0)\Private Bytes",
            r"\Process(app#1)\Private Bytes",
            r"\Process(app#01)\Private Bytes",
            r"\Process(worker*)\Private Bytes",
        ]
    );
    assert_eq!(
        config.counters[4].instance_config.excluded_aggregation(),
        Some("_Total#00")
    );
}

/// Scenario: An instance name starts with `#` and otherwise resembles a PDH duplicate suffix.
/// Guarantees: `#5` remains a literal native instance because duplicate indexes require a nonempty name before `#`.
#[test]
fn preserves_hash_prefixed_instance_name() {
    let config = Config::from_json(&json!({
        "metrics": {"test": metric()},
        "perfcounters": [{
            "object": "Process",
            "instances": "#5",
            "counters": [{"name": "Private Bytes", "metric": "test"}]
        }]
    }))
    .unwrap();

    assert_eq!(config.counters[0].path, r"\Process(#5)\Private Bytes");
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
/// Guarantees: Delta Sum definitions remain rejected, including when unused.
#[test]
fn normalizes_delta_sum_configuration() {
    for monotonic in [false, true] {
        let value = json!({
            "metrics": {"test": {
                "description": "Value.",
                "unit": "1",
                "sum": {"aggregation": "delta", "monotonic": monotonic}
            }},
            "perfcounters": [{
                "object": "Memory",
                "counters": [{"name": "Available Bytes", "metric": "test"}]
            }]
        });
        assert_config_error(
            value.clone(),
            "metrics.test.sum supports only cumulative, non-monotonic",
        );
        let mut unused = value;
        let _ = unused["perfcounters"][0]["counters"][0]
            .as_object_mut()
            .unwrap()
            .remove("metric");
        assert_config_error(
            unused,
            "metrics.test.sum supports only cumulative, non-monotonic",
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
