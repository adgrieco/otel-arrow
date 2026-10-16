// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_config::error::Error;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

pub(super) const MIN_SCALE_POWER10: i32 = -18;
pub(super) const MAX_SCALE_POWER10: i32 = 18;

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
    /// Optional discard deadline for one scrape; `None` waits for completion.
    pub(super) timeout: Option<Duration>,
    /// Time between wildcard discovery refreshes; defaults to the collection interval.
    pub(super) wildcard_refresh_interval: Option<Duration>,
    /// Maximum expanded instances retained for one wildcard path.
    pub(super) max_instances_per_wildcard: usize,
    /// Receiver-wide entries reserved for exact or retained expanded native counters.
    pub(super) max_expanded_counters: usize,
}

impl Config {
    /// Parse, validate, and normalize the public configuration contract.
    pub(super) fn from_json(value: &serde_json::Value) -> Result<Self, Error> {
        let user: UserConfig =
            serde_json::from_value(value.clone()).map_err(|err| invalid(err.to_string()))?;
        validate_user_config(&user)?;
        let metrics = normalize_metrics(&user.metrics)?;
        let counters = normalize_counters(user.perfcounters, &metrics, user.max_expanded_counters)?;
        validate_counter_outputs(&counters)?;
        warn_for_unused_path_metric_definitions(&metrics, &counters);

        Ok(Self {
            counters,
            collection_interval: user.collection_interval,
            initial_delay: user.initial_delay,
            timeout: match user.timeout {
                Some(Duration::ZERO) => None,
                Some(timeout) => Some(timeout),
                None => None,
            },
            wildcard_refresh_interval: user.wildcard_refresh_interval,
            max_instances_per_wildcard: user.max_instances_per_wildcard,
            max_expanded_counters: user.max_expanded_counters,
        })
    }

    /// Effective wildcard refresh interval.
    #[must_use]
    pub(super) fn wildcard_refresh_interval(&self) -> Duration {
        self.wildcard_refresh_interval
            .unwrap_or(self.collection_interval)
    }
}

/// One normalized counter consumed by the existing PDH worker and OTAP builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CounterConfig {
    /// PDH path, optionally wildcarding only the instance segment.
    pub(super) path: String,
    /// OTel metric name shared by counters that use the same definition.
    pub(super) name: Arc<str>,
    /// OTel metric unit shared by counters that use the same definition.
    pub(super) unit: Arc<str>,
    /// OTel metric description shared by counters that use the same definition.
    pub(super) description: Arc<str>,
    /// OTel metric stream kind.
    pub(super) metric_kind: MetricKind,
    /// Static attributes shared by paths expanded from one counter mapping.
    pub(super) attributes: Arc<BTreeMap<String, String>>,
    /// Instance parsing, attribute, and wildcard aggregation behavior.
    pub(super) instance_config: InstanceConfig,
    /// Base-10 scaling applied after PDH calculates the native value.
    pub(super) scale_power10: i32,
    /// Whether this configured mapping receives a fresh, primed query after each scrape.
    pub(super) recreate_query: bool,
    /// Identity shared by normalized paths expanded from one configured mapping.
    pub(super) source_mapping_index: usize,
}

/// Valid instance behavior for one normalized counter path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum InstanceConfig {
    /// The path has no instance segment.
    None,
    /// The path selects one exact instance.
    Exact { emit_attribute: bool },
    /// The path discovers instances and may exclude one provider aggregate.
    Wildcard {
        excluded_aggregation: Option<String>,
    },
}

impl InstanceConfig {
    pub(super) fn has_instance(&self) -> bool {
        !matches!(self, Self::None)
    }

    pub(super) fn emit_attribute(&self) -> bool {
        match self {
            Self::None => false,
            Self::Exact { emit_attribute } => *emit_attribute,
            Self::Wildcard { .. } => true,
        }
    }

    pub(super) fn is_wildcard(&self) -> bool {
        matches!(self, Self::Wildcard { .. })
    }

    pub(super) fn excluded_aggregation(&self) -> Option<&str> {
        match self {
            Self::Wildcard {
                excluded_aggregation,
            } => excluded_aggregation.as_deref(),
            Self::None | Self::Exact { .. } => None,
        }
    }
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
    // Preserve presence so a null reference can identify the exact invalid mapping.
    #[serde(default, deserialize_with = "present_metric")]
    metric: Option<Option<String>>,
    #[serde(default)]
    recreate_query: bool,
    #[serde(default)]
    attributes: BTreeMap<String, String>,
    #[serde(default)]
    scale_power10: i32,
}

fn present_metric<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
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

fn validate_no_control_characters(field: &str, value: &str) -> Result<(), Error> {
    if value.chars().any(char::is_control) {
        return Err(invalid(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_native_component<'a>(field: &str, value: &'a str) -> Result<&'a str, Error> {
    require_name(field, value)?;
    validate_no_control_characters(field, value)?;
    Ok(value)
}

fn validate_object<'a>(field: &str, value: &'a str) -> Result<&'a str, Error> {
    validate_native_component(field, value)
}

/// Splits a PDH duplicate-instance suffix, such as `app#2`, into name and index.
fn instance_index(value: &str) -> Option<(&str, &str)> {
    value
        .rsplit_once('#')
        .filter(|(_, index)| !index.is_empty() && index.bytes().all(|b| b.is_ascii_digit()))
}

fn validate_instance<'a>(field: &str, value: &'a str) -> Result<&'a str, Error> {
    let value = validate_native_component(field, value)?;
    if instance_index(value).is_some_and(|(name, _)| name.is_empty()) {
        return Err(invalid(format!(
            "{field} must include an instance name before its index"
        )));
    }
    Ok(value)
}

fn validate_instance_pattern<'a>(field: &str, value: &'a str) -> Result<&'a str, Error> {
    validate_native_component(field, value)
}

/// Canonicalizes PDH's decimal instance index, where the first occurrence omits `#0`.
/// Returns `(canonical, configured)`.
fn normalize_instance<'a>(field: &str, value: &'a str) -> Result<(String, &'a str), Error> {
    let value = validate_instance(field, value)?;
    let canonical = match instance_index(value) {
        Some((name, index)) => match index.trim_start_matches('0') {
            "" => name.to_owned(),
            index => format!("{name}#{index}"),
        },
        None => value.to_owned(),
    };
    Ok((canonical, value))
}

fn validate_counter_name<'a>(field: &str, value: &'a str) -> Result<&'a str, Error> {
    validate_native_component(field, value)
}

fn is_path_metric_name(name: &str) -> bool {
    name.starts_with('\\')
}

fn normalize_metric_name(name: &str) -> Result<(&str, bool), Error> {
    let path_shaped = is_path_metric_name(name);
    let trimmed = name.trim();
    require_name("metric name", trimmed)?;
    // Some Windows counter names end in a space, so a trimmed path may not resolve.
    let name = if path_shaped { name } else { trimmed };
    validate_no_control_characters("metric name", name)?;
    Ok((name, path_shaped))
}

fn normalize_metric_unit(unit: &str) -> Result<&str, Error> {
    let unit = unit.trim();
    let unit = if unit.is_empty() { "1" } else { unit };
    validate_no_control_characters("metric unit", unit)?;
    Ok(unit)
}

fn should_warn_instance_override(
    has_multiple_or_wildcard_instances: bool,
    attributes: &BTreeMap<String, String>,
) -> bool {
    attributes.contains_key("instance") && has_multiple_or_wildcard_instances
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

struct NormalizedMetric {
    description: Arc<str>,
    unit: Arc<str>,
    kind: MetricKind,
    path_shaped: bool,
}

static DEFAULT_METRIC: LazyLock<NormalizedMetric> = LazyLock::new(default_metric);

fn default_metric() -> NormalizedMetric {
    NormalizedMetric {
        description: Arc::from(""),
        unit: Arc::from("1"),
        kind: MetricKind::Gauge,
        path_shaped: false,
    }
}

#[derive(Clone)]
struct ResolvedMetric {
    name: Arc<str>,
    unit: Arc<str>,
    description: Arc<str>,
    kind: MetricKind,
    attributes: Arc<BTreeMap<String, String>>,
}

enum NormalizedInstances {
    None,
    All { include_aggregation: bool },
    Selected(Vec<InstanceSegment>),
}

enum InstanceSegment {
    Exact { native: String, configured: String },
    Wildcard(String),
}

struct ExpandedPath {
    native: String,
    configured: String,
    instance_config: InstanceConfig,
}

struct ObjectNormalizer<'a> {
    object_index: usize,
    object_name: &'a str,
    aggregation_name: &'a str,
    instances: &'a NormalizedInstances,
    metrics: &'a BTreeMap<Arc<str>, NormalizedMetric>,
}

fn validate_user_config(user: &UserConfig) -> Result<(), Error> {
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
    Ok(())
}

fn normalize_metrics(
    configured: &BTreeMap<String, MetricConfig>,
) -> Result<BTreeMap<Arc<str>, NormalizedMetric>, Error> {
    let mut metrics = BTreeMap::new();
    let mut configured_by_identity = BTreeMap::new();
    for (configured_name, metric) in configured {
        let (name, path_shaped) = normalize_metric_name(configured_name)?;
        if let Some(first) =
            configured_by_identity.insert(name.to_owned(), configured_name.as_str())
        {
            return Err(invalid(format!(
                "metrics keys {first:?} and {configured_name:?} normalize to duplicate {name:?}"
            )));
        }
        let _ = metrics.insert(
            Arc::from(name),
            NormalizedMetric {
                description: Arc::from(metric.description.trim()),
                unit: Arc::from(normalize_metric_unit(&metric.unit)?),
                kind: metric.kind(name)?,
                path_shaped,
            },
        );
    }
    for group in case_distinct_metric_definitions(metrics.keys().map(Arc::as_ref)).into_values() {
        otel_arrow_dfe_telemetry::otel_warn!(
            "windows_perf_counters_receiver.config.metric_definition.case_collision",
            message = "metric definitions differ only by case; exact lookup keeps them distinct, \
                       but this may be a configuration typo",
            names = format!("{group:?}")
        );
    }
    Ok(metrics)
}

fn case_distinct_metric_definitions<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> BTreeMap<String, Vec<&'a str>> {
    let mut groups = BTreeMap::<String, Vec<&str>>::new();
    for name in names {
        groups.entry(name.to_lowercase()).or_default().push(name);
    }
    groups.retain(|_, group| group.len() > 1);
    groups
}

fn unused_path_metric_definitions<'a>(
    definitions: impl IntoIterator<Item = (&'a str, bool)>,
    selected: impl IntoIterator<Item = &'a str>,
) -> Vec<&'a str> {
    let selected = selected.into_iter().collect::<HashSet<_>>();
    definitions
        .into_iter()
        .filter_map(|(name, path_shaped)| (path_shaped && !selected.contains(name)).then_some(name))
        .collect()
}

fn warn_for_unused_path_metric_definitions(
    metrics: &BTreeMap<Arc<str>, NormalizedMetric>,
    counters: &[CounterConfig],
) {
    for definition in unused_path_metric_definitions(
        metrics
            .iter()
            .map(|(name, metric)| (name.as_ref(), metric.path_shaped)),
        counters.iter().map(|counter| counter.name.as_ref()),
    ) {
        otel_arrow_dfe_telemetry::otel_warn!(
            "windows_perf_counters_receiver.config.metric_definition.unused_path",
            message = "path-shaped metric definition is not selected by any counter mapping",
            definition = definition
        );
    }
}

fn normalize_counters(
    objects: Vec<ObjectConfig>,
    metrics: &BTreeMap<Arc<str>, NormalizedMetric>,
    max_native_counters: usize,
) -> Result<Vec<CounterConfig>, Error> {
    let mut counters = Vec::new();
    let mut source_mapping_index = 0;
    let mut exact_counter_count = 0;
    for (object_index, object) in objects.into_iter().enumerate() {
        normalize_object(
            object_index,
            object,
            metrics,
            max_native_counters,
            &mut exact_counter_count,
            &mut counters,
            &mut source_mapping_index,
        )?;
    }
    Ok(counters)
}

fn normalize_object(
    object_index: usize,
    object: ObjectConfig,
    metrics: &BTreeMap<Arc<str>, NormalizedMetric>,
    max_native_counters: usize,
    exact_counter_count: &mut usize,
    output: &mut Vec<CounterConfig>,
    source_mapping_index: &mut usize,
) -> Result<(), Error> {
    let object_field = format!("perfcounters[{object_index}]");
    let object_name = validate_object(&format!("{object_field}.object"), &object.object)?;
    if object.counters.is_empty() {
        return Err(invalid(format!(
            "{object_field}.counters must contain at least one entry"
        )));
    }
    let aggregation_name = normalize_aggregation_name(&object_field, object.aggregation_name)?;
    let instances =
        NormalizedInstances::from_config(&object_field, object.instances, &aggregation_name)?;
    let normalizer = ObjectNormalizer {
        object_index,
        object_name,
        aggregation_name: &aggregation_name,
        instances: &instances,
        metrics,
    };
    for (counter_index, mapping) in object.counters.into_iter().enumerate() {
        normalizer.normalize_mapping(
            counter_index,
            mapping,
            *source_mapping_index,
            max_native_counters,
            exact_counter_count,
            output,
        )?;
        *source_mapping_index += 1;
    }
    Ok(())
}

fn normalize_aggregation_name(
    object_field: &str,
    configured: Option<String>,
) -> Result<String, Error> {
    let configured = configured
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_AGGREGATION_NAME.to_owned());
    normalize_instance(&format!("{object_field}.aggregation_name"), &configured)
        .map(|(canonical, _)| canonical)
}

impl ObjectNormalizer<'_> {
    fn normalize_mapping(
        &self,
        counter_index: usize,
        mapping: CounterMapping,
        source_mapping_index: usize,
        max_native_counters: usize,
        exact_counter_count: &mut usize,
        output: &mut Vec<CounterConfig>,
    ) -> Result<(), Error> {
        let counter_field = format!(
            "perfcounters[{}].counters[{counter_index}]",
            self.object_index
        );
        let counter_name = validate_counter_name(&format!("{counter_field}.name"), &mapping.name)?;
        let metric_name = normalize_metric_reference(&counter_field, &mapping.metric)?;
        let explicit_metric = self.normalize_explicit_metric(
            counter_index,
            &counter_field,
            metric_name,
            &mapping.attributes,
        )?;
        for expanded in self
            .instances
            .expand(self.object_name, counter_name, self.aggregation_name)
        {
            let metric = self.resolve_metric(&expanded.configured, explicit_metric.as_ref());
            if !expanded.instance_config.is_wildcard() {
                *exact_counter_count = exact_counter_count.saturating_add(1);
                if *exact_counter_count > max_native_counters {
                    return Err(invalid(format!(
                        "perfcounters normalize to {exact_counter_count} exact counter paths, exceeding max_expanded_counters {max_native_counters}"
                    )));
                }
            }
            output.push(CounterConfig {
                path: expanded.native,
                name: metric.name,
                unit: metric.unit,
                description: metric.description,
                metric_kind: metric.kind,
                attributes: metric.attributes,
                instance_config: expanded.instance_config,
                scale_power10: mapping.scale_power10,
                recreate_query: mapping.recreate_query,
                source_mapping_index,
            });
        }
        Ok(())
    }

    fn resolve_metric(
        &self,
        configured_path: &str,
        explicit: Option<&ResolvedMetric>,
    ) -> ResolvedMetric {
        if let Some(explicit) = explicit {
            return explicit.clone();
        }
        if let Some((name, metric)) = self.metrics.get_key_value(configured_path) {
            ResolvedMetric {
                name: Arc::clone(name),
                unit: Arc::clone(&metric.unit),
                description: Arc::clone(&metric.description),
                kind: metric.kind,
                attributes: Arc::new(BTreeMap::new()),
            }
        } else {
            ResolvedMetric {
                name: Arc::from(configured_path),
                unit: Arc::clone(&DEFAULT_METRIC.unit),
                description: Arc::clone(&DEFAULT_METRIC.description),
                kind: DEFAULT_METRIC.kind,
                attributes: Arc::new(BTreeMap::new()),
            }
        }
    }

    fn normalize_explicit_metric(
        &self,
        counter_index: usize,
        counter_field: &str,
        metric_name: Option<&str>,
        configured_attributes: &BTreeMap<String, String>,
    ) -> Result<Option<ResolvedMetric>, Error> {
        let Some(name) = metric_name else {
            return Ok(None);
        };
        let (name, metric) = self.metrics.get_key_value(name).ok_or_else(|| {
            invalid(format!(
                "{counter_field}.metric references undefined metric {name:?}"
            ))
        })?;
        let attributes = normalize_attributes(counter_field, configured_attributes)?;
        if should_warn_instance_override(self.instances.has_multiple_or_wildcard(), &attributes) {
            otel_arrow_dfe_telemetry::otel_warn!(
                "windows_perf_counters_receiver.config.instance_override",
                message = "a static instance attribute overrides generated instance identity for \
                           multiple or wildcard instances and may merge distinct series",
                object_index = self.object_index,
                counter_index = counter_index
            );
        }
        Ok(Some(ResolvedMetric {
            name: Arc::clone(name),
            unit: Arc::clone(&metric.unit),
            description: Arc::clone(&metric.description),
            kind: metric.kind,
            attributes: Arc::new(attributes),
        }))
    }
}

fn normalize_metric_reference<'a>(
    counter_field: &str,
    configured: &'a Option<Option<String>>,
) -> Result<Option<&'a str>, Error> {
    match configured {
        None => Ok(None),
        Some(Some(name)) => {
            if name.trim().is_empty() {
                return Ok(None);
            }
            let (name, _) = normalize_metric_name(name)?;
            Ok(Some(name))
        }
        Some(None) => Err(invalid(format!(
            "{counter_field}.metric must be a string when present; null metric is invalid"
        ))),
    }
}

impl NormalizedInstances {
    fn from_config(
        object_field: &str,
        configured: Option<OneOrMany>,
        aggregation_name: &str,
    ) -> Result<Self, Error> {
        let Some(configured) = configured.map(OneOrMany::into_vec) else {
            return Ok(Self::None);
        };
        if configured.is_empty() {
            return Ok(Self::None);
        }
        let field = format!("{object_field}.instances");
        let mut identities = HashSet::new();
        let mut segments = Vec::with_capacity(configured.len());
        for configured in configured {
            let segment = InstanceSegment::from_config(&field, configured)?;
            if !identities.insert(segment.native().to_lowercase()) {
                return Err(invalid(format!(
                    "{object_field}.instances contains duplicate {:?}",
                    segment.configured()
                )));
            }
            segments.push(segment);
        }
        if segments.iter().any(InstanceSegment::selects_all) {
            return Ok(Self::All {
                include_aggregation: segments
                    .iter()
                    .any(|segment| segment.is_exact(aggregation_name)),
            });
        }
        Ok(Self::Selected(segments))
    }

    fn has_multiple_or_wildcard(&self) -> bool {
        match self {
            Self::None => false,
            Self::All { .. } => true,
            Self::Selected(segments) => {
                segments.len() > 1 || segments.iter().any(InstanceSegment::is_wildcard)
            }
        }
    }

    fn expand(&self, object: &str, counter: &str, aggregation_name: &str) -> Vec<ExpandedPath> {
        match self {
            Self::None => vec![ExpandedPath {
                native: counter_path(object, None, counter),
                configured: counter_path(object, None, counter),
                instance_config: InstanceConfig::None,
            }],
            Self::All {
                include_aggregation,
            } => vec![ExpandedPath {
                native: counter_path(object, Some("*"), counter),
                configured: counter_path(object, Some("*"), counter),
                instance_config: InstanceConfig::Wildcard {
                    excluded_aggregation: (!include_aggregation)
                        .then(|| aggregation_name.to_owned()),
                },
            }],
            Self::Selected(segments) => segments
                .iter()
                .map(|segment| ExpandedPath {
                    native: counter_path(object, Some(segment.native()), counter),
                    configured: counter_path(object, Some(segment.configured()), counter),
                    instance_config: if segment.is_wildcard() {
                        InstanceConfig::Wildcard {
                            excluded_aggregation: Some(aggregation_name.to_owned()),
                        }
                    } else {
                        InstanceConfig::Exact {
                            emit_attribute: !segment.is_exact(aggregation_name),
                        }
                    },
                })
                .collect(),
        }
    }
}

impl InstanceSegment {
    fn from_config(field: &str, configured: String) -> Result<Self, Error> {
        if configured.contains('*') {
            Ok(Self::Wildcard(
                validate_instance_pattern(field, &configured)?.to_owned(),
            ))
        } else {
            let (native, configured) = normalize_instance(field, &configured)?;
            Ok(Self::Exact {
                native,
                configured: configured.to_owned(),
            })
        }
    }

    fn native(&self) -> &str {
        match self {
            Self::Exact { native, .. } => native,
            Self::Wildcard(configured) => configured,
        }
    }

    fn configured(&self) -> &str {
        match self {
            Self::Exact { configured, .. } | Self::Wildcard(configured) => configured,
        }
    }

    fn is_wildcard(&self) -> bool {
        matches!(self, Self::Wildcard(_))
    }

    fn selects_all(&self) -> bool {
        matches!(self, Self::Wildcard(configured) if configured == "*")
    }

    fn is_exact(&self, expected: &str) -> bool {
        matches!(self, Self::Exact { native, .. } if native == expected)
    }
}

fn counter_path(object: &str, instance: Option<&str>, counter: &str) -> String {
    match instance {
        Some(instance) => format!(r"\{object}({instance})\{counter}"),
        None => format!(r"\{object}\{counter}"),
    }
}

fn validate_counter_outputs(counters: &[CounterConfig]) -> Result<(), Error> {
    let mut output_identities = HashSet::with_capacity(counters.len());
    for counter in counters {
        validate_counter(counter)?;
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
    Ok(())
}

fn validate_path_length(field: &str, path: &str) -> Result<(), Error> {
    if path.encode_utf16().count() > MAX_COUNTER_PATH_LEN {
        return Err(invalid(format!(
            "{field} {path:?} must contain at most {MAX_COUNTER_PATH_LEN} UTF-16 code units"
        )));
    }
    Ok(())
}

fn validate_counter(counter: &CounterConfig) -> Result<(), Error> {
    validate_path_length("counter path", &counter.path)?;
    validate_no_control_characters("metric name", &counter.name)?;
    validate_no_control_characters("metric unit", &counter.unit)?;
    if !(MIN_SCALE_POWER10..=MAX_SCALE_POWER10).contains(&counter.scale_power10) {
        return Err(invalid(format!(
            "counter path {:?} scale_power10 must be between {MIN_SCALE_POWER10} and {MAX_SCALE_POWER10}",
            counter.path
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
