// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Portable values shared by configuration, PDH collection, and OTAP projection.

use super::config::{MAX_SCALE_POWER10, MIN_SCALE_POWER10};
use std::collections::HashMap;
use std::sync::Arc;

// F6-sized measurements put the metadata-index crossover above 128x128 comparisons.
const METADATA_INDEX_WORK_THRESHOLD: usize = 32_768;
const MAX_METADATA_INDEX_SPARSITY: usize = 4;

#[derive(Clone, Copy, Eq, PartialEq)]
struct MetadataKey {
    counter_index: usize,
    reason: &'static str,
}

impl MetadataKey {
    fn new(counter_index: usize, reason: &'static str) -> Self {
        Self {
            counter_index,
            reason,
        }
    }
}

/// A numeric performance-counter value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Number {
    /// An exact signed integer.
    Integer(i64),
    /// A finite calculated or scaled value, including subnormals and signed zero.
    Double(f64),
}

/// A bounded reason why a numeric value cannot be scaled.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(super) enum ScaleError {
    /// The input or rounded result is NaN or infinite.
    #[error("scaling input or result is not finite")]
    NonFinite,
    /// The decimal exponent is outside the supported range.
    #[error(
        "scale_power10 must be between {} and {}",
        MIN_SCALE_POWER10,
        MAX_SCALE_POWER10
    )]
    InvalidScale,
}

/// One configured counter's state for a collection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum SampleValue {
    /// A ready numeric value.
    Value(Number),
    /// A two-sample counter is in its bounded first-scrape warm-up.
    Warming,
    /// A valid base did not advance because no operations were observed.
    NoObservation,
}

/// Identity parsed from one expanded wildcard instance path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct InstanceIdentity {
    /// Instance name without the duplicate index suffix.
    pub(super) name: String,
    /// Optional parent instance name.
    pub(super) parent: Option<String>,
    /// Duplicate instance index used by PDH to distinguish equal names.
    pub(super) index: u32,
}

/// One delivered monotonic cumulative point whose start can be committed by the worker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MonotonicPublication {
    pub(super) counter_index: usize,
    pub(super) path: Arc<str>,
    pub(super) query_generation: u64,
    pub(super) timestamp_unix_nano: i64,
}

/// Publication state for an unknown monotonic cumulative start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum MonotonicPublicationCandidate {
    /// The query identified a point whose start is not published yet.
    Pending,
    /// The worker attached the query identity needed to publish the start.
    Ready(MonotonicPublication),
}

/// One exact or expanded counter point in a collection.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SamplePoint {
    /// Index of the configured counter that defines metric metadata.
    pub(super) counter_index: usize,
    /// Exact configured path or concrete expanded wildcard path.
    pub(super) path: Arc<str>,
    /// Parsed identity for an expanded wildcard instance.
    pub(super) instance: Option<Arc<InstanceIdentity>>,
    /// Whether to emit the Go-compatible `instance` attribute for this point.
    pub(super) emit_instance_attribute: bool,
    /// End of this point's observation or delta interval.
    pub(super) timestamp_unix_nano: i64,
    /// Start of this point's cumulative sequence or delta interval.
    pub(super) start_time_unix_nano: Option<i64>,
    /// Worker-owned acknowledgement state for an unknown monotonic cumulative start.
    pub(super) monotonic_publication: Option<MonotonicPublicationCandidate>,
    /// Ready value or expected omission.
    pub(super) value: SampleValue,
}

/// One counter-local failure omitted from an otherwise successful collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SampleFailure {
    /// Index of the configured exact or wildcard path.
    pub(super) counter_index: usize,
    /// Concrete native path, or the configured template for query-wide failures.
    pub(super) path: Arc<str>,
    /// Low-cardinality failure category.
    pub(super) reason: &'static str,
    /// Native status or calculation detail without expanded instance identity.
    pub(super) error: String,
    /// Whether this entry exists only to provide per-mapping log context.
    pub(super) diagnostic_only: bool,
}

/// One explicit wildcard expansion limit reached during discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ExpansionOverflow {
    /// Index of the configured wildcard path.
    pub(super) counter_index: usize,
    /// Low-cardinality limit category.
    pub(super) reason: &'static str,
    /// Number of concrete paths discovered before this limit was applied.
    pub(super) discovered: usize,
    /// Number of concrete paths retained after this limit was applied.
    pub(super) retained: usize,
    /// Number of concrete paths omitted by this limit.
    pub(super) omitted: usize,
}

/// Bounded operational changes observed while producing one sample.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct SampleDiagnostics {
    /// Number of currently active expanded wildcard counters.
    pub(super) active_expanded_counters: usize,
    /// Number of wildcard discovery refreshes attempted.
    pub(super) discovery_refreshes: u64,
    /// Number of wildcard discovery attempts that failed.
    pub(super) discovery_failures: u64,
    /// Number of newly active expanded instances.
    pub(super) instances_added: u64,
    /// Number of expanded instances removed.
    pub(super) instances_removed: u64,
    /// Number of expanded instances omitted by configured limits.
    pub(super) instances_omitted_over_limit: u64,
    /// Number of counter-add attempts that failed.
    pub(super) counter_add_failures: u64,
    /// Number of counter read, status, or projection failures.
    pub(super) counter_read_failures: u64,
    /// Number of query-level collection failures preserved outside a requested scrape.
    pub(super) scrape_failures: u64,
    /// Number of deferred counter retry attempts.
    pub(super) retry_attempts: u64,
    /// Number of deferred counter retries that recovered.
    pub(super) retry_recoveries: u64,
    /// Number of worker-owned query rebuilds attempted.
    pub(super) query_rebuild_attempts: u64,
    /// Number of worker-owned query rebuilds that recovered.
    pub(super) query_rebuild_recoveries: u64,
    /// Number of configured post-scrape query recreations attempted.
    pub(super) query_recreation_attempts: u64,
    /// Number of configured post-scrape query recreations that failed.
    pub(super) query_recreation_failures: u64,
    /// Number of independently warming points omitted.
    pub(super) warmup_omissions: u64,
}

#[derive(Debug, Clone, Default)]
pub(super) struct PendingMetadata {
    pub(super) diagnostics: SampleDiagnostics,
    pub(super) failures: Vec<SampleFailure>,
    pub(super) overflows: Vec<ExpansionOverflow>,
}

impl PendingMetadata {
    pub(super) fn merge(&mut self, delta: Self) {
        merge_diagnostics(&mut self.diagnostics, delta.diagnostics);
        merge_latest_failures(&mut self.failures, delta.failures);
        merge_latest_by_key(&mut self.overflows, delta.overflows, |overflow| {
            MetadataKey::new(overflow.counter_index, overflow.reason)
        });
    }

    pub(super) fn merge_older(&mut self, older: Self) {
        merge_diagnostics(&mut self.diagnostics, older.diagnostics);
        merge_older_failures(&mut self.failures, older.failures);
        merge_older_by_key(&mut self.overflows, older.overflows, |overflow| {
            MetadataKey::new(overflow.counter_index, overflow.reason)
        });
    }
}

fn merge_latest_failures(current: &mut Vec<SampleFailure>, latest: Vec<SampleFailure>) {
    if current.len().saturating_mul(latest.len()) <= METADATA_INDEX_WORK_THRESHOLD {
        for item in latest {
            if let Some(existing) = current.iter_mut().find(|existing| {
                existing.counter_index == item.counter_index
                    && existing.reason == item.reason
                    && existing.path == item.path
            }) {
                *existing = item;
            } else {
                current.push(item);
            }
        }
        return;
    }
    let mut indices = current
        .iter()
        .enumerate()
        .map(|(index, failure)| {
            (
                (
                    failure.counter_index,
                    failure.reason,
                    Arc::clone(&failure.path),
                ),
                index,
            )
        })
        .collect::<HashMap<_, _>>();
    for item in latest {
        let key = (item.counter_index, item.reason, Arc::clone(&item.path));
        if let Some(index) = indices.get(&key).copied() {
            current[index] = item;
        } else {
            let index = current.len();
            current.push(item);
            let _ = indices.insert(key, index);
        }
    }
}

fn merge_older_failures(current: &mut Vec<SampleFailure>, older: Vec<SampleFailure>) {
    if current.len().saturating_mul(older.len()) <= METADATA_INDEX_WORK_THRESHOLD {
        for item in older {
            if !current.iter().any(|existing| {
                existing.counter_index == item.counter_index
                    && existing.reason == item.reason
                    && existing.path == item.path
            }) {
                current.push(item);
            }
        }
        return;
    }
    let mut keys = current
        .iter()
        .map(|failure| {
            (
                failure.counter_index,
                failure.reason,
                Arc::clone(&failure.path),
            )
        })
        .collect::<std::collections::HashSet<_>>();
    for item in older {
        let key = (item.counter_index, item.reason, Arc::clone(&item.path));
        if keys.insert(key) {
            current.push(item);
        }
    }
}

fn merge_latest_by_key<T>(current: &mut Vec<T>, latest: Vec<T>, key: impl Fn(&T) -> MetadataKey) {
    if latest.is_empty() {
        return;
    }
    if current.len().saturating_mul(latest.len()) <= METADATA_INDEX_WORK_THRESHOLD {
        merge_latest_linearly(current, latest, key);
        return;
    }
    let Some(mut indices) = dense_metadata_indices(current, &latest, &key) else {
        merge_latest_linearly(current, latest, key);
        return;
    };
    for item in latest {
        let item_key = key(&item);
        if let Some(index) = indices.get(item_key) {
            current[index] = item;
        } else {
            let index = current.len();
            current.push(item);
            indices.insert(item_key, index);
        }
    }
}

fn merge_older_by_key<T>(current: &mut Vec<T>, older: Vec<T>, key: impl Fn(&T) -> MetadataKey) {
    if older.is_empty() {
        return;
    }
    if current.len().saturating_mul(older.len()) <= METADATA_INDEX_WORK_THRESHOLD {
        merge_older_linearly(current, older, key);
        return;
    }
    let Some(mut indices) = dense_metadata_indices(current, &older, &key) else {
        merge_older_linearly(current, older, key);
        return;
    };
    for item in older {
        let item_key = key(&item);
        if indices.get(item_key).is_none() {
            let index = current.len();
            current.push(item);
            indices.insert(item_key, index);
        }
    }
}

struct DenseMetadataIndices {
    reasons: Vec<&'static str>,
    indices: Vec<Option<usize>>,
}

impl DenseMetadataIndices {
    fn slot(&self, key: MetadataKey) -> usize {
        let reason_index = self
            .reasons
            .iter()
            .position(|reason| *reason == key.reason)
            .expect("metadata reason was indexed");
        key.counter_index * self.reasons.len() + reason_index
    }

    fn get(&self, key: MetadataKey) -> Option<usize> {
        self.indices[self.slot(key)]
    }

    fn insert(&mut self, key: MetadataKey, index: usize) {
        let slot = self.slot(key);
        self.indices[slot] = Some(index);
    }
}

fn dense_metadata_indices<T>(
    current: &[T],
    incoming: &[T],
    key: &impl Fn(&T) -> MetadataKey,
) -> Option<DenseMetadataIndices> {
    let entry_count = current.len().saturating_add(incoming.len());
    let mut reasons = Vec::new();
    let mut max_counter_index = None::<usize>;
    for item in current.iter().chain(incoming) {
        let item_key = key(item);
        max_counter_index = max_counter_index.max(Some(item_key.counter_index));
        if !reasons.contains(&item_key.reason) {
            reasons.push(item_key.reason);
        }
    }
    let counter_slots = max_counter_index?.checked_add(1)?;
    let slot_count = counter_slots.checked_mul(reasons.len())?;
    if slot_count > entry_count.saturating_mul(MAX_METADATA_INDEX_SPARSITY) {
        return None;
    }
    let mut dense = DenseMetadataIndices {
        reasons,
        indices: vec![None; slot_count],
    };
    for (index, item) in current.iter().enumerate() {
        let item_key = key(item);
        if dense.get(item_key).is_none() {
            dense.insert(item_key, index);
        }
    }
    Some(dense)
}

fn merge_latest_linearly<T>(current: &mut Vec<T>, latest: Vec<T>, key: impl Fn(&T) -> MetadataKey) {
    for item in latest {
        if let Some(existing) = current
            .iter_mut()
            .find(|existing| key(existing) == key(&item))
        {
            *existing = item;
        } else {
            current.push(item);
        }
    }
}

fn merge_older_linearly<T>(current: &mut Vec<T>, older: Vec<T>, key: impl Fn(&T) -> MetadataKey) {
    for item in older {
        if !current.iter().any(|existing| key(existing) == key(&item)) {
            current.push(item);
        }
    }
}

/// A successful point-in-time PDH reading, independent of Windows handles.
#[derive(Debug, Clone)]
pub(super) struct Sample {
    /// Collection time as nanoseconds since the Unix epoch.
    pub(super) timestamp_unix_nano: i64,
    /// Exact and expanded points keyed by configured counter and concrete path.
    pub(super) points: Vec<SamplePoint>,
    /// Diagnostics and bounded counter-local evidence for this collection.
    pub(super) metadata: PendingMetadata,
}

impl Sample {
    pub(super) fn empty(timestamp_unix_nano: i64) -> Self {
        Self {
            timestamp_unix_nano,
            points: Vec::new(),
            metadata: PendingMetadata::default(),
        }
    }
}

pub(super) fn merge_diagnostics(target: &mut SampleDiagnostics, delta: SampleDiagnostics) {
    target.discovery_refreshes += delta.discovery_refreshes;
    target.discovery_failures += delta.discovery_failures;
    target.instances_added += delta.instances_added;
    target.instances_removed += delta.instances_removed;
    target.instances_omitted_over_limit += delta.instances_omitted_over_limit;
    target.counter_add_failures += delta.counter_add_failures;
    target.counter_read_failures += delta.counter_read_failures;
    target.scrape_failures += delta.scrape_failures;
    target.retry_attempts += delta.retry_attempts;
    target.retry_recoveries += delta.retry_recoveries;
    target.query_rebuild_attempts += delta.query_rebuild_attempts;
    target.query_rebuild_recoveries += delta.query_rebuild_recoveries;
    target.query_recreation_attempts += delta.query_recreation_attempts;
    target.query_recreation_failures += delta.query_recreation_failures;
    target.warmup_omissions += delta.warmup_omissions;
}

/// Scales an integer by a power of ten.
/// Zero scale preserves the integer.
/// Nonzero scales return a double; magnitudes above 2^53 may round during conversion.
/// Powers outside the configuration's supported range return an error.
pub(super) fn scale_integer(value: i64, power10: i32) -> Result<Number, ScaleError> {
    if power10 == 0 {
        return Ok(Number::Integer(value));
    }
    scale_double(value as f64, power10)
}

/// Scales a calculated value by a power of ten without clamping.
/// Uses normal f64 rounding and accepts subnormals and signed zero.
/// Non-finite inputs or results and unsupported powers return an error.
pub(super) fn scale_double(value: f64, power10: i32) -> Result<Number, ScaleError> {
    if !value.is_finite() {
        return Err(ScaleError::NonFinite);
    }
    // The supported powers of ten are exact in f64, so this cast does not round.
    let factor = scale_factor(power10)? as f64;
    let scaled = if power10 < 0 {
        value / factor
    } else {
        value * factor
    };
    if !scaled.is_finite() {
        return Err(ScaleError::NonFinite);
    }
    Ok(Number::Double(scaled))
}

/// Returns 10 raised to the absolute value of a supported power.
fn scale_factor(power10: i32) -> Result<i64, ScaleError> {
    if !(MIN_SCALE_POWER10..=MAX_SCALE_POWER10).contains(&power10) {
        return Err(ScaleError::InvalidScale);
    }
    // Keep the overflow check in case the supported range is widened.
    10_i64
        .checked_pow(power10.unsigned_abs())
        .ok_or(ScaleError::InvalidScale)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure_with_reason(
        counter_index: usize,
        reason: &'static str,
        detail: &str,
    ) -> SampleFailure {
        SampleFailure {
            counter_index,
            path: Arc::from(format!("counter-{counter_index}")),
            reason,
            error: detail.to_owned(),
            diagnostic_only: false,
        }
    }

    fn failure(counter_index: usize, detail: &str) -> SampleFailure {
        failure_with_reason(counter_index, "n12", detail)
    }

    fn failure_with_path(counter_index: usize, path: &str, detail: &str) -> SampleFailure {
        SampleFailure {
            counter_index,
            path: Arc::from(path),
            reason: "counter CStatus",
            error: detail.to_owned(),
            diagnostic_only: false,
        }
    }

    fn metadata(size: usize, detail: &str) -> PendingMetadata {
        PendingMetadata {
            failures: (0..size).map(|index| failure(index, detail)).collect(),
            ..Default::default()
        }
    }

    /// Scenario: A high-cardinality merge updates every existing key, repeats one key, and appends one new key.
    /// Guarantees: Indexed merging preserves first-position ordering while the newest evidence wins, keeping diagnostic output stable and current.
    #[test]
    fn metadata_merge_preserves_order_and_latest_values() {
        let mut current = metadata(256, "current");
        let mut latest = metadata(256, "latest");
        latest.failures.push(failure(0, "final"));
        latest.failures.push(failure(256, "new"));

        current.merge(latest);

        assert_eq!(current.failures.len(), 257);
        assert_eq!(current.failures[0].counter_index, 0);
        assert_eq!(current.failures[0].error, "final");
        assert_eq!(current.failures[255].counter_index, 255);
        assert_eq!(current.failures[255].error, "latest");
        assert_eq!(current.failures[256].counter_index, 256);
        assert_eq!(current.failures[256].error, "new");
    }

    /// Scenario: Newer and older batches each repeat one key while the destination is empty.
    /// Guarantees: Initial merges retain the established last-newer and first-older rules so startup evidence stays deterministic.
    #[test]
    fn empty_metadata_merge_deduplicates_by_freshness() {
        let mut latest_target = PendingMetadata::default();
        latest_target.merge(PendingMetadata {
            failures: vec![failure(0, "first"), failure(0, "last")],
            ..Default::default()
        });
        assert_eq!(latest_target.failures.len(), 1);
        assert_eq!(latest_target.failures[0].error, "last");

        let mut older_target = PendingMetadata::default();
        older_target.merge_older(PendingMetadata {
            failures: vec![failure(0, "first"), failure(0, "last")],
            ..Default::default()
        });
        assert_eq!(older_target.failures.len(), 1);
        assert_eq!(older_target.failures[0].error, "first");
    }

    /// Scenario: Two concrete wildcard members report the same failure class under one mapping.
    /// Guarantees: Metadata merging retains both failed values independently of query merge order.
    #[test]
    fn metadata_merge_keys_failures_by_concrete_path() {
        let first = failure_with_path(0, r"\Process(first)\Value", "first");
        let second = failure_with_path(0, r"\Process(second)\Value", "second");
        for failures in [
            vec![first.clone(), second.clone()],
            vec![second.clone(), first.clone()],
        ] {
            let mut metadata = PendingMetadata::default();
            metadata.merge(PendingMetadata {
                failures,
                ..Default::default()
            });
            assert_eq!(metadata.failures.len(), 2);
            assert!(metadata.failures.contains(&first));
            assert!(metadata.failures.contains(&second));
        }
    }

    /// Scenario: High-cardinality pending metadata overlaps current evidence and repeats a previously unseen older key.
    /// Guarantees: Recovery cannot replace newer diagnostics, and the first retained older detail remains deterministic for operators.
    #[test]
    fn older_metadata_never_overwrites_current_values() {
        let mut current = metadata(256, "current");
        let mut older = metadata(256, "older");
        older.failures.push(failure(256, "first older"));
        older.failures.push(failure(256, "second older"));

        current.merge_older(older);

        assert_eq!(current.failures.len(), 257);
        assert!(
            current.failures[..256]
                .iter()
                .all(|failure| failure.error == "current")
        );
        assert_eq!(current.failures[256].counter_index, 256);
        assert_eq!(current.failures[256].error, "first older");
    }

    /// Scenario: Metadata contains a sparse, pathological counter index.
    /// Guarantees: Index storage remains proportional to evidence count rather than the largest configured-counter index.
    #[test]
    fn metadata_index_bounds_sparse_storage() {
        let current = vec![failure(usize::MAX, "current")];
        let incoming = vec![failure(0, "incoming")];

        assert!(
            dense_metadata_indices(&current, &incoming, &|failure| {
                MetadataKey::new(failure.counter_index, failure.reason)
            })
            .is_none()
        );
    }

    /// Scenario: Every configured index has two failure reasons while older evidence partially overlaps and partially extends current metadata.
    /// Guarantees: Indexed merge_older matches the linear freshness and ordering semantics for the full (counter_index, reason) key.
    #[test]
    fn metadata_index_matches_linear_merge_for_two_reasons_per_index() {
        let reasons = ["prime", "read"];
        let mut indexed = (0..256)
            .flat_map(|counter_index| {
                reasons.map(|reason| failure_with_reason(counter_index, reason, "current"))
            })
            .collect::<Vec<_>>();
        let older = (128..384)
            .flat_map(|counter_index| {
                reasons.map(|reason| failure_with_reason(counter_index, reason, "older"))
            })
            .collect::<Vec<_>>();
        let mut linear = indexed.clone();

        merge_older_by_key(&mut indexed, older.clone(), |failure| {
            MetadataKey::new(failure.counter_index, failure.reason)
        });
        merge_older_linearly(&mut linear, older, |failure| {
            MetadataKey::new(failure.counter_index, failure.reason)
        });

        assert_eq!(indexed, linear);
        assert_eq!(indexed.len(), 768);
    }

    /// Scenario: Integer values use zero, positive, negative and boundary decimal scales.
    /// Guarantees: Zero scale preserves signed integers; every nonzero scale returns a double.
    #[test]
    fn scales_direct_values_by_decimal_power() {
        assert_eq!(
            scale_integer(i64::MAX, 0).unwrap(),
            Number::Integer(i64::MAX)
        );
        assert_eq!(
            scale_integer(i64::MIN, 0).unwrap(),
            Number::Integer(i64::MIN)
        );
        assert_eq!(scale_integer(42, 3).unwrap(), Number::Double(42_000.0));
        assert_eq!(scale_integer(-42, 3).unwrap(), Number::Double(-42_000.0));
        assert_eq!(scale_integer(9, 18).unwrap(), Number::Double(9e18));
        assert_eq!(scale_integer(-9, 18).unwrap(), Number::Double(-9e18));
        assert_eq!(scale_integer(0, 18).unwrap(), Number::Double(0.0));
        assert_eq!(scale_integer(42, -1).unwrap(), Number::Double(4.2));
        assert_eq!(scale_integer(-42, -1).unwrap(), Number::Double(-4.2));
        assert_eq!(scale_integer(3, -1).unwrap(), Number::Double(0.3));
        assert_eq!(scale_integer(7, -1).unwrap(), Number::Double(0.7));
        assert_eq!(
            scale_integer(86_580_518_913, -6).unwrap(),
            Number::Double(86_580.518_913)
        );
        assert_eq!(
            scale_integer(1_000_000_000_000_000_000, -18).unwrap(),
            Number::Double(1.0)
        );
        assert_eq!(scale_integer(0, -18).unwrap(), Number::Double(0.0));
    }

    /// Scenario: Integer extrema are scaled at both supported exponent limits.
    /// Guarantees: Large scaled values remain finite doubles rather than overflowing or being omitted.
    #[test]
    fn scales_integer_extremes_without_overflow() {
        assert_eq!(scale_integer(10, 18).unwrap(), Number::Double(1e19));
        assert_eq!(
            scale_integer(i64::MAX, 18).unwrap(),
            Number::Double(9.223_372_036_854_776e36)
        );
        assert_eq!(
            scale_integer(i64::MIN, 18).unwrap(),
            Number::Double(-9.223_372_036_854_776e36)
        );
        assert_eq!(
            scale_integer(i64::MAX, -18).unwrap(),
            Number::Double(9.223_372_036_854_776)
        );
        assert_eq!(
            scale_integer(i64::MIN, -18).unwrap(),
            Number::Double(-9.223_372_036_854_776)
        );
    }

    /// Scenario: Calculated values include percentages above 100, signed fractions and small magnitudes.
    /// Guarantees: Scaling preserves these values without clamping, integer coercion or discarding subnormals.
    #[test]
    fn scales_calculated_values_without_clamping() {
        assert_eq!(scale_double(250.0, 0).unwrap(), Number::Double(250.0));
        assert_eq!(scale_double(250.0, -2).unwrap(), Number::Double(2.5));
        assert_eq!(scale_double(-12.5, -1).unwrap(), Number::Double(-1.25));
        assert_eq!(scale_double(1.25, 1).unwrap(), Number::Double(12.5));
        assert_eq!(scale_double(1.0, 18).unwrap(), Number::Double(1e18));
        assert_eq!(scale_double(1.0, -18).unwrap(), Number::Double(1e-18));
        assert_eq!(scale_double(0.0, -18).unwrap(), Number::Double(0.0));
        assert_eq!(scale_double(1e-300, -18).unwrap(), Number::Double(1e-318));
        assert_eq!(scale_double(-1e-300, -18).unwrap(), Number::Double(-1e-318));
    }

    /// Scenario: Small calculated values round to zero, or a signed zero is scaled in either direction.
    /// Guarantees: Zero results retain the input's sign.
    #[test]
    fn preserves_signed_zero_when_scaling_underflows() {
        for value in [f64::MIN_POSITIVE, -f64::MIN_POSITIVE] {
            let Number::Double(scaled) = scale_double(value, -18).unwrap() else {
                panic!("calculated values must remain doubles");
            };
            assert_eq!(scaled, 0.0);
            assert_eq!(scaled.is_sign_negative(), value.is_sign_negative());
        }
        for value in [0.0_f64, -0.0_f64] {
            for power10 in [-18, 0, 18] {
                let Number::Double(scaled) = scale_double(value, power10).unwrap() else {
                    panic!("calculated values must remain doubles");
                };
                assert_eq!(scaled, 0.0);
                assert_eq!(scaled.is_sign_negative(), value.is_sign_negative());
            }
        }
    }

    /// Scenario: A calculated value is non-finite or scaling overflows the finite f64 range.
    /// Guarantees: Invalid observations return a bounded error rather than clamping or emitting infinities.
    #[test]
    fn rejects_non_finite_double_scaling() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            for power10 in [-18, 0, 18] {
                assert_eq!(scale_double(value, power10), Err(ScaleError::NonFinite));
            }
        }
        assert_eq!(scale_double(f64::MAX, 18), Err(ScaleError::NonFinite));
        assert_eq!(scale_double(-f64::MAX, 18), Err(ScaleError::NonFinite));
    }

    /// Scenario: A scaling helper receives an exponent outside the configuration's supported range.
    /// Guarantees: Invalid powers return explicit errors without overflow, panic or non-finite output.
    #[test]
    fn rejects_unsupported_scale_powers() {
        for power10 in [i32::MIN, -19, 19, i32::MAX] {
            assert_eq!(
                scale_integer(i64::MAX, power10),
                Err(ScaleError::InvalidScale)
            );
            assert_eq!(scale_double(1.0, power10), Err(ScaleError::InvalidScale));
        }
    }
}
