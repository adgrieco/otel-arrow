// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Persistent synchronous PDH worker. Windows handles never leave its thread.
#![allow(unsafe_code)]

use super::config::CounterConfig;
use super::model::{
    ExpansionOverflow, InstanceIdentity, Sample, SampleDiagnostics, SampleFailure, SamplePoint,
    SampleValue, scale_double, scale_integer,
};
use super::native_type::{CounterKind, UnsupportedNativeType, classify_native_type};
use std::collections::{BTreeMap, BTreeSet};
use std::mem::{size_of, size_of_val};
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use windows_sys::Win32::System::Performance::{
    PDH_COUNTER_INFO_W, PDH_COUNTER_PATH_ELEMENTS_W, PDH_CSTATUS_NEW_DATA, PDH_CSTATUS_VALID_DATA,
    PDH_FMT_COUNTERVALUE, PDH_FMT_DOUBLE, PDH_FMT_LARGE, PDH_HCOUNTER, PDH_HQUERY,
    PDH_INVALID_ARGUMENT, PDH_MAX_COUNTER_PATH, PDH_MORE_DATA, PDH_RAW_COUNTER,
    PDH_REFRESHCOUNTERS, PdhAddCounterW, PdhAddEnglishCounterW, PdhCloseQuery, PdhCollectQueryData,
    PdhExpandWildCardPathW, PdhGetCounterInfoW, PdhGetFormattedCounterValue, PdhGetRawCounterValue,
    PdhOpenQueryW, PdhParseCounterPathW, PdhRemoveCounter,
};

const INIT_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_REBUILD_AFTER_FAILURES: u32 = 3;
// These format flags are not exposed by windows-sys 0.61.2.
const PDH_FMT_NOSCALE: u32 = 0x0000_1000;
const PDH_FMT_NOCAP100: u32 = 0x0000_8000;

#[cfg(test)]
static QUERY_OPENS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
static QUERY_CLOSES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("{operation} for {path} failed with PDH status 0x{status:08X}")]
    Pdh {
        operation: &'static str,
        path: String,
        status: u32,
    },
    #[error(transparent)]
    UnsupportedType(#[from] UnsupportedNativeType),
    #[error("invalid performance-counter sample: {0}")]
    InvalidSample(&'static str),
    #[error("counter calculation for {path} failed: {message}")]
    Calculation { path: String, message: String },
    #[error("failed to start PDH worker thread: {0}")]
    WorkerStart(String),
    #[error("PDH worker initialization timed out after 30 seconds")]
    WorkerInitTimeout,
    #[error("PDH worker stopped before initialization completed")]
    WorkerInitStopped,
    #[error("a performance-counter collection request is already pending")]
    WorkerBusy,
    #[error("PDH worker stopped unexpectedly")]
    WorkerStopped,
    #[error("PDH worker thread panicked")]
    WorkerPanicked,
    #[error("PDH query recovery is deferred for another {retry_after_ms} ms")]
    QueryRecoveryPending { retry_after_ms: u128 },
}

impl Error {
    pub(super) fn is_collection_failure(&self) -> bool {
        matches!(
            self,
            Self::Pdh { .. }
                | Self::InvalidSample(_)
                | Self::Calculation { .. }
                | Self::WorkerBusy
                | Self::QueryRecoveryPending { .. }
        )
    }

    pub(super) fn is_overrun(&self) -> bool {
        matches!(self, Self::WorkerBusy)
    }

    fn is_query_collection_failure(&self) -> bool {
        matches!(
            self,
            Self::Pdh {
                operation: "PdhCollectQueryData" | "PdhCollectQueryData(prime)",
                ..
            }
        )
    }

    fn requires_immediate_query_rebuild(&self) -> bool {
        matches!(
            self,
            Self::Pdh { operation, .. }
                if matches!(*operation, "PdhRemoveCounter" | "PdhRemoveCounter(retry)")
        )
    }

    fn reason(&self) -> &'static str {
        match self {
            Self::Pdh { operation, .. } => operation,
            Self::UnsupportedType(_) => "unsupported_type",
            Self::InvalidSample(_) => "invalid_sample",
            Self::Calculation { .. } => "calculation",
            Self::WorkerStart(_) => "worker_start",
            Self::WorkerInitTimeout => "worker_init_timeout",
            Self::WorkerInitStopped => "worker_init_stopped",
            Self::WorkerBusy => "worker_busy",
            Self::WorkerStopped => "worker_stopped",
            Self::WorkerPanicked => "worker_panicked",
            Self::QueryRecoveryPending { .. } => "query_recovery_pending",
        }
    }

    fn bounded_detail(&self) -> String {
        match self {
            Self::Pdh { status, .. } => format!("PDH status 0x{status:08X}"),
            Self::UnsupportedType(error) => {
                format!("unsupported native type 0x{:08X}", error.native_type)
            }
            Self::InvalidSample(message) => (*message).to_owned(),
            Self::Calculation { message, .. } => message.clone(),
            other => other.to_string(),
        }
    }
}

fn check(operation: &'static str, path: &str, status: u32) -> Result<(), Error> {
    if status == 0 {
        Ok(())
    } else {
        Err(Error::Pdh {
            operation,
            path: path.to_owned(),
            status,
        })
    }
}

fn required_base(path: &str, base: Option<i64>) -> Result<i64, Error> {
    base.ok_or_else(|| Error::Calculation {
        path: path.to_owned(),
        message: "PDH did not return the required base value".to_owned(),
    })
}

fn validate_positive_base(path: &str, base: i64) -> Result<(), Error> {
    if base > 0 {
        Ok(())
    } else {
        Err(Error::Calculation {
            path: path.to_owned(),
            message: format!("base denominator must be positive, got {base}"),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BaseDelta {
    Increased,
    NoObservation,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum CollectionDisposition {
    Format,
    Omit(SampleValue),
}

fn classify_base_delta(path: &str, previous: i64, current: i64) -> Result<BaseDelta, Error> {
    match current.cmp(&previous) {
        std::cmp::Ordering::Greater => Ok(BaseDelta::Increased),
        std::cmp::Ordering::Equal => Ok(BaseDelta::NoObservation),
        std::cmp::Ordering::Less => Err(Error::Calculation {
            path: path.to_owned(),
            message: format!(
                "base denominator decreased between samples, got {previous} then {current}"
            ),
        }),
    }
}

fn classify_base_disposition(
    path: &str,
    previous_base: &mut Option<i64>,
    current_base: i64,
) -> Result<CollectionDisposition, Error> {
    let previous_base = previous_base
        .replace(current_base)
        .ok_or_else(|| Error::Calculation {
            path: path.to_owned(),
            message: "a previous base sample is required".to_owned(),
        })?;
    match classify_base_delta(path, previous_base, current_base)? {
        BaseDelta::Increased => Ok(CollectionDisposition::Format),
        BaseDelta::NoObservation => Ok(CollectionDisposition::Omit(SampleValue::NoObservation)),
    }
}

fn inspect_counter(path: &str, counter: PDH_HCOUNTER) -> Result<CounterKind, Error> {
    let buffer = counter_info(path, counter)?;
    let info = buffer.as_ptr().cast::<PDH_COUNTER_INFO_W>();
    // SAFETY: The successful metadata call initialized the fixed fields used here.
    let (native_type, scale) = unsafe { ((*info).dwType, (*info).lDefaultScale) };
    // The default scale is a display hint. The receiver always requests
    // PDH_FMT_NOSCALE and applies only the configured scale.
    let _ = scale;
    classify_native_type(path, native_type).map_err(Error::from)
}

fn counter_info(path: &str, counter: PDH_HCOUNTER) -> Result<Vec<usize>, Error> {
    let mut size = 0;
    // SAFETY: The counter is live. A null buffer with size zero requests the
    // required variable-buffer size.
    let status = unsafe { PdhGetCounterInfoW(counter, false, &mut size, null_mut()) };
    if status != PDH_MORE_DATA {
        return Err(Error::Pdh {
            operation: "PdhGetCounterInfoW(size)",
            path: path.to_owned(),
            status,
        });
    }
    if (size as usize) < size_of::<PDH_COUNTER_INFO_W>() {
        return Err(Error::InvalidSample(
            "PdhGetCounterInfoW returned an undersized buffer",
        ));
    }
    let word_count = (size as usize).div_ceil(size_of::<usize>());
    let mut buffer = vec![0usize; word_count];
    let info = buffer.as_mut_ptr().cast::<PDH_COUNTER_INFO_W>();
    // SAFETY: The usize buffer is suitably aligned and at least `size` bytes.
    check("PdhGetCounterInfoW", path, unsafe {
        PdhGetCounterInfoW(counter, false, &mut size, info)
    })?;
    Ok(buffer)
}

fn wide_string(pointer: *const u16, buffer: &[usize]) -> Result<Option<String>, Error> {
    if pointer.is_null() {
        return Ok(None);
    }
    let buffer_start = buffer.as_ptr() as usize;
    let buffer_end = buffer_start + size_of_val(buffer);
    let pointer_address = pointer as usize;
    if pointer_address < buffer_start || pointer_address >= buffer_end {
        return Err(Error::InvalidSample(
            "PDH returned a string pointer outside its result buffer",
        ));
    }
    let remaining = (buffer_end - pointer_address) / size_of::<u16>();
    // SAFETY: Bounds above constrain the slice to the caller-owned PDH buffer.
    let values = unsafe { std::slice::from_raw_parts(pointer, remaining) };
    let length = values
        .iter()
        .position(|value| *value == 0)
        .ok_or(Error::InvalidSample("PDH returned an unterminated string"))?;
    Ok(Some(String::from_utf16_lossy(&values[..length])))
}

fn localized_wildcard_path(query: PDH_HQUERY, path: &str) -> Result<String, Error> {
    let wide_path: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
    let mut counter = null_mut();
    // SAFETY: The query is live, path is NUL-terminated, and counter is writable.
    check("PdhAddEnglishCounterW(wildcard)", path, unsafe {
        PdhAddEnglishCounterW(query, wide_path.as_ptr(), 0, &mut counter)
    })?;
    let result = (|| {
        let buffer = counter_info(path, counter)?;
        let info = buffer.as_ptr().cast::<PDH_COUNTER_INFO_W>();
        // SAFETY: szFullPath points into the live metadata buffer.
        // SAFETY: The successful call initialized szFullPath.
        wide_string(unsafe { (*info).szFullPath }, &buffer)?.ok_or(Error::InvalidSample(
            "PdhGetCounterInfoW returned a null full path",
        ))
    })();
    // SAFETY: The temporary translation counter belongs to this query and is
    // not used after this call.
    let remove_status = unsafe { PdhRemoveCounter(counter) };
    if remove_status != 0 && result.is_ok() {
        return Err(Error::Pdh {
            operation: "PdhRemoveCounter(wildcard)",
            path: path.to_owned(),
            status: remove_status,
        });
    }
    result
}

fn parse_multi_sz(buffer: &[u16]) -> Result<Vec<String>, Error> {
    let mut paths = Vec::new();
    let mut start = 0;
    while start < buffer.len() {
        let Some(relative_end) = buffer[start..].iter().position(|value| *value == 0) else {
            return Err(Error::InvalidSample(
                "PdhExpandWildCardPathW returned an unterminated path list",
            ));
        };
        if relative_end == 0 {
            return Ok(paths);
        }
        let end = start + relative_end;
        paths.push(String::from_utf16_lossy(&buffer[start..end]));
        start = end + 1;
    }
    Err(Error::InvalidSample(
        "PdhExpandWildCardPathW returned a path list without a final terminator",
    ))
}

fn expand_wildcard(query: PDH_HQUERY, path: &str) -> Result<Vec<String>, Error> {
    let localized = localized_wildcard_path(query, path)?;
    let wide_path: Vec<u16> = localized.encode_utf16().chain(Some(0)).collect();
    let mut last_status = PDH_MORE_DATA;
    for _ in 0..3 {
        let mut size = 0;
        // SAFETY: Null output with zero size requests the required TCHAR count.
        let size_status = unsafe {
            PdhExpandWildCardPathW(
                null(),
                wide_path.as_ptr(),
                null_mut(),
                &mut size,
                PDH_REFRESHCOUNTERS,
            )
        };
        if size_status == 0 && size == 0 {
            return Ok(Vec::new());
        }
        if size_status != PDH_MORE_DATA {
            return Err(Error::Pdh {
                operation: "PdhExpandWildCardPathW(size)",
                path: path.to_owned(),
                status: size_status,
            });
        }
        let mut buffer = vec![0u16; size as usize];
        // SAFETY: The buffer contains `size` writable UTF-16 code units.
        let status = unsafe {
            PdhExpandWildCardPathW(
                null(),
                wide_path.as_ptr(),
                buffer.as_mut_ptr(),
                &mut size,
                PDH_REFRESHCOUNTERS,
            )
        };
        if status == 0 {
            let used = usize::try_from(size)
                .map_err(|_| Error::InvalidSample("wildcard path list size exceeds usize"))?;
            if used > buffer.len() {
                return Err(Error::InvalidSample(
                    "PdhExpandWildCardPathW reported an oversized path list",
                ));
            }
            return parse_multi_sz(&buffer[..used]);
        }
        last_status = status;
        if !matches!(status, PDH_MORE_DATA | PDH_INVALID_ARGUMENT) {
            break;
        }
    }
    Err(Error::Pdh {
        operation: "PdhExpandWildCardPathW",
        path: path.to_owned(),
        status: last_status,
    })
}

fn parse_instance(path: &str) -> Result<InstanceIdentity, Error> {
    let wide_path: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
    let mut size = 0;
    // SAFETY: Null output with zero size requests the required byte count.
    let status = unsafe { PdhParseCounterPathW(wide_path.as_ptr(), null_mut(), &mut size, 0) };
    if status != PDH_MORE_DATA {
        return Err(Error::Pdh {
            operation: "PdhParseCounterPathW(size)",
            path: path.to_owned(),
            status,
        });
    }
    let word_count = (size as usize).div_ceil(size_of::<usize>());
    if (size as usize) < size_of::<PDH_COUNTER_PATH_ELEMENTS_W>() {
        return Err(Error::InvalidSample(
            "PdhParseCounterPathW returned an undersized buffer",
        ));
    }
    let mut buffer = vec![0usize; word_count];
    let elements = buffer.as_mut_ptr().cast::<PDH_COUNTER_PATH_ELEMENTS_W>();
    // SAFETY: The aligned buffer is at least `size` bytes and path is NUL-terminated.
    check("PdhParseCounterPathW", path, unsafe {
        PdhParseCounterPathW(wide_path.as_ptr(), elements, &mut size, 0)
    })?;
    // SAFETY: The successful call initialized pointers into the live buffer.
    // SAFETY: The successful call initialized the instance pointer.
    let name = wide_string(unsafe { (*elements).szInstanceName }, &buffer)?.ok_or(
        Error::InvalidSample("expanded wildcard path has no instance name"),
    )?;
    // SAFETY: The successful call initialized the optional parent pointer.
    let parent = wide_string(unsafe { (*elements).szParentInstance }, &buffer)?
        .filter(|value| !value.is_empty());
    // SAFETY: The successful call initialized the fixed instance index.
    let index = unsafe { (*elements).dwInstanceIndex };
    Ok(InstanceIdentity {
        name,
        parent,
        index,
    })
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CounterKey {
    config_index: usize,
    normalized_path: String,
}

impl CounterKey {
    fn new(config_index: usize, path: &str) -> Self {
        Self {
            config_index,
            normalized_path: path.to_lowercase(),
        }
    }
}

#[derive(Clone)]
struct CounterTarget {
    config_index: usize,
    path: String,
    localized: bool,
    instance: Option<InstanceIdentity>,
}

#[derive(Clone)]
struct RetryState {
    target: CounterTarget,
    attempts: u32,
    next_attempt: Instant,
}

fn retry_delay(attempts: u32, cap: Duration) -> Duration {
    let exponent = attempts.saturating_sub(1).min(20);
    Duration::from_secs(1_u64 << exponent).min(cap)
}

fn should_rebuild_query(consecutive_failures: u32) -> bool {
    consecutive_failures >= QUERY_REBUILD_AFTER_FAILURES
}

fn schedule_retry_state(
    states: &mut BTreeMap<CounterKey, RetryState>,
    key: CounterKey,
    target: CounterTarget,
    now: Instant,
    cap: Duration,
) {
    let attempts = states
        .get(&key)
        .map_or(1, |state| state.attempts.saturating_add(1));
    let _ = states.insert(
        key,
        RetryState {
            target,
            attempts,
            next_attempt: now + retry_delay(attempts, cap),
        },
    );
}

fn limit_expanded_paths(
    paths: &mut Vec<String>,
    per_wildcard_limit: usize,
    total_remaining: usize,
) -> (usize, usize) {
    let (retained, per_wildcard_omitted, total_omitted) =
        expansion_allocation(paths.len(), per_wildcard_limit, total_remaining);
    paths.truncate(retained);
    (per_wildcard_omitted, total_omitted)
}

fn expansion_allocation(
    discovered: usize,
    per_wildcard_limit: usize,
    total_remaining: usize,
) -> (usize, usize, usize) {
    let per_wildcard_retained = discovered.min(per_wildcard_limit);
    let per_wildcard_omitted = discovered - per_wildcard_retained;
    let retained = per_wildcard_retained.min(total_remaining);
    let total_omitted = per_wildcard_retained - retained;
    (retained, per_wildcard_omitted, total_omitted)
}

fn sample_failure(counter_index: usize, error: &Error) -> SampleFailure {
    SampleFailure {
        counter_index,
        reason: error.reason(),
        error: error.bounded_detail(),
    }
}

fn unix_timestamp_nanos() -> Result<i64, Error> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::InvalidSample("system clock precedes Unix epoch"))?
        .as_nanos();
    i64::try_from(timestamp).map_err(|_| Error::InvalidSample("timestamp exceeds i64 nanoseconds"))
}

fn cumulative_start_time(
    start_time_unix_nano: &mut i64,
    previous_timestamp_unix_nano: &mut i64,
    timestamp_unix_nano: i64,
) -> i64 {
    if timestamp_unix_nano < *previous_timestamp_unix_nano {
        *start_time_unix_nano = timestamp_unix_nano;
    }
    *previous_timestamp_unix_nano = timestamp_unix_nano;
    *start_time_unix_nano
}

fn is_aggregation_instance(path: &str, aggregation_name: &str) -> bool {
    parse_instance(path).is_ok_and(|instance| instance.name.eq_ignore_ascii_case(aggregation_name))
}

fn remove_aggregation_instances(paths: &mut Vec<String>, aggregation_name: Option<&str>) {
    if let Some(aggregation_name) = aggregation_name {
        paths.retain(|path| !is_aggregation_instance(path, aggregation_name));
    }
}

fn merge_diagnostics(target: &mut SampleDiagnostics, delta: SampleDiagnostics) {
    target.discovery_refreshes += delta.discovery_refreshes;
    target.discovery_failures += delta.discovery_failures;
    target.instances_added += delta.instances_added;
    target.instances_removed += delta.instances_removed;
    target.instances_omitted_over_limit += delta.instances_omitted_over_limit;
    target.counter_add_failures += delta.counter_add_failures;
    target.counter_read_failures += delta.counter_read_failures;
    target.retry_attempts += delta.retry_attempts;
    target.retry_recoveries += delta.retry_recoveries;
    target.query_rebuild_attempts += delta.query_rebuild_attempts;
    target.query_rebuild_recoveries += delta.query_rebuild_recoveries;
    target.warmup_omissions += delta.warmup_omissions;
}

fn merge_sample_metadata(
    target_diagnostics: &mut SampleDiagnostics,
    target_failures: &mut Vec<SampleFailure>,
    target_overflows: &mut Vec<ExpansionOverflow>,
    diagnostics: SampleDiagnostics,
    failures: Vec<SampleFailure>,
    overflows: Vec<ExpansionOverflow>,
) {
    merge_diagnostics(target_diagnostics, diagnostics);
    for failure in failures {
        if let Some(existing) = target_failures.iter_mut().find(|existing| {
            existing.counter_index == failure.counter_index && existing.reason == failure.reason
        }) {
            *existing = failure;
        } else {
            target_failures.push(failure);
        }
    }
    for overflow in overflows {
        if let Some(existing) = target_overflows.iter_mut().find(|existing| {
            existing.counter_index == overflow.counter_index && existing.reason == overflow.reason
        }) {
            *existing = overflow;
        } else {
            target_overflows.push(overflow);
        }
    }
}

#[derive(Default)]
struct PendingMetadata {
    diagnostics: SampleDiagnostics,
    failures: Vec<SampleFailure>,
    overflows: Vec<ExpansionOverflow>,
}

impl PendingMetadata {
    fn merge(
        &mut self,
        diagnostics: SampleDiagnostics,
        failures: Vec<SampleFailure>,
        overflows: Vec<ExpansionOverflow>,
    ) {
        merge_sample_metadata(
            &mut self.diagnostics,
            &mut self.failures,
            &mut self.overflows,
            diagnostics,
            failures,
            overflows,
        );
    }

    fn merge_into_sample(&mut self, sample: &mut Sample) {
        let mut pending = std::mem::take(self);
        merge_diagnostics(&mut sample.diagnostics, pending.diagnostics);
        pending.failures.append(&mut sample.failures);
        sample.failures = pending.failures;
        pending.overflows.append(&mut sample.overflows);
        sample.overflows = pending.overflows;
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InstanceGroupKey {
    config_index: usize,
    normalized_name: String,
    normalized_parent: Option<String>,
}

impl InstanceGroupKey {
    fn new(config_index: usize, instance: &InstanceIdentity) -> Self {
        Self {
            config_index,
            normalized_name: instance.name.to_lowercase(),
            normalized_parent: instance.parent.as_ref().map(|value| value.to_lowercase()),
        }
    }
}

fn instance_groups<'a>(
    counters: impl Iterator<Item = (&'a CounterKey, &'a InstanceIdentity)>,
) -> BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>> {
    let mut groups = BTreeMap::new();
    for (key, instance) in counters {
        let _ = groups
            .entry(InstanceGroupKey::new(key.config_index, instance))
            .or_insert_with(BTreeSet::new)
            .insert(key.clone());
    }
    groups
}

fn changed_instance_groups(
    existing: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
    wanted: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
) -> BTreeSet<InstanceGroupKey> {
    existing
        .keys()
        .chain(wanted.keys())
        .filter(|group| existing.get(*group) != wanted.get(*group))
        .cloned()
        .collect()
}

fn refresh_changes_with_group_resets(
    live: &BTreeSet<CounterKey>,
    retrying: &BTreeSet<CounterKey>,
    wanted: &BTreeSet<CounterKey>,
    known_groups: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
    wanted_groups: &BTreeMap<InstanceGroupKey, BTreeSet<CounterKey>>,
) -> (Vec<CounterKey>, Vec<CounterKey>) {
    let changed_groups = changed_instance_groups(known_groups, wanted_groups);
    let reset_live = changed_groups
        .iter()
        .filter_map(|group| known_groups.get(group))
        .flatten()
        .filter(|key| live.contains(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    let reset_wanted = changed_groups
        .iter()
        .filter_map(|group| wanted_groups.get(group))
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut removed = live.difference(wanted).cloned().collect::<Vec<_>>();
    let mut added = wanted
        .difference(live)
        .filter(|key| !retrying.contains(*key))
        .cloned()
        .collect::<Vec<_>>();
    removed.extend(reset_live);
    removed.sort();
    removed.dedup();
    added.extend(reset_wanted);
    added.sort();
    added.dedup();
    (removed, added)
}

struct CounterHandle {
    config_index: usize,
    path: String,
    handle: PDH_HCOUNTER,
    kind: CounterKind,
    scale_power10: i32,
    previous_base: Option<i64>,
    warming: bool,
    wildcard: bool,
    instance: Option<InstanceIdentity>,
}

/// The query owns every added counter; closing it releases all handles.
struct Query {
    handle: PDH_HQUERY,
    node: String,
    start_time_unix_nano: i64,
    previous_timestamp_unix_nano: i64,
    configs: Vec<CounterConfig>,
    counters: BTreeMap<CounterKey, CounterHandle>,
    retry_states: BTreeMap<CounterKey, RetryState>,
    pending: PendingMetadata,
    pending_prime_failure: Option<Error>,
    wildcard_refresh_interval: Duration,
    max_instances_per_wildcard: usize,
    max_expanded_counters: usize,
    last_wildcard_refresh: Instant,
}

impl Query {
    fn open(
        configs: Vec<CounterConfig>,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_expanded_counters: usize,
        node: String,
    ) -> Result<Self, Error> {
        let start_time_unix_nano = unix_timestamp_nanos()?;
        let mut handle = null_mut();
        // SAFETY: Null selects the live local data source; handle is writable.
        check("PdhOpenQueryW", "<query>", unsafe {
            PdhOpenQueryW(null(), 0, &mut handle)
        })?;
        let mut query = Self {
            handle,
            node,
            start_time_unix_nano,
            previous_timestamp_unix_nano: start_time_unix_nano,
            configs,
            counters: BTreeMap::new(),
            retry_states: BTreeMap::new(),
            pending: PendingMetadata::default(),
            pending_prime_failure: None,
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_expanded_counters,
            last_wildcard_refresh: Instant::now(),
        };
        #[cfg(test)]
        let _ = QUERY_OPENS.fetch_add(1, Ordering::Relaxed);

        for config_index in 0..query.configs.len() {
            if !query.configs[config_index].path.contains('*') {
                let path = query.configs[config_index].path.clone();
                let key = CounterKey::new(config_index, &path);
                let target = CounterTarget {
                    config_index,
                    path,
                    localized: false,
                    instance: None,
                };
                match query.add_counter(
                    target.config_index,
                    target.path.clone(),
                    target.localized,
                    None,
                ) {
                    Ok(()) => {}
                    Err(error @ Error::UnsupportedType(_)) => return Err(error),
                    Err(error) => {
                        query.pending.diagnostics.counter_add_failures += 1;
                        query
                            .pending
                            .failures
                            .push(sample_failure(config_index, &error));
                        query.schedule_retry(key, target, Instant::now());
                    }
                }
            }
        }
        let mut diagnostics = SampleDiagnostics::default();
        let mut failures = Vec::new();
        let mut overflows = Vec::new();
        query.refresh_wildcards(&mut diagnostics, &mut failures, &mut overflows)?;
        query.pending.merge(diagnostics, failures, overflows);
        if !query.counters.is_empty()
            && let Err(error) = query.prime()
        {
            query.pending_prime_failure = Some(error);
        }
        Ok(query)
    }

    fn add_counter(
        &mut self,
        config_index: usize,
        path: String,
        localized: bool,
        instance: Option<InstanceIdentity>,
    ) -> Result<(), Error> {
        let wide_path: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
        let mut counter = null_mut();
        // SAFETY: The query is live, path is NUL-terminated, and counter is writable.
        let status = unsafe {
            if localized {
                PdhAddCounterW(self.handle, wide_path.as_ptr(), 0, &mut counter)
            } else {
                PdhAddEnglishCounterW(self.handle, wide_path.as_ptr(), 0, &mut counter)
            }
        };
        check(
            if localized {
                "PdhAddCounterW"
            } else {
                "PdhAddEnglishCounterW"
            },
            &path,
            status,
        )?;
        let kind = match inspect_counter(&path, counter) {
            Ok(kind) => kind,
            Err(error) => {
                // SAFETY: Metadata inspection failed before the handle entered
                // receiver state, so remove it on the owner thread.
                let _ = unsafe { PdhRemoveCounter(counter) };
                return Err(error);
            }
        };
        let key = CounterKey::new(config_index, &path);
        let _ = self.counters.insert(
            key,
            CounterHandle {
                config_index,
                path,
                handle: counter,
                kind,
                scale_power10: self.configs[config_index].scale_power10,
                previous_base: None,
                warming: matches!(
                    kind,
                    CounterKind::CalculatedTwoSample | CounterKind::CalculatedTwoSampleWithBase
                ),
                wildcard: localized,
                instance,
            },
        );
        Ok(())
    }

    fn schedule_retry(&mut self, key: CounterKey, target: CounterTarget, now: Instant) {
        schedule_retry_state(
            &mut self.retry_states,
            key,
            target,
            now,
            self.wildcard_refresh_interval,
        );
    }

    fn try_add_target(
        &mut self,
        key: CounterKey,
        target: CounterTarget,
        now: Instant,
        diagnostics: &mut SampleDiagnostics,
        failures: &mut Vec<SampleFailure>,
    ) -> bool {
        match self.add_counter(
            target.config_index,
            target.path.clone(),
            target.localized,
            target.instance.clone(),
        ) {
            Ok(()) => {
                if self.retry_states.remove(&key).is_some() {
                    diagnostics.retry_recoveries += 1;
                }
                true
            }
            Err(error) => {
                diagnostics.counter_add_failures += 1;
                failures.push(sample_failure(target.config_index, &error));
                self.schedule_retry(key, target, now);
                false
            }
        }
    }

    fn retry_counters(
        &mut self,
        now: Instant,
        diagnostics: &mut SampleDiagnostics,
        failures: &mut Vec<SampleFailure>,
    ) {
        let due = self
            .retry_states
            .iter()
            .filter(|(_, state)| state.next_attempt <= now)
            .map(|(key, state)| (key.clone(), state.target.clone()))
            .collect::<Vec<_>>();
        for (key, target) in due {
            diagnostics.retry_attempts += 1;
            let _ = self.try_add_target(key, target, now, diagnostics, failures);
        }
    }

    fn refresh_wildcards(
        &mut self,
        diagnostics: &mut SampleDiagnostics,
        failures: &mut Vec<SampleFailure>,
        overflows: &mut Vec<ExpansionOverflow>,
    ) -> Result<(), Error> {
        diagnostics.discovery_refreshes += 1;
        let mut desired = BTreeMap::new();
        let mut expanded_total = 0;
        let discoveries = self
            .configs
            .iter()
            .enumerate()
            .filter(|(_, config)| config.path.contains('*'))
            .map(|(config_index, config)| {
                (config_index, expand_wildcard(self.handle, &config.path))
            })
            .collect::<Vec<_>>();

        // Retain failed templates before admitting successful discoveries so
        // their live handles and retry identities reserve global capacity
        // independently of configuration order.
        for (config_index, discovery) in &discoveries {
            let Err(error) = discovery else {
                continue;
            };
            diagnostics.discovery_failures += 1;
            failures.push(sample_failure(*config_index, error));
            let mut retained = self
                .counters
                .iter()
                .filter_map(|(key, counter)| {
                    (key.config_index == *config_index && counter.wildcard).then(|| {
                        counter
                            .instance
                            .clone()
                            .map(|instance| (key.clone(), (counter.path.clone(), instance)))
                    })?
                })
                .chain(self.retry_states.iter().filter_map(|(key, state)| {
                    (key.config_index == *config_index && state.target.localized).then(|| {
                        state
                            .target
                            .instance
                            .clone()
                            .map(|instance| (key.clone(), (state.target.path.clone(), instance)))
                    })?
                }))
                .collect::<BTreeMap<_, _>>();
            let discovered = retained.len();
            let (allowed, per_wildcard_omitted, total_omitted) = expansion_allocation(
                discovered,
                self.max_instances_per_wildcard,
                self.max_expanded_counters.saturating_sub(expanded_total),
            );
            if per_wildcard_omitted > 0 {
                overflows.push(ExpansionOverflow {
                    counter_index: *config_index,
                    reason: "per_wildcard",
                    discovered,
                    retained: discovered - per_wildcard_omitted,
                    omitted: per_wildcard_omitted,
                });
            }
            if total_omitted > 0 {
                overflows.push(ExpansionOverflow {
                    counter_index: *config_index,
                    reason: "receiver_total",
                    discovered: discovered - per_wildcard_omitted,
                    retained: allowed,
                    omitted: total_omitted,
                });
            }
            diagnostics.instances_omitted_over_limit +=
                u64::try_from(per_wildcard_omitted.saturating_add(total_omitted))
                    .unwrap_or(u64::MAX);
            while retained.len() > allowed {
                let Some(key) = retained.keys().next_back().cloned() else {
                    break;
                };
                let _ = retained.remove(&key);
            }
            expanded_total += retained.len();
            for (key, (path, instance)) in retained {
                let _ = desired.insert(key, (path, instance));
            }
        }

        for (config_index, discovery) in discoveries {
            let Ok(mut paths) = discovery else {
                continue;
            };
            remove_aggregation_instances(
                &mut paths,
                self.configs[config_index]
                    .excluded_aggregation_instance
                    .as_deref(),
            );
            let oversized = paths
                .iter()
                .filter(|path| path.encode_utf16().count() >= PDH_MAX_COUNTER_PATH as usize)
                .count();
            if oversized > 0 {
                diagnostics.counter_add_failures += u64::try_from(oversized).unwrap_or(u64::MAX);
                failures.push(SampleFailure {
                    counter_index: config_index,
                    reason: "expanded_path_too_long",
                    error: format!(
                        "{oversized} expanded paths reached the PDH {PDH_MAX_COUNTER_PATH}-unit limit"
                    ),
                });
                paths.retain(|path| path.encode_utf16().count() < PDH_MAX_COUNTER_PATH as usize);
            }
            paths.sort_by_key(|path| path.to_lowercase());
            paths.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
            let discovered = paths.len();
            let (per_wildcard_omitted, total_omitted) = limit_expanded_paths(
                &mut paths,
                self.max_instances_per_wildcard,
                self.max_expanded_counters.saturating_sub(expanded_total),
            );
            if per_wildcard_omitted > 0 {
                overflows.push(ExpansionOverflow {
                    counter_index: config_index,
                    reason: "per_wildcard",
                    discovered,
                    retained: discovered - per_wildcard_omitted,
                    omitted: per_wildcard_omitted,
                });
            }
            if total_omitted > 0 {
                overflows.push(ExpansionOverflow {
                    counter_index: config_index,
                    reason: "receiver_total",
                    discovered: discovered - per_wildcard_omitted,
                    retained: paths.len(),
                    omitted: total_omitted,
                });
            }
            diagnostics.instances_omitted_over_limit +=
                u64::try_from(per_wildcard_omitted.saturating_add(total_omitted))
                    .unwrap_or(u64::MAX);
            expanded_total += paths.len();
            for path in paths {
                let key = CounterKey::new(config_index, &path);
                match parse_instance(&path) {
                    Ok(instance) => {
                        let _ = desired.insert(key, (path, instance));
                    }
                    Err(error) => {
                        diagnostics.counter_add_failures += 1;
                        failures.push(sample_failure(config_index, &error));
                    }
                }
            }
        }

        let live = self
            .counters
            .iter()
            .filter_map(|(key, counter)| counter.wildcard.then_some(key.clone()))
            .collect::<BTreeSet<_>>();
        let retrying = self
            .retry_states
            .iter()
            .filter_map(|(key, state)| state.target.localized.then_some(key.clone()))
            .collect::<BTreeSet<_>>();
        let wanted = desired.keys().cloned().collect::<BTreeSet<_>>();
        let removed_retry_instances = self
            .retry_states
            .iter()
            .filter(|(key, state)| state.target.localized && !wanted.contains(*key))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        diagnostics.instances_removed +=
            u64::try_from(removed_retry_instances.len()).unwrap_or(u64::MAX);
        for key in removed_retry_instances {
            let _ = self.retry_states.remove(&key);
        }
        let known_groups = instance_groups(
            self.counters
                .iter()
                .filter_map(|(key, counter)| {
                    counter.instance.as_ref().map(|instance| (key, instance))
                })
                .chain(self.retry_states.iter().filter_map(|(key, state)| {
                    state
                        .target
                        .instance
                        .as_ref()
                        .map(|instance| (key, instance))
                })),
        );
        let wanted_groups =
            instance_groups(desired.iter().map(|(key, (_, instance))| (key, instance)));
        let (removed, added) = refresh_changes_with_group_resets(
            &live,
            &retrying,
            &wanted,
            &known_groups,
            &wanted_groups,
        );
        for key in removed {
            let _ = self.retry_states.remove(&key);
            let counter = self.counters.remove(&key).ok_or(Error::InvalidSample(
                "wildcard refresh lost an existing counter",
            ));
            let Ok(counter) = counter else {
                diagnostics.discovery_failures += 1;
                continue;
            };
            // SAFETY: The handle belongs to this query and is not used after removal.
            check("PdhRemoveCounter", &counter.path, unsafe {
                PdhRemoveCounter(counter.handle)
            })?;
            if !wanted.contains(&key) {
                diagnostics.instances_removed += 1;
            }
        }
        let now = Instant::now();
        for key in added {
            let Some((path, instance)) = desired.remove(&key) else {
                diagnostics.discovery_failures += 1;
                continue;
            };
            let target = CounterTarget {
                config_index: key.config_index,
                path,
                localized: true,
                instance: Some(instance),
            };
            let was_retry = self.retry_states.contains_key(&key);
            if self.try_add_target(key.clone(), target, now, diagnostics, failures)
                && !live.contains(&key)
                && !was_retry
            {
                diagnostics.instances_added += 1;
            }
        }
        self.last_wildcard_refresh = Instant::now();
        Ok(())
    }

    fn prime(&self) -> Result<(), Error> {
        // SAFETY: The query and counters remain on their owner thread.
        check("PdhCollectQueryData(prime)", "<query>", unsafe {
            PdhCollectQueryData(self.handle)
        })
    }

    fn restore_pending_metadata(
        &mut self,
        diagnostics: SampleDiagnostics,
        failures: Vec<SampleFailure>,
        overflows: Vec<ExpansionOverflow>,
    ) {
        self.pending.merge(diagnostics, failures, overflows);
    }

    fn move_pending_metadata_to(
        &mut self,
        diagnostics: &mut SampleDiagnostics,
        failures: &mut Vec<SampleFailure>,
        overflows: &mut Vec<ExpansionOverflow>,
    ) {
        let pending = std::mem::take(&mut self.pending);
        merge_sample_metadata(
            diagnostics,
            failures,
            overflows,
            pending.diagnostics,
            pending.failures,
            pending.overflows,
        );
    }

    fn collect(&mut self) -> Result<Sample, Error> {
        if let Some(error) = self.pending_prime_failure.take() {
            return Err(error);
        }
        let pending = std::mem::take(&mut self.pending);
        let mut diagnostics = pending.diagnostics;
        let mut failures = pending.failures;
        let mut overflows = pending.overflows;
        if self.last_wildcard_refresh.elapsed() >= self.wildcard_refresh_interval {
            if let Err(error) =
                self.refresh_wildcards(&mut diagnostics, &mut failures, &mut overflows)
            {
                self.restore_pending_metadata(diagnostics, failures, overflows);
                return Err(error);
            }
        }
        self.retry_counters(Instant::now(), &mut diagnostics, &mut failures);
        if self.counters.is_empty() {
            let timestamp_unix_nano = match unix_timestamp_nanos() {
                Ok(timestamp) => timestamp,
                Err(error) => {
                    self.restore_pending_metadata(diagnostics, failures, overflows);
                    return Err(error);
                }
            };
            let start_time_unix_nano = cumulative_start_time(
                &mut self.start_time_unix_nano,
                &mut self.previous_timestamp_unix_nano,
                timestamp_unix_nano,
            );
            return Ok(Sample {
                start_time_unix_nano,
                timestamp_unix_nano,
                points: Vec::new(),
                failures,
                overflows,
                diagnostics,
            });
        }
        // SAFETY: The query and counters remain on their owner thread.
        if let Err(error) = check("PdhCollectQueryData", "<query>", unsafe {
            PdhCollectQueryData(self.handle)
        }) {
            self.restore_pending_metadata(diagnostics, failures, overflows);
            return Err(error);
        }
        let timestamp_unix_nano = match unix_timestamp_nanos() {
            Ok(timestamp) => timestamp,
            Err(error) => {
                self.restore_pending_metadata(diagnostics, failures, overflows);
                return Err(error);
            }
        };
        let start_time_unix_nano = cumulative_start_time(
            &mut self.start_time_unix_nano,
            &mut self.previous_timestamp_unix_nano,
            timestamp_unix_nano,
        );

        // PDH advances the entire query at once. Refresh every valid receiver-side
        // base before formatting so one counter failure cannot leave later base
        // histories out of sync with PDH's previous sample.
        let mut dispositions = BTreeMap::new();
        let mut failed_keys = BTreeSet::new();
        for (key, counter) in &mut self.counters {
            let raw_base = match counter.kind {
                CounterKind::RawFraction | CounterKind::CalculatedTwoSampleWithBase => {
                    let mut raw = PDH_RAW_COUNTER::default();
                    // SAFETY: The counter is live and raw is writable.
                    let result = check("PdhGetRawCounterValue", &counter.path, unsafe {
                        PdhGetRawCounterValue(counter.handle, null_mut(), &mut raw)
                    });
                    if let Err(error) = result {
                        if counter.kind == CounterKind::CalculatedTwoSampleWithBase {
                            counter.previous_base = None;
                        }
                        failures.push(sample_failure(counter.config_index, &error));
                        diagnostics.counter_read_failures += 1;
                        let _ = failed_keys.insert(key.clone());
                        continue;
                    }
                    if !matches!(raw.CStatus, PDH_CSTATUS_VALID_DATA | PDH_CSTATUS_NEW_DATA) {
                        if counter.kind == CounterKind::CalculatedTwoSampleWithBase {
                            counter.previous_base = None;
                        }
                        let error = Error::Pdh {
                            operation: "raw counter CStatus",
                            path: counter.path.clone(),
                            status: raw.CStatus,
                        };
                        failures.push(sample_failure(counter.config_index, &error));
                        diagnostics.counter_read_failures += 1;
                        let _ = failed_keys.insert(key.clone());
                        continue;
                    }
                    Some(raw.SecondValue)
                }
                CounterKind::Direct | CounterKind::CalculatedTwoSample => None,
            };
            if std::mem::take(&mut counter.warming)
                && matches!(
                    counter.kind,
                    CounterKind::CalculatedTwoSample | CounterKind::CalculatedTwoSampleWithBase
                )
            {
                counter.previous_base = raw_base;
                let _ = dispositions.insert(
                    key.clone(),
                    CollectionDisposition::Omit(SampleValue::Warming),
                );
                diagnostics.warmup_omissions += 1;
                continue;
            }
            let disposition = match counter.kind {
                CounterKind::RawFraction => required_base(&counter.path, raw_base)
                    .and_then(|base| validate_positive_base(&counter.path, base))
                    .map(|()| CollectionDisposition::Format),
                CounterKind::CalculatedTwoSampleWithBase => required_base(&counter.path, raw_base)
                    .and_then(|current_base| {
                        classify_base_disposition(
                            &counter.path,
                            &mut counter.previous_base,
                            current_base,
                        )
                    }),
                CounterKind::Direct | CounterKind::CalculatedTwoSample => {
                    Ok(CollectionDisposition::Format)
                }
            };
            match disposition {
                Ok(disposition) => {
                    let _ = dispositions.insert(key.clone(), disposition);
                }
                Err(error) => {
                    failures.push(sample_failure(counter.config_index, &error));
                    diagnostics.counter_read_failures += 1;
                }
            }
        }

        let mut points = Vec::with_capacity(self.counters.len());
        for (key, counter) in &self.counters {
            let Some(disposition) = dispositions.remove(key) else {
                continue;
            };
            match disposition {
                CollectionDisposition::Format => {}
                CollectionDisposition::Omit(value) => {
                    points.push(SamplePoint {
                        counter_index: counter.config_index,
                        path: counter.path.clone(),
                        instance: counter.instance.clone(),
                        value,
                    });
                    continue;
                }
            }
            let mut value = PDH_FMT_COUNTERVALUE::default();
            let format = match counter.kind {
                CounterKind::Direct => PDH_FMT_LARGE | PDH_FMT_NOSCALE,
                CounterKind::RawFraction
                | CounterKind::CalculatedTwoSample
                | CounterKind::CalculatedTwoSampleWithBase => {
                    PDH_FMT_DOUBLE | PDH_FMT_NOSCALE | PDH_FMT_NOCAP100
                }
            };
            // SAFETY: The counter belongs to this live query and value is writable.
            let status = unsafe {
                PdhGetFormattedCounterValue(counter.handle, format, null_mut(), &mut value)
            };
            if let Err(error) = check("PdhGetFormattedCounterValue", &counter.path, status) {
                failures.push(sample_failure(counter.config_index, &error));
                diagnostics.counter_read_failures += 1;
                let _ = failed_keys.insert(key.clone());
                continue;
            }
            if !matches!(value.CStatus, PDH_CSTATUS_VALID_DATA | PDH_CSTATUS_NEW_DATA) {
                let error = Error::Pdh {
                    operation: "counter CStatus",
                    path: counter.path.clone(),
                    status: value.CStatus,
                };
                failures.push(sample_failure(counter.config_index, &error));
                diagnostics.counter_read_failures += 1;
                let _ = failed_keys.insert(key.clone());
                continue;
            }
            let scaled_result = match counter.kind {
                CounterKind::Direct => {
                    // SAFETY: A successful PDH_FMT_LARGE request initialized largeValue.
                    scale_integer(unsafe { value.Anonymous.largeValue }, counter.scale_power10)
                }
                CounterKind::RawFraction
                | CounterKind::CalculatedTwoSample
                | CounterKind::CalculatedTwoSampleWithBase => {
                    // SAFETY: A successful PDH_FMT_DOUBLE request initialized doubleValue.
                    scale_double(
                        unsafe { value.Anonymous.doubleValue },
                        counter.scale_power10,
                    )
                }
            };
            let scaled = match scaled_result {
                Ok(scaled) => scaled,
                Err(error) => {
                    let error = Error::Calculation {
                        path: counter.path.clone(),
                        message: error.to_string(),
                    };
                    failures.push(sample_failure(counter.config_index, &error));
                    diagnostics.counter_read_failures += 1;
                    continue;
                }
            };
            points.push(SamplePoint {
                counter_index: counter.config_index,
                path: counter.path.clone(),
                instance: counter.instance.clone(),
                value: SampleValue::Value(scaled),
            });
        }
        let retry_now = Instant::now();
        for key in failed_keys {
            let Some(counter) = self.counters.remove(&key) else {
                continue;
            };
            // SAFETY: The handle belongs to this query and is not used after removal.
            if let Err(error) = check("PdhRemoveCounter(retry)", &counter.path, unsafe {
                PdhRemoveCounter(counter.handle)
            }) {
                self.restore_pending_metadata(diagnostics, failures, overflows);
                return Err(error);
            }
            self.schedule_retry(
                key,
                CounterTarget {
                    config_index: counter.config_index,
                    path: counter.path,
                    localized: counter.wildcard,
                    instance: counter.instance,
                },
                retry_now,
            );
        }
        diagnostics.active_expanded_counters = self
            .counters
            .values()
            .filter(|counter| counter.wildcard)
            .count();
        Ok(Sample {
            start_time_unix_nano,
            timestamp_unix_nano,
            points,
            failures,
            overflows,
            diagnostics,
        })
    }
}

impl Drop for Query {
    fn drop(&mut self) {
        // SAFETY: This handle has one owner and all PDH calls occur on this
        // thread. Closing the query also releases every counter handle.
        let status = unsafe { PdhCloseQuery(self.handle) };
        #[cfg(test)]
        let _ = QUERY_CLOSES.fetch_add(1, Ordering::Relaxed);
        if status != 0 {
            otel_arrow_dfe_telemetry::otel_warn!(
                "otelcol.node.windowsperfcounters.close.fail",
                node = self.node.as_str(),
                status = status as u64
            );
        }
    }
}

enum Command {
    Collect(oneshot::Sender<Result<Sample, Error>>),
    Shutdown,
}

struct AcceptedCollectGuard {
    accepted: Arc<AtomicBool>,
}

impl AcceptedCollectGuard {
    fn new(accepted: Arc<AtomicBool>) -> Self {
        Self { accepted }
    }
}

impl Drop for AcceptedCollectGuard {
    fn drop(&mut self) {
        self.accepted.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
struct QuerySettings {
    counters: Vec<CounterConfig>,
    wildcard_refresh_interval: Duration,
    max_instances_per_wildcard: usize,
    max_expanded_counters: usize,
    node: String,
}

impl QuerySettings {
    fn open(&self) -> Result<Query, Error> {
        Query::open(
            self.counters.clone(),
            self.wildcard_refresh_interval,
            self.max_instances_per_wildcard,
            self.max_expanded_counters,
            self.node.clone(),
        )
    }
}

/// Capacity-one command client for the thread that owns the persistent query.
pub(super) struct Worker {
    tx: mpsc::SyncSender<Command>,
    accepted: Arc<AtomicBool>,
    shutdown_requested: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    completion: Option<oneshot::Receiver<()>>,
}

impl Worker {
    pub(super) async fn start(
        counters: Vec<CounterConfig>,
        wildcard_refresh_interval: Duration,
        max_instances_per_wildcard: usize,
        max_expanded_counters: usize,
        node: String,
    ) -> Result<Self, Error> {
        let (tx, rx) = mpsc::sync_channel(1);
        let (init_tx, init_rx) = oneshot::channel();
        let (completion_tx, completion_rx) = oneshot::channel();
        let accepted = Arc::new(AtomicBool::new(false));
        let worker_accepted = Arc::clone(&accepted);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown_requested);
        let settings = QuerySettings {
            counters,
            wildcard_refresh_interval,
            max_instances_per_wildcard,
            max_expanded_counters,
            node,
        };
        let join = std::thread::Builder::new()
            .name("windowsperfcounters-pdh".to_owned())
            .spawn(move || {
                {
                    let query = match settings.open() {
                        Ok(query) => query,
                        Err(error) => {
                            let _ = init_tx.send(Err(error));
                            return;
                        }
                    };
                    if init_tx.send(Ok(())).is_err() {
                        return;
                    }
                    let mut query = Some(query);
                    let mut query_failure_attempts = 0_u32;
                    let mut next_query_retry = Instant::now();
                    let mut rebuild_attempts = 0_u32;
                    let mut next_rebuild = Instant::now();
                    let mut pending = PendingMetadata::default();
                    while !worker_shutdown.load(Ordering::Acquire) {
                        match rx.recv() {
                            Ok(Command::Collect(response)) => {
                                let _accepted =
                                    AcceptedCollectGuard::new(Arc::clone(&worker_accepted));
                                let now = Instant::now();
                                let was_query_retry = query.is_some()
                                    && query_failure_attempts > 0
                                    && now >= next_query_retry;
                                if was_query_retry {
                                    pending.diagnostics.retry_attempts += 1;
                                }
                                let mut result = if let Some(active_query) = query.as_mut() {
                                    if now < next_query_retry {
                                        Err(Error::QueryRecoveryPending {
                                            retry_after_ms: next_query_retry
                                                .duration_since(now)
                                                .as_millis(),
                                        })
                                    } else {
                                        active_query.collect()
                                    }
                                } else if now < next_rebuild {
                                    Err(Error::QueryRecoveryPending {
                                        retry_after_ms: next_rebuild
                                            .duration_since(now)
                                            .as_millis(),
                                    })
                                } else {
                                    pending.diagnostics.query_rebuild_attempts += 1;
                                    match settings.open() {
                                        Ok(rebuilt) => {
                                            pending.diagnostics.query_rebuild_recoveries += 1;
                                            rebuild_attempts = 0;
                                            query_failure_attempts = 0;
                                            next_query_retry = now;
                                            query = Some(rebuilt);
                                            query
                                                .as_mut()
                                                .expect("query was just restored")
                                                .collect()
                                        }
                                        Err(error) => {
                                            rebuild_attempts = rebuild_attempts.saturating_add(1);
                                            next_rebuild = now
                                                + retry_delay(
                                                    rebuild_attempts,
                                                    settings.wildcard_refresh_interval,
                                                );
                                            Err(error)
                                        }
                                    }
                                };
                                if result
                                    .as_ref()
                                    .is_err_and(Error::is_query_collection_failure)
                                {
                                    query_failure_attempts =
                                        query_failure_attempts.saturating_add(1);
                                    next_query_retry = now
                                        + retry_delay(
                                            query_failure_attempts,
                                            settings.wildcard_refresh_interval,
                                        );
                                    if should_rebuild_query(query_failure_attempts) {
                                        if let Some(active_query) = query.as_mut() {
                                            active_query.move_pending_metadata_to(
                                                &mut pending.diagnostics,
                                                &mut pending.failures,
                                                &mut pending.overflows,
                                            );
                                        }
                                        query = None;
                                        rebuild_attempts = 0;
                                        next_rebuild = next_query_retry;
                                    }
                                } else if result
                                    .as_ref()
                                    .is_err_and(Error::requires_immediate_query_rebuild)
                                {
                                    if let Some(active_query) = query.as_mut() {
                                        active_query.move_pending_metadata_to(
                                            &mut pending.diagnostics,
                                            &mut pending.failures,
                                            &mut pending.overflows,
                                        );
                                    }
                                    query = None;
                                    rebuild_attempts = 0;
                                    next_rebuild =
                                        now + retry_delay(1, settings.wildcard_refresh_interval);
                                } else if result.is_ok() {
                                    if was_query_retry {
                                        pending.diagnostics.retry_recoveries += 1;
                                    }
                                    query_failure_attempts = 0;
                                    next_query_retry = now;
                                }
                                if let Ok(sample) = &mut result {
                                    pending.merge_into_sample(sample);
                                }
                                let _ = response.send(result);
                            }
                            Ok(Command::Shutdown) | Err(_) => break,
                        }
                    }
                }
                let _ = completion_tx.send(());
            })
            .map_err(|err| Error::WorkerStart(err.to_string()))?;

        match tokio::time::timeout(INIT_TIMEOUT, init_rx).await {
            Ok(Ok(Ok(()))) => Ok(Self {
                tx,
                accepted,
                shutdown_requested,
                join: Some(join),
                completion: Some(completion_rx),
            }),
            Ok(Ok(Err(error))) => {
                let _ = join.join();
                Err(error)
            }
            Err(_) => {
                shutdown_requested.store(true, Ordering::Release);
                drop(join);
                Err(Error::WorkerInitTimeout)
            }
            Ok(Err(_)) => {
                let _ = join.join();
                Err(Error::WorkerInitStopped)
            }
        }
    }

    pub(super) async fn collect(&self) -> Result<Sample, Error> {
        if self
            .accepted
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::WorkerBusy);
        }
        let (response_tx, response_rx) = oneshot::channel();
        match self.tx.try_send(Command::Collect(response_tx)) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(_)) => {
                self.accepted.store(false, Ordering::Release);
                return Err(Error::WorkerBusy);
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                self.accepted.store(false, Ordering::Release);
                return Err(Error::WorkerStopped);
            }
        }
        response_rx.await.map_err(|_| Error::WorkerStopped)?
    }

    /// Request shutdown and return `false` if the worker outlives the deadline.
    pub(super) async fn shutdown(&mut self, deadline: Instant) -> Result<bool, Error> {
        self.shutdown_requested.store(true, Ordering::Release);
        match self.tx.try_send(Command::Shutdown) {
            Ok(()) | Err(mpsc::TrySendError::Full(_)) => {}
            Err(mpsc::TrySendError::Disconnected(_)) => {}
        }
        let Some(join) = self.join.take() else {
            return Ok(true);
        };
        let Some(completion) = self.completion.as_mut() else {
            self.join = Some(join);
            return Err(Error::WorkerStopped);
        };
        match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), completion).await {
            Ok(_) => {
                self.completion = None;
                join.join().map_err(|_| Error::WorkerPanicked)?;
                Ok(true)
            }
            Err(_) => {
                self.join = Some(join);
                Ok(false)
            }
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.shutdown_requested.store(true, Ordering::Release);
        let _ = self.tx.try_send(Command::Shutdown);
    }
}

#[cfg(test)]
mod tests {
    use super::super::config::MetricKind;
    use super::super::model::Number;
    use super::*;
    use windows_sys::Win32::System::Performance::{
        PDH_CSTATUS_INVALID_DATA, PDH_CSTATUS_NEW_DATA, PDH_FMT_COUNTERVALUE, PDH_RAW_COUNTER,
        PdhFormatFromRawValue,
    };

    fn counter(path: &str, name: &str) -> CounterConfig {
        CounterConfig {
            path: path.to_owned(),
            name: name.to_owned(),
            unit: "By".to_owned(),
            description: Arc::from(format!("Description for {name}.")),
            metric_kind: MetricKind::Gauge,
            attributes: Arc::new(BTreeMap::new()),
            excluded_aggregation_instance: None,
            scale_power10: 0,
        }
    }

    fn query_counts() -> (usize, usize) {
        (
            QUERY_OPENS.load(Ordering::Relaxed),
            QUERY_CLOSES.load(Ordering::Relaxed),
        )
    }

    fn raw_value(first: i64, second: i64) -> PDH_RAW_COUNTER {
        PDH_RAW_COUNTER {
            CStatus: PDH_CSTATUS_NEW_DATA,
            FirstValue: first,
            SecondValue: second,
            ..PDH_RAW_COUNTER::default()
        }
    }

    fn format_raw_fixture(
        native_type: u32,
        current: &PDH_RAW_COUNTER,
        previous: Option<&PDH_RAW_COUNTER>,
        time_base: i64,
    ) -> Result<f64, String> {
        match classify_native_type("fixture", native_type).map_err(|error| error.to_string())? {
            CounterKind::RawFraction => {
                validate_positive_base("fixture", current.SecondValue)
                    .map_err(|error| error.to_string())?;
            }
            CounterKind::CalculatedTwoSampleWithBase => {
                let previous = previous.ok_or_else(|| "previous fixture is required".to_owned())?;
                match classify_base_delta("fixture", previous.SecondValue, current.SecondValue)
                    .map_err(|error| error.to_string())?
                {
                    BaseDelta::Increased => {}
                    BaseDelta::NoObservation => {
                        return Err("fixture has no observations".to_owned());
                    }
                }
            }
            CounterKind::Direct | CounterKind::CalculatedTwoSample => {}
        }
        let mut formatted = PDH_FMT_COUNTERVALUE::default();
        // SAFETY: All pointers reference initialized fixture values for this call.
        let status = unsafe {
            PdhFormatFromRawValue(
                native_type,
                PDH_FMT_DOUBLE | PDH_FMT_NOSCALE | PDH_FMT_NOCAP100,
                &time_base,
                current,
                previous.map_or(null(), std::ptr::from_ref),
                &mut formatted,
            )
        };
        if status != 0
            || !matches!(
                formatted.CStatus,
                PDH_CSTATUS_VALID_DATA | PDH_CSTATUS_NEW_DATA
            )
        {
            return Err(format!(
                "PdhFormatFromRawValue failed with status 0x{status:08X}, CStatus 0x{:08X}",
                formatted.CStatus
            ));
        }
        // SAFETY: A successful PDH_FMT_DOUBLE request initialized doubleValue.
        let value = unsafe { formatted.Anonymous.doubleValue };
        if value.is_finite() {
            Ok(value)
        } else {
            Err("PdhFormatFromRawValue returned a non-finite value".to_owned())
        }
    }

    fn simulated_blocked_worker(cleanup_complete: Arc<AtomicBool>) -> (Worker, mpsc::Sender<()>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::channel();
        let (completion_tx, completion_rx) = oneshot::channel();
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let join = std::thread::spawn(move || {
            let _commands = rx;
            let _ = release_rx.recv();
            cleanup_complete.store(true, Ordering::Release);
            let _ = completion_tx.send(());
        });
        (
            Worker {
                tx,
                accepted: Arc::new(AtomicBool::new(false)),
                shutdown_requested,
                join: Some(join),
                completion: Some(completion_rx),
            },
            release_tx,
        )
    }

    fn empty_sample() -> Sample {
        Sample {
            start_time_unix_nano: 1,
            timestamp_unix_nano: 2,
            points: Vec::new(),
            failures: Vec::new(),
            overflows: Vec::new(),
            diagnostics: SampleDiagnostics::default(),
        }
    }

    fn controlled_worker() -> (
        Worker,
        oneshot::Receiver<()>,
        oneshot::Sender<()>,
        oneshot::Receiver<()>,
    ) {
        let (tx, rx) = mpsc::sync_channel(1);
        let accepted = Arc::new(AtomicBool::new(false));
        let worker_accepted = Arc::clone(&accepted);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown_requested);
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (released_tx, released_rx) = oneshot::channel();
        let (completion_tx, completion_rx) = oneshot::channel();
        let join = std::thread::spawn(move || {
            let mut first = Some((started_tx, release_rx, released_tx));
            while !worker_shutdown.load(Ordering::Acquire) {
                match rx.recv() {
                    Ok(Command::Collect(response)) => {
                        let _accepted = AcceptedCollectGuard::new(Arc::clone(&worker_accepted));
                        if let Some((started, release, released)) = first.take() {
                            let _ = started.send(());
                            let _ = release.blocking_recv();
                            let _ = response.send(Ok(empty_sample()));
                            let _ = released.send(());
                        } else {
                            let _ = response.send(Ok(empty_sample()));
                        }
                    }
                    Ok(Command::Shutdown) | Err(_) => break,
                }
            }
            let _ = completion_tx.send(());
        });
        (
            Worker {
                tx,
                accepted,
                shutdown_requested,
                join: Some(join),
                completion: Some(completion_rx),
            },
            started_rx,
            release_tx,
            released_rx,
        )
    }

    /// Scenario: A collection future times out while the worker remains inside a native call.
    /// Guarantees: Later requests fail fast as busy and collection resumes after the call returns.
    #[tokio::test(flavor = "current_thread")]
    async fn bounds_timed_out_collection_and_recovers() {
        let (mut worker, started, release, released) = controlled_worker();
        {
            let first = tokio::time::timeout(Duration::from_millis(20), worker.collect());
            tokio::pin!(first);
            tokio::select! {
                result = &mut first => panic!("collection resolved before release: {result:?}"),
                result = started => result.expect("worker should start the first collection"),
            }
            assert!(first.await.is_err());
        }
        assert!(matches!(worker.collect().await, Err(Error::WorkerBusy)));
        release.send(()).expect("release worker collection");
        released
            .await
            .expect("worker should finish blocked collection");
        assert!(worker.collect().await.is_ok());
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await
                .expect("worker shutdown")
        );
    }

    /// Scenario: A persistent worker collects two supported counters repeatedly and shuts down.
    /// Guarantees: One query serves every scrape and normal shutdown closes it exactly once.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    async fn persistent_worker_reuses_and_closes_query() {
        let _serial = super::super::TEST_PDH_LOCK.lock().await;
        let before = query_counts();
        let counters = vec![
            counter(r"\Memory\Available Bytes", "windows.memory.available"),
            counter(r"\Memory\Committed Bytes", "windows.memory.committed"),
        ];
        let mut worker = Worker::start(
            counters,
            Duration::from_secs(30),
            256,
            4_096,
            "test".to_owned(),
        )
        .await
        .unwrap();
        for _ in 0..3 {
            let sample = worker.collect().await.unwrap();
            assert_eq!(sample.points.len(), 2);
            assert!(sample.points.iter().all(
                |point| matches!(point.value, SampleValue::Value(Number::Integer(value)) if value >= 0)
            ));
            assert_eq!(sample.points[0].counter_index, 0);
            assert_eq!(sample.points[0].path, r"\Memory\Available Bytes");
            assert_eq!(sample.points[1].counter_index, 1);
            assert_eq!(sample.points[1].path, r"\Memory\Committed Bytes");
        }
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
        let after = query_counts();
        assert_eq!(after.0 - before.0, 1);
        assert_eq!(after.1 - before.1, 1);
    }

    /// Scenario: Two independently configured receiver workers collect in one process.
    /// Guarantees: Each receiver owns a distinct query and both can collect and shut down cleanly.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    async fn independent_workers_can_collect_concurrently() {
        let _serial = super::super::TEST_PDH_LOCK.lock().await;
        let before = query_counts();
        let counters = vec![counter(
            r"\Memory\Available Bytes",
            "windows.memory.available",
        )];
        let mut first = Worker::start(
            counters.clone(),
            Duration::from_secs(30),
            256,
            4_096,
            "first".to_owned(),
        )
        .await
        .unwrap();
        let mut second = Worker::start(
            counters,
            Duration::from_secs(30),
            256,
            4_096,
            "second".to_owned(),
        )
        .await
        .unwrap();
        assert!(!first.collect().await.unwrap().points.is_empty());
        assert!(!second.collect().await.unwrap().points.is_empty());
        assert!(
            first
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
        assert!(
            second
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
        let after = query_counts();
        assert_eq!(after.0 - before.0, 2);
        assert_eq!(after.1 - before.1, 2);
    }

    /// Scenario: The first post-prime scrape has direct, raw-fraction, and two-sample values.
    /// Guarantees: One-sample values emit immediately while only the two-sample timer is omitted.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    async fn first_scrape_emits_one_sample_values_and_warms_two_sample() {
        let _serial = super::super::TEST_PDH_LOCK.lock().await;
        let counters = vec![
            counter(r"\Memory\Available Bytes", "windows.memory.available"),
            counter(
                r"\Memory\% Committed Bytes In Use",
                "windows.memory.committed_percent",
            ),
            counter(
                r"\Processor(_Total)\% Processor Time",
                "windows.processor.time",
            ),
        ];
        let mut worker = Worker::start(
            counters,
            Duration::from_secs(30),
            256,
            4_096,
            "test".to_owned(),
        )
        .await
        .unwrap();
        let sample = worker.collect().await.unwrap();
        assert!(matches!(
            sample.points[0].value,
            SampleValue::Value(Number::Integer(_))
        ));
        assert!(
            matches!(sample.points[1].value, SampleValue::Value(Number::Double(value)) if value.is_finite())
        );
        assert_eq!(sample.points[2].value, SampleValue::Warming);
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
    }

    /// Scenario: The local host exposes one exact gauge and dynamically enumerated process gauges.
    /// Guarantees: Native wildcard expansion returns concrete identity-keyed points without changing the exact point.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    async fn collects_exact_and_wildcard_counters() {
        let _serial = super::super::TEST_PDH_LOCK.lock().await;
        let counters = vec![
            counter(r"\Memory\Available Bytes", "windows.memory.available"),
            counter(r"\Process(*)\Private Bytes", "windows.process.private"),
        ];
        let mut worker = Worker::start(
            counters,
            Duration::from_secs(1),
            256,
            4_096,
            "test".to_owned(),
        )
        .await
        .unwrap();
        let sample = worker.collect().await.unwrap();
        let exact = sample
            .points
            .iter()
            .find(|point| point.counter_index == 0)
            .expect("exact Memory point");
        assert_eq!(exact.path, r"\Memory\Available Bytes");
        assert!(exact.instance.is_none());
        assert!(matches!(
            exact.value,
            SampleValue::Value(Number::Integer(value)) if value >= 0
        ));

        let wildcard = sample
            .points
            .iter()
            .filter(|point| point.counter_index == 1)
            .collect::<Vec<_>>();
        assert!(!wildcard.is_empty());
        let keys = wildcard
            .iter()
            .map(|point| CounterKey::new(point.counter_index, &point.path))
            .collect::<BTreeSet<_>>();
        assert_eq!(keys.len(), wildcard.len());
        assert!(wildcard.iter().all(|point| {
            point.instance.is_some()
                && point.path != r"\Process(*)\Private Bytes"
                && matches!(
                    point.value,
                    SampleValue::Value(Number::Integer(value)) if value >= 0
                )
        }));
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
    }

    /// Scenario: Live Windows PDH formats common disk queue-length and precision-timer counters.
    /// Guarantees: Both additional two-sample families produce finite values from one query.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    async fn collects_common_calculated_disk_counters() {
        let _serial = super::super::TEST_PDH_LOCK.lock().await;
        let counters = vec![
            counter(
                r"\PhysicalDisk(_Total)\Avg. Disk Queue Length",
                "windows.disk.queue_length",
            ),
            counter(r"\PhysicalDisk(_Total)\% Disk Time", "windows.disk.time"),
        ];
        let mut worker = Worker::start(
            counters,
            Duration::from_secs(30),
            256,
            4_096,
            "test".to_owned(),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        let sample = worker.collect().await.unwrap();
        assert!(sample.failures.is_empty(), "{:?}", sample.failures);
        assert_eq!(sample.points.len(), 2);
        assert!(sample.points.iter().all(|point| {
            matches!(
                point.value,
                SampleValue::Value(Number::Double(value)) if value.is_finite()
            )
        }));
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
    }

    /// Scenario: Startup adds a healthy exact counter while another configured counter is absent.
    /// Guarantees: The worker starts, emits the healthy point, and retains a visible retry failure.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    async fn unavailable_exact_counter_retries_after_startup() {
        let _serial = super::super::TEST_PDH_LOCK.lock().await;
        let before = query_counts();
        let counters = vec![
            counter(r"\Memory\Available Bytes", "windows.memory.available"),
            counter(
                r"\Missing Object\Missing Counter",
                "windows.missing.counter",
            ),
        ];
        let mut worker = Worker::start(
            counters,
            Duration::from_secs(30),
            256,
            4_096,
            "test".to_owned(),
        )
        .await
        .unwrap();
        let sample = worker.collect().await.unwrap();
        assert!(
            sample
                .points
                .iter()
                .any(|point| point.path == r"\Memory\Available Bytes")
        );
        assert!(sample.failures.iter().any(|failure| {
            failure.counter_index == 1 && failure.reason == "PdhAddEnglishCounterW"
        }));
        assert_eq!(sample.diagnostics.counter_add_failures, 1);
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
        let after = query_counts();
        assert_eq!(after.0 - before.0, 1);
        assert_eq!(after.1 - before.1, 1);
    }

    /// Scenario: Every configured exact counter is unavailable when the worker starts.
    /// Guarantees: Startup succeeds with an empty active set and reports retryable failures without points.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    async fn entirely_unavailable_counter_set_starts_empty() {
        let _serial = super::super::TEST_PDH_LOCK.lock().await;
        let mut worker = Worker::start(
            vec![counter(
                r"\Missing Object\Missing Counter",
                "windows.missing.counter",
            )],
            Duration::from_secs(30),
            256,
            4_096,
            "test".to_owned(),
        )
        .await
        .unwrap();
        let sample = worker.collect().await.unwrap();
        assert!(sample.points.is_empty());
        assert_eq!(sample.failures.len(), 1);
        assert_eq!(sample.diagnostics.counter_add_failures, 1);
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
    }

    /// Scenario: PDH formats deterministic raw fraction, sample fraction, and average fixtures.
    /// Guarantees: Every advertised Part B formula produces the documented finite value.
    #[test]
    fn formats_part_b_raw_fixtures() {
        for native_type in [0x2002_0400, 0x2002_0500] {
            let value = format_raw_fixture(native_type, &raw_value(25, 100), None, 1).unwrap();
            assert!((value - 25.0).abs() < f64::EPSILON);
        }

        let previous = raw_value(100, 200);
        let current = raw_value(130, 250);
        let value = format_raw_fixture(0x20C2_0400, &current, Some(&previous), 1).unwrap();
        assert!((value - 60.0).abs() < f64::EPSILON);

        let previous = raw_value(1_000, 10);
        let current = raw_value(3_000, 20);
        let value = format_raw_fixture(0x3002_0400, &current, Some(&previous), 1_000).unwrap();
        assert!((value - 0.2).abs() < 1e-12);

        let previous = raw_value(1_000, 10);
        let current = raw_value(4_000, 20);
        let value = format_raw_fixture(0x4002_0500, &current, Some(&previous), 1).unwrap();
        assert!((value - 300.0).abs() < f64::EPSILON);
    }

    /// Scenario: A fraction or average fixture has a zero, decreasing, or invalid denominator.
    /// Guarantees: Invalid base data is rejected and no zero-filled formatted value is accepted.
    #[test]
    fn rejects_invalid_part_b_denominators() {
        assert!(format_raw_fixture(0x2002_0400, &raw_value(25, 0), None, 1).is_err());
        assert!(
            format_raw_fixture(
                0x20C2_0400,
                &raw_value(130, 150),
                Some(&raw_value(100, 200)),
                1,
            )
            .is_err()
        );
        assert!(
            format_raw_fixture(
                0x3002_0400,
                &raw_value(3_000, 5),
                Some(&raw_value(1_000, 10)),
                1_000,
            )
            .is_err()
        );
        assert!(
            format_raw_fixture(
                0x4002_0500,
                &raw_value(4_000, 5),
                Some(&raw_value(1_000, 10)),
                1,
            )
            .is_err()
        );

        let mut invalid = raw_value(25, 100);
        invalid.CStatus = PDH_CSTATUS_INVALID_DATA;
        assert!(format_raw_fixture(0x2002_0400, &invalid, None, 1).is_err());
    }

    /// Scenario: A sample fraction or average base is unchanged between valid samples.
    /// Guarantees: The interval is classified as no observations, not invalid data or zero.
    #[test]
    fn distinguishes_no_observations_from_invalid_base() {
        assert_eq!(
            classify_base_delta("average", 10, 10).unwrap(),
            BaseDelta::NoObservation
        );
        assert_eq!(
            classify_base_delta("average", 10, 11).unwrap(),
            BaseDelta::Increased
        );
        assert!(classify_base_delta("average", 10, 9).is_err());
    }

    /// Scenario: A provider base resets and then advances from its new value.
    /// Guarantees: Reset data fails once but establishes the baseline needed for later recovery.
    #[test]
    fn refreshes_base_after_reset_failure() {
        let mut previous = Some(10);
        assert!(classify_base_disposition("average", &mut previous, 5).is_err());
        assert_eq!(previous, Some(5));
        assert_eq!(
            classify_base_disposition("average", &mut previous, 6).unwrap(),
            CollectionDisposition::Format
        );
    }

    /// Scenario: The worker's capacity-one command queue already contains a request.
    /// Guarantees: Additional work fails explicitly instead of growing an unbounded backlog.
    #[test]
    fn pending_work_is_bounded() {
        let (tx, _rx) = mpsc::sync_channel(1);
        let (first, _) = oneshot::channel();
        let (second, _) = oneshot::channel();
        tx.try_send(Command::Collect(first)).unwrap();
        assert!(matches!(
            tx.try_send(Command::Collect(second)),
            Err(mpsc::TrySendError::Full(_))
        ));
    }

    /// Scenario: Expanded paths arrive in different orders and include duplicate instance names.
    /// Guarantees: Stable keys use configured identity plus the full case-insensitive PDH path.
    #[test]
    fn stable_keys_do_not_depend_on_expansion_order() {
        let first = [
            CounterKey::new(0, r"\Process(worker)\Private Bytes"),
            CounterKey::new(0, r"\Process(worker#1)\Private Bytes"),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        let reordered = [
            CounterKey::new(0, r"\PROCESS(WORKER#1)\PRIVATE BYTES"),
            CounterKey::new(0, r"\PROCESS(WORKER)\PRIVATE BYTES"),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        assert_eq!(first, reordered);
        assert_ne!(
            CounterKey::new(0, r"\Process(worker)\Private Bytes"),
            CounterKey::new(0, r"\Process(worker#1)\Private Bytes")
        );
        assert_ne!(
            CounterKey::new(0, r"\Process(worker)\Private Bytes"),
            CounterKey::new(1, r"\Process(worker)\Private Bytes")
        );
    }

    /// Scenario: A refresh reorders retained instances while adding one and removing another.
    /// Guarantees: Only stable-key set differences change handles, so retained history cannot move positionally.
    #[test]
    fn refresh_delta_preserves_retained_instance_keys() {
        let retained = CounterKey::new(0, r"\Process(worker#1)\Private Bytes");
        let removed = CounterKey::new(0, r"\Process(old)\Private Bytes");
        let added = CounterKey::new(0, r"\Process(new)\Private Bytes");
        let existing = [removed.clone(), retained.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let wanted = [added.clone(), retained.clone()]
            .into_iter()
            .rev()
            .collect::<BTreeSet<_>>();
        let (actual_removed, actual_added) = refresh_changes_with_group_resets(
            &existing,
            &BTreeSet::new(),
            &wanted,
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert_eq!(actual_removed, vec![removed]);
        assert_eq!(actual_added, vec![added]);
        assert!(existing.contains(&retained));
        assert!(wanted.contains(&retained));
    }

    /// Scenario: Discovery returns more sorted paths than either configured expansion bound allows.
    /// Guarantees: The retained prefix is deterministic and every omitted instance is counted explicitly.
    #[test]
    fn expansion_limits_are_deterministic_and_report_omissions() {
        let mut paths = vec!["charlie".to_owned(), "alpha".to_owned(), "bravo".to_owned()];
        paths.sort();

        let omitted = limit_expanded_paths(&mut paths, 2, 10);

        assert_eq!(paths, ["alpha", "bravo"]);
        assert_eq!(omitted, (1, 0));

        let omitted = limit_expanded_paths(&mut paths, 2, 1);
        assert_eq!(paths, ["alpha"]);
        assert_eq!(omitted, (0, 1));
    }

    /// Scenario: An earlier configured wildcard succeeds while a later wildcard's discovery fails with retained identities.
    /// Guarantees: Failed-discovery retention reserves global capacity before successful discoveries regardless of configuration order.
    #[test]
    fn failed_discovery_retention_reserves_global_capacity_first() {
        let total_limit = 4;
        let (retained_failed, failed_per_omitted, failed_total_omitted) =
            expansion_allocation(3, 10, total_limit);
        let (retained_success, success_per_omitted, success_total_omitted) =
            expansion_allocation(3, 10, total_limit - retained_failed);

        assert_eq!(retained_failed, 3);
        assert_eq!((failed_per_omitted, failed_total_omitted), (0, 0));
        assert_eq!(retained_success, 1);
        assert_eq!((success_per_omitted, success_total_omitted), (0, 2));
        assert_eq!(retained_failed + retained_success, total_limit);
    }

    /// Scenario: Startup or recovery deltas are merged into a completed collection.
    /// Guarantees: Cumulative diagnostic deltas are added without overwriting the collection's current active-counter gauge.
    #[test]
    fn diagnostic_merge_preserves_current_active_count() {
        let mut current = SampleDiagnostics {
            active_expanded_counters: 2,
            instances_added: 2,
            ..Default::default()
        };
        let pending = SampleDiagnostics {
            active_expanded_counters: 0,
            discovery_refreshes: 1,
            ..Default::default()
        };

        merge_diagnostics(&mut current, pending);

        assert_eq!(current.active_expanded_counters, 2);
        assert_eq!(current.instances_added, 2);
        assert_eq!(current.discovery_refreshes, 1);
    }

    /// Scenario: The wall clock moves backward while a cumulative metric query remains open.
    /// Guarantees: The next point begins a new cumulative sequence that later scrapes retain.
    #[test]
    fn cumulative_start_time_resets_after_clock_rollback() {
        let mut start = 10;
        let mut previous = 10;
        assert_eq!(cumulative_start_time(&mut start, &mut previous, 20), 10);
        assert_eq!(cumulative_start_time(&mut start, &mut previous, 15), 15);
        assert_eq!(cumulative_start_time(&mut start, &mut previous, 16), 15);
    }

    /// Scenario: Query metadata survives a failed collection and is merged with later recovery evidence.
    /// Guarantees: Diagnostic counts, counter failures, and expansion overflows remain ordered and observable.
    #[test]
    fn sample_metadata_merge_preserves_pending_evidence() {
        let mut diagnostics = SampleDiagnostics {
            counter_add_failures: 1,
            ..Default::default()
        };
        let mut failures = vec![SampleFailure {
            counter_index: 0,
            reason: "initial",
            error: "initial failure".to_owned(),
        }];
        let mut overflows = vec![ExpansionOverflow {
            counter_index: 0,
            reason: "per_wildcard",
            discovered: 3,
            retained: 2,
            omitted: 1,
        }];

        merge_sample_metadata(
            &mut diagnostics,
            &mut failures,
            &mut overflows,
            SampleDiagnostics {
                retry_recoveries: 1,
                ..Default::default()
            },
            vec![SampleFailure {
                counter_index: 1,
                reason: "recovery",
                error: "recovery failure".to_owned(),
            }],
            vec![ExpansionOverflow {
                counter_index: 1,
                reason: "receiver_total",
                discovered: 4,
                retained: 3,
                omitted: 1,
            }],
        );

        assert_eq!(diagnostics.counter_add_failures, 1);
        assert_eq!(diagnostics.retry_recoveries, 1);
        assert_eq!(failures.len(), 2);
        assert_eq!(failures[0].reason, "initial");
        assert_eq!(failures[1].reason, "recovery");
        assert_eq!(overflows.len(), 2);
        assert_eq!(overflows[0].reason, "per_wildcard");
        assert_eq!(overflows[1].reason, "receiver_total");

        merge_sample_metadata(
            &mut diagnostics,
            &mut failures,
            &mut overflows,
            SampleDiagnostics::default(),
            vec![SampleFailure {
                counter_index: 0,
                reason: "initial",
                error: "latest failure".to_owned(),
            }],
            vec![ExpansionOverflow {
                counter_index: 0,
                reason: "per_wildcard",
                discovered: 5,
                retained: 2,
                omitted: 3,
            }],
        );

        assert_eq!(failures.len(), 2);
        assert_eq!(failures[0].error, "latest failure");
        assert_eq!(overflows.len(), 2);
        assert_eq!(overflows[0].discovered, 5);
    }

    /// Scenario: Worker recovery metadata precedes metadata from the successful scrape that releases it.
    /// Guarantees: Bundling pending state preserves failure and overflow order while adding diagnostics.
    #[test]
    fn pending_metadata_bundle_preserves_worker_merge_order() {
        let mut pending = PendingMetadata {
            diagnostics: SampleDiagnostics {
                retry_attempts: 1,
                ..Default::default()
            },
            failures: vec![SampleFailure {
                counter_index: 0,
                reason: "pending",
                error: "pending failure".to_owned(),
            }],
            overflows: vec![ExpansionOverflow {
                counter_index: 0,
                reason: "per_wildcard",
                discovered: 3,
                retained: 2,
                omitted: 1,
            }],
        };
        let mut sample = Sample {
            start_time_unix_nano: 1,
            timestamp_unix_nano: 2,
            points: Vec::new(),
            failures: vec![SampleFailure {
                counter_index: 1,
                reason: "current",
                error: "current failure".to_owned(),
            }],
            overflows: vec![ExpansionOverflow {
                counter_index: 1,
                reason: "receiver_total",
                discovered: 4,
                retained: 3,
                omitted: 1,
            }],
            diagnostics: SampleDiagnostics {
                retry_recoveries: 1,
                ..Default::default()
            },
        };

        pending.merge_into_sample(&mut sample);

        assert_eq!(sample.diagnostics.retry_attempts, 1);
        assert_eq!(sample.diagnostics.retry_recoveries, 1);
        assert_eq!(sample.failures[0].reason, "pending");
        assert_eq!(sample.failures[1].reason, "current");
        assert_eq!(sample.overflows[0].reason, "per_wildcard");
        assert_eq!(sample.overflows[1].reason, "receiver_total");
        assert_eq!(pending.failures.len(), 0);
        assert_eq!(pending.overflows.len(), 0);
    }

    /// Scenario: A counter repeatedly fails while the refresh cadence caps recovery delay.
    /// Guarantees: Retry delay grows exponentially, never becomes zero, and never exceeds the refresh cadence.
    #[test]
    fn retry_delay_is_exponential_and_capped() {
        let cap = Duration::from_secs(10);
        assert_eq!(retry_delay(1, cap), Duration::from_secs(1));
        assert_eq!(retry_delay(2, cap), Duration::from_secs(2));
        assert_eq!(retry_delay(3, cap), Duration::from_secs(4));
        assert_eq!(retry_delay(4, cap), Duration::from_secs(8));
        assert_eq!(retry_delay(5, cap), cap);
        assert_eq!(retry_delay(u32::MAX, cap), cap);
        assert_eq!(
            retry_delay(1, Duration::from_secs(1)),
            Duration::from_secs(1)
        );
    }

    /// Scenario: Query collection fails transiently and then remains unavailable.
    /// Guarantees: Valid query history survives the first two retries and a rebuild is requested at the bounded threshold.
    #[test]
    fn query_rebuild_waits_for_bounded_retries() {
        assert!(!should_rebuild_query(1));
        assert!(!should_rebuild_query(2));
        assert!(should_rebuild_query(3));
        assert!(should_rebuild_query(u32::MAX));
    }

    /// Scenario: The same failed counter is scheduled repeatedly before it recovers.
    /// Guarantees: One stable identity owns one retry entry and repeated failures advance its bounded backoff.
    #[test]
    fn retry_state_is_unique_per_stable_identity() {
        let key = CounterKey::new(0, r"\Process(worker)\Private Bytes");
        let target = CounterTarget {
            config_index: 0,
            path: r"\Process(worker)\Private Bytes".to_owned(),
            localized: true,
            instance: Some(InstanceIdentity {
                name: "worker".to_owned(),
                parent: None,
                index: 0,
            }),
        };
        let now = Instant::now();
        let mut states = BTreeMap::new();

        schedule_retry_state(
            &mut states,
            key.clone(),
            target.clone(),
            now,
            Duration::from_secs(30),
        );
        schedule_retry_state(
            &mut states,
            key.clone(),
            target,
            now,
            Duration::from_secs(30),
        );

        assert_eq!(states.len(), 1);
        let state = states.get(&key).unwrap();
        assert_eq!(state.attempts, 2);
        assert_eq!(state.next_attempt, now + Duration::from_secs(2));
    }

    /// Scenario: Refresh returns the same duplicate-name membership in a different order.
    /// Guarantees: Stable membership does not recreate handles or reset calculated history.
    #[test]
    fn unchanged_duplicate_group_preserves_handles() {
        let worker = CounterKey::new(0, r"\Process(worker)\% Processor Time");
        let worker_duplicate = CounterKey::new(0, r"\Process(worker#1)\% Processor Time");
        let instance = InstanceIdentity {
            name: "worker".to_owned(),
            parent: None,
            index: 0,
        };
        let duplicate_instance = InstanceIdentity {
            name: "worker".to_owned(),
            parent: None,
            index: 1,
        };
        let existing = [worker.clone(), worker_duplicate.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let wanted = [worker_duplicate.clone(), worker.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let existing_groups = instance_groups(
            [
                (&worker, &instance),
                (&worker_duplicate, &duplicate_instance),
            ]
            .into_iter(),
        );
        let wanted_groups = instance_groups(
            [
                (&worker_duplicate, &duplicate_instance),
                (&worker, &instance),
            ]
            .into_iter(),
        );

        let (removed, added) = refresh_changes_with_group_resets(
            &existing,
            &BTreeSet::new(),
            &wanted,
            &existing_groups,
            &wanted_groups,
        );

        assert!(removed.is_empty());
        assert!(added.is_empty());
    }

    /// Scenario: One member of a duplicate-name group has a live handle while its peer waits in retry state and discovery still returns both.
    /// Guarantees: Logical membership remains unchanged, so the healthy live handle is not recreated and the retry keeps its backoff.
    #[test]
    fn retrying_duplicate_does_not_reset_healthy_peer() {
        let live_worker = CounterKey::new(0, r"\Process(worker)\% Processor Time");
        let retrying_worker = CounterKey::new(0, r"\Process(worker#1)\% Processor Time");
        let live_instance = InstanceIdentity {
            name: "worker".to_owned(),
            parent: None,
            index: 0,
        };
        let retrying_instance = InstanceIdentity {
            name: "worker".to_owned(),
            parent: None,
            index: 1,
        };
        let live = [live_worker.clone()].into_iter().collect::<BTreeSet<_>>();
        let retrying = [retrying_worker.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let wanted = [live_worker.clone(), retrying_worker.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let known_groups = instance_groups(
            [
                (&live_worker, &live_instance),
                (&retrying_worker, &retrying_instance),
            ]
            .into_iter(),
        );
        let wanted_groups = instance_groups(
            [
                (&retrying_worker, &retrying_instance),
                (&live_worker, &live_instance),
            ]
            .into_iter(),
        );

        let (removed, added) = refresh_changes_with_group_resets(
            &live,
            &retrying,
            &wanted,
            &known_groups,
            &wanted_groups,
        );

        assert!(removed.is_empty());
        assert!(added.is_empty());
    }

    /// Scenario: One of two duplicate-name processes exits and PDH renumbers the survivor.
    /// Guarantees: Every handle in that duplicate group is recreated so no native or receiver history crosses process identities.
    #[test]
    fn duplicate_group_churn_recreates_retained_paths() {
        let worker = CounterKey::new(0, r"\Process(worker)\% Processor Time");
        let worker_duplicate = CounterKey::new(0, r"\Process(worker#1)\% Processor Time");
        let unrelated = CounterKey::new(0, r"\Process(other)\% Processor Time");
        let worker_instance = InstanceIdentity {
            name: "worker".to_owned(),
            parent: None,
            index: 0,
        };
        let worker_duplicate_instance = InstanceIdentity {
            name: "worker".to_owned(),
            parent: None,
            index: 1,
        };
        let unrelated_instance = InstanceIdentity {
            name: "other".to_owned(),
            parent: None,
            index: 0,
        };
        let existing = [worker.clone(), worker_duplicate.clone(), unrelated.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let wanted = [worker.clone(), unrelated.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let existing_groups = instance_groups(
            [
                (&worker, &worker_instance),
                (&worker_duplicate, &worker_duplicate_instance),
                (&unrelated, &unrelated_instance),
            ]
            .into_iter(),
        );
        let wanted_groups = instance_groups(
            [
                (&worker, &worker_instance),
                (&unrelated, &unrelated_instance),
            ]
            .into_iter(),
        );

        let (removed, added) = refresh_changes_with_group_resets(
            &existing,
            &BTreeSet::new(),
            &wanted,
            &existing_groups,
            &wanted_groups,
        );

        assert_eq!(
            removed.iter().cloned().collect::<BTreeSet<_>>(),
            [worker.clone(), worker_duplicate]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(added, vec![worker]);
        assert!(!removed.contains(&unrelated));
        assert!(!added.contains(&unrelated));
    }

    /// Scenario: PDH returns a valid multi-string expansion list or malformed missing terminators.
    /// Guarantees: Every concrete path is decoded exactly and malformed native buffers fail explicitly.
    #[test]
    fn parses_expanded_path_multi_string() {
        let mut buffer = Vec::new();
        for path in [
            r"\Process(worker)\Private Bytes",
            r"\Process(worker#1)\Private Bytes",
        ] {
            buffer.extend(path.encode_utf16());
            buffer.push(0);
        }
        buffer.push(0);
        assert_eq!(
            parse_multi_sz(&buffer).unwrap(),
            vec![
                r"\Process(worker)\Private Bytes",
                r"\Process(worker#1)\Private Bytes"
            ]
        );
        assert!(parse_multi_sz(&[b'a' as u16]).is_err());
    }

    /// Scenario: A wildcard expansion contains aggregate and concrete provider instances.
    /// Guarantees: Aggregate matching is case-insensitive and does not confuse concrete instances.
    #[test]
    fn identifies_provider_aggregation_instances() {
        assert!(is_aggregation_instance(
            r"\Process(_Total)\Private Bytes",
            "_Total"
        ));
        assert!(is_aggregation_instance(
            r"\Custom(_GLOBAL_)\Counter",
            "_Global_"
        ));
        assert!(!is_aggregation_instance(
            r"\Process(worker)\Private Bytes",
            "_Total"
        ));
    }

    /// Scenario: A wildcard aggregate sorts before concrete instances under a one-instance limit.
    /// Guarantees: Aggregate filtering happens before limits so capacity is available to real instances.
    #[test]
    fn filters_aggregation_before_expansion_limits() {
        let mut paths = vec![
            r"\Process(_Total)\Private Bytes".to_owned(),
            r"\Process(worker)\Private Bytes".to_owned(),
        ];
        remove_aggregation_instances(&mut paths, Some("_Total"));
        let omitted = limit_expanded_paths(&mut paths, 1, 1);
        assert_eq!(omitted, (0, 0));
        assert_eq!(paths, [r"\Process(worker)\Private Bytes"]);
    }

    /// Scenario: A simulated PDH call remains blocked past the shutdown deadline.
    /// Guarantees: Shutdown returns at the deadline, cleanup occurs later on the worker,
    /// and worker-owned resources remain alive until that cleanup completes.
    #[tokio::test(flavor = "current_thread")]
    async fn blocked_worker_releases_resources_after_timeout() {
        let _serial = super::super::TEST_PDH_LOCK.lock().await;
        let cleanup_complete = Arc::new(AtomicBool::new(false));
        let (mut worker, release) = simulated_blocked_worker(Arc::clone(&cleanup_complete));

        let start = Instant::now();
        assert!(
            !worker
                .shutdown(start + Duration::from_millis(25))
                .await
                .unwrap()
        );
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(!cleanup_complete.load(Ordering::Acquire));
        release.send(()).unwrap();
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
        assert!(cleanup_complete.load(Ordering::Acquire));
    }
}
