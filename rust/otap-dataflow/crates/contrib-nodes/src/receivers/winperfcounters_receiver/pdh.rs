// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Persistent synchronous PDH worker. Windows handles never leave its thread.
#![allow(unsafe_code)]

use super::Lease;
use crate::receivers::winperfcounters::{
    CounterConfig, Sample, SampleValue, scale_double, scale_integer,
};
use std::mem::size_of;
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use windows_sys::Win32::System::Performance::{
    PDH_COUNTER_INFO_W, PDH_CSTATUS_NEW_DATA, PDH_CSTATUS_VALID_DATA, PDH_FMT_COUNTERVALUE,
    PDH_FMT_DOUBLE, PDH_FMT_LARGE, PDH_HCOUNTER, PDH_HQUERY, PDH_MORE_DATA, PDH_RAW_COUNTER,
    PERF_DISPLAY_NO_SUFFIX, PERF_NUMBER_DECIMAL, PERF_NUMBER_HEX, PERF_SIZE_DWORD, PERF_SIZE_LARGE,
    PERF_TYPE_NUMBER, PdhAddEnglishCounterW, PdhCloseQuery, PdhCollectQueryData,
    PdhGetCounterInfoW, PdhGetFormattedCounterValue, PdhGetRawCounterValue, PdhOpenQueryW,
};

const INIT_TIMEOUT: Duration = Duration::from_secs(30);
const PDH_FMT_NOSCALE: u32 = 0x0000_1000;
const PDH_FMT_NOCAP100: u32 = 0x0000_8000;
const PERF_TYPE_COUNTER: u32 = 0x0000_0400;
const PERF_COUNTER_RATE: u32 = 0x0001_0000;
const PERF_COUNTER_FRACTION: u32 = 0x0002_0000;
const PERF_TIMER_100NS: u32 = 0x0010_0000;
const PERF_DELTA_COUNTER: u32 = 0x0040_0000;
const PERF_DELTA_BASE: u32 = 0x0080_0000;
const PERF_INVERSE_COUNTER: u32 = 0x0100_0000;
const PERF_DISPLAY_PER_SEC: u32 = 0x1000_0000;
const PERF_DISPLAY_PERCENT: u32 = 0x2000_0000;
const PERF_DISPLAY_SECONDS: u32 = 0x3000_0000;
const PERF_DISPLAY_NOSHOW: u32 = 0x4000_0000;
const PERF_COUNTER_RAWCOUNT: u32 =
    PERF_SIZE_DWORD | PERF_TYPE_NUMBER | PERF_NUMBER_DECIMAL | PERF_DISPLAY_NO_SUFFIX;
const PERF_COUNTER_LARGE_RAWCOUNT: u32 =
    PERF_SIZE_LARGE | PERF_TYPE_NUMBER | PERF_NUMBER_DECIMAL | PERF_DISPLAY_NO_SUFFIX;
const PERF_COUNTER_RAWCOUNT_HEX: u32 =
    PERF_SIZE_DWORD | PERF_TYPE_NUMBER | PERF_NUMBER_HEX | PERF_DISPLAY_NO_SUFFIX;
const PERF_COUNTER_LARGE_RAWCOUNT_HEX: u32 =
    PERF_SIZE_LARGE | PERF_TYPE_NUMBER | PERF_NUMBER_HEX | PERF_DISPLAY_NO_SUFFIX;
const PERF_COUNTER_COUNTER: u32 = PERF_SIZE_DWORD
    | PERF_TYPE_COUNTER
    | PERF_COUNTER_RATE
    | PERF_DELTA_COUNTER
    | PERF_DISPLAY_PER_SEC;
const PERF_COUNTER_BULK_COUNT: u32 = PERF_SIZE_LARGE
    | PERF_TYPE_COUNTER
    | PERF_COUNTER_RATE
    | PERF_DELTA_COUNTER
    | PERF_DISPLAY_PER_SEC;
const PERF_COUNTER_TIMER: u32 = PERF_SIZE_LARGE
    | PERF_TYPE_COUNTER
    | PERF_COUNTER_RATE
    | PERF_DELTA_COUNTER
    | PERF_DISPLAY_PERCENT;
const PERF_COUNTER_TIMER_INV: u32 = PERF_COUNTER_TIMER | PERF_INVERSE_COUNTER;
const PERF_100NSEC_TIMER: u32 = PERF_COUNTER_TIMER | PERF_TIMER_100NS;
const PERF_100NSEC_TIMER_INV: u32 = PERF_100NSEC_TIMER | PERF_INVERSE_COUNTER;
const PERF_RAW_FRACTION: u32 =
    PERF_SIZE_DWORD | PERF_TYPE_COUNTER | PERF_COUNTER_FRACTION | PERF_DISPLAY_PERCENT;
const PERF_LARGE_RAW_FRACTION: u32 =
    PERF_SIZE_LARGE | PERF_TYPE_COUNTER | PERF_COUNTER_FRACTION | PERF_DISPLAY_PERCENT;
const PERF_SAMPLE_FRACTION: u32 = PERF_SIZE_DWORD
    | PERF_TYPE_COUNTER
    | PERF_COUNTER_FRACTION
    | PERF_DELTA_COUNTER
    | PERF_DELTA_BASE
    | PERF_DISPLAY_PERCENT;
const PERF_AVERAGE_TIMER: u32 =
    PERF_SIZE_DWORD | PERF_TYPE_COUNTER | PERF_COUNTER_FRACTION | PERF_DISPLAY_SECONDS;
const PERF_AVERAGE_BULK: u32 =
    PERF_SIZE_LARGE | PERF_TYPE_COUNTER | PERF_COUNTER_FRACTION | PERF_DISPLAY_NOSHOW;

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
    #[error(
        "unsupported native counter type 0x{native_type:08X} for {path}; \
         supported types are direct raw counts, rates, timers, fractions, and averages"
    )]
    UnsupportedType { path: String, native_type: u32 },
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
}

impl Error {
    pub(super) fn is_collection_failure(&self) -> bool {
        matches!(
            self,
            Self::Pdh { .. } | Self::InvalidSample(_) | Self::Calculation { .. }
        )
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CounterKind {
    Direct,
    RawFraction,
    CalculatedTwoSample,
    CalculatedTwoSampleWithBase,
}

fn classify_native_type(path: &str, native_type: u32, scale: i32) -> Result<CounterKind, Error> {
    let kind = match native_type {
        PERF_COUNTER_RAWCOUNT
        | PERF_COUNTER_LARGE_RAWCOUNT
        | PERF_COUNTER_RAWCOUNT_HEX
        | PERF_COUNTER_LARGE_RAWCOUNT_HEX => CounterKind::Direct,
        PERF_COUNTER_COUNTER
        | PERF_COUNTER_BULK_COUNT
        | PERF_COUNTER_TIMER
        | PERF_COUNTER_TIMER_INV
        | PERF_100NSEC_TIMER
        | PERF_100NSEC_TIMER_INV => CounterKind::CalculatedTwoSample,
        PERF_SAMPLE_FRACTION | PERF_AVERAGE_TIMER | PERF_AVERAGE_BULK => {
            CounterKind::CalculatedTwoSampleWithBase
        }
        PERF_RAW_FRACTION | PERF_LARGE_RAW_FRACTION => CounterKind::RawFraction,
        _ => {
            return Err(Error::UnsupportedType {
                path: path.to_owned(),
                native_type,
            });
        }
    };
    // The default scale is a display hint. The receiver always requests
    // PDH_FMT_NOSCALE and applies only the configured scale.
    let _ = scale;
    Ok(kind)
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
    // SAFETY: The successful call initialized the fixed fields used here.
    let (native_type, scale) = unsafe { ((*info).dwType, (*info).lDefaultScale) };
    classify_native_type(path, native_type, scale)
}

struct CounterHandle {
    path: String,
    handle: PDH_HCOUNTER,
    kind: CounterKind,
    scale_power10: i32,
    previous_base: Option<i64>,
}

/// The query owns every added counter; closing it releases all handles.
struct Query {
    handle: PDH_HQUERY,
    counters: Vec<CounterHandle>,
    first_collection_after_prime: bool,
}

impl Query {
    fn open(configs: &[CounterConfig]) -> Result<Self, Error> {
        let mut handle = null_mut();
        // SAFETY: Null selects the live local data source; handle is writable.
        check("PdhOpenQueryW", "<query>", unsafe {
            PdhOpenQueryW(null(), 0, &mut handle)
        })?;
        let mut query = Self {
            handle,
            counters: Vec::with_capacity(configs.len()),
            first_collection_after_prime: true,
        };
        #[cfg(test)]
        let _ = QUERY_OPENS.fetch_add(1, Ordering::Relaxed);

        for config in configs {
            let path: Vec<u16> = config.path.encode_utf16().chain(Some(0)).collect();
            let mut counter = null_mut();
            // SAFETY: The query is live, path is NUL-terminated for this call,
            // and counter is writable. The query owns the returned handle.
            check("PdhAddEnglishCounterW", &config.path, unsafe {
                PdhAddEnglishCounterW(query.handle, path.as_ptr(), 0, &mut counter)
            })?;
            let kind = inspect_counter(&config.path, counter)?;
            query.counters.push(CounterHandle {
                path: config.path.clone(),
                handle: counter,
                kind,
                scale_power10: config.scale_power10,
                previous_base: None,
            });
        }
        query.prime()?;
        Ok(query)
    }

    fn prime(&self) -> Result<(), Error> {
        // SAFETY: The query and counters remain on their owner thread.
        check("PdhCollectQueryData(prime)", "<query>", unsafe {
            PdhCollectQueryData(self.handle)
        })
    }

    fn collect(&mut self) -> Result<Sample, Error> {
        // SAFETY: The query and counters remain on their owner thread.
        check("PdhCollectQueryData", "<query>", unsafe {
            PdhCollectQueryData(self.handle)
        })?;
        let skip_calculated = std::mem::take(&mut self.first_collection_after_prime);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::InvalidSample("system clock precedes Unix epoch"))?
            .as_nanos();
        let timestamp_unix_nano = i64::try_from(timestamp)
            .map_err(|_| Error::InvalidSample("timestamp exceeds i64 nanoseconds"))?;

        // PDH advances the entire query at once. Refresh every valid receiver-side
        // base before formatting so one counter failure cannot leave later base
        // histories out of sync with PDH's previous sample.
        let mut dispositions = Vec::with_capacity(self.counters.len());
        for counter in &mut self.counters {
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
                        dispositions.push(Err(error));
                        continue;
                    }
                    if !matches!(raw.CStatus, PDH_CSTATUS_VALID_DATA | PDH_CSTATUS_NEW_DATA) {
                        if counter.kind == CounterKind::CalculatedTwoSampleWithBase {
                            counter.previous_base = None;
                        }
                        dispositions.push(Err(Error::Pdh {
                            operation: "raw counter CStatus",
                            path: counter.path.clone(),
                            status: raw.CStatus,
                        }));
                        continue;
                    }
                    Some(raw.SecondValue)
                }
                CounterKind::Direct | CounterKind::CalculatedTwoSample => None,
            };
            if skip_calculated
                && matches!(
                    counter.kind,
                    CounterKind::CalculatedTwoSample | CounterKind::CalculatedTwoSampleWithBase
                )
            {
                counter.previous_base = raw_base;
                dispositions.push(Ok(CollectionDisposition::Omit(SampleValue::Warming)));
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
            dispositions.push(disposition);
        }

        let mut values = Vec::with_capacity(self.counters.len());
        for (counter, disposition) in self.counters.iter().zip(dispositions) {
            match disposition? {
                CollectionDisposition::Format => {}
                CollectionDisposition::Omit(value) => {
                    values.push(value);
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
            check("PdhGetFormattedCounterValue", &counter.path, status)?;
            if !matches!(value.CStatus, PDH_CSTATUS_VALID_DATA | PDH_CSTATUS_NEW_DATA) {
                return Err(Error::Pdh {
                    operation: "counter CStatus",
                    path: counter.path.clone(),
                    status: value.CStatus,
                });
            }
            let scaled = match counter.kind {
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
            }
            .map_err(|message| Error::Calculation {
                path: counter.path.clone(),
                message,
            })?;
            values.push(SampleValue::Value(scaled));
        }
        Ok(Sample {
            timestamp_unix_nano,
            values,
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
                "winperfcounters.close_failed",
                status = status as u64
            );
        }
    }
}

enum Command {
    Collect(oneshot::Sender<Result<Sample, Error>>),
    Shutdown,
}

/// Capacity-one command client for the thread that owns the persistent query.
pub(super) struct Worker {
    tx: mpsc::SyncSender<Command>,
    shutdown_requested: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    completion: Option<oneshot::Receiver<()>>,
}

impl Worker {
    pub(super) fn start(counters: Vec<CounterConfig>, lease: Arc<Lease>) -> Result<Self, Error> {
        let (tx, rx) = mpsc::sync_channel(1);
        let (init_tx, init_rx) = mpsc::sync_channel(1);
        let (completion_tx, completion_rx) = oneshot::channel();
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown_requested);
        let join = std::thread::Builder::new()
            .name("winperfcounters-pdh".to_owned())
            .spawn(move || {
                {
                    let mut query = match Query::open(&counters) {
                        Ok(query) => query,
                        Err(error) => {
                            let _ = init_tx.send(Err(error));
                            return;
                        }
                    };
                    if init_tx.send(Ok(())).is_err() {
                        return;
                    }
                    while !worker_shutdown.load(Ordering::Acquire) {
                        match rx.recv() {
                            Ok(Command::Collect(response)) => {
                                let _ = response.send(query.collect());
                            }
                            Ok(Command::Shutdown) | Err(_) => break,
                        }
                    }
                }
                drop(lease);
                let _ = completion_tx.send(());
            })
            .map_err(|err| Error::WorkerStart(err.to_string()))?;

        match init_rx.recv_timeout(INIT_TIMEOUT) {
            Ok(Ok(())) => Ok(Self {
                tx,
                shutdown_requested,
                join: Some(join),
                completion: Some(completion_rx),
            }),
            Ok(Err(error)) => {
                let _ = join.join();
                Err(error)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                shutdown_requested.store(true, Ordering::Release);
                drop(join);
                Err(Error::WorkerInitTimeout)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = join.join();
                Err(Error::WorkerInitStopped)
            }
        }
    }

    pub(super) async fn collect(&self) -> Result<Sample, Error> {
        let (response_tx, response_rx) = oneshot::channel();
        match self.tx.try_send(Command::Collect(response_tx)) {
            Ok(()) => {}
            Err(mpsc::TrySendError::Full(_)) => return Err(Error::WorkerBusy),
            Err(mpsc::TrySendError::Disconnected(_)) => return Err(Error::WorkerStopped),
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
    use super::*;
    use crate::receivers::winperfcounters::Number;
    use windows_sys::Win32::System::Performance::{
        PDH_CSTATUS_INVALID_DATA, PDH_CSTATUS_NEW_DATA, PDH_FMT_COUNTERVALUE, PDH_RAW_COUNTER,
        PdhFormatFromRawValue,
    };

    fn counter(path: &str, name: &str) -> CounterConfig {
        CounterConfig {
            path: path.to_owned(),
            name: name.to_owned(),
            unit: "By".to_owned(),
            description: format!("Description for {name}."),
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
        match native_type {
            PERF_RAW_FRACTION | PERF_LARGE_RAW_FRACTION => {
                validate_positive_base("fixture", current.SecondValue)
                    .map_err(|error| error.to_string())?;
            }
            PERF_SAMPLE_FRACTION | PERF_AVERAGE_TIMER | PERF_AVERAGE_BULK => {
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
            _ => {}
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

    fn simulated_blocked_worker(
        lease: Arc<Lease>,
        cleanup_complete: Arc<AtomicBool>,
    ) -> (Worker, mpsc::Sender<()>) {
        let (tx, rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::channel();
        let (completion_tx, completion_rx) = oneshot::channel();
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let join = std::thread::spawn(move || {
            let _commands = rx;
            let _ = release_rx.recv();
            cleanup_complete.store(true, Ordering::Release);
            drop(lease);
            let _ = completion_tx.send(());
        });
        (
            Worker {
                tx,
                shutdown_requested,
                join: Some(join),
                completion: Some(completion_rx),
            },
            release_tx,
        )
    }

    /// Scenario: A persistent worker collects two supported counters repeatedly and shuts down.
    /// Guarantees: One query serves every scrape and normal shutdown closes it exactly once.
    #[tokio::test(flavor = "current_thread")]
    async fn persistent_worker_reuses_and_closes_query() {
        let _serial = super::super::TEST_LEASE_LOCK.lock().await;
        let before = query_counts();
        let counters = vec![
            counter(r"\Memory\Available Bytes", "windows.memory.available"),
            counter(r"\Memory\Committed Bytes", "windows.memory.committed"),
        ];
        let lease = Arc::new(Lease::acquire().unwrap());
        let mut worker = Worker::start(counters, lease).unwrap();
        for _ in 0..3 {
            let sample = worker.collect().await.unwrap();
            assert_eq!(sample.values.len(), 2);
            assert!(sample.values.iter().all(
                |value| matches!(value, SampleValue::Value(Number::Integer(value)) if *value >= 0)
            ));
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

    /// Scenario: The first post-prime scrape has direct, raw-fraction, and two-sample values.
    /// Guarantees: One-sample values emit immediately while only the two-sample timer is omitted.
    #[tokio::test(flavor = "current_thread")]
    async fn first_scrape_emits_one_sample_values_and_warms_two_sample() {
        let _serial = super::super::TEST_LEASE_LOCK.lock().await;
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
        let lease = Arc::new(Lease::acquire().unwrap());
        let mut worker = Worker::start(counters, lease).unwrap();
        let sample = worker.collect().await.unwrap();
        assert!(matches!(
            sample.values[0],
            SampleValue::Value(Number::Integer(_))
        ));
        assert!(
            matches!(sample.values[1], SampleValue::Value(Number::Double(value)) if value.is_finite())
        );
        assert_eq!(sample.values[2], SampleValue::Warming);
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
    }

    /// Scenario: Initialization adds a valid path followed by an invalid exact path.
    /// Guarantees: Startup reports the failing path and closes the partially built query.
    #[tokio::test(flavor = "current_thread")]
    async fn initialization_error_closes_query() {
        let _serial = super::super::TEST_LEASE_LOCK.lock().await;
        let before = query_counts();
        let counters = vec![
            counter(r"\Memory\Available Bytes", "windows.memory.available"),
            counter(
                r"\Missing Object\Missing Counter",
                "windows.missing.counter",
            ),
        ];
        let lease = Arc::new(Lease::acquire().unwrap());
        let error = match Worker::start(counters, lease) {
            Ok(_) => panic!("invalid path must fail worker initialization"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains(r"\Missing Object\Missing Counter"),
            "unexpected initialization error: {error}"
        );
        assert!(error.contains("PdhAddEnglishCounterW"));
        let after = query_counts();
        assert_eq!(after.0 - before.0, 1);
        assert_eq!(after.1 - before.1, 1);
    }

    /// Scenario: Native metadata describes every Part A direct, rate, and timer family.
    /// Guarantees: Each verified SDK type selects its required one- or two-sample format.
    #[test]
    fn classifies_part_a_native_types() {
        assert_eq!(PERF_COUNTER_RAWCOUNT, 0x0001_0000);
        assert_eq!(PERF_COUNTER_LARGE_RAWCOUNT, 0x0001_0100);
        assert_eq!(PERF_COUNTER_RAWCOUNT_HEX, 0x0000_0000);
        assert_eq!(PERF_COUNTER_LARGE_RAWCOUNT_HEX, 0x0000_0100);
        assert_eq!(PERF_COUNTER_COUNTER, 0x1041_0400);
        assert_eq!(PERF_COUNTER_BULK_COUNT, 0x1041_0500);
        assert_eq!(PERF_COUNTER_TIMER, 0x2041_0500);
        assert_eq!(PERF_COUNTER_TIMER_INV, 0x2141_0500);
        assert_eq!(PERF_100NSEC_TIMER, 0x2051_0500);
        assert_eq!(PERF_100NSEC_TIMER_INV, 0x2151_0500);
        for native_type in [
            PERF_COUNTER_RAWCOUNT,
            PERF_COUNTER_LARGE_RAWCOUNT,
            PERF_COUNTER_RAWCOUNT_HEX,
            PERF_COUNTER_LARGE_RAWCOUNT_HEX,
        ] {
            assert_eq!(
                classify_native_type("direct", native_type, -6).unwrap(),
                CounterKind::Direct
            );
        }
        for native_type in [
            PERF_COUNTER_COUNTER,
            PERF_COUNTER_BULK_COUNT,
            PERF_COUNTER_TIMER,
            PERF_COUNTER_TIMER_INV,
            PERF_100NSEC_TIMER,
            PERF_100NSEC_TIMER_INV,
        ] {
            assert_eq!(
                classify_native_type("calculated", native_type, 0).unwrap(),
                CounterKind::CalculatedTwoSample
            );
        }
    }

    /// Scenario: Native metadata describes every supported fraction and average numerator.
    /// Guarantees: Raw fractions are one-sample values and delta/base formulas warm for one scrape.
    #[test]
    fn classifies_part_b_native_types() {
        assert_eq!(PERF_RAW_FRACTION, 0x2002_0400);
        assert_eq!(PERF_LARGE_RAW_FRACTION, 0x2002_0500);
        assert_eq!(PERF_SAMPLE_FRACTION, 0x20C2_0400);
        assert_eq!(PERF_AVERAGE_TIMER, 0x3002_0400);
        assert_eq!(PERF_AVERAGE_BULK, 0x4002_0500);
        for native_type in [PERF_RAW_FRACTION, PERF_LARGE_RAW_FRACTION] {
            assert_eq!(
                classify_native_type("raw fraction", native_type, 0).unwrap(),
                CounterKind::RawFraction
            );
        }
        for native_type in [PERF_SAMPLE_FRACTION, PERF_AVERAGE_TIMER, PERF_AVERAGE_BULK] {
            assert_eq!(
                classify_native_type("delta/base", native_type, 0).unwrap(),
                CounterKind::CalculatedTwoSampleWithBase
            );
        }
    }

    /// Scenario: Provider base metadata is configured as a visible counter path.
    /// Guarantees: Non-printing base types remain rejected instead of becoming zero-like gauges.
    #[test]
    fn rejects_standalone_base_types() {
        for native_type in [0x4003_0401, 0x4003_0402, 0x4003_0403, 0x4003_0500] {
            let error = classify_native_type(r"\Object\Counter Base", native_type, 0)
                .unwrap_err()
                .to_string();
            assert!(error.contains(r"\Object\Counter Base"));
            assert!(error.contains(&format!("0x{native_type:08X}")));
        }
    }

    /// Scenario: PDH formats deterministic raw fraction, sample fraction, and average fixtures.
    /// Guarantees: Every advertised Part B formula produces the documented finite value.
    #[test]
    fn formats_part_b_raw_fixtures() {
        for native_type in [PERF_RAW_FRACTION, PERF_LARGE_RAW_FRACTION] {
            let value = format_raw_fixture(native_type, &raw_value(25, 100), None, 1).unwrap();
            assert!((value - 25.0).abs() < f64::EPSILON);
        }

        let previous = raw_value(100, 200);
        let current = raw_value(130, 250);
        let value = format_raw_fixture(PERF_SAMPLE_FRACTION, &current, Some(&previous), 1).unwrap();
        assert!((value - 60.0).abs() < f64::EPSILON);

        let previous = raw_value(1_000, 10);
        let current = raw_value(3_000, 20);
        let value =
            format_raw_fixture(PERF_AVERAGE_TIMER, &current, Some(&previous), 1_000).unwrap();
        assert!((value - 0.2).abs() < 1e-12);

        let previous = raw_value(1_000, 10);
        let current = raw_value(4_000, 20);
        let value = format_raw_fixture(PERF_AVERAGE_BULK, &current, Some(&previous), 1).unwrap();
        assert!((value - 300.0).abs() < f64::EPSILON);
    }

    /// Scenario: A fraction or average fixture has a zero, decreasing, or invalid denominator.
    /// Guarantees: Invalid base data is rejected and no zero-filled formatted value is accepted.
    #[test]
    fn rejects_invalid_part_b_denominators() {
        assert!(format_raw_fixture(PERF_RAW_FRACTION, &raw_value(25, 0), None, 1).is_err());
        assert!(
            format_raw_fixture(
                PERF_SAMPLE_FRACTION,
                &raw_value(130, 150),
                Some(&raw_value(100, 200)),
                1,
            )
            .is_err()
        );
        assert!(
            format_raw_fixture(
                PERF_AVERAGE_TIMER,
                &raw_value(3_000, 5),
                Some(&raw_value(1_000, 10)),
                1_000,
            )
            .is_err()
        );
        assert!(
            format_raw_fixture(
                PERF_AVERAGE_BULK,
                &raw_value(4_000, 5),
                Some(&raw_value(1_000, 10)),
                1,
            )
            .is_err()
        );

        let mut invalid = raw_value(25, 100);
        invalid.CStatus = PDH_CSTATUS_INVALID_DATA;
        assert!(format_raw_fixture(PERF_RAW_FRACTION, &invalid, None, 1).is_err());
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

    /// Scenario: A simulated PDH call remains blocked past the shutdown deadline.
    /// Guarantees: Shutdown returns at the deadline, cleanup occurs later on the worker,
    /// and the singleton lease remains held until that cleanup completes.
    #[tokio::test(flavor = "current_thread")]
    async fn blocked_worker_releases_resources_after_timeout() {
        let _serial = super::super::TEST_LEASE_LOCK.lock().await;
        let cleanup_complete = Arc::new(AtomicBool::new(false));
        let lease = Arc::new(Lease::acquire().unwrap());
        let (mut worker, release) = simulated_blocked_worker(lease, Arc::clone(&cleanup_complete));

        let start = Instant::now();
        assert!(
            !worker
                .shutdown(start + Duration::from_millis(25))
                .await
                .unwrap()
        );
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(!cleanup_complete.load(Ordering::Acquire));
        assert!(Lease::acquire().is_err());

        release.send(()).unwrap();
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
        assert!(cleanup_complete.load(Ordering::Acquire));
        let next = Lease::acquire().unwrap();
        drop(next);
    }
}
