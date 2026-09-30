// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Persistent synchronous PDH worker. Windows handles never leave its thread.
#![allow(unsafe_code)]

use super::config::CounterConfig;
use super::model::{Sample, SampleFailure, SamplePoint, SampleValue, scale_double, scale_integer};
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
    PERF_DISPLAY_NO_SUFFIX, PERF_DISPLAY_PERCENT, PERF_NUMBER_DECIMAL, PERF_NUMBER_HEX,
    PERF_SIZE_DWORD, PERF_SIZE_LARGE, PERF_TYPE_COUNTER, PERF_TYPE_NUMBER, PdhAddEnglishCounterW,
    PdhCloseQuery, PdhCollectQueryData, PdhGetCounterInfoW, PdhGetFormattedCounterValue,
    PdhGetRawCounterValue, PdhOpenQueryW,
};

const INIT_TIMEOUT: Duration = Duration::from_secs(30);
const PDH_FMT_NOSCALE: u32 = 0x0000_1000;
const PDH_FMT_NOCAP100: u32 = 0x0000_8000;
const PERF_COUNTER_RATE: u32 = 0x0001_0000;
const PERF_COUNTER_FRACTION: u32 = 0x0002_0000;
const PERF_COUNTER_QUEUELEN: u32 = 0x0005_0000;
const PERF_COUNTER_PRECISION: u32 = 0x0007_0000;
const PERF_TIMER_100NS: u32 = 0x0010_0000;
const PERF_OBJECT_TIMER: u32 = 0x0020_0000;
const PERF_DELTA_COUNTER: u32 = 0x0040_0000;
const PERF_DELTA_BASE: u32 = 0x0080_0000;
const PERF_INVERSE_COUNTER: u32 = 0x0100_0000;
const PERF_DISPLAY_PER_SEC: u32 = 0x1000_0000;
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
const PERF_COUNTER_QUEUELEN_TYPE: u32 =
    PERF_SIZE_DWORD | PERF_TYPE_COUNTER | PERF_COUNTER_QUEUELEN | PERF_DELTA_COUNTER;
const PERF_COUNTER_LARGE_QUEUELEN_TYPE: u32 =
    PERF_SIZE_LARGE | PERF_TYPE_COUNTER | PERF_COUNTER_QUEUELEN | PERF_DELTA_COUNTER;
const PERF_COUNTER_100NS_QUEUELEN_TYPE: u32 = PERF_SIZE_LARGE
    | PERF_TYPE_COUNTER
    | PERF_COUNTER_QUEUELEN
    | PERF_TIMER_100NS
    | PERF_DELTA_COUNTER;
const PERF_COUNTER_OBJ_TIME_QUEUELEN_TYPE: u32 = PERF_SIZE_LARGE
    | PERF_TYPE_COUNTER
    | PERF_COUNTER_QUEUELEN
    | PERF_OBJECT_TIMER
    | PERF_DELTA_COUNTER;
const PERF_COUNTER_TIMER: u32 = PERF_SIZE_LARGE
    | PERF_TYPE_COUNTER
    | PERF_COUNTER_RATE
    | PERF_DELTA_COUNTER
    | PERF_DISPLAY_PERCENT;
const PERF_COUNTER_TIMER_INV: u32 = PERF_COUNTER_TIMER | PERF_INVERSE_COUNTER;
const PERF_100NSEC_TIMER: u32 = PERF_COUNTER_TIMER | PERF_TIMER_100NS;
const PERF_100NSEC_TIMER_INV: u32 = PERF_100NSEC_TIMER | PERF_INVERSE_COUNTER;
const PERF_OBJ_TIME_TIMER: u32 = PERF_COUNTER_TIMER | PERF_OBJECT_TIMER;
const PERF_PRECISION_SYSTEM_TIMER: u32 = PERF_SIZE_LARGE
    | PERF_TYPE_COUNTER
    | PERF_COUNTER_PRECISION
    | PERF_DELTA_COUNTER
    | PERF_DISPLAY_PERCENT;
const PERF_PRECISION_100NS_TIMER: u32 = PERF_PRECISION_SYSTEM_TIMER | PERF_TIMER_100NS;
const PERF_PRECISION_OBJECT_TIMER: u32 = PERF_PRECISION_SYSTEM_TIMER | PERF_OBJECT_TIMER;
const PERF_COUNTER_DELTA: u32 =
    PERF_SIZE_DWORD | PERF_TYPE_COUNTER | PERF_DELTA_COUNTER | PERF_DISPLAY_NO_SUFFIX;
const PERF_COUNTER_LARGE_DELTA: u32 =
    PERF_SIZE_LARGE | PERF_TYPE_COUNTER | PERF_DELTA_COUNTER | PERF_DISPLAY_NO_SUFFIX;
const PERF_SAMPLE_COUNTER: u32 = PERF_SIZE_DWORD
    | PERF_TYPE_COUNTER
    | PERF_COUNTER_RATE
    | PERF_DELTA_COUNTER
    | PERF_DISPLAY_NO_SUFFIX;
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

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("{operation} for {path} failed with PDH status 0x{status:08X}")]
    Pdh {
        operation: &'static str,
        path: String,
        status: u32,
    },
    #[error("unsupported native counter type 0x{native_type:08X} for {path}")]
    UnsupportedType { path: String, native_type: u32 },
    #[error("invalid performance-counter sample: {0}")]
    InvalidSample(&'static str),
    #[error("counter calculation for {path} failed: {message}")]
    Calculation { path: String, message: String },
    #[error("failed to start PDH worker thread: {0}")]
    WorkerStart(String),
    #[error("PDH worker initialization timed out after {seconds} seconds")]
    WorkerInitTimeout { seconds: u64 },
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
            Self::Pdh { .. } | Self::InvalidSample(_) | Self::Calculation { .. } | Self::WorkerBusy
        )
    }

    pub(super) fn is_overrun(&self) -> bool {
        matches!(self, Self::WorkerBusy)
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

fn classify_native_type(path: &str, native_type: u32) -> Result<CounterKind, Error> {
    match native_type {
        PERF_COUNTER_RAWCOUNT
        | PERF_COUNTER_LARGE_RAWCOUNT
        | PERF_COUNTER_RAWCOUNT_HEX
        | PERF_COUNTER_LARGE_RAWCOUNT_HEX => Ok(CounterKind::Direct),
        PERF_COUNTER_COUNTER
        | PERF_COUNTER_BULK_COUNT
        | PERF_COUNTER_QUEUELEN_TYPE
        | PERF_COUNTER_LARGE_QUEUELEN_TYPE
        | PERF_COUNTER_100NS_QUEUELEN_TYPE
        | PERF_COUNTER_OBJ_TIME_QUEUELEN_TYPE
        | PERF_COUNTER_TIMER
        | PERF_COUNTER_TIMER_INV
        | PERF_100NSEC_TIMER
        | PERF_100NSEC_TIMER_INV
        | PERF_OBJ_TIME_TIMER
        | PERF_PRECISION_SYSTEM_TIMER
        | PERF_PRECISION_100NS_TIMER
        | PERF_PRECISION_OBJECT_TIMER
        | PERF_COUNTER_DELTA
        | PERF_COUNTER_LARGE_DELTA
        | PERF_SAMPLE_COUNTER => Ok(CounterKind::CalculatedTwoSample),
        PERF_SAMPLE_FRACTION | PERF_AVERAGE_TIMER | PERF_AVERAGE_BULK => {
            Ok(CounterKind::CalculatedTwoSampleWithBase)
        }
        PERF_RAW_FRACTION | PERF_LARGE_RAW_FRACTION => Ok(CounterKind::RawFraction),
        _ => Err(Error::UnsupportedType {
            path: path.to_owned(),
            native_type,
        }),
    }
}

fn counter_info(path: &str, counter: PDH_HCOUNTER) -> Result<Vec<usize>, Error> {
    let mut size = 0;
    // SAFETY: The counter is live. A null buffer requests the required size.
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
    let mut buffer = vec![0usize; (size as usize).div_ceil(size_of::<usize>())];
    let info = buffer.as_mut_ptr().cast::<PDH_COUNTER_INFO_W>();
    // SAFETY: The usize buffer is aligned and at least `size` bytes.
    check("PdhGetCounterInfoW", path, unsafe {
        PdhGetCounterInfoW(counter, false, &mut size, info)
    })?;
    Ok(buffer)
}

fn inspect_counter(path: &str, counter: PDH_HCOUNTER) -> Result<CounterKind, Error> {
    let buffer = counter_info(path, counter)?;
    let info = buffer.as_ptr().cast::<PDH_COUNTER_INFO_W>();
    // SAFETY: The successful metadata call initialized the fixed header.
    classify_native_type(path, unsafe { (*info).dwType })
}

fn unix_timestamp_nanos() -> Result<i64, Error> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::InvalidSample("system clock precedes Unix epoch"))?
        .as_nanos();
    i64::try_from(timestamp).map_err(|_| Error::InvalidSample("timestamp exceeds i64 nanoseconds"))
}

fn advance_sequence_start(start: &mut i64, previous: &mut i64, current: i64) {
    if current < *previous {
        *start = current;
    }
    *previous = current;
}

struct CounterHandle {
    config_index: usize,
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
    node: String,
    start_time_unix_nano: i64,
    previous_timestamp_unix_nano: i64,
}

impl Query {
    fn open(configs: Vec<CounterConfig>, node: String) -> Result<Self, Error> {
        let start_time_unix_nano = unix_timestamp_nanos()?;
        let mut handle = null_mut();
        // SAFETY: Null selects the live local data source; handle is writable.
        check("PdhOpenQueryW", "<query>", unsafe {
            PdhOpenQueryW(null(), 0, &mut handle)
        })?;
        let mut query = Self {
            handle,
            counters: Vec::with_capacity(configs.len()),
            node,
            start_time_unix_nano,
            previous_timestamp_unix_nano: start_time_unix_nano,
        };
        for (config_index, config) in configs.into_iter().enumerate() {
            let wide_path = config
                .path
                .encode_utf16()
                .chain(Some(0))
                .collect::<Vec<_>>();
            let mut counter = null_mut();
            // SAFETY: The query is live, path is NUL-terminated, and output is writable.
            check("PdhAddEnglishCounterW", &config.path, unsafe {
                PdhAddEnglishCounterW(query.handle, wide_path.as_ptr(), 0, &mut counter)
            })?;
            let kind = inspect_counter(&config.path, counter)?;
            query.counters.push(CounterHandle {
                config_index,
                path: config.path,
                handle: counter,
                kind,
                scale_power10: config.scale_power10,
                previous_base: None,
            });
        }
        query.prime()?;
        Ok(query)
    }

    fn prime(&mut self) -> Result<(), Error> {
        // SAFETY: The query and counters remain on their owner thread.
        check("PdhCollectQueryData(prime)", "<query>", unsafe {
            PdhCollectQueryData(self.handle)
        })?;
        prime_counter_bases(&mut self.counters, read_raw_base);
        Ok(())
    }

    fn collect(&mut self) -> Result<Sample, Error> {
        // SAFETY: The query and counters remain on their owner thread.
        check("PdhCollectQueryData", "<query>", unsafe {
            PdhCollectQueryData(self.handle)
        })?;
        let timestamp_unix_nano = unix_timestamp_nanos()?;
        advance_sequence_start(
            &mut self.start_time_unix_nano,
            &mut self.previous_timestamp_unix_nano,
            timestamp_unix_nano,
        );

        let (points, failures) = collect_counter_points(&mut self.counters, collect_counter);
        Ok(Sample {
            start_time_unix_nano: self.start_time_unix_nano,
            timestamp_unix_nano,
            points,
            failures,
        })
    }
}

fn prime_counter_bases(
    counters: &mut [CounterHandle],
    mut read: impl FnMut(&CounterHandle) -> Result<i64, Error>,
) {
    for counter in counters {
        if matches!(counter.kind, CounterKind::CalculatedTwoSampleWithBase) {
            counter.previous_base = read(counter).ok();
        }
    }
}

fn collect_counter_points(
    counters: &mut [CounterHandle],
    mut read: impl FnMut(&mut CounterHandle) -> Result<SampleValue, Error>,
) -> (Vec<SamplePoint>, Vec<SampleFailure>) {
    let mut points = Vec::with_capacity(counters.len());
    let mut failures = Vec::new();
    for counter in counters {
        match read(counter) {
            Ok(value) => points.push(SamplePoint {
                counter_index: counter.config_index,
                value,
            }),
            Err(error) => failures.push(SampleFailure {
                counter_index: counter.config_index,
                error: error.to_string(),
            }),
        }
    }
    (points, failures)
}

impl Drop for Query {
    fn drop(&mut self) {
        // SAFETY: This handle has one owner and all calls occur on this thread.
        let status = unsafe { PdhCloseQuery(self.handle) };
        if status != 0 {
            otel_warn!(
                "otelcol.node.windowsperfcounters.close.fail",
                node = self.node.as_str(),
                status = status as u64
            );
        }
    }
}

fn read_raw_base(counter: &CounterHandle) -> Result<i64, Error> {
    let mut raw = PDH_RAW_COUNTER::default();
    // SAFETY: The counter is live and raw is writable.
    check("PdhGetRawCounterValue", &counter.path, unsafe {
        PdhGetRawCounterValue(counter.handle, null_mut(), &mut raw)
    })?;
    if !matches!(raw.CStatus, PDH_CSTATUS_VALID_DATA | PDH_CSTATUS_NEW_DATA) {
        return Err(Error::Pdh {
            operation: "raw counter CStatus",
            path: counter.path.clone(),
            status: raw.CStatus,
        });
    }
    Ok(raw.SecondValue)
}

fn collect_counter(counter: &mut CounterHandle) -> Result<SampleValue, Error> {
    match counter.kind {
        CounterKind::RawFraction => {
            let base = read_raw_base(counter)?;
            if base <= 0 {
                return Err(Error::Calculation {
                    path: counter.path.clone(),
                    message: format!("base denominator must be positive, got {base}"),
                });
            }
        }
        CounterKind::CalculatedTwoSampleWithBase => {
            let current = read_raw_base(counter)?;
            let previous =
                counter
                    .previous_base
                    .replace(current)
                    .ok_or_else(|| Error::Calculation {
                        path: counter.path.clone(),
                        message: "a previous base sample is required".to_owned(),
                    })?;
            match current.cmp(&previous) {
                std::cmp::Ordering::Less => {
                    return Err(Error::Calculation {
                        path: counter.path.clone(),
                        message: format!(
                            "base denominator decreased between samples, got {previous} then {current}"
                        ),
                    });
                }
                std::cmp::Ordering::Equal => return Ok(SampleValue::NoObservation),
                std::cmp::Ordering::Greater => {}
            }
        }
        CounterKind::Direct | CounterKind::CalculatedTwoSample => {}
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
    // SAFETY: The counter is live and the output union matches the requested format.
    check("PdhGetFormattedCounterValue", &counter.path, unsafe {
        PdhGetFormattedCounterValue(counter.handle, format, null_mut(), &mut value)
    })?;
    if !matches!(value.CStatus, PDH_CSTATUS_VALID_DATA | PDH_CSTATUS_NEW_DATA) {
        return Err(Error::Pdh {
            operation: "counter CStatus",
            path: counter.path.clone(),
            status: value.CStatus,
        });
    }
    let number = match counter.kind {
        CounterKind::Direct => {
            // SAFETY: PDH_FMT_LARGE initialized largeValue.
            scale_integer(unsafe { value.Anonymous.largeValue }, counter.scale_power10)
        }
        CounterKind::RawFraction
        | CounterKind::CalculatedTwoSample
        | CounterKind::CalculatedTwoSampleWithBase => {
            // SAFETY: PDH_FMT_DOUBLE initialized doubleValue.
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
    Ok(SampleValue::Value(number))
}

/// Capacity-one command client for the thread that owns the persistent query.
pub(super) struct Worker {
    tx: mpsc::SyncSender<Command>,
    accepted: Arc<AtomicBool>,
    // Reaches the worker even when the capacity-one channel already contains a collect request.
    shutdown_requested: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    completion: Option<oneshot::Receiver<()>>,
}

impl Worker {
    pub(super) async fn start(counters: Vec<CounterConfig>, node: String) -> Result<Self, Error> {
        let (tx, rx) = mpsc::sync_channel(1);
        let (init_tx, init_rx) = oneshot::channel();
        let (completion_tx, completion_rx) = oneshot::channel();
        let accepted = Arc::new(AtomicBool::new(false));
        let worker_accepted = Arc::clone(&accepted);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown_requested);
        // PDH calls are synchronous and its handles stay on one thread. A dedicated
        // worker avoids blocking the pipeline runtime or Tokio's shared blocking pool,
        // while the capacity-one channel bounds cross-thread work.
        let join = std::thread::Builder::new()
            .name("windowsperfcounters-pdh".to_owned())
            .spawn(move || {
                let mut query = match Query::open(counters, node) {
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
                            let _accepted = AcceptedCollectGuard::new(Arc::clone(&worker_accepted));
                            let _ = response.send(query.collect());
                        }
                        Ok(Command::Shutdown) | Err(_) => break,
                    }
                }
                drop(query);
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
                Err(Error::WorkerInitTimeout {
                    seconds: INIT_TIMEOUT.as_secs(),
                })
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
        let _ = self.tx.try_send(Command::Shutdown);
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
    use std::collections::BTreeMap;

    fn empty_sample() -> Sample {
        Sample {
            start_time_unix_nano: 1,
            timestamp_unix_nano: 2,
            points: Vec::new(),
            failures: Vec::new(),
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
                        if let Some((started, release, released)) = first.take() {
                            let accepted = AcceptedCollectGuard::new(Arc::clone(&worker_accepted));
                            let _ = started.send(());
                            let _ = release.blocking_recv();
                            let _ = response.send(Ok(empty_sample()));
                            drop(accepted);
                            let _ = released.send(());
                        } else {
                            let _accepted = AcceptedCollectGuard::new(Arc::clone(&worker_accepted));
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

    fn counter(path: &str, name: &str) -> CounterConfig {
        CounterConfig {
            path: path.to_owned(),
            name: name.to_owned(),
            unit: "1".to_owned(),
            description: format!("Description for {name}."),
            kind: MetricKind::Gauge,
            attributes: BTreeMap::new(),
            scale_power10: 0,
        }
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

    /// Scenario: Native metadata describes supported direct, rate, timer, fraction, and average types.
    /// Guarantees: Each advertised PDH family selects the required formatting strategy.
    #[test]
    fn classifies_supported_native_types() {
        for native_type in [
            PERF_COUNTER_RAWCOUNT,
            PERF_COUNTER_LARGE_RAWCOUNT,
            PERF_COUNTER_RAWCOUNT_HEX,
            PERF_COUNTER_LARGE_RAWCOUNT_HEX,
        ] {
            assert_eq!(
                classify_native_type("direct", native_type).unwrap(),
                CounterKind::Direct
            );
        }
        for native_type in [
            PERF_COUNTER_COUNTER,
            PERF_COUNTER_BULK_COUNT,
            PERF_COUNTER_QUEUELEN_TYPE,
            PERF_COUNTER_LARGE_QUEUELEN_TYPE,
            PERF_COUNTER_100NS_QUEUELEN_TYPE,
            PERF_COUNTER_OBJ_TIME_QUEUELEN_TYPE,
            PERF_COUNTER_TIMER,
            PERF_COUNTER_TIMER_INV,
            PERF_100NSEC_TIMER,
            PERF_100NSEC_TIMER_INV,
            PERF_OBJ_TIME_TIMER,
            PERF_PRECISION_SYSTEM_TIMER,
            PERF_PRECISION_100NS_TIMER,
            PERF_PRECISION_OBJECT_TIMER,
            PERF_COUNTER_DELTA,
            PERF_COUNTER_LARGE_DELTA,
            PERF_SAMPLE_COUNTER,
        ] {
            assert_eq!(
                classify_native_type("calculated", native_type).unwrap(),
                CounterKind::CalculatedTwoSample
            );
        }
        for native_type in [PERF_RAW_FRACTION, PERF_LARGE_RAW_FRACTION] {
            assert_eq!(
                classify_native_type("fraction", native_type).unwrap(),
                CounterKind::RawFraction
            );
        }
        for native_type in [PERF_SAMPLE_FRACTION, PERF_AVERAGE_TIMER, PERF_AVERAGE_BULK] {
            assert_eq!(
                classify_native_type("average", native_type).unwrap(),
                CounterKind::CalculatedTwoSampleWithBase
            );
        }
    }

    /// Scenario: A standalone base counter type is configured.
    /// Guarantees: Non-printing native types fail rather than emitting a misleading value.
    #[test]
    fn rejects_unsupported_native_type() {
        assert!(classify_native_type("base", 0x4003_0401).is_err());
    }

    /// Scenario: The wall clock advances normally and then moves backward.
    /// Guarantees: Only rollback starts a new cumulative metric sequence.
    #[test]
    fn clock_rollback_resets_sequence_start() {
        let mut start = 100;
        let mut previous = 110;
        advance_sequence_start(&mut start, &mut previous, 120);
        assert_eq!((start, previous), (100, 120));
        advance_sequence_start(&mut start, &mut previous, 90);
        assert_eq!((start, previous), (90, 90));
    }

    /// Scenario: An average counter has no raw base during query priming.
    /// Guarantees: Priming preserves the counter for counter-local recovery on a later scrape.
    #[test]
    fn tolerates_missing_average_base_during_priming() {
        let mut counters = [CounterHandle {
            config_index: 0,
            path: r"\PhysicalDisk(missing)\Avg. Disk sec/Read".to_owned(),
            handle: null_mut(),
            kind: CounterKind::CalculatedTwoSampleWithBase,
            scale_power10: 0,
            previous_base: None,
        }];
        prime_counter_bases(&mut counters, |counter| {
            Err(Error::Calculation {
                path: counter.path.clone(),
                message: "injected missing base".to_owned(),
            })
        });
        assert_eq!(counters[0].previous_base, None);
    }

    /// Scenario: One exact counter fails during a scrape while a healthy peer remains readable.
    /// Guarantees: The failed point is isolated, the healthy point emits, and both emit after recovery.
    #[test]
    fn isolates_counter_failure_and_allows_recovery() {
        let mut counters = [
            CounterHandle {
                config_index: 0,
                path: r"\Object\Unstable".to_owned(),
                handle: null_mut(),
                kind: CounterKind::Direct,
                scale_power10: 0,
                previous_base: None,
            },
            CounterHandle {
                config_index: 1,
                path: r"\Object\Healthy".to_owned(),
                handle: null_mut(),
                kind: CounterKind::Direct,
                scale_power10: 0,
                previous_base: None,
            },
        ];
        let (points, failures) = collect_counter_points(&mut counters, |counter| {
            if counter.config_index == 0 {
                Err(Error::Calculation {
                    path: counter.path.clone(),
                    message: "injected failure".to_owned(),
                })
            } else {
                Ok(SampleValue::Value(Number::Integer(7)))
            }
        });
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].counter_index, 1);
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].counter_index, 0);

        let (points, failures) = collect_counter_points(&mut counters, |_| {
            Ok(SampleValue::Value(Number::Integer(9)))
        });
        assert_eq!(
            points
                .iter()
                .map(|point| point.counter_index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(failures.is_empty());
    }

    /// Scenario: Live Windows PDH collects an exact direct counter through the persistent worker.
    /// Guarantees: The worker emits a positive timestamp and exact integer point, then shuts down.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    async fn collects_exact_counter_from_live_pdh() {
        let mut worker = Worker::start(
            vec![counter(
                r"\Memory\Available Bytes",
                "windows.memory.available",
            )],
            "test".to_owned(),
        )
        .await
        .unwrap();
        let sample = worker.collect().await.unwrap();
        assert!(sample.timestamp_unix_nano > 0);
        assert!(matches!(
            sample.points.as_slice(),
            [SamplePoint {
                value: SampleValue::Value(Number::Integer(value)),
                ..
            }] if *value >= 0
        ));
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
    }

    /// Scenario: A configured average counter instance is absent while a direct counter is healthy.
    /// Guarantees: The missing base does not block startup and only that counter fails collection.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    async fn isolates_missing_average_instance_from_live_pdh() {
        let mut worker = Worker::start(
            vec![
                counter(r"\Memory\Available Bytes", "windows.memory.available"),
                counter(
                    r"\PhysicalDisk(otel-arrow-missing-instance)\Avg. Disk sec/Read",
                    "windows.disk.read_latency",
                ),
            ],
            "test".to_owned(),
        )
        .await
        .unwrap();
        let sample = worker.collect().await.unwrap();
        assert_eq!(sample.points.len(), 1);
        assert_eq!(sample.points[0].counter_index, 0);
        assert_eq!(sample.failures.len(), 1);
        assert_eq!(sample.failures[0].counter_index, 1);
        assert!(
            worker
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
                .unwrap()
        );
    }

    /// Scenario: Live Windows PDH formats common disk queue-length and precision-timer counters.
    /// Guarantees: Both newly supported two-sample families produce finite values from one query.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    async fn collects_common_calculated_disk_counters_from_live_pdh() {
        let mut worker = Worker::start(
            vec![
                counter(
                    r"\PhysicalDisk(_Total)\Avg. Disk Queue Length",
                    "windows.disk.queue_length",
                ),
                counter(r"\PhysicalDisk(_Total)\% Disk Time", "windows.disk.time"),
            ],
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
}
