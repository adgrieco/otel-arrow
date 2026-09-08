// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Persistent synchronous PDH worker. Windows handles never leave its thread.
#![allow(unsafe_code)]

use super::Lease;
use crate::receivers::winperfcounters::{CounterConfig, Sample};
use std::mem::size_of;
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use windows_sys::Win32::System::Performance::{
    PDH_COUNTER_INFO_W, PDH_CSTATUS_NEW_DATA, PDH_CSTATUS_VALID_DATA, PDH_FMT_COUNTERVALUE,
    PDH_FMT_LARGE, PDH_HCOUNTER, PDH_HQUERY, PDH_MORE_DATA, PERF_DISPLAY_NO_SUFFIX,
    PERF_NUMBER_DECIMAL, PERF_SIZE_DWORD, PERF_SIZE_LARGE, PERF_TYPE_NUMBER, PdhAddEnglishCounterW,
    PdhCloseQuery, PdhCollectQueryData, PdhGetCounterInfoW, PdhGetFormattedCounterValue,
    PdhOpenQueryW,
};

const INIT_TIMEOUT: Duration = Duration::from_secs(30);
const PDH_FMT_NOSCALE: u32 = 0x0000_1000;
const PERF_COUNTER_RAWCOUNT: u32 =
    PERF_SIZE_DWORD | PERF_TYPE_NUMBER | PERF_NUMBER_DECIMAL | PERF_DISPLAY_NO_SUFFIX;
const PERF_COUNTER_LARGE_RAWCOUNT: u32 =
    PERF_SIZE_LARGE | PERF_TYPE_NUMBER | PERF_NUMBER_DECIMAL | PERF_DISPLAY_NO_SUFFIX;

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
         milestone 1 supports PERF_COUNTER_RAWCOUNT and PERF_COUNTER_LARGE_RAWCOUNT only"
    )]
    UnsupportedType { path: String, native_type: u32 },
    #[error("invalid performance-counter sample: {0}")]
    InvalidSample(&'static str),
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
        matches!(self, Self::Pdh { .. } | Self::InvalidSample(_))
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

fn validate_native_type(path: &str, native_type: u32, scale: i32) -> Result<(), Error> {
    if !matches!(
        native_type,
        PERF_COUNTER_RAWCOUNT | PERF_COUNTER_LARGE_RAWCOUNT
    ) {
        return Err(Error::UnsupportedType {
            path: path.to_owned(),
            native_type,
        });
    }
    // The default scale is a display hint. Milestone 1 always requests
    // PDH_FMT_NOSCALE, so the configured unit describes the unscaled integer.
    let _ = scale;
    Ok(())
}

fn inspect_counter(path: &str, counter: PDH_HCOUNTER) -> Result<(), Error> {
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
    validate_native_type(path, native_type, scale)
}

struct CounterHandle {
    path: String,
    handle: PDH_HCOUNTER,
}

/// The query owns every added counter; closing it releases all handles.
struct Query {
    handle: PDH_HQUERY,
    counters: Vec<CounterHandle>,
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
            inspect_counter(&config.path, counter)?;
            query.counters.push(CounterHandle {
                path: config.path.clone(),
                handle: counter,
            });
        }
        Ok(query)
    }

    fn collect(&self) -> Result<Sample, Error> {
        // SAFETY: The query and counters remain on their owner thread.
        check("PdhCollectQueryData", "<query>", unsafe {
            PdhCollectQueryData(self.handle)
        })?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::InvalidSample("system clock precedes Unix epoch"))?
            .as_nanos();
        let timestamp_unix_nano = i64::try_from(timestamp)
            .map_err(|_| Error::InvalidSample("timestamp exceeds i64 nanoseconds"))?;
        let mut values = Vec::with_capacity(self.counters.len());
        for counter in &self.counters {
            let mut value = PDH_FMT_COUNTERVALUE::default();
            // SAFETY: The counter belongs to this live query and value is
            // writable. LARGE with NOSCALE preserves the exact integer.
            check("PdhGetFormattedCounterValue", &counter.path, unsafe {
                PdhGetFormattedCounterValue(
                    counter.handle,
                    PDH_FMT_LARGE | PDH_FMT_NOSCALE,
                    null_mut(),
                    &mut value,
                )
            })?;
            if !matches!(value.CStatus, PDH_CSTATUS_VALID_DATA | PDH_CSTATUS_NEW_DATA) {
                return Err(Error::Pdh {
                    operation: "counter CStatus",
                    path: counter.path.clone(),
                    status: value.CStatus,
                });
            }
            // SAFETY: A successful PDH_FMT_LARGE request initialized largeValue.
            values.push(unsafe { value.Anonymous.largeValue });
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
                    let query = match Query::open(&counters) {
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

    fn counter(path: &str, name: &str) -> CounterConfig {
        CounterConfig {
            path: path.to_owned(),
            name: name.to_owned(),
            unit: "By".to_owned(),
            description: format!("Description for {name}."),
        }
    }

    fn query_counts() -> (usize, usize) {
        (
            QUERY_OPENS.load(Ordering::Relaxed),
            QUERY_CLOSES.load(Ordering::Relaxed),
        )
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
            assert!(sample.values.iter().all(|value| *value >= 0));
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

    /// Scenario: Native metadata describes a rate or a display-scaled raw counter.
    /// Guarantees: Rates are rejected, while supported raw values remain deliberately unscaled.
    #[test]
    fn validates_supported_native_metadata() {
        let rate = validate_native_type(r"\Processor(_Total)\% Processor Time", 0x1041_0500, 0)
            .unwrap_err()
            .to_string();
        assert!(rate.contains(r"\Processor(_Total)\% Processor Time"));
        assert!(rate.contains("0x10410500"));

        validate_native_type(r"\Memory\Available Bytes", PERF_COUNTER_LARGE_RAWCOUNT, -6).unwrap();
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
