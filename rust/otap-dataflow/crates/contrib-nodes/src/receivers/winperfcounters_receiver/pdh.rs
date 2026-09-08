// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Synchronous PDH boundary. Call only from a blocking worker.
#![allow(unsafe_code)]

use crate::receivers::winperfcounters::{COUNTER_PATH, Sample};
use std::ptr::{null, null_mut};
use std::time::{SystemTime, UNIX_EPOCH};
use windows_sys::Win32::System::Performance::{
    PDH_CSTATUS_NEW_DATA, PDH_CSTATUS_VALID_DATA, PDH_FMT_COUNTERVALUE, PDH_FMT_LARGE, PDH_HQUERY,
    PdhAddEnglishCounterW, PdhCloseQuery, PdhCollectQueryData, PdhGetFormattedCounterValue,
    PdhOpenQueryW,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("{operation} for {COUNTER_PATH} failed with PDH status 0x{status:08X}")]
    Pdh {
        operation: &'static str,
        status: u32,
    },
    #[error("invalid memory reading: {0}")]
    InvalidSample(&'static str),
}

fn check(operation: &'static str, status: u32) -> Result<(), Error> {
    if status == 0 {
        Ok(())
    } else {
        Err(Error::Pdh { operation, status })
    }
}

/// The query owns its added counter; closing the query releases both.
struct Query(PDH_HQUERY);

impl Drop for Query {
    fn drop(&mut self) {
        // SAFETY: This handle came from a successful open, has one owner, and no
        // PDH call can outlive this stack-local owner. Close also frees counters.
        let status = unsafe { PdhCloseQuery(self.0) };
        if status != 0 {
            otel_arrow_dfe_telemetry::otel_warn!(
                "winperfcounters.close_failed",
                status = status as u64
            );
        }
    }
}

/// Open, read and close one direct gauge. No handles cross a thread boundary.
pub(super) fn collect() -> Result<Sample, Error> {
    let mut handle = null_mut();
    // SAFETY: Null selects the live local data source; handle is writable.
    check("PdhOpenQueryW", unsafe {
        PdhOpenQueryW(null(), 0, &mut handle)
    })?;
    let query = Query(handle);
    let path: Vec<u16> = COUNTER_PATH.encode_utf16().chain(Some(0)).collect();
    let mut counter = null_mut();
    // SAFETY: The query is live, path is NUL-terminated for the duration of the
    // call, and counter is writable. Query owns the returned counter handle.
    check("PdhAddEnglishCounterW", unsafe {
        PdhAddEnglishCounterW(query.0, path.as_ptr(), 0, &mut counter)
    })?;
    // SAFETY: Query and its counter remain alive and are used on this thread only.
    check("PdhCollectQueryData", unsafe {
        PdhCollectQueryData(query.0)
    })?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::InvalidSample("system clock precedes Unix epoch"))?
        .as_nanos();
    let timestamp_unix_nano = i64::try_from(timestamp)
        .map_err(|_| Error::InvalidSample("timestamp exceeds i64 nanoseconds"))?;
    let mut value = PDH_FMT_COUNTERVALUE::default();
    // SAFETY: The counter belongs to the live query; value is writable. We ask
    // for LARGE, and inspect the corresponding union member only after success.
    check("PdhGetFormattedCounterValue", unsafe {
        PdhGetFormattedCounterValue(counter, PDH_FMT_LARGE, null_mut(), &mut value)
    })?;
    if !matches!(value.CStatus, PDH_CSTATUS_VALID_DATA | PDH_CSTATUS_NEW_DATA) {
        return Err(Error::Pdh {
            operation: "counter CStatus",
            status: value.CStatus,
        });
    }
    // SAFETY: A successful PDH_FMT_LARGE request initialized largeValue.
    let available_bytes = unsafe { value.Anonymous.largeValue };
    if available_bytes < 0 {
        return Err(Error::InvalidSample("available bytes is negative"));
    }
    Ok(Sample {
        timestamp_unix_nano,
        available_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: Live Windows PDH is queried repeatedly for the fixed direct gauge.
    /// Guarantees: Each fresh query yields valid bytes and projects without rate warm-up.
    #[test]
    fn real_memory_gauge() {
        for _ in 0..3 {
            let sample = collect().expect("live Memory counter must be installed and accessible");
            assert!(sample.timestamp_unix_nano > 0);
            assert!(sample.available_bytes >= 0);
            let _ = crate::receivers::winperfcounters::into_otap(sample).unwrap();
        }
    }

    /// Scenario: PDH reports an error status rather than a valid result.
    /// Guarantees: The failing API name and hexadecimal Windows status survive reporting.
    #[test]
    fn preserves_error_status() {
        let error = check("PdhCollectQueryData", 0xC0000BC6)
            .unwrap_err()
            .to_string();
        assert!(error.contains("PdhCollectQueryData"));
        assert!(error.contains("0xC0000BC6"));
    }
}
