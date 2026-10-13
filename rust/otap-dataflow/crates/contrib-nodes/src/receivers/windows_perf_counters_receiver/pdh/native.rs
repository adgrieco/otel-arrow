// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Owns native handles, aligned metadata storage, and PDH status/value decoding.
#![allow(unsafe_code)]

use super::{Error, InstanceIdentity, Number, PdhApi, RawSample, ValueFormat};
use std::mem::{size_of, size_of_val};
use std::ptr::{null, null_mut};
use std::slice;
use std::time::{SystemTime, UNIX_EPOCH};
use windows_sys::Win32::System::Performance::{
    PDH_CALC_NEGATIVE_DENOMINATOR, PDH_CALC_NEGATIVE_TIMEBASE, PDH_CALC_NEGATIVE_VALUE,
    PDH_COUNTER_INFO_W, PDH_COUNTER_PATH_ELEMENTS_W, PDH_CSTATUS_INVALID_DATA,
    PDH_CSTATUS_NEW_DATA, PDH_CSTATUS_VALID_DATA, PDH_FMT_COUNTERVALUE, PDH_FMT_DOUBLE,
    PDH_FMT_LARGE, PDH_HCOUNTER, PDH_HQUERY, PDH_INVALID_ARGUMENT, PDH_INVALID_DATA, PDH_MORE_DATA,
    PDH_NO_DATA, PDH_RAW_COUNTER, PDH_REFRESHCOUNTERS, PdhAddCounterW, PdhAddEnglishCounterW,
    PdhCloseQuery, PdhCollectQueryData, PdhExpandWildCardPathW, PdhGetCounterInfoW,
    PdhGetFormattedCounterValue, PdhGetRawCounterValue, PdhOpenQueryW, PdhParseCounterPathW,
    PdhRemoveCounter,
};

// These flags are not exposed by windows-sys 0.61.2.
const PDH_FMT_NOSCALE: u32 = 0x0000_1000;
const PDH_FMT_NOCAP100: u32 = 0x0000_8000;

pub(super) fn check(operation: &'static str, path: &str, status: u32) -> Result<(), Error> {
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

fn check_data(operation: &'static str, path: &str, status: u32) -> Result<(), Error> {
    if matches!(status, PDH_CSTATUS_VALID_DATA | PDH_CSTATUS_NEW_DATA) {
        Ok(())
    } else {
        Err(Error::CounterData {
            operation,
            path: path.to_owned(),
            status,
        })
    }
}

fn is_no_observation_status(status: u32) -> bool {
    matches!(
        status,
        PDH_INVALID_DATA
            | PDH_NO_DATA
            | PDH_CALC_NEGATIVE_DENOMINATOR
            | PDH_CALC_NEGATIVE_TIMEBASE
            | PDH_CALC_NEGATIVE_VALUE
    )
}

fn is_no_observation_data_status(status: u32) -> bool {
    matches!(
        status,
        PDH_CSTATUS_INVALID_DATA
            | PDH_NO_DATA
            | PDH_CALC_NEGATIVE_DENOMINATOR
            | PDH_CALC_NEGATIVE_TIMEBASE
            | PDH_CALC_NEGATIVE_VALUE
    )
}

/// Opaque slot in an adapter's query. Slots are never reused.
pub(in super::super) struct CounterToken {
    index: usize,
}

pub(in super::super) struct NativePdh {
    query: PDH_HQUERY,
    counters: Vec<Option<PDH_HCOUNTER>>,
    node: String,
}

impl NativePdh {
    pub(super) fn open(node: String) -> Result<Self, Error> {
        let mut query = null_mut();
        // SAFETY: Null selects live local data; the output handle is writable.
        check("PdhOpenQueryW", "<query>", unsafe {
            PdhOpenQueryW(null(), 0, &mut query)
        })?;
        #[cfg(test)]
        let _ = super::QUERY_OPENS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Self {
            query,
            counters: Vec::new(),
            node,
        })
    }

    pub(super) fn parse_instance_path(path: &str) -> Result<InstanceIdentity, Error> {
        NativePdh::parse_instance_identity(path)
    }

    fn counter_info_buffer(path: &str, counter: PDH_HCOUNTER) -> Result<Vec<usize>, Error> {
        let mut size = 0;
        // SAFETY: The counter is live. Null and zero request the required size.
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
        let word = size_of::<usize>();
        let words = (size as usize).div_ceil(word);
        let mut buffer = vec![0_usize; words];
        let capacity = buffer
            .len()
            .checked_mul(word)
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or(Error::InvalidSample(
                "PdhGetCounterInfoW buffer capacity exceeds u32",
            ))?;
        size = capacity;
        // SAFETY: The aligned usize buffer is writable for size bytes.
        check("PdhGetCounterInfoW", path, unsafe {
            PdhGetCounterInfoW(
                counter,
                false,
                &mut size,
                buffer.as_mut_ptr().cast::<PDH_COUNTER_INFO_W>(),
            )
        })?;
        if (size as usize) < size_of::<PDH_COUNTER_INFO_W>()
            || (size as usize) > buffer.len() * word
        {
            return Err(Error::InvalidSample(
                "PdhGetCounterInfoW returned an invalid filled size",
            ));
        }
        Ok(buffer)
    }

    fn wide_string(pointer: *const u16, buffer: &[usize]) -> Result<Option<String>, Error> {
        if pointer.is_null() {
            return Ok(None);
        }
        let start = buffer.as_ptr() as usize;
        let bytes = size_of_val(buffer);
        let end = start
            .checked_add(bytes)
            .ok_or(Error::InvalidSample("counter info buffer overflowed"))?;
        let address = pointer as usize;
        if address < start || address >= end || !(address - start).is_multiple_of(size_of::<u16>())
        {
            return Err(Error::InvalidSample(
                "counter info returned an out-of-buffer string",
            ));
        }
        let available = (end - address) / size_of::<u16>();
        // SAFETY: Bounds and alignment were validated against the live buffer.
        let units = unsafe { slice::from_raw_parts(pointer, available) };
        let Some(length) = units.iter().position(|unit| *unit == 0) else {
            return Err(Error::InvalidSample(
                "counter info returned an unterminated string",
            ));
        };
        Ok(Some(String::from_utf16_lossy(&units[..length])))
    }

    fn localized_wildcard_path(&mut self, path: &str) -> Result<String, Error> {
        let wide_path: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
        let mut counter = null_mut();
        // SAFETY: The adapter owns the live query, path is NUL-terminated,
        // and the output handle is writable.
        check("PdhAddEnglishCounterW(wildcard)", path, unsafe {
            PdhAddEnglishCounterW(self.query, wide_path.as_ptr(), 0, &mut counter)
        })?;
        let result = (|| {
            let buffer = Self::counter_info_buffer(path, counter)?;
            let info = buffer.as_ptr().cast::<PDH_COUNTER_INFO_W>();
            // SAFETY: A successful metadata fill initialized the fixed header.
            let full_path = unsafe { Self::wide_string((*info).szFullPath, &buffer)? };
            full_path.ok_or(Error::InvalidSample(
                "PdhGetCounterInfoW returned a null full path",
            ))
        })();
        // SAFETY: The temporary handle belongs to this query and is no longer used.
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
        let mut values = Vec::new();
        let mut start = 0;
        while start < buffer.len() {
            let Some(relative_end) = buffer[start..].iter().position(|unit| *unit == 0) else {
                return Err(Error::InvalidSample(
                    "PdhExpandWildCardPathW returned an unterminated path",
                ));
            };
            if relative_end == 0 {
                return Ok(values);
            }
            let end = start + relative_end;
            values.push(String::from_utf16_lossy(&buffer[start..end]));
            start = end + 1;
        }
        Err(Error::InvalidSample(
            "PdhExpandWildCardPathW returned no multi-string terminator",
        ))
    }

    fn expand_wildcard(&mut self, path: &str) -> Result<Vec<String>, Error> {
        let localized = self.localized_wildcard_path(path)?;
        let wide: Vec<u16> = localized.encode_utf16().chain(Some(0)).collect();
        let mut last_status = PDH_MORE_DATA;
        for _ in 0..3 {
            let mut size = 0;
            // SAFETY: Null and zero request the required output size.
            let size_status = unsafe {
                PdhExpandWildCardPathW(
                    null(),
                    wide.as_ptr(),
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
            let mut buffer = vec![0_u16; size as usize];
            // SAFETY: The buffer is writable for size UTF-16 units.
            let status = unsafe {
                PdhExpandWildCardPathW(
                    null(),
                    wide.as_ptr(),
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
                return Self::parse_multi_sz(&buffer[..used]);
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

    fn parse_instance_identity(path: &str) -> Result<InstanceIdentity, Error> {
        let wide_path: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
        let mut size = 0;
        // SAFETY: Null and zero request the required output size.
        let status = unsafe { PdhParseCounterPathW(wide_path.as_ptr(), null_mut(), &mut size, 0) };
        if status != PDH_MORE_DATA {
            return Err(Error::Pdh {
                operation: "PdhParseCounterPathW(size)",
                path: path.to_owned(),
                status,
            });
        }
        if (size as usize) < size_of::<PDH_COUNTER_PATH_ELEMENTS_W>() {
            return Err(Error::InvalidSample(
                "PdhParseCounterPathW returned an undersized buffer",
            ));
        }
        let words = (size as usize).div_ceil(size_of::<usize>());
        let mut buffer = vec![0_usize; words];
        let capacity = buffer
            .len()
            .checked_mul(size_of::<usize>())
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or(Error::InvalidSample(
                "PdhParseCounterPathW buffer capacity exceeds u32",
            ))?;
        let elements = buffer.as_mut_ptr().cast::<PDH_COUNTER_PATH_ELEMENTS_W>();
        // SAFETY: The aligned buffer is writable for the requested bytes.
        check("PdhParseCounterPathW", path, unsafe {
            PdhParseCounterPathW(wide_path.as_ptr(), elements, &mut size, 0)
        })?;
        if (size as usize) < size_of::<PDH_COUNTER_PATH_ELEMENTS_W>() || size > capacity {
            return Err(Error::InvalidSample(
                "PdhParseCounterPathW returned an invalid filled size",
            ));
        }
        // SAFETY: Successful parsing initialized the fixed pointer fields.
        let elements = unsafe { &*elements };
        let name = Self::wide_string(elements.szInstanceName, &buffer)?.ok_or(
            Error::InvalidSample("expanded path has no instance identity"),
        )?;
        let parent = Self::wide_string(elements.szParentInstance, &buffer)?;
        Ok(InstanceIdentity {
            name,
            parent: parent.filter(|value| !value.is_empty()),
            index: elements.dwInstanceIndex,
        })
    }

    fn counter_handle(&self, counter: &CounterToken, path: &str) -> Result<PDH_HCOUNTER, Error> {
        self.counters
            .get(counter.index)
            .copied()
            .flatten()
            .ok_or_else(|| Error::InvalidCounterToken {
                path: path.to_owned(),
            })
    }

    pub(super) fn collect_status(status: u32) -> Result<bool, Error> {
        if is_no_observation_status(status) {
            return Ok(false);
        }
        check("PdhCollectQueryData", "<query>", status)?;
        Ok(true)
    }

    pub(super) fn raw_with(
        path: &str,
        read: impl FnOnce(&mut PDH_RAW_COUNTER) -> u32,
    ) -> Result<Option<RawSample>, Error> {
        let mut raw = PDH_RAW_COUNTER::default();
        let status = read(&mut raw);
        if is_no_observation_status(status) {
            return Ok(None);
        }
        check("PdhGetRawCounterValue", path, status)?;
        if is_no_observation_data_status(raw.CStatus) {
            return Ok(None);
        }
        check_data("raw counter CStatus", path, raw.CStatus)?;
        Ok(Some(RawSample {
            second_value: raw.SecondValue,
            multi_count: raw.MultiCount,
        }))
    }

    pub(super) fn counter_type_with(
        path: &str,
        mut info: impl FnMut(&mut u32, Option<&mut [PDH_COUNTER_INFO_W]>) -> Result<u32, Error>,
    ) -> Result<u32, Error> {
        let mut size = 0;
        let status = info(&mut size, None)?;
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
        // Typed storage guarantees the header's alignment and initializes the
        // trailing buffer; PDH fills both the header and variable-sized data.
        let mut buffer = vec![
            PDH_COUNTER_INFO_W::default();
            (size as usize).div_ceil(size_of::<PDH_COUNTER_INFO_W>())
        ];
        check(
            "PdhGetCounterInfoW",
            path,
            info(&mut size, Some(&mut buffer))?,
        )?;
        if (size as usize) < size_of::<PDH_COUNTER_INFO_W>()
            || (size as usize) > size_of_val(buffer.as_slice())
        {
            return Err(Error::InvalidSample(
                "PdhGetCounterInfoW returned an invalid filled size",
            ));
        }
        // Only a successful fill reaches classification; zero is a valid type.
        Ok(buffer[0].dwType)
    }

    pub(super) fn formatted_with(
        path: &str,
        format: ValueFormat,
        formatted: impl FnOnce(u32, &mut PDH_FMT_COUNTERVALUE) -> u32,
    ) -> Result<Option<Number>, Error> {
        let flags = match format {
            ValueFormat::Integer => PDH_FMT_LARGE | PDH_FMT_NOSCALE,
            ValueFormat::Double => PDH_FMT_DOUBLE | PDH_FMT_NOSCALE | PDH_FMT_NOCAP100,
        };
        let mut value = PDH_FMT_COUNTERVALUE::default();
        let status = formatted(flags, &mut value);
        if status != 0 && status != PDH_INVALID_DATA {
            if is_no_observation_status(status) {
                return Ok(None);
            }
            check("PdhGetFormattedCounterValue", path, status)?;
        }
        // PDH_INVALID_DATA can carry a more specific failure in CStatus.
        if is_no_observation_data_status(value.CStatus) {
            return Ok(None);
        }
        check_data("formatted counter CStatus", path, value.CStatus)?;
        if status == PDH_INVALID_DATA {
            return Ok(None);
        }
        let number = match format {
            // SAFETY: A successful PDH_FMT_LARGE call initialized largeValue.
            ValueFormat::Integer => Number::Integer(unsafe { value.Anonymous.largeValue }),
            // SAFETY: A successful PDH_FMT_DOUBLE call initialized doubleValue.
            ValueFormat::Double => Number::Double(unsafe { value.Anonymous.doubleValue }),
        };
        Ok(Some(number))
    }
}

impl PdhApi for NativePdh {
    type Counter = CounterToken;

    fn add(&mut self, path: &str, localized: bool) -> Result<CounterToken, Error> {
        let wide_path: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
        let mut handle = null_mut();
        // SAFETY: The adapter owns the live query, path is NUL-terminated,
        // and the output handle is writable.
        let status = unsafe {
            if localized {
                PdhAddCounterW(self.query, wide_path.as_ptr(), 0, &mut handle)
            } else {
                PdhAddEnglishCounterW(self.query, wide_path.as_ptr(), 0, &mut handle)
            }
        };
        check(
            if localized {
                "PdhAddCounterW"
            } else {
                "PdhAddEnglishCounterW"
            },
            path,
            status,
        )?;
        let token = CounterToken {
            index: self.counters.len(),
        };
        self.counters.push(Some(handle));
        Ok(token)
    }

    fn remove(&mut self, counter: &CounterToken, path: &str) -> Result<(), Error> {
        let handle = self.counter_handle(counter, path)?;
        // SAFETY: Lookup returns an owned, live counter; successful removal
        // invalidates its slot before another operation can borrow the adapter.
        check("PdhRemoveCounter", path, unsafe {
            PdhRemoveCounter(handle)
        })?;
        self.counters[counter.index] = None;
        Ok(())
    }

    fn counter_type(&mut self, counter: &CounterToken, path: &str) -> Result<u32, Error> {
        let handle = self.counter_handle(counter, path)?;
        Self::counter_type_with(path, |size, buffer| {
            let info = if let Some(buffer) = buffer {
                *size = u32::try_from(size_of_val(buffer)).map_err(|_| {
                    Error::InvalidSample("PdhGetCounterInfoW buffer capacity exceeds u32")
                })?;
                buffer.as_mut_ptr()
            } else {
                *size = 0;
                null_mut()
            };
            // SAFETY: Lookup returned an owned, live counter. For filling, size
            // and the aligned pointer come from the same writable slice above;
            // sizing uses null and zero. The size output is writable.
            Ok(unsafe { PdhGetCounterInfoW(handle, false, size, info) })
        })
    }

    fn collect(&mut self) -> Result<bool, Error> {
        // SAFETY: The adapter owns the live query and is borrowed exclusively.
        Self::collect_status(unsafe { PdhCollectQueryData(self.query) })
    }

    fn raw(&mut self, counter: &CounterToken, path: &str) -> Result<Option<RawSample>, Error> {
        let handle = self.counter_handle(counter, path)?;
        Self::raw_with(path, |raw| {
            // SAFETY: Lookup returned an owned, live counter and the adapter
            // supplies initialized, writable output.
            unsafe { PdhGetRawCounterValue(handle, null_mut(), raw) }
        })
    }

    fn formatted(
        &mut self,
        counter: &CounterToken,
        path: &str,
        format: ValueFormat,
    ) -> Result<Option<Number>, Error> {
        let handle = self.counter_handle(counter, path)?;
        Self::formatted_with(path, format, |flags, value| {
            // SAFETY: Lookup returned an owned, live counter and the adapter
            // supplies initialized, writable storage for the format it decodes.
            unsafe { PdhGetFormattedCounterValue(handle, flags, null_mut(), value) }
        })
    }

    fn timestamp(&mut self) -> Result<i64, Error> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::InvalidSample("system clock precedes Unix epoch"))?
            .as_nanos();
        i64::try_from(timestamp)
            .map_err(|_| Error::InvalidSample("timestamp exceeds i64 nanoseconds"))
    }

    fn expand(&mut self, path: &str) -> Result<Vec<String>, Error> {
        self.expand_wildcard(path)
    }

    fn parse_instance(&mut self, path: &str) -> Result<InstanceIdentity, Error> {
        Self::parse_instance_identity(path)
    }
}

impl Drop for NativePdh {
    fn drop(&mut self) {
        // SAFETY: Construction opened this exclusively owned query. Drop is
        // its only close path; no counter can be read through this adapter afterward.
        let status = unsafe { PdhCloseQuery(self.query) };
        #[cfg(test)]
        let _ = super::QUERY_CLOSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if status != 0 {
            otel_arrow_dfe_telemetry::otel_warn!(
                "otelcol.node.windows_perf_counters.close.fail",
                node = self.node.as_str(),
                status = status as u64
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use windows_sys::Win32::System::Performance::PdhFormatFromRawValue;

    /// Scenario: Native formatting is requested for an exact large integer and a double above 100.
    /// Guarantees: PDH display scaling is disabled, doubles are not capped, and the matching union field preserves each value.
    #[test]
    fn formatting_requests_unscaled_uncapped_values() {
        for (format, flags, number) in [
            (
                ValueFormat::Integer,
                PDH_FMT_LARGE | PDH_FMT_NOSCALE,
                Number::Integer(i64::MAX),
            ),
            (
                ValueFormat::Double,
                PDH_FMT_DOUBLE | PDH_FMT_NOSCALE | PDH_FMT_NOCAP100,
                Number::Double(250.0),
            ),
        ] {
            let result = NativePdh::formatted_with("fixture", format, |requested, value| {
                assert_eq!(requested, flags);
                value.CStatus = PDH_CSTATUS_VALID_DATA;
                match number {
                    Number::Integer(number) => value.Anonymous.largeValue = number,
                    Number::Double(number) => value.Anonymous.doubleValue = number,
                }
                0
            })
            .unwrap();
            assert_eq!(result, Some(number));
        }
    }

    /// Scenario: Windows formats successive rate, delta, timer, fraction and average samples.
    /// Guarantees: Native formulas preserve delta differences, CPU above 100 percent, and no display scaling.
    #[test]
    fn native_formulas_from_raw_fixtures() {
        for (native_type, first, second, previous_first, previous_second, time_base, expected) in [
            (0x1041_0400, 160, 12, 100, 10, 10, 300.0),
            (0x0040_0500, 200, 1, 100, 1, 1, 100.0),
            (0x2051_0500, 150, 200, 100, 180, 10_000_000, 250.0),
            (0x2002_0400, 25, 100, 0, 0, 1, 25.0),
            (0x20C2_0400, 160, 200, 100, 100, 1, 60.0),
            (0x3002_0400, 200, 5, 100, 3, 100, 0.5),
            (0x4002_0500, 200, 5, 100, 3, 1, 50.0),
        ] {
            let current = PDH_RAW_COUNTER {
                CStatus: PDH_CSTATUS_VALID_DATA,
                FirstValue: first,
                SecondValue: second,
                ..Default::default()
            };
            let previous = PDH_RAW_COUNTER {
                CStatus: PDH_CSTATUS_VALID_DATA,
                FirstValue: previous_first,
                SecondValue: previous_second,
                ..Default::default()
            };
            let value =
                NativePdh::formatted_with("fixture", ValueFormat::Double, |flags, value| {
                    // SAFETY: All pointers reference initialized fixture values and writable output.
                    unsafe {
                        PdhFormatFromRawValue(
                            native_type,
                            flags,
                            &time_base,
                            &current,
                            &previous,
                            value,
                        )
                    }
                })
                .unwrap();
            let Some(Number::Double(actual)) = value else {
                panic!("successful native double formatting must return a double");
            };
            assert!((actual - expected).abs() < 1e-9);
        }
    }

    /// Scenario: Windows formats elapsed time from one raw sample and no previous sample.
    /// Guarantees: PERF_ELAPSED_TIME uses the current object timestamp and frequency without two-sample warm-up.
    #[test]
    fn native_elapsed_formula_from_single_raw_fixture() {
        let current = PDH_RAW_COUNTER {
            CStatus: PDH_CSTATUS_VALID_DATA,
            FirstValue: 100,
            SecondValue: 250,
            ..Default::default()
        };
        let time_base = 10;
        let value = NativePdh::formatted_with("fixture", ValueFormat::Double, |flags, value| {
            // SAFETY: Current and output point to initialized storage; elapsed time needs no older raw sample.
            unsafe {
                PdhFormatFromRawValue(0x3024_0500, flags, &time_base, &current, null(), value)
            }
        })
        .unwrap();
        assert_eq!(value, Some(Number::Double(15.0)));
    }

    /// Scenario: Windows formats normal and inverse system-clock and 100-nanosecond multi-timers.
    /// Guarantees: PDH consumes each current native MultiCount without capping percentages above 100.
    #[test]
    fn native_multi_timer_formulas_from_raw_fixtures() {
        for (
            native_type,
            first,
            second,
            previous_first,
            previous_second,
            time_base,
            multi_count,
            expected,
        ) in [
            (0x2241_0500, 160, 12, 100, 10, 10, 4, 750.0),
            (0x2341_0500, 102, 20, 100, 10, 10, 4, 380.0),
            (0x2251_0500, 150, 200, 100, 180, 10_000_000, 4, 62.5),
            (0x2351_0500, 150, 200, 100, 180, 10_000_000, 4, 150.0),
        ] {
            let current = PDH_RAW_COUNTER {
                CStatus: PDH_CSTATUS_VALID_DATA,
                FirstValue: first,
                SecondValue: second,
                MultiCount: multi_count,
                ..Default::default()
            };
            let previous = PDH_RAW_COUNTER {
                CStatus: PDH_CSTATUS_VALID_DATA,
                FirstValue: previous_first,
                SecondValue: previous_second,
                MultiCount: multi_count,
                ..Default::default()
            };
            let value =
                NativePdh::formatted_with("fixture", ValueFormat::Double, |flags, value| {
                    // SAFETY: All pointers reference initialized fixture values and writable output.
                    unsafe {
                        PdhFormatFromRawValue(
                            native_type,
                            flags,
                            &time_base,
                            &current,
                            &previous,
                            value,
                        )
                    }
                })
                .unwrap();
            let Some(Number::Double(actual)) = value else {
                panic!("successful native double formatting must return a double");
            };
            assert!((actual - expected).abs() < 1e-9);
        }
    }

    /// Scenario: Query collection returns each documented transient status or a hard failure.
    /// Guarantees: Transients become no observations while unrelated failures remain errors.
    #[test]
    fn collection_statuses_distinguish_omissions_from_failures() {
        for status in [
            PDH_INVALID_DATA,
            PDH_NO_DATA,
            PDH_CALC_NEGATIVE_VALUE,
            PDH_CALC_NEGATIVE_DENOMINATOR,
            PDH_CALC_NEGATIVE_TIMEBASE,
        ] {
            assert!(!NativePdh::collect_status(status).unwrap());
        }
        assert!(NativePdh::collect_status(PDH_INVALID_ARGUMENT).is_err());
        assert!(NativePdh::collect_status(0).unwrap());
    }

    /// Scenario: Raw reads return transient API statuses, transient data statuses, and valid bases.
    /// Guarantees: Only checked valid data crosses the native boundary and all omissions return None.
    #[test]
    fn raw_statuses_return_checked_optional_bases() {
        for status in [
            PDH_INVALID_DATA,
            PDH_NO_DATA,
            PDH_CALC_NEGATIVE_VALUE,
            PDH_CALC_NEGATIVE_DENOMINATOR,
            PDH_CALC_NEGATIVE_TIMEBASE,
        ] {
            assert_eq!(NativePdh::raw_with("fixture", |_| status).unwrap(), None);
        }
        for status in [
            PDH_CSTATUS_INVALID_DATA,
            PDH_NO_DATA,
            PDH_CALC_NEGATIVE_VALUE,
            PDH_CALC_NEGATIVE_DENOMINATOR,
            PDH_CALC_NEGATIVE_TIMEBASE,
        ] {
            assert_eq!(
                NativePdh::raw_with("fixture", |raw| {
                    raw.CStatus = status;
                    0
                })
                .unwrap(),
                None
            );
        }
        assert_eq!(
            NativePdh::raw_with("fixture", |raw| {
                raw.CStatus = PDH_CSTATUS_VALID_DATA;
                raw.SecondValue = 42;
                raw.MultiCount = 3;
                0
            })
            .unwrap(),
            Some(RawSample {
                second_value: 42,
                multi_count: 3,
            })
        );
        assert!(NativePdh::raw_with("fixture", |_| PDH_INVALID_ARGUMENT).is_err());
    }

    /// Scenario: Formatted reads combine an API status with a more specific counter data status.
    /// Guarantees: Transient data omits points, specific hard data failures are preserved, and valid values decode.
    #[test]
    fn formatted_statuses_preserve_specific_counter_failures() {
        for status in [
            PDH_CSTATUS_INVALID_DATA,
            PDH_NO_DATA,
            PDH_CALC_NEGATIVE_VALUE,
            PDH_CALC_NEGATIVE_DENOMINATOR,
            PDH_CALC_NEGATIVE_TIMEBASE,
        ] {
            assert_eq!(
                NativePdh::formatted_with("fixture", ValueFormat::Integer, |_, value| {
                    value.CStatus = status;
                    PDH_INVALID_DATA
                })
                .unwrap(),
                None
            );
        }
        let error = NativePdh::formatted_with("fixture", ValueFormat::Integer, |_, value| {
            value.CStatus = PDH_INVALID_ARGUMENT;
            PDH_INVALID_DATA
        })
        .unwrap_err();
        assert!(matches!(
            error,
            Error::CounterData {
                operation: "formatted counter CStatus",
                status: PDH_INVALID_ARGUMENT,
                ..
            }
        ));
        assert_eq!(
            NativePdh::formatted_with("fixture", ValueFormat::Integer, |_, value| {
                value.CStatus = PDH_CSTATUS_NEW_DATA;
                value.Anonymous.largeValue = 42;
                0
            })
            .unwrap(),
            Some(Number::Integer(42))
        );
    }

    /// Scenario: Wildcard expansion returns a valid multi-string or malformed termination.
    /// Guarantees: Every concrete path is decoded exactly and malformed native buffers fail.
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
            NativePdh::parse_multi_sz(&buffer).unwrap(),
            vec![
                r"\Process(worker)\Private Bytes",
                r"\Process(worker#1)\Private Bytes"
            ]
        );
        assert!(NativePdh::parse_multi_sz(&[b'a' as u16]).is_err());
    }
}
