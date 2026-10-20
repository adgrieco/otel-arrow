// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Owns native handles, aligned metadata storage, and PDH status/value decoding.
#![allow(unsafe_code)]

use super::{Error, InstanceIdentity, Number, PdhApi, PdhOperation, RawSample, ValueFormat};
use std::mem::{size_of, size_of_val};
use std::ptr::{null, null_mut};
use std::slice;
use std::time::{SystemTime, UNIX_EPOCH};
use windows_sys::Win32::Globalization::{GetThreadUILanguage, SetThreadUILanguage};
use windows_sys::Win32::System::Performance::{
    PDH_CALC_NEGATIVE_DENOMINATOR, PDH_CALC_NEGATIVE_TIMEBASE, PDH_CALC_NEGATIVE_VALUE,
    PDH_COUNTER_INFO_W, PDH_COUNTER_PATH_ELEMENTS_W, PDH_CSTATUS_INVALID_DATA,
    PDH_CSTATUS_NEW_DATA, PDH_CSTATUS_NO_INSTANCE, PDH_CSTATUS_VALID_DATA, PDH_FMT_COUNTERVALUE,
    PDH_FMT_DOUBLE, PDH_FMT_LARGE, PDH_HCOUNTER, PDH_HQUERY, PDH_INVALID_ARGUMENT,
    PDH_INVALID_DATA, PDH_MORE_DATA, PDH_NO_DATA, PDH_RAW_COUNTER, PDH_REFRESHCOUNTERS,
    PERF_DETAIL_WIZARD, PdhAddEnglishCounterW, PdhCloseQuery, PdhCollectQueryData, PdhEnumObjectsW,
    PdhExpandWildCardPathW, PdhGetCounterInfoW, PdhGetFormattedCounterValue, PdhGetRawCounterValue,
    PdhOpenQueryW, PdhParseCounterPathW, PdhRemoveCounter,
};

// These flags are not exposed by windows-sys 0.61.2.
const PDH_FMT_NOSCALE: u32 = 0x0000_1000;
const PDH_FMT_NOCAP100: u32 = 0x0000_8000;
const ENGLISH_UI_LANGUAGE: u16 = 0x0409;

pub(super) fn initialize_worker(node: &str, refresh_objects: bool) -> Result<(), Error> {
    initialize_worker_with(
        node,
        refresh_objects,
        || {
            // SAFETY: This changes only the calling worker thread's UI language.
            unsafe {
                let returned = SetThreadUILanguage(ENGLISH_UI_LANGUAGE);
                (returned, GetThreadUILanguage())
            }
        },
        |refresh, size, buffer| {
            // SAFETY: Null selects the local live source and machine. The optional
            // output slice is writable for the supplied UTF-16 element count.
            unsafe {
                PdhEnumObjectsW(
                    null(),
                    null(),
                    buffer.map_or(null_mut(), |buffer| buffer.as_mut_ptr()),
                    size,
                    PERF_DETAIL_WIZARD,
                    refresh.into(),
                )
            }
        },
    )
}

fn initialize_worker_with(
    node: &str,
    refresh_objects: bool,
    pin_language: impl FnOnce() -> (u16, u16),
    mut enumerate_objects: impl FnMut(bool, &mut u32, Option<&mut [u16]>) -> u32,
) -> Result<(), Error> {
    let (language_result, verified_language) = pin_language();
    let mut refresh_status = None;
    let mut enumeration_status = None;
    let pin_error = (language_result != ENGLISH_UI_LANGUAGE
        || verified_language != ENGLISH_UI_LANGUAGE)
        .then_some(Error::WorkerLanguagePin {
            requested: ENGLISH_UI_LANGUAGE,
            returned: language_result,
            verified: verified_language,
        });
    let result = if let Some(error) = pin_error {
        if refresh_objects {
            Err(error)
        } else {
            otel_arrow_dfe_telemetry::otel_warn!(
                "windows_perf_counters_receiver.worker.language.warn",
                node = node,
                error = %error
            );
            Ok(())
        }
    } else if refresh_objects {
        let mut size = 0;
        let status = enumerate_objects(true, &mut size, None);
        refresh_status = Some(status);
        if status != PDH_MORE_DATA || size == 0 {
            Err(Error::WorkerObjectRefresh {
                stage: "refresh sizing",
                status,
            })
        } else {
            let mut buffer = vec![
                0_u16;
                usize::try_from(size).map_err(|_| Error::WorkerObjectRefresh {
                    stage: "refresh size conversion",
                    status,
                })?
            ];
            size = u32::try_from(buffer.len()).map_err(|_| Error::WorkerObjectRefresh {
                stage: "enumeration buffer size",
                status,
            })?;
            let status = enumerate_objects(false, &mut size, Some(&mut buffer));
            enumeration_status = Some(status);
            if status == 0 {
                Ok(())
            } else {
                Err(Error::WorkerObjectRefresh {
                    stage: "enumeration fill",
                    status,
                })
            }
        }
    } else {
        Ok(())
    };
    otel_arrow_dfe_telemetry::otel_info!(
        "windows_perf_counters_receiver.worker.startup",
        node = node,
        requested_language_id = ENGLISH_UI_LANGUAGE as u64,
        language_result = language_result as u64,
        verified_language_id = verified_language as u64,
        wildcards_configured = refresh_objects,
        object_refresh_status = ?refresh_status,
        object_enumeration_status = ?enumeration_status
    );
    result
}

pub(super) fn check(operation: PdhOperation, path: &str, status: u32) -> Result<(), Error> {
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

fn check_data(operation: PdhOperation, path: &str, status: u32) -> Result<(), Error> {
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

#[derive(Debug)]
struct NativeBuffer<T> {
    storage: Vec<T>,
}

impl<T: Clone + Default> NativeBuffer<T> {
    fn with_byte_capacity(size: u32, capacity_error: &'static str) -> Result<Self, Error> {
        let elements = (size as usize).div_ceil(size_of::<T>());
        let storage = vec![T::default(); elements];
        let buffer = Self { storage };
        let _ = buffer.byte_capacity_u32(capacity_error)?;
        Ok(buffer)
    }

    fn byte_capacity(&self) -> usize {
        size_of_val(self.storage.as_slice())
    }

    fn byte_capacity_u32(&self, error: &'static str) -> Result<u32, Error> {
        u32::try_from(self.byte_capacity()).map_err(|_| Error::InvalidSample(error))
    }

    fn as_ptr<U>(&self) -> *const U {
        self.storage.as_ptr().cast::<U>()
    }

    fn as_mut_ptr<U>(&mut self) -> *mut U {
        self.storage.as_mut_ptr().cast::<U>()
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.storage
    }
}

fn is_no_observation_status(status: u32) -> bool {
    matches!(
        status,
        PDH_INVALID_DATA
            | PDH_NO_DATA
            | PDH_CSTATUS_NO_INSTANCE
            | PDH_CALC_NEGATIVE_DENOMINATOR
            | PDH_CALC_NEGATIVE_TIMEBASE
            | PDH_CALC_NEGATIVE_VALUE
    )
}

fn is_no_observation_data_status(status: u32) -> bool {
    matches!(
        status,
        PDH_CSTATUS_INVALID_DATA
            | PDH_CSTATUS_NO_INSTANCE
            | PDH_NO_DATA
            | PDH_CALC_NEGATIVE_DENOMINATOR
            | PDH_CALC_NEGATIVE_TIMEBASE
            | PDH_CALC_NEGATIVE_VALUE
    )
}

/// Opaque generational slot in an adapter's query.
pub(in super::super) struct CounterToken {
    index: usize,
    generation: u64,
}

struct CounterSlot {
    generation: u64,
    handle: Option<PDH_HCOUNTER>,
}

struct CounterHandles {
    slots: Vec<CounterSlot>,
    free_slots: Vec<usize>,
    next_generation: u64,
}

impl CounterHandles {
    const fn new() -> Self {
        Self {
            slots: Vec::new(),
            free_slots: Vec::new(),
            next_generation: 0,
        }
    }

    fn take_generation(&mut self, path: &str) -> Result<u64, Error> {
        let generation = self.next_generation;
        self.next_generation =
            self.next_generation
                .checked_add(1)
                .ok_or_else(|| Error::CounterSetup {
                    path: path.to_owned(),
                    message: "counter token generation exhausted".to_owned(),
                    permanent: true,
                    cleanup_failed: false,
                })?;
        Ok(generation)
    }

    fn insert(&mut self, generation: u64, handle: PDH_HCOUNTER) -> CounterToken {
        let index = if let Some(index) = self.free_slots.pop() {
            let slot = &mut self.slots[index];
            debug_assert!(slot.handle.is_none());
            slot.generation = generation;
            slot.handle = Some(handle);
            index
        } else {
            self.slots.push(CounterSlot {
                generation,
                handle: Some(handle),
            });
            self.slots.len() - 1
        };
        CounterToken { index, generation }
    }

    fn handle(&self, counter: &CounterToken, path: &str) -> Result<PDH_HCOUNTER, Error> {
        self.slots
            .get(counter.index)
            .filter(|slot| slot.generation == counter.generation)
            .and_then(|slot| slot.handle)
            .ok_or_else(|| Error::InvalidCounterToken {
                path: path.to_owned(),
            })
    }

    fn remove_with(
        &mut self,
        counter: &CounterToken,
        path: &str,
        remove: impl FnOnce(PDH_HCOUNTER) -> u32,
    ) -> Result<(), Error> {
        let handle = self.handle(counter, path)?;
        check(PdhOperation::RemoveCounter, path, remove(handle))?;
        let slot = &mut self.slots[counter.index];
        debug_assert_eq!(slot.generation, counter.generation);
        debug_assert!(slot.handle.is_some());
        slot.handle = None;
        self.free_slots.push(counter.index);
        Ok(())
    }
}

pub(in super::super) struct NativePdh {
    query: PDH_HQUERY,
    counters: CounterHandles,
    node: String,
}

impl NativePdh {
    pub(super) fn open(node: String) -> Result<Self, Error> {
        let mut query = null_mut();
        // SAFETY: Null selects live local data; the output handle is writable.
        check(PdhOperation::OpenQuery, "<query>", unsafe {
            PdhOpenQueryW(null(), 0, &mut query)
        })?;
        #[cfg(test)]
        let _ = super::QUERY_OPENS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Self {
            query,
            counters: CounterHandles::new(),
            node,
        })
    }

    pub(super) fn parse_instance_path(path: &str) -> Result<InstanceIdentity, Error> {
        NativePdh::parse_instance_identity(path)
    }

    #[cfg(test)]
    fn counter_info_buffer_with(
        path: &str,
        mut info: impl FnMut(&mut u32, Option<&mut [usize]>) -> u32,
    ) -> Result<(NativeBuffer<usize>, usize), Error> {
        let mut size = 0;
        let status = info(&mut size, None);
        if status != PDH_MORE_DATA {
            return Err(Error::Pdh {
                operation: PdhOperation::GetCounterInfoSize,
                path: path.to_owned(),
                status,
            });
        }
        if (size as usize) < size_of::<PDH_COUNTER_INFO_W>() {
            return Err(Error::InvalidSample(
                "PdhGetCounterInfoW returned an undersized buffer",
            ));
        }
        let mut buffer = NativeBuffer::with_byte_capacity(
            size,
            "PdhGetCounterInfoW buffer capacity exceeds u32",
        )?;
        size = buffer.byte_capacity_u32("PdhGetCounterInfoW buffer capacity exceeds u32")?;
        check(
            PdhOperation::GetCounterInfo,
            path,
            info(&mut size, Some(buffer.as_mut_slice())),
        )?;
        if (size as usize) < size_of::<PDH_COUNTER_INFO_W>()
            || (size as usize) > buffer.byte_capacity()
        {
            return Err(Error::InvalidSample(
                "PdhGetCounterInfoW returned an invalid filled size",
            ));
        }
        Ok((buffer, size as usize))
    }

    fn wide_string(
        pointer: *const u16,
        buffer: &NativeBuffer<usize>,
        filled_size: usize,
    ) -> Result<Option<String>, Error> {
        if pointer.is_null() {
            return Ok(None);
        }
        if filled_size > buffer.byte_capacity() {
            return Err(Error::InvalidSample(
                "counter info returned an invalid filled size",
            ));
        }
        let start = buffer.as_ptr::<usize>() as usize;
        let bytes = filled_size;
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

    fn expanded_path_buffer_with(
        path: &str,
        mut expand: impl FnMut(&mut u32, Option<&mut [u16]>) -> u32,
    ) -> Result<Vec<u16>, Error> {
        let mut size = 0;
        let size_status = expand(&mut size, None);
        if matches!(size_status, 0 | PDH_CSTATUS_NO_INSTANCE) && size == 0 {
            return Ok(Vec::new());
        }
        if size_status != PDH_MORE_DATA {
            return Err(Error::Pdh {
                operation: PdhOperation::ExpandWildcardPathSize,
                path: path.to_owned(),
                status: size_status,
            });
        }
        let measured = usize::try_from(size)
            .map_err(|_| Error::InvalidSample("wildcard path list size exceeds usize"))?;
        let headroom = measured / 4;
        let mut capacity = measured
            .checked_add(headroom.max(64))
            .ok_or(Error::InvalidSample("wildcard path list size overflowed"))?;
        let mut last_status = PDH_MORE_DATA;
        for _ in 0..8 {
            let mut buffer = vec![0_u16; capacity];
            size = u32::try_from(buffer.len())
                .map_err(|_| Error::InvalidSample("wildcard path list size exceeds u32"))?;
            let status = expand(&mut size, Some(&mut buffer));
            if matches!(status, 0 | PDH_CSTATUS_NO_INSTANCE) {
                if status == PDH_CSTATUS_NO_INSTANCE || size == 0 {
                    return Ok(Vec::new());
                }
                let used = usize::try_from(size)
                    .map_err(|_| Error::InvalidSample("wildcard path list size exceeds usize"))?;
                if used > buffer.len() {
                    return Err(Error::InvalidSample(
                        "PdhExpandWildCardPathW reported an oversized path list",
                    ));
                }
                buffer.truncate(used);
                return Ok(buffer);
            }
            last_status = status;
            if !matches!(status, PDH_MORE_DATA | PDH_INVALID_ARGUMENT) {
                break;
            }
            let reported = usize::try_from(size)
                .map_err(|_| Error::InvalidSample("wildcard path list size exceeds usize"))?;
            let geometric = capacity
                .checked_add(capacity / 2)
                .and_then(|value| value.checked_add(64))
                .ok_or(Error::InvalidSample("wildcard path list size overflowed"))?;
            capacity = reported.max(geometric);
        }
        Err(Error::Pdh {
            operation: PdhOperation::ExpandWildcardPath,
            path: path.to_owned(),
            status: last_status,
        })
    }

    fn expand_wildcard(&mut self, path: &str) -> Result<Vec<String>, Error> {
        let wide: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
        let buffer = Self::expanded_path_buffer_with(path, |size, buffer| {
            // SAFETY: The path is NUL-terminated. The optional buffer, when present,
            // is writable for the size supplied to PDH.
            unsafe {
                PdhExpandWildCardPathW(
                    null(),
                    wide.as_ptr(),
                    buffer.map_or(null_mut(), |buffer| buffer.as_mut_ptr()),
                    size,
                    PDH_REFRESHCOUNTERS,
                )
            }
        })?;
        if buffer.is_empty() {
            Ok(Vec::new())
        } else {
            Self::parse_multi_sz(&buffer)
        }
    }

    fn parse_instance_identity(path: &str) -> Result<InstanceIdentity, Error> {
        let wide_path: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
        Self::parse_instance_identity_with(path, |size, buffer| {
            // SAFETY: The path is NUL-terminated. Sizing uses null and zero;
            // filling uses an aligned writable slice.
            unsafe {
                PdhParseCounterPathW(
                    wide_path.as_ptr(),
                    buffer.map_or(null_mut(), |buffer| buffer.as_mut_ptr().cast()),
                    size,
                    0,
                )
            }
        })
    }

    fn parse_instance_identity_with(
        path: &str,
        mut parse: impl FnMut(&mut u32, Option<&mut [usize]>) -> u32,
    ) -> Result<InstanceIdentity, Error> {
        let mut size = 0;
        let status = parse(&mut size, None);
        if status != PDH_MORE_DATA {
            return Err(Error::Pdh {
                operation: PdhOperation::ParseCounterPathSize,
                path: path.to_owned(),
                status,
            });
        }
        if (size as usize) < size_of::<PDH_COUNTER_PATH_ELEMENTS_W>() {
            return Err(Error::InvalidSample(
                "PdhParseCounterPathW returned an undersized buffer",
            ));
        }
        let mut buffer = NativeBuffer::with_byte_capacity(
            size,
            "PdhParseCounterPathW buffer capacity exceeds u32",
        )?;
        let capacity =
            buffer.byte_capacity_u32("PdhParseCounterPathW buffer capacity exceeds u32")?;
        let elements = buffer.as_mut_ptr::<PDH_COUNTER_PATH_ELEMENTS_W>();
        check(
            PdhOperation::ParseCounterPath,
            path,
            parse(&mut size, Some(buffer.as_mut_slice())),
        )?;
        if (size as usize) < size_of::<PDH_COUNTER_PATH_ELEMENTS_W>() || size > capacity {
            return Err(Error::InvalidSample(
                "PdhParseCounterPathW returned an invalid filled size",
            ));
        }
        // SAFETY: Successful parsing initialized the fixed pointer fields.
        let elements = unsafe { &*elements };
        let filled_size = size as usize;
        let name = Self::wide_string(elements.szInstanceName, &buffer, filled_size)?.ok_or(
            Error::InvalidSample("expanded path has no instance identity"),
        )?;
        let parent = Self::wide_string(elements.szParentInstance, &buffer, filled_size)?;
        Ok(InstanceIdentity {
            name,
            parent: parent.filter(|value| !value.is_empty()),
            index: elements.dwInstanceIndex,
        })
    }

    fn counter_handle(&self, counter: &CounterToken, path: &str) -> Result<PDH_HCOUNTER, Error> {
        self.counters.handle(counter, path)
    }

    pub(super) fn collect_status(status: u32) -> Result<bool, Error> {
        if is_no_observation_status(status) {
            return Ok(false);
        }
        check(PdhOperation::CollectQueryData, "<query>", status)?;
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
        check(PdhOperation::GetRawCounterValue, path, status)?;
        if is_no_observation_data_status(raw.CStatus) {
            return Ok(None);
        }
        check_data(PdhOperation::RawCounterDataStatus, path, raw.CStatus)?;
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
                operation: PdhOperation::GetCounterInfoSize,
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
        let mut buffer = NativeBuffer::<PDH_COUNTER_INFO_W>::with_byte_capacity(
            size,
            "PdhGetCounterInfoW buffer capacity exceeds u32",
        )?;
        check(
            PdhOperation::GetCounterInfo,
            path,
            info(&mut size, Some(buffer.as_mut_slice()))?,
        )?;
        if (size as usize) < size_of::<PDH_COUNTER_INFO_W>()
            || (size as usize) > buffer.byte_capacity()
        {
            return Err(Error::InvalidSample(
                "PdhGetCounterInfoW returned an invalid filled size",
            ));
        }
        // Only a successful fill reaches classification; zero is a valid type.
        Ok(buffer.as_mut_slice()[0].dwType)
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
            check(PdhOperation::GetFormattedCounterValue, path, status)?;
        }
        // PDH_INVALID_DATA can carry a more specific failure in CStatus.
        if is_no_observation_data_status(value.CStatus) {
            return Ok(None);
        }
        check_data(
            PdhOperation::FormattedCounterDataStatus,
            path,
            value.CStatus,
        )?;
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

    fn add(&mut self, path: &str) -> Result<CounterToken, Error> {
        let generation = self.counters.take_generation(path)?;
        let wide_path: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
        let mut handle = null_mut();
        // SAFETY: The adapter owns the live query, path is NUL-terminated,
        // and the output handle is writable.
        let status =
            unsafe { PdhAddEnglishCounterW(self.query, wide_path.as_ptr(), 0, &mut handle) };
        check(PdhOperation::AddEnglishCounter, path, status)?;
        Ok(self.counters.insert(generation, handle))
    }

    fn remove(&mut self, counter: &CounterToken, path: &str) -> Result<(), Error> {
        self.counters.remove_with(counter, path, |handle| {
            // SAFETY: Lookup returns an owned, live counter; only a successful
            // native removal releases its generational slot.
            unsafe { PdhRemoveCounter(handle) }
        })
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
                "windows_perf_counters_receiver.query.close.fail",
                node = self.node.as_str(),
                status = status as u64
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use windows_sys::Win32::System::Performance::PdhFormatFromRawValue;

    /// Scenario: An exact-only worker pins English UI language without refreshing the process-wide PDH object list.
    /// Guarantees: Exact counters avoid the wildcard-only refresh side effect while still enforcing the language pin.
    #[test]
    fn exact_worker_pins_language_without_object_refresh() {
        initialize_worker_with(
            "exact",
            false,
            || (ENGLISH_UI_LANGUAGE, ENGLISH_UI_LANGUAGE),
            |_, _, _| panic!("exact-only worker must not enumerate objects"),
        )
        .unwrap();
    }

    /// Scenario: A wildcard worker starts on an uninitialized thread and refreshes the object list.
    /// Guarantees: Pinning precedes one refreshing size call and one non-refreshing fill call.
    #[test]
    fn wildcard_worker_pins_then_refreshes_object_list() {
        let pinned = Cell::new(false);
        let calls = RefCell::new(Vec::new());
        initialize_worker_with(
            "wildcard",
            true,
            || {
                pinned.set(true);
                (ENGLISH_UI_LANGUAGE, ENGLISH_UI_LANGUAGE)
            },
            |refresh, size, buffer| {
                assert!(pinned.get());
                calls.borrow_mut().push((refresh, buffer.is_some()));
                if buffer.is_none() {
                    *size = 8;
                    PDH_MORE_DATA
                } else {
                    0
                }
            },
        )
        .unwrap();
        assert_eq!(calls.into_inner(), [(true, false), (false, true)]);
    }

    /// Scenario: SetThreadUILanguage fails or returns a language other than en-US.
    /// Guarantees: Worker startup returns a typed language-pin error before any PDH object refresh.
    #[test]
    fn worker_rejects_failed_language_pin() {
        for (returned, verified) in [(0, 0x0409), (0x0407, 0x0409), (0x0409, 0x0407)] {
            let error = initialize_worker_with(
                "language-failure",
                true,
                || (returned, verified),
                |_, _, _| panic!("failed pin must stop before object enumeration"),
            )
            .unwrap_err();
            assert!(matches!(
                error,
                Error::WorkerLanguagePin {
                    requested: ENGLISH_UI_LANGUAGE,
                    returned: actual,
                    verified: actual_verified,
                } if actual == returned && actual_verified == verified
            ));
        }
    }

    /// Scenario: An exact-only worker cannot set or verify English UI language.
    /// Guarantees: Startup continues without object refresh because English adds do not require the pin.
    #[test]
    fn exact_worker_continues_after_failed_or_ineffective_pin() {
        for result in [(0, 0x0407), (0x0409, 0x0407)] {
            initialize_worker_with(
                "exact-pin-warning",
                false,
                || result,
                |_, _, _| panic!("exact-only worker must not refresh objects"),
            )
            .unwrap();
        }
    }

    /// Scenario: The pinned wildcard worker cannot refresh or fill the PDH object list.
    /// Guarantees: Both native stages fail startup with the typed object-refresh error.
    #[test]
    fn worker_rejects_failed_object_refresh() {
        let sizing = initialize_worker_with(
            "refresh-failure",
            true,
            || (ENGLISH_UI_LANGUAGE, ENGLISH_UI_LANGUAGE),
            |_, _, _| 5,
        )
        .unwrap_err();
        assert!(matches!(
            sizing,
            Error::WorkerObjectRefresh {
                stage: "refresh sizing",
                status: 5,
            }
        ));

        let fill = initialize_worker_with(
            "fill-failure",
            true,
            || (ENGLISH_UI_LANGUAGE, ENGLISH_UI_LANGUAGE),
            |_, size, buffer| {
                if buffer.is_none() {
                    *size = 8;
                    PDH_MORE_DATA
                } else {
                    6
                }
            },
        )
        .unwrap_err();
        assert!(matches!(
            fill,
            Error::WorkerObjectRefresh {
                stage: "enumeration fill",
                status: 6,
            }
        ));
    }

    fn insert_fixture_counter(counters: &mut CounterHandles, path: &str) -> CounterToken {
        let generation = counters.take_generation(path).unwrap();
        counters.insert(generation, std::ptr::dangling_mut())
    }

    #[derive(Clone, Copy)]
    enum BufferApi {
        CounterInfo,
        CounterType,
        ParseInstance,
    }

    impl BufferApi {
        fn header_size(self) -> u32 {
            u32::try_from(match self {
                Self::CounterInfo | Self::CounterType => size_of::<PDH_COUNTER_INFO_W>(),
                Self::ParseInstance => size_of::<PDH_COUNTER_PATH_ELEMENTS_W>(),
            })
            .unwrap()
        }

        fn size_operation(self) -> PdhOperation {
            match self {
                Self::CounterInfo | Self::CounterType => PdhOperation::GetCounterInfoSize,
                Self::ParseInstance => PdhOperation::ParseCounterPathSize,
            }
        }

        fn malformed_error(self, case: MalformedBuffer) -> Error {
            let header = self.header_size();
            let status = |size: &mut u32, filling: bool| match case {
                MalformedBuffer::NonSizingStatus => {
                    *size = header;
                    PDH_INVALID_ARGUMENT
                }
                MalformedBuffer::UndersizedRequest => {
                    *size = header - 1;
                    PDH_MORE_DATA
                }
                MalformedBuffer::UndersizedFill if filling => {
                    *size = header - 1;
                    0
                }
                MalformedBuffer::OversizedFill if filling => {
                    *size = u32::MAX;
                    0
                }
                MalformedBuffer::UndersizedFill | MalformedBuffer::OversizedFill => {
                    *size = header;
                    PDH_MORE_DATA
                }
            };
            match self {
                Self::CounterInfo => {
                    NativePdh::counter_info_buffer_with("fixture", |size, buffer| {
                        status(size, buffer.is_some())
                    })
                    .map(|_| ())
                }
                Self::CounterType => NativePdh::counter_type_with("fixture", |size, buffer| {
                    Ok(status(size, buffer.is_some()))
                })
                .map(|_| ()),
                Self::ParseInstance => {
                    NativePdh::parse_instance_identity_with("fixture", |size, buffer| {
                        status(size, buffer.is_some())
                    })
                    .map(|_| ())
                }
            }
            .unwrap_err()
        }
    }

    #[derive(Clone, Copy)]
    enum MalformedBuffer {
        NonSizingStatus,
        UndersizedRequest,
        UndersizedFill,
        OversizedFill,
    }

    fn parse_instance_fixture_error(
        filled_size: u32,
        mut configure: impl FnMut(*mut u8, *mut PDH_COUNTER_PATH_ELEMENTS_W),
    ) -> Error {
        NativePdh::parse_instance_identity_with("fixture", |size, buffer| {
            let Some(buffer) = buffer else {
                *size = filled_size;
                return PDH_MORE_DATA;
            };
            let base = buffer.as_mut_ptr().cast::<u8>();
            configure(base, base.cast());
            *size = filled_size;
            0
        })
        .unwrap_err()
    }

    /// Scenario: A long-lived query repeatedly adds and removes a fixed peak number of counters.
    /// Guarantees: Generational bookkeeping retains storage proportional to peak concurrent ownership, not lifetime additions.
    #[test]
    fn counter_slots_remain_bounded_under_sustained_churn() {
        let path = "fixture";
        let mut counters = CounterHandles::new();
        let peak = 4;
        for _ in 0..1_024 {
            let tokens = (0..peak)
                .map(|_| insert_fixture_counter(&mut counters, path))
                .collect::<Vec<_>>();
            assert_eq!(counters.slots.len(), peak);
            assert!(counters.free_slots.is_empty());
            for token in tokens {
                counters.remove_with(&token, path, |_| 0).unwrap();
            }
            assert_eq!(counters.slots.len(), peak);
            assert_eq!(counters.free_slots.len(), peak);
        }
    }

    /// Scenario: A removed counter's slot is reused by a later addition.
    /// Guarantees: Generation fencing prevents the stale token from resolving or invoking native removal against the replacement.
    #[test]
    fn reused_counter_slot_rejects_stale_token() {
        let path = "fixture";
        let mut counters = CounterHandles::new();
        let removed = insert_fixture_counter(&mut counters, path);
        counters.remove_with(&removed, path, |_| 0).unwrap();
        let replacement = insert_fixture_counter(&mut counters, path);

        assert_eq!(removed.index, replacement.index);
        assert_ne!(removed.generation, replacement.generation);
        assert!(matches!(
            counters.handle(&removed, path),
            Err(Error::InvalidCounterToken { .. })
        ));
        let mut native_remove_called = false;
        assert!(matches!(
            counters.remove_with(&removed, path, |_| {
                native_remove_called = true;
                0
            }),
            Err(Error::InvalidCounterToken { .. })
        ));
        assert!(!native_remove_called);
        assert!(counters.handle(&replacement, path).is_ok());
    }

    /// Scenario: Native removal fails for an owned counter and later succeeds.
    /// Guarantees: Failure retains the same live ownership, while only successful removal frees the slot and invalidates the token.
    #[test]
    fn failed_counter_removal_retains_ownership() {
        let path = "fixture";
        let mut counters = CounterHandles::new();
        let token = insert_fixture_counter(&mut counters, path);
        let handle = counters.handle(&token, path).unwrap();

        let error = counters
            .remove_with(&token, path, |_| PDH_INVALID_ARGUMENT)
            .unwrap_err();
        assert!(matches!(
            error,
            Error::Pdh {
                operation: PdhOperation::RemoveCounter,
                status: PDH_INVALID_ARGUMENT,
                ..
            }
        ));
        assert_eq!(counters.handle(&token, path).unwrap(), handle);
        assert!(counters.free_slots.is_empty());

        counters.remove_with(&token, path, |_| 0).unwrap();
        assert_eq!(counters.free_slots, [token.index]);
        assert!(matches!(
            counters.handle(&token, path),
            Err(Error::InvalidCounterToken { .. })
        ));
    }

    /// Scenario: A query reaches the end of the counter-token generation space.
    /// Guarantees: Exhaustion fails permanently before native ownership is acquired or slot bookkeeping changes.
    #[test]
    fn counter_generation_exhaustion_is_explicit() {
        let mut counters = CounterHandles::new();
        counters.next_generation = u64::MAX;

        let error = counters.take_generation("fixture").unwrap_err();
        assert!(matches!(
            error,
            Error::CounterSetup {
                permanent: true,
                ..
            }
        ));
        assert!(error.to_string().contains("generation exhausted"));
        assert!(counters.slots.is_empty());
        assert!(counters.free_slots.is_empty());
        assert_eq!(counters.next_generation, u64::MAX);
    }

    /// Scenario: Native metadata APIs return malformed sizing and filling results.
    /// Guarantees: Counter info, counter type, and parsed instance paths all reject non-sizing status, undersized headers, and invalid filled sizes.
    #[test]
    fn native_metadata_buffers_reject_malformed_size_and_fill_contracts() {
        for api in [
            BufferApi::CounterInfo,
            BufferApi::CounterType,
            BufferApi::ParseInstance,
        ] {
            for case in [
                MalformedBuffer::NonSizingStatus,
                MalformedBuffer::UndersizedRequest,
                MalformedBuffer::UndersizedFill,
                MalformedBuffer::OversizedFill,
            ] {
                let error = api.malformed_error(case);
                if matches!(case, MalformedBuffer::NonSizingStatus) {
                    assert!(matches!(
                        error,
                        Error::Pdh {
                            operation,
                            status: PDH_INVALID_ARGUMENT,
                            ..
                        } if operation == api.size_operation()
                    ));
                } else {
                    assert!(matches!(error, Error::InvalidSample(_)));
                }
            }
        }
    }

    /// Scenario: Native metadata strings point outside filled bytes, start in allocation slack, or omit a terminator.
    /// Guarantees: Instance and direct string decoding never reads beyond the native filled-size boundary.
    #[test]
    fn native_strings_are_bounded_by_reported_fill_size() {
        let header = size_of::<PDH_COUNTER_PATH_ELEMENTS_W>();
        let filled = u32::try_from(header + 4 * size_of::<u16>()).unwrap();
        let error = parse_instance_fixture_error(filled, |base, elements| {
            // SAFETY: The fixture buffer is aligned for the header, and the
            // pointer intentionally targets the first byte outside filled data.
            unsafe {
                (*elements).szInstanceName = base.add(filled as usize).cast();
            }
        });
        assert!(matches!(error, Error::InvalidSample(_)));

        let error = parse_instance_fixture_error(filled, |base, elements| {
            let name = unsafe { base.add(header).cast::<u16>() };
            // SAFETY: Four UTF-16 units fit in the reported fixture bytes.
            unsafe {
                (*elements).szInstanceName = name;
                for index in 0..4 {
                    *name.add(index) = b'a' as u16;
                }
            }
        });
        assert!(matches!(error, Error::InvalidSample(_)));

        let mut buffer = NativeBuffer::<usize>::with_byte_capacity(
            4 * u32::try_from(size_of::<u16>()).unwrap(),
            "fixture",
        )
        .unwrap();
        let pointer = buffer.as_mut_ptr::<u16>();
        // SAFETY: The fixture buffer has room for four UTF-16 units.
        unsafe {
            *pointer = b'a' as u16;
            *pointer.add(1) = b'b' as u16;
            *pointer.add(2) = 0;
        }

        assert!(matches!(
            NativePdh::wide_string(pointer.wrapping_add(2), &buffer, 2 * size_of::<u16>()),
            Err(Error::InvalidSample(_))
        ));
        assert!(matches!(
            NativePdh::wide_string(pointer, &buffer, 2 * size_of::<u16>()),
            Err(Error::InvalidSample(_))
        ));
    }

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
    /// Guarantees: Native formulas, rather than receiver approximations, preserve provider semantics and uncapped values.
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

    /// Scenario: Windows formats a raw-fraction fixture whose current denominator is zero.
    /// Guarantees: The receiver preserves PDH's defined zero result instead of rejecting a valid native observation.
    #[test]
    fn native_raw_fraction_zero_base_formats_as_zero() {
        let current = PDH_RAW_COUNTER {
            CStatus: PDH_CSTATUS_VALID_DATA,
            FirstValue: 25,
            SecondValue: 0,
            ..Default::default()
        };
        let time_base = 1;
        let value = NativePdh::formatted_with("fixture", ValueFormat::Double, |flags, value| {
            // SAFETY: Current and output point to initialized storage; raw fractions need no older sample.
            unsafe {
                PdhFormatFromRawValue(0x2002_0400, flags, &time_base, &current, null(), value)
            }
        })
        .unwrap();
        assert_eq!(value, Some(Number::Double(0.0)));
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

    /// Scenario: Windows formats all four normal/inverse system-clock and 100-nanosecond multi-timer SDK types.
    /// Guarantees: Fixture coverage protects every supported formula even when the local provider exposes only a subset.
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
            PDH_CSTATUS_NO_INSTANCE,
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
            PDH_CSTATUS_NO_INSTANCE,
            PDH_CALC_NEGATIVE_VALUE,
            PDH_CALC_NEGATIVE_DENOMINATOR,
            PDH_CALC_NEGATIVE_TIMEBASE,
        ] {
            assert_eq!(NativePdh::raw_with("fixture", |_| status).unwrap(), None);
        }
        for status in [
            PDH_CSTATUS_INVALID_DATA,
            PDH_NO_DATA,
            PDH_CSTATUS_NO_INSTANCE,
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
            PDH_CSTATUS_NO_INSTANCE,
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
                operation: PdhOperation::FormattedCounterDataStatus,
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

    /// Scenario: Wildcard membership grows after PDH's size measurement and exceeds the initial headroom.
    /// Guarantees: Expansion grows geometrically from the reported fill size and returns the later complete multi-string.
    #[test]
    fn wildcard_expansion_retries_growing_membership() {
        let mut calls = 0;
        let buffer = NativePdh::expanded_path_buffer_with("fixture", |size, buffer| {
            calls += 1;
            match buffer {
                None => {
                    *size = 4;
                    PDH_MORE_DATA
                }
                Some(_) if calls == 2 => {
                    *size = 128;
                    PDH_MORE_DATA
                }
                Some(buffer) => {
                    buffer[..3].copy_from_slice(&[b'a' as u16, 0, 0]);
                    *size = 3;
                    0
                }
            }
        })
        .unwrap();

        assert_eq!(calls, 3);
        assert_eq!(buffer, [b'a' as u16, 0, 0]);
    }

    /// Scenario: PDH reports that a valid wildcard currently has no matching instances.
    /// Guarantees: Both documented no-instance status and successful zero-size responses become an empty membership set.
    #[test]
    fn wildcard_expansion_accepts_zero_matches() {
        for status in [0, PDH_CSTATUS_NO_INSTANCE] {
            let mut calls = 0;
            let buffer = NativePdh::expanded_path_buffer_with("fixture", |size, buffer| {
                calls += 1;
                assert!(buffer.is_none());
                *size = 0;
                status
            })
            .unwrap();
            assert!(buffer.is_empty());
            assert_eq!(calls, 1);
        }
    }
}
