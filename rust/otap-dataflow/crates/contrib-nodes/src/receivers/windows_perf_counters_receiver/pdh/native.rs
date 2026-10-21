// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Owns native handles, aligned metadata storage, and PDH status/value decoding.
#![allow(unsafe_code)]

use super::{
    CounterLanguage, Error, InstanceIdentity, Number, PdhApi, PdhOperation, RawSample, ValueFormat,
};
use std::collections::BTreeMap;
use std::mem::{size_of, size_of_val};
use std::ptr::{null, null_mut};
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use windows_sys::Win32::Globalization::GetThreadUILanguage;
use windows_sys::Win32::System::Performance::{
    PDH_CALC_NEGATIVE_DENOMINATOR, PDH_CALC_NEGATIVE_TIMEBASE, PDH_CALC_NEGATIVE_VALUE,
    PDH_COUNTER_INFO_W, PDH_COUNTER_PATH_ELEMENTS_W, PDH_CSTATUS_INVALID_DATA,
    PDH_CSTATUS_NEW_DATA, PDH_CSTATUS_NO_INSTANCE, PDH_CSTATUS_VALID_DATA, PDH_FMT_COUNTERVALUE,
    PDH_FMT_DOUBLE, PDH_FMT_LARGE, PDH_HCOUNTER, PDH_HQUERY, PDH_INVALID_ARGUMENT,
    PDH_INVALID_DATA, PDH_MORE_DATA, PDH_NO_DATA, PDH_RAW_COUNTER, PDH_REFRESHCOUNTERS,
    PERF_DETAIL_WIZARD, PdhAddCounterW, PdhAddEnglishCounterW, PdhCloseQuery, PdhCollectQueryData,
    PdhEnumObjectsW, PdhExpandWildCardPathW, PdhGetCounterInfoW, PdhGetFormattedCounterValue,
    PdhGetRawCounterValue, PdhOpenQueryW, PdhParseCounterPathW, PdhRemoveCounter,
};

// These flags are not exposed by windows-sys 0.61.2.
const PDH_FMT_NOSCALE: u32 = 0x0000_1000;
const PDH_FMT_NOCAP100: u32 = 0x0000_8000;
pub(super) fn initialize_worker(node: &str, shutdown: &AtomicBool) -> Result<(), Error> {
    check_running(Some(shutdown))?;
    // SAFETY: This reads only the calling worker thread's UI language.
    let language = unsafe { GetThreadUILanguage() };
    otel_arrow_dfe_telemetry::otel_info!(
        "windows_perf_counters_receiver.worker.startup",
        node = node,
        thread_ui_language_id = language as u64
    );
    Ok(())
}

fn refresh_objects_with(
    mut enumerate_objects: impl FnMut(bool, &mut u32, Option<&mut [u16]>) -> Result<u32, Error>,
) -> Result<usize, Error> {
    let mut size = 0;
    let status = enumerate_objects(true, &mut size, None)?;
    if !matches!(status, 0 | PDH_MORE_DATA) || (status == PDH_MORE_DATA && size == 0) {
        Err(Error::WorkerObjectRefresh {
            stage: "refresh sizing",
            status,
        })
    } else if size == 0 {
        Ok(0)
    } else {
        let mut buffer = vec![0_u16; refresh_size(size, "refresh size conversion", status)?];
        size = refresh_size(buffer.len(), "enumeration buffer size", status)?;
        let mut status = enumerate_objects(false, &mut size, Some(&mut buffer))?;
        if status == PDH_MORE_DATA {
            let capacity: u32 = refresh_size(buffer.len(), "enumeration buffer size", status)?;
            size = size.max(capacity.checked_mul(2).ok_or(Error::WorkerObjectRefresh {
                stage: "enumeration buffer growth",
                status,
            })?);
            buffer.resize(
                refresh_size(size, "enumeration size conversion", status)?,
                0,
            );
            status = enumerate_objects(false, &mut size, Some(&mut buffer))?;
        }
        if status == 0 {
            let filled = refresh_size(size, "enumeration filled size", status)?;
            let objects = buffer.get(..filled).ok_or(Error::WorkerObjectRefresh {
                stage: "enumeration filled size",
                status,
            })?;
            Ok(NativePdh::parse_multi_sz(objects)?.len())
        } else {
            Err(Error::WorkerObjectRefresh {
                stage: "enumeration fill",
                status,
            })
        }
    }
}

fn refresh_size<T, U>(value: T, stage: &'static str, status: u32) -> Result<U, Error>
where
    U: TryFrom<T>,
{
    U::try_from(value).map_err(|_| Error::WorkerObjectRefresh { stage, status })
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
        let bytes = elements
            .checked_mul(size_of::<T>())
            .ok_or(Error::InvalidSample(capacity_error))?;
        let _ = u32::try_from(bytes).map_err(|_| Error::InvalidSample(capacity_error))?;
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

    #[cfg(test)]
    fn as_mut_ptr<U>(&mut self) -> *mut U {
        self.storage.as_mut_ptr().cast::<U>()
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.storage
    }
}

struct FixedHeaderBufferSpec {
    minimum_size: usize,
    size_operation: PdhOperation,
    fill_operation: PdhOperation,
    size_error: &'static str,
    capacity_error: &'static str,
    fill_error: &'static str,
}

const COUNTER_INFO_BUFFER: FixedHeaderBufferSpec = FixedHeaderBufferSpec {
    minimum_size: size_of::<PDH_COUNTER_INFO_W>(),
    size_operation: PdhOperation::GetCounterInfoSize,
    fill_operation: PdhOperation::GetCounterInfo,
    size_error: "PdhGetCounterInfoW returned an undersized buffer",
    capacity_error: "PdhGetCounterInfoW buffer capacity exceeds u32",
    fill_error: "PdhGetCounterInfoW returned an invalid filled size",
};

const COUNTER_PATH_BUFFER: FixedHeaderBufferSpec = FixedHeaderBufferSpec {
    minimum_size: size_of::<PDH_COUNTER_PATH_ELEMENTS_W>(),
    size_operation: PdhOperation::ParseCounterPathSize,
    fill_operation: PdhOperation::ParseCounterPath,
    size_error: "PdhParseCounterPathW returned an undersized buffer",
    capacity_error: "PdhParseCounterPathW buffer capacity exceeds u32",
    fill_error: "PdhParseCounterPathW returned an invalid filled size",
};

#[derive(Clone, Copy)]
enum BufferFillSize {
    Reported,
    #[cfg(test)]
    Capacity,
}

fn fixed_header_buffer_with<T: Clone + Default>(
    path: &str,
    spec: &FixedHeaderBufferSpec,
    fill_size: BufferFillSize,
    mut read: impl FnMut(&mut u32, Option<&mut [T]>) -> Result<u32, Error>,
) -> Result<(NativeBuffer<T>, usize), Error> {
    let mut size = 0;
    let status = read(&mut size, None)?;
    if status != PDH_MORE_DATA {
        return Err(Error::Pdh {
            operation: spec.size_operation,
            path: path.to_owned(),
            status,
        });
    }
    if (size as usize) < spec.minimum_size {
        return Err(Error::InvalidSample(spec.size_error));
    }
    let mut buffer = NativeBuffer::with_byte_capacity(size, spec.capacity_error)?;
    size = match fill_size {
        BufferFillSize::Reported => size,
        #[cfg(test)]
        BufferFillSize::Capacity => buffer.byte_capacity_u32(spec.capacity_error)?,
    };
    check(
        spec.fill_operation,
        path,
        read(&mut size, Some(buffer.as_mut_slice()))?,
    )?;
    if (size as usize) < spec.minimum_size || (size as usize) > buffer.byte_capacity() {
        return Err(Error::InvalidSample(spec.fill_error));
    }
    Ok((buffer, size as usize))
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
    shutdown: Option<Arc<AtomicBool>>,
    lookup_queries: BTreeMap<String, LookupQuery>,
}

struct LookupQuery {
    // A template awaiting removal must never join serving-query collection.
    api: NativePdh,
    pending: Option<CounterToken>,
}

pub(super) fn resolve_template<A: PdhApi>(
    api: &mut A,
    pending: &mut Option<A::Counter>,
    path: &str,
    full_path: impl FnOnce(&mut A, &A::Counter, &str) -> Result<String, Error>,
) -> Result<String, Error> {
    if let Some(counter) = pending.as_ref() {
        api.remove(counter, path)
            .map_err(|error| Error::WildcardCleanup {
                path: path.to_owned(),
                message: error.to_string(),
                lookup_failed: false,
            })?;
        *pending = None;
    }
    api.check_running()?;
    let counter = api.add(path, CounterLanguage::English)?;
    let resolved = full_path(api, &counter, path);
    let cleanup = api.remove(&counter, path);
    if cleanup.is_err() {
        *pending = Some(counter);
    }
    if resolved.as_ref().is_err_and(Error::is_stop) {
        return resolved;
    }
    match cleanup {
        Ok(()) => resolved,
        Err(cleanup) => Err(Error::WildcardCleanup {
            path: path.to_owned(),
            message: resolved.as_ref().err().map_or_else(
                || cleanup.to_string(),
                |lookup| format!("{lookup}; {cleanup}"),
            ),
            lookup_failed: resolved.is_err(),
        }),
    }
}

impl NativePdh {
    #[cfg(test)]
    pub(super) fn open(node: String) -> Result<Self, Error> {
        Self::open_controlled(node, None)
    }

    pub(super) fn open_controlled(
        node: String,
        shutdown: Option<Arc<AtomicBool>>,
    ) -> Result<Self, Error> {
        check_running(shutdown.as_deref())?;
        let mut query = null_mut();
        // SAFETY: Null selects live local data; the output handle is writable.
        check(PdhOperation::OpenQuery, "<query>", unsafe {
            PdhOpenQueryW(null(), 0, &mut query)
        })?;
        #[cfg(test)]
        let _ = super::QUERY_OPENS.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            query,
            counters: CounterHandles::new(),
            node,
            shutdown,
            lookup_queries: BTreeMap::new(),
        })
    }

    #[cfg(test)]
    pub(super) fn parse_instance_path(path: &str) -> Result<InstanceIdentity, Error> {
        Self::parse_instance_controlled(path, None)
    }

    #[cfg(test)]
    fn counter_info_buffer_with(
        path: &str,
        mut info: impl FnMut(&mut u32, Option<&mut [usize]>) -> u32,
    ) -> Result<(NativeBuffer<usize>, usize), Error> {
        fixed_header_buffer_with(
            path,
            &COUNTER_INFO_BUFFER,
            BufferFillSize::Capacity,
            |size, buffer| Ok(info(size, buffer)),
        )
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

    fn full_path(&mut self, counter: &CounterToken, path: &str) -> Result<String, Error> {
        let handle = self.counter_handle(counter, path)?;
        let (buffer, filled) = fixed_header_buffer_with::<usize>(
            path,
            &COUNTER_INFO_BUFFER,
            BufferFillSize::Reported,
            |size, buffer| {
                self.check_running()?;
                // SAFETY: The counter belongs to this lookup query. The helper
                // supplies aligned writable storage or null for sizing.
                Ok(unsafe {
                    PdhGetCounterInfoW(
                        handle,
                        false,
                        size,
                        buffer.map_or(null_mut(), |buffer| buffer.as_mut_ptr().cast()),
                    )
                })
            },
        )?;
        // SAFETY: A successful fill initialized the validated fixed header.
        let info = unsafe { &*buffer.as_ptr::<PDH_COUNTER_INFO_W>() };
        Self::wide_string(info.szFullPath, &buffer, filled)?
            .filter(|path| !path.is_empty())
            .ok_or(Error::InvalidSample("counter info returned no full path"))
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
        mut expand: impl FnMut(&mut u32, Option<&mut [u16]>) -> Result<u32, Error>,
    ) -> Result<Vec<u16>, Error> {
        let mut size = 0;
        let size_status = expand(&mut size, None)?;
        if size_status == PDH_CSTATUS_NO_INSTANCE || (size_status == 0 && size == 0) {
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
            size = u32::try_from(capacity)
                .map_err(|_| Error::InvalidSample("wildcard path list size exceeds u32"))?;
            let mut buffer = vec![0_u16; capacity];
            let status = expand(&mut size, Some(&mut buffer))?;
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
            self.check_running()?;
            // SAFETY: The path is NUL-terminated. The optional buffer, when present,
            // is writable for the size supplied to PDH.
            Ok(unsafe {
                PdhExpandWildCardPathW(
                    null(),
                    wide.as_ptr(),
                    buffer.map_or(null_mut(), |buffer| buffer.as_mut_ptr()),
                    size,
                    PDH_REFRESHCOUNTERS,
                )
            })
        });
        self.check_running()?;
        let buffer = buffer?;
        if buffer.is_empty() {
            Ok(Vec::new())
        } else {
            Self::parse_multi_sz(&buffer)
        }
    }

    fn parse_instance_controlled(
        path: &str,
        shutdown: Option<&AtomicBool>,
    ) -> Result<InstanceIdentity, Error> {
        let wide_path: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
        let result = Self::parse_instance_identity_with(path, |size, buffer| {
            check_running(shutdown)?;
            // SAFETY: The path is NUL-terminated. Sizing uses null and zero;
            // filling uses an aligned writable slice.
            Ok(unsafe {
                PdhParseCounterPathW(
                    wide_path.as_ptr(),
                    buffer.map_or(null_mut(), |buffer| buffer.as_mut_ptr().cast()),
                    size,
                    0,
                )
            })
        });
        check_running(shutdown)?;
        result
    }

    fn parse_instance_identity_with(
        path: &str,
        parse: impl FnMut(&mut u32, Option<&mut [usize]>) -> Result<u32, Error>,
    ) -> Result<InstanceIdentity, Error> {
        let (buffer, filled_size) =
            fixed_header_buffer_with(path, &COUNTER_PATH_BUFFER, BufferFillSize::Reported, parse)?;
        // SAFETY: Successful parsing initialized the fixed pointer fields.
        let elements = unsafe { &*buffer.as_ptr::<PDH_COUNTER_PATH_ELEMENTS_W>() };
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
        info: impl FnMut(&mut u32, Option<&mut [PDH_COUNTER_INFO_W]>) -> Result<u32, Error>,
    ) -> Result<u32, Error> {
        // Typed storage guarantees the header's alignment and initializes the
        // trailing buffer; PDH fills both the header and variable-sized data.
        let (mut buffer, _) =
            fixed_header_buffer_with(path, &COUNTER_INFO_BUFFER, BufferFillSize::Reported, info)?;
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

    fn check_running(&self) -> Result<(), Error> {
        check_running(self.shutdown.as_deref())
    }

    fn add(&mut self, path: &str, language: CounterLanguage) -> Result<CounterToken, Error> {
        self.check_running()?;
        let generation = self.counters.take_generation(path)?;
        let wide_path: Vec<u16> = path.encode_utf16().chain(Some(0)).collect();
        let mut handle = null_mut();
        // SAFETY: The adapter owns the live query, path is NUL-terminated,
        // and the output handle is writable.
        let (status, operation) = unsafe {
            match language {
                CounterLanguage::English => (
                    PdhAddEnglishCounterW(self.query, wide_path.as_ptr(), 0, &mut handle),
                    PdhOperation::AddEnglishCounter,
                ),
                CounterLanguage::Localized => (
                    PdhAddCounterW(self.query, wide_path.as_ptr(), 0, &mut handle),
                    PdhOperation::AddCounter,
                ),
            }
        };
        check(operation, path, status)?;
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
            self.check_running()?;
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
        self.check_running()?;
        // SAFETY: The adapter owns the live query and is borrowed exclusively.
        Self::collect_status(unsafe { PdhCollectQueryData(self.query) })
    }

    fn raw(&mut self, counter: &CounterToken, path: &str) -> Result<Option<RawSample>, Error> {
        self.check_running()?;
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
        self.check_running()?;
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
        self.check_running()?;
        if !self.lookup_queries.contains_key(path) {
            let api = Self::open_controlled(self.node.clone(), self.shutdown.clone())?;
            let _ = self
                .lookup_queries
                .insert(path.to_owned(), LookupQuery { api, pending: None });
        }
        let lookup = self
            .lookup_queries
            .get_mut(path)
            .expect("lookup query was inserted");
        let localized =
            resolve_template(&mut lookup.api, &mut lookup.pending, path, Self::full_path)?;
        self.expand_wildcard(&localized)
    }

    fn refresh_objects(&mut self) -> Result<usize, Error> {
        let started = Instant::now();
        let result = refresh_objects_with(|refresh, size, buffer| {
            self.check_running()?;
            // SAFETY: Null selects the live local source and machine. The output
            // buffer is writable for the supplied UTF-16 element count.
            Ok(unsafe {
                PdhEnumObjectsW(
                    null(),
                    null(),
                    buffer.map_or(null_mut(), |buffer| buffer.as_mut_ptr()),
                    size,
                    PERF_DETAIL_WIZARD,
                    refresh.into(),
                )
            })
        });
        otel_arrow_dfe_telemetry::otel_info!(
            "windows_perf_counters_receiver.objects.refresh",
            node = self.node.as_str(),
            duration_ms = started.elapsed().as_secs_f64() * 1000.0,
            object_count = ?result.as_ref().ok(),
            error = ?result.as_ref().err()
        );
        result
    }

    fn parse_instance(&mut self, path: &str) -> Result<InstanceIdentity, Error> {
        Self::parse_instance_controlled(path, self.shutdown.as_deref())
    }
}

fn check_running(shutdown: Option<&AtomicBool>) -> Result<(), Error> {
    if shutdown.is_some_and(|stop| stop.load(Ordering::Acquire)) {
        Err(Error::WorkerStopRequested)
    } else {
        Ok(())
    }
}

impl Drop for NativePdh {
    fn drop(&mut self) {
        // SAFETY: Construction opened this exclusively owned query. Drop is
        // its only close path; no counter can be read through this adapter afterward.
        let status = unsafe { PdhCloseQuery(self.query) };
        #[cfg(test)]
        let _ = super::QUERY_CLOSES.fetch_add(1, Ordering::Relaxed);
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
    use super::super::super::TEST_PDH_LOCK;
    use super::*;
    use std::cell::RefCell;
    use windows_sys::Win32::System::Performance::PdhFormatFromRawValue;

    /// Scenario: Native startup and local object-list refresh run in the calling thread's existing UI language.
    /// Guarantees: Refresh returns an object count without changing language or requiring English as a startup prerequisite.
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    async fn live_object_refresh_preserves_thread_language() {
        let _serial = TEST_PDH_LOCK.lock().await;
        // SAFETY: This reads the current test thread's native UI language.
        let before = unsafe { GetThreadUILanguage() };
        initialize_worker("object-refresh", &AtomicBool::new(false)).unwrap();
        let mut api = NativePdh::open("object-refresh".to_owned()).unwrap();
        assert!(api.refresh_objects().unwrap() > 0);
        // SAFETY: This reads the same thread after startup and refresh.
        assert_eq!(unsafe { GetThreadUILanguage() }, before);
    }

    /// Scenario: Object-list sizing succeeds or requests a buffer, and the first fill may require growth.
    /// Guarantees: Only sizing refreshes the local list; fill retries without another refresh and returns the object count.
    #[test]
    fn object_refresh_sizing_and_fill_follow_native_probe_sequence() {
        for sizing_status in [0, PDH_MORE_DATA] {
            for grow in [false, true] {
                let calls = RefCell::new(Vec::new());
                let count = refresh_objects_with(|refresh, size, buffer| {
                    calls.borrow_mut().push((refresh, buffer.is_some()));
                    match buffer {
                        None => {
                            *size = 8;
                            Ok(sizing_status)
                        }
                        Some(buffer) if grow && calls.borrow().len() == 2 => {
                            assert_eq!(buffer.len(), 8);
                            *size = 24;
                            Ok(PDH_MORE_DATA)
                        }
                        Some(buffer) => {
                            assert_eq!(buffer.len(), if grow { 24 } else { 8 });
                            buffer[..5].copy_from_slice(&[65, 0, 66, 0, 0]);
                            *size = 5;
                            Ok(0)
                        }
                    }
                })
                .unwrap();
                assert_eq!(count, 2);
                let mut expected = vec![(true, false), (false, true)];
                if grow {
                    expected.push((false, true));
                }
                assert_eq!(calls.into_inner(), expected);
            }
        }
    }

    /// Scenario: A conditional object-list refresh fails during sizing or buffer fill.
    /// Guarantees: Both native stages preserve an explicit typed error for the informational refresh result.
    #[test]
    fn worker_rejects_failed_object_refresh() {
        let sizing = refresh_objects_with(|_, _, _| Ok(5)).unwrap_err();
        assert!(matches!(
            sizing,
            Error::WorkerObjectRefresh {
                stage: "refresh sizing",
                status: 5,
            }
        ));

        let fill = refresh_objects_with(|_, size, buffer| {
            if buffer.is_none() {
                *size = 8;
                Ok(PDH_MORE_DATA)
            } else {
                Ok(6)
            }
        })
        .unwrap_err();
        assert!(matches!(
            fill,
            Error::WorkerObjectRefresh {
                stage: "enumeration fill",
                status: 6,
            }
        ));
    }

    /// Scenario: Object membership keeps growing beyond the single allowed fill retry.
    /// Guarantees: Refresh returns a typed failure after two fills and never refreshes a second time.
    #[test]
    fn object_refresh_fill_growth_is_bounded() {
        let mut calls = 0;
        let error = refresh_objects_with(|refresh, size, buffer| {
            calls += 1;
            assert_eq!(refresh, buffer.is_none());
            *size = size.saturating_add(8);
            Ok(PDH_MORE_DATA)
        })
        .unwrap_err();
        assert_eq!(calls, 3);
        assert!(matches!(
            error,
            Error::WorkerObjectRefresh {
                stage: "enumeration fill",
                status: PDH_MORE_DATA
            }
        ));
    }

    /// Scenario: A stop request arrives between native sizing and fill operations.
    /// Guarantees: Object refresh, expansion and path parsing return a typed stop immediately without regrowing buffers.
    #[test]
    fn native_buffer_calls_propagate_stop_without_retry() {
        let mut calls = 0;
        let error = refresh_objects_with(|_, size, buffer| {
            calls += 1;
            if buffer.is_none() {
                *size = 8;
                Ok(PDH_MORE_DATA)
            } else {
                Err(Error::WorkerStopRequested)
            }
        })
        .unwrap_err();
        assert!(matches!(error, Error::WorkerStopRequested));
        assert_eq!(calls, 2);
        calls = 0;
        let error = NativePdh::expanded_path_buffer_with("fixture", |size, buffer| {
            calls += 1;
            if buffer.is_none() {
                *size = 8;
                Ok(PDH_MORE_DATA)
            } else {
                Err(Error::WorkerStopRequested)
            }
        })
        .unwrap_err();
        assert!(matches!(error, Error::WorkerStopRequested));
        assert_eq!(calls, 2);
        calls = 0;
        let error = NativePdh::parse_instance_identity_with("fixture", |size, buffer| {
            calls += 1;
            if buffer.is_none() {
                *size = u32::try_from(size_of::<PDH_COUNTER_PATH_ELEMENTS_W>()).unwrap();
                Ok(PDH_MORE_DATA)
            } else {
                Err(Error::WorkerStopRequested)
            }
        })
        .unwrap_err();
        assert!(matches!(error, Error::WorkerStopRequested));
        assert_eq!(calls, 2);
        assert!(matches!(
            check_running(Some(&AtomicBool::new(true))),
            Err(Error::WorkerStopRequested)
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

        fn fill_operation(self) -> PdhOperation {
            match self {
                Self::CounterInfo | Self::CounterType => PdhOperation::GetCounterInfo,
                Self::ParseInstance => PdhOperation::ParseCounterPath,
            }
        }

        fn malformed_error(self, case: MalformedBuffer) -> Error {
            let header = self.header_size();
            let status = |size: &mut u32, filling: bool| match case {
                MalformedBuffer::NonSizingStatus => {
                    *size = header;
                    PDH_INVALID_ARGUMENT
                }
                MalformedBuffer::SuccessSizingStatus => {
                    *size = header;
                    0
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
                MalformedBuffer::FailedFill if filling => PDH_MORE_DATA,
                MalformedBuffer::UndersizedFill
                | MalformedBuffer::OversizedFill
                | MalformedBuffer::FailedFill => {
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
                        Ok(status(size, buffer.is_some()))
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
        SuccessSizingStatus,
        UndersizedRequest,
        UndersizedFill,
        OversizedFill,
        FailedFill,
    }

    fn parse_instance_fixture_error(
        filled_size: u32,
        mut configure: impl FnMut(*mut u8, *mut PDH_COUNTER_PATH_ELEMENTS_W),
    ) -> Error {
        NativePdh::parse_instance_identity_with("fixture", |size, buffer| {
            let Some(buffer) = buffer else {
                *size = filled_size;
                return Ok(PDH_MORE_DATA);
            };
            let base = buffer.as_mut_ptr().cast::<u8>();
            configure(base, base.cast());
            *size = filled_size;
            Ok(0)
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
    /// Guarantees: Counter info, counter type, and parsed instance paths reject non-sizing status, undersized headers, invalid filled sizes, and failed fills.
    #[test]
    fn native_metadata_buffers_reject_malformed_size_and_fill_contracts() {
        for api in [
            BufferApi::CounterInfo,
            BufferApi::CounterType,
            BufferApi::ParseInstance,
        ] {
            for case in [
                MalformedBuffer::NonSizingStatus,
                MalformedBuffer::SuccessSizingStatus,
                MalformedBuffer::UndersizedRequest,
                MalformedBuffer::UndersizedFill,
                MalformedBuffer::OversizedFill,
                MalformedBuffer::FailedFill,
            ] {
                let error = api.malformed_error(case);
                let expected = match case {
                    MalformedBuffer::NonSizingStatus => {
                        Some((api.size_operation(), PDH_INVALID_ARGUMENT))
                    }
                    MalformedBuffer::SuccessSizingStatus => Some((api.size_operation(), 0)),
                    MalformedBuffer::FailedFill => Some((api.fill_operation(), PDH_MORE_DATA)),
                    _ => None,
                };
                match expected {
                    Some((expected_operation, expected_status)) => assert!(matches!(
                        error,
                        Error::Pdh { operation, status, path }
                            if operation == expected_operation
                                && status == expected_status
                                && path == "fixture"
                    )),
                    None => assert!(matches!(error, Error::InvalidSample(_))),
                }
            }
        }
    }

    /// Scenario: Metadata sizing rounds storage up, while fills report exactly the header or the full allocation.
    /// Guarantees: The shared helper preserves reported-versus-capacity fill inputs and accepts both valid filled-size boundaries.
    #[test]
    fn fixed_header_buffer_preserves_fill_size_boundaries() {
        let header = COUNTER_INFO_BUFFER.minimum_size;
        let requested = u32::try_from(header + 1).unwrap();
        let capacity = (requested as usize).div_ceil(size_of::<usize>()) * size_of::<usize>();
        for fill_size in [BufferFillSize::Reported, BufferFillSize::Capacity] {
            for filled in [header, capacity] {
                let mut calls = 0;
                let (buffer, filled_size) = fixed_header_buffer_with::<usize>(
                    "fixture",
                    &COUNTER_INFO_BUFFER,
                    fill_size,
                    |size, buffer| {
                        calls += 1;
                        let Some(buffer) = buffer else {
                            assert_eq!(*size, 0);
                            *size = requested;
                            return Ok(PDH_MORE_DATA);
                        };
                        let expected_size = match fill_size {
                            BufferFillSize::Reported => requested as usize,
                            BufferFillSize::Capacity => capacity,
                        };
                        assert_eq!(*size as usize, expected_size);
                        assert_eq!(size_of_val(buffer), capacity);
                        *size = u32::try_from(filled).unwrap();
                        Ok(0)
                    },
                )
                .unwrap();
                assert_eq!(calls, 2);
                assert_eq!(buffer.byte_capacity(), capacity);
                assert_eq!(filled_size, filled);
            }
        }
    }

    /// Scenario: Native sizing overflows rounded capacity or wildcard headroom, or returns an unaligned string.
    /// Guarantees: Overflow fails before allocation and string decoding checks UTF-16 alignment.
    #[test]
    fn native_size_overflow_and_pointer_alignment_are_checked() {
        assert!(matches!(
            NativeBuffer::<usize>::with_byte_capacity(u32::MAX, "overflow"),
            Err(Error::InvalidSample("overflow"))
        ));
        let error = NativePdh::expanded_path_buffer_with("fixture", |size, buffer| {
            assert!(
                buffer.is_none(),
                "overflow must fail before fill allocation"
            );
            *size = u32::MAX;
            Ok(PDH_MORE_DATA)
        })
        .unwrap_err();
        assert!(matches!(error, Error::InvalidSample(_)));
        let buffer = NativeBuffer::<usize>::with_byte_capacity(32, "fixture").unwrap();
        let pointer = buffer.as_ptr::<u8>().wrapping_add(1).cast::<u16>();
        assert!(matches!(
            NativePdh::wide_string(pointer, &buffer, 32),
            Err(Error::InvalidSample(_))
        ));
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
            Ok(match buffer {
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
            })
        })
        .unwrap();

        assert_eq!(calls, 3);
        assert_eq!(buffer, [b'a' as u16, 0, 0]);
    }

    /// Scenario: PDH reports that a valid wildcard currently has no matching instances.
    /// Guarantees: Both documented no-instance status and successful zero-size responses become an empty membership set.
    #[test]
    fn wildcard_expansion_accepts_zero_matches() {
        for (status, reported) in [
            (0, 0),
            (PDH_CSTATUS_NO_INSTANCE, 0),
            (PDH_CSTATUS_NO_INSTANCE, 37),
        ] {
            let mut calls = 0;
            let buffer = NativePdh::expanded_path_buffer_with("fixture", |size, buffer| {
                calls += 1;
                assert!(buffer.is_none());
                *size = reported;
                Ok(status)
            })
            .unwrap();
            assert!(buffer.is_empty());
            assert_eq!(calls, 1);
        }
    }
}
