// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Temporary local-only shim for 11 status constants missing from windows-rs
//! e978672b068a7ef1b1f494c014660f0325e977eb. Values come from Windows SDK
//! 10.0.26100.0 um/PdhMsg.h and match released windows-sys 0.61.2.
//! Full header hashes, source lines and comparisons: validation logs
//! 16-shim-provenance.json. All PDH functions/types come from Git HEAD.

pub(super) const PDH_CSTATUS_VALID_DATA: u32 = 0x0000_0000;
pub(super) const PDH_CSTATUS_NEW_DATA: u32 = 0x0000_0001;
#[cfg(test)]
pub(super) const PDH_CSTATUS_NO_INSTANCE: u32 = 0x8000_07D1;
pub(super) const PDH_MORE_DATA: u32 = 0x8000_07D2;
pub(super) const PDH_NO_DATA: u32 = 0x8000_07D5;
pub(super) const PDH_CALC_NEGATIVE_DENOMINATOR: u32 = 0x8000_07D6;
pub(super) const PDH_CALC_NEGATIVE_TIMEBASE: u32 = 0x8000_07D7;
pub(super) const PDH_CALC_NEGATIVE_VALUE: u32 = 0x8000_07D8;
pub(super) const PDH_CSTATUS_INVALID_DATA: u32 = 0xC000_0BBA;
#[cfg(test)]
pub(super) const PDH_INVALID_ARGUMENT: u32 = 0xC000_0BBD;
pub(super) const PDH_INVALID_DATA: u32 = 0xC000_0BC6;
