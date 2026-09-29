// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows performance-counter receiver.

mod config;
mod metrics;
mod model;
mod otap_builder;
#[cfg(target_os = "windows")]
mod pdh;
#[cfg(target_os = "windows")]
mod runtime;

#[cfg(test)]
static TEST_PDH_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
