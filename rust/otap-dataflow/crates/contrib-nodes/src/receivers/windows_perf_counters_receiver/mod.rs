// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows performance-counter receiver.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = runtime::WINDOWSPERFCOUNTERS_RECEIVER_URN,
    target = "otel.receiver.windowsperfcounters",
);

mod config;
mod metrics;
mod model;
mod native_type;
mod otap_builder;
#[cfg(target_os = "windows")]
mod pdh;
#[cfg(target_os = "windows")]
mod runtime;

#[cfg(test)]
static TEST_PDH_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
