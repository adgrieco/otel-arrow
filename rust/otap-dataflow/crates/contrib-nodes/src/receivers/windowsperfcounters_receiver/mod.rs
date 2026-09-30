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
mod otap_builder;
mod pdh;
mod runtime;
