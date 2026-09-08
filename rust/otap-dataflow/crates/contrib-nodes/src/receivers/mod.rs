// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

/// ETW (Event Tracing for Windows) receiver.
#[cfg(all(feature = "etw-receiver", target_os = "windows"))]
pub mod etw_receiver;

/// Portable configuration and projection for the Windows counter POC.
#[cfg(feature = "winperfcounters-receiver")]
pub mod winperfcounters;

/// Windows performance-counter receiver.
#[cfg(all(feature = "winperfcounters-receiver", target_os = "windows"))]
pub mod winperfcounters_receiver;

/// Kafka receiver.
#[cfg(feature = "kafka-receiver")]
pub mod kafka_receiver;

/// Linux user_events receiver.
#[cfg(all(feature = "user_events-receiver", target_os = "linux"))]
pub mod user_events_receiver;
