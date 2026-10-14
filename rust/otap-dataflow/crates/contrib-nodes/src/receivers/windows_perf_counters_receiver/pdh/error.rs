// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::super::model::{SampleFailure, ScaleError};
use super::counter_type::UnsupportedNativeType;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum Error {
    #[error("{operation} for {path} failed with PDH status 0x{status:08X}")]
    Pdh {
        operation: &'static str,
        path: String,
        status: u32,
    },
    /// A counter's CStatus failure, separate from a PDH API return code.
    #[error("{operation} for {path} failed with counter data status 0x{status:08X}")]
    CounterData {
        operation: &'static str,
        path: String,
        status: u32,
    },
    #[error(transparent)]
    UnsupportedType(#[from] UnsupportedNativeType),
    #[error("invalid metric mapping for {path}: {message}")]
    InvalidMetricMapping { path: String, message: String },
    #[error("counter setup for {path} failed: {message}")]
    CounterSetup {
        path: String,
        message: String,
        permanent: bool,
    },
    #[error("invalid performance-counter token for {path}")]
    InvalidCounterToken { path: String },
    #[error("invalid performance-counter sample: {0}")]
    InvalidSample(&'static str),
    #[error("counter calculation for {path} failed: {message}")]
    Calculation { path: String, message: String },
    #[error("counter scaling for {path} failed: {source}")]
    Scale { path: String, source: ScaleError },
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
    #[error("PDH query recovery is deferred for another {retry_after_ms} ms")]
    QueryRecoveryPending { retry_after_ms: u128 },
}

impl Error {
    pub(in super::super) fn is_collection_failure(&self) -> bool {
        matches!(
            self,
            Self::Pdh { .. }
                | Self::CounterData { .. }
                | Self::InvalidSample(_)
                | Self::Calculation { .. }
                | Self::Scale { .. }
                | Self::WorkerBusy
                | Self::QueryRecoveryPending { .. }
        )
    }

    pub(in super::super) fn is_overrun(&self) -> bool {
        matches!(self, Self::WorkerBusy)
    }

    pub(super) fn is_query_collection_failure(&self) -> bool {
        matches!(
            self,
            Self::Pdh {
                operation: "PdhCollectQueryData" | "PdhCollectQueryData(prime)",
                ..
            }
        )
    }

    pub(super) fn requires_immediate_query_rebuild(&self) -> bool {
        matches!(
            self,
            Self::Pdh { operation, .. }
                if matches!(*operation, "PdhRemoveCounter" | "PdhRemoveCounter(retry)")
        )
    }

    pub(super) fn reason(&self) -> &'static str {
        match self {
            Self::Pdh { operation, .. } | Self::CounterData { operation, .. } => operation,
            Self::UnsupportedType(_) => "unsupported_type",
            Self::InvalidMetricMapping { .. } => "invalid_metric_mapping",
            Self::CounterSetup { .. } => "counter_setup",
            Self::InvalidCounterToken { .. } => "invalid_counter_token",
            Self::InvalidSample(_) => "invalid_sample",
            Self::Calculation { .. } => "calculation",
            Self::Scale { .. } => "scale",
            Self::WorkerStart(_) => "worker_start",
            Self::WorkerInitTimeout => "worker_init_timeout",
            Self::WorkerInitStopped => "worker_init_stopped",
            Self::WorkerBusy => "worker_busy",
            Self::WorkerStopped => "worker_stopped",
            Self::WorkerPanicked => "worker_panicked",
            Self::QueryRecoveryPending { .. } => "query_recovery_pending",
        }
    }

    pub(super) fn bounded_detail(&self) -> String {
        match self {
            Self::Pdh { status, .. } => format!("PDH status 0x{status:08X}"),
            Self::CounterData { status, .. } => {
                format!("counter data status 0x{status:08X}")
            }
            Self::UnsupportedType(error) => {
                format!("unsupported native type 0x{:08X}", error.native_type)
            }
            Self::InvalidMetricMapping { message, .. } => message.clone(),
            Self::CounterSetup { message, .. } => message.clone(),
            Self::InvalidCounterToken { .. } => self.to_string(),
            Self::InvalidSample(message) => (*message).to_owned(),
            Self::Calculation { message, .. } => message.clone(),
            Self::Scale { source, .. } => source.to_string(),
            other => other.to_string(),
        }
    }

    pub(super) fn is_permanent_counter_error(&self) -> bool {
        matches!(
            self,
            Self::UnsupportedType(_)
                | Self::InvalidMetricMapping { .. }
                | Self::CounterSetup {
                    permanent: true,
                    ..
                }
        )
    }
}

pub(super) fn sample_failure(counter_index: usize, error: &Error) -> SampleFailure {
    SampleFailure {
        counter_index,
        reason: error.reason(),
        error: error.bounded_detail(),
        diagnostic_only: false,
    }
}
