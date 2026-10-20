// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::super::model::{SampleFailure, ScaleError};
use super::counter_type::UnsupportedNativeType;
use std::sync::Arc;
use windows_sys::Win32::System::Performance::PDH_INVALID_HANDLE;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum PdhOperation {
    AddEnglishCounter,
    CollectQueryData,
    #[cfg(test)]
    CounterDataStatus,
    ExpandWildcardPath,
    ExpandWildcardPathSize,
    FormattedCounterDataStatus,
    GetCounterInfo,
    GetCounterInfoSize,
    GetFormattedCounterValue,
    GetRawCounterValue,
    OpenQuery,
    ParseCounterPath,
    ParseCounterPathSize,
    RawCounterDataStatus,
    RemoveCounter,
}

impl PdhOperation {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::AddEnglishCounter => "PdhAddEnglishCounterW",
            Self::CollectQueryData => "PdhCollectQueryData",
            #[cfg(test)]
            Self::CounterDataStatus => "counter CStatus",
            Self::ExpandWildcardPath => "PdhExpandWildCardPathW",
            Self::ExpandWildcardPathSize => "PdhExpandWildCardPathW(size)",
            Self::FormattedCounterDataStatus => "formatted counter CStatus",
            Self::GetCounterInfo => "PdhGetCounterInfoW",
            Self::GetCounterInfoSize => "PdhGetCounterInfoW(size)",
            Self::GetFormattedCounterValue => "PdhGetFormattedCounterValue",
            Self::GetRawCounterValue => "PdhGetRawCounterValue",
            Self::OpenQuery => "PdhOpenQueryW",
            Self::ParseCounterPath => "PdhParseCounterPathW",
            Self::ParseCounterPathSize => "PdhParseCounterPathW(size)",
            Self::RawCounterDataStatus => "raw counter CStatus",
            Self::RemoveCounter => "PdhRemoveCounter",
        }
    }
}

impl std::fmt::Display for PdhOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum Error {
    #[error("{operation} for {path} failed with PDH status 0x{status:08X}")]
    Pdh {
        operation: PdhOperation,
        path: String,
        status: u32,
    },
    /// A counter's CStatus failure, separate from a PDH API return code.
    #[error("{operation} for {path} failed with counter data status 0x{status:08X}")]
    CounterData {
        operation: PdhOperation,
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
        cleanup_failed: bool,
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
    #[error(
        "PDH worker failed to pin UI language to 0x{requested:04X}; SetThreadUILanguage returned 0x{returned:04X}"
    )]
    WorkerLanguagePin { requested: u16, returned: u16 },
    #[error("PDH worker object-list {stage} failed with status 0x{status:08X}")]
    WorkerObjectRefresh { stage: &'static str, status: u32 },
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
    #[error("replacement query capacity {available} is below its current reservation {reserved}")]
    QueryRecreationCapacity { available: usize, reserved: usize },
}

impl Error {
    pub(in super::super) fn is_collection_failure(&self) -> bool {
        matches!(
            self,
            Self::Pdh { .. }
                | Self::CounterData { .. }
                | Self::UnsupportedType(_)
                | Self::InvalidMetricMapping { .. }
                | Self::CounterSetup { .. }
                | Self::InvalidCounterToken { .. }
                | Self::Calculation { .. }
                | Self::Scale { .. }
                | Self::QueryRecoveryPending { .. }
        )
    }

    pub(super) fn is_query_collection_failure(&self) -> bool {
        matches!(
            self,
            Self::Pdh {
                operation: PdhOperation::CollectQueryData,
                ..
            }
        )
    }

    pub(super) fn requires_immediate_query_rebuild(&self) -> bool {
        matches!(
            self,
            Self::Pdh {
                operation: PdhOperation::RemoveCounter,
                ..
            }
        ) || matches!(
            self,
            Self::CounterSetup {
                cleanup_failed: true,
                ..
            }
        )
    }

    pub(super) fn is_recoverable_query_open_failure(&self) -> bool {
        matches!(
            self,
            Self::Pdh {
                operation: PdhOperation::OpenQuery,
                ..
            }
        ) || self.requires_immediate_query_rebuild()
    }

    pub(super) fn is_invalid_counter_handle_read(&self) -> bool {
        matches!(
            self,
            Self::Pdh {
                operation:
                    PdhOperation::GetRawCounterValue | PdhOperation::GetFormattedCounterValue,
                status,
                ..
            } if *status == PDH_INVALID_HANDLE
        )
    }

    pub(super) fn reason(&self) -> &'static str {
        match self {
            Self::Pdh { operation, .. } | Self::CounterData { operation, .. } => operation.as_str(),
            Self::UnsupportedType(_) => "unsupported_type",
            Self::InvalidMetricMapping { .. } => "invalid_metric_mapping",
            Self::CounterSetup { .. } => "counter_setup",
            Self::InvalidCounterToken { .. } => "invalid_counter_token",
            Self::InvalidSample(_) => "invalid_sample",
            Self::Calculation { .. } => "calculation",
            Self::Scale { .. } => "scale",
            Self::WorkerStart(_) => "worker_start",
            Self::WorkerLanguagePin { .. } => "worker_language_pin",
            Self::WorkerObjectRefresh { .. } => "worker_object_refresh",
            Self::WorkerInitStopped => "worker_init_stopped",
            Self::WorkerBusy => "worker_busy",
            Self::WorkerStopped => "worker_stopped",
            Self::WorkerPanicked => "worker_panicked",
            Self::QueryRecoveryPending { .. } => "query_recovery_pending",
            Self::QueryRecreationCapacity { .. } => "query_recreation_capacity",
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

    fn path(&self) -> Option<&str> {
        match self {
            Self::Pdh { path, .. }
            | Self::CounterData { path, .. }
            | Self::InvalidMetricMapping { path, .. }
            | Self::CounterSetup { path, .. }
            | Self::InvalidCounterToken { path }
            | Self::Calculation { path, .. }
            | Self::Scale { path, .. } => Some(path),
            _ => None,
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

    pub(super) fn with_cleanup_failure(path: &str, primary: Option<Self>, cleanup: Self) -> Self {
        let permanent = primary
            .as_ref()
            .is_some_and(Self::is_permanent_counter_error);
        let cleanup = cleanup.to_string();
        let message = primary.map_or_else(
            || cleanup.clone(),
            |primary| format!("{primary}; {cleanup}"),
        );
        Self::CounterSetup {
            path: path.to_owned(),
            message,
            permanent,
            cleanup_failed: true,
        }
    }
}

pub(super) fn sample_failure(counter_index: usize, error: &Error) -> SampleFailure {
    SampleFailure {
        counter_index,
        path: Arc::from(error.path().unwrap_or("<worker>")),
        reason: error.reason(),
        error: error.bounded_detail(),
        diagnostic_only: false,
    }
}
