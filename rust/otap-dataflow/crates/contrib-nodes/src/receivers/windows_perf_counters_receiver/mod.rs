// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows performance-counter receiver.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = runtime::WINDOWSPERFCOUNTERS_RECEIVER_URN,
    target = "otel.receiver.windows_perf_counters",
);

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

#[cfg(test)]
mod test_utils {
    use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otel_arrow_dfe_pdata::{OtlpProtoBytes, TryIntoWithOptions};
    use otel_arrow_dfe_pdata_codec::OtapPayload;
    use prost::Message;

    pub(super) fn decode(records: OtapArrowRecords) -> ExportMetricsServiceRequest {
        let bytes: OtlpProtoBytes = OtapPayload::from_otap(records)
            .try_into_with_default()
            .unwrap();
        ExportMetricsServiceRequest::decode(bytes.as_bytes()).unwrap()
    }
}
