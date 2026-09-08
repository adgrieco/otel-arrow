// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows-only, single-core receiver for the fixed memory-gauge POC.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = WINPERFCOUNTERS_RECEIVER_URN,
    target = "otel.receiver.winperfcounters",
);

mod pdh;

use crate::receivers::winperfcounters::{Config, into_otap};
use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::{Error, ReceiverErrorKind};
use otel_arrow_dfe_engine::local::receiver as local;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{MessageSourceLocalEffectHandlerExtension, ReceiverFactory};
use otel_arrow_dfe_otap::OTAP_RECEIVER_FACTORIES;
use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};
use otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::MissedTickBehavior;

/// Factory identity for the Windows performance-counter receiver.
pub const WINPERFCOUNTERS_RECEIVER_URN: &str = "urn:otel:receiver:winperfcounters";

// Host-wide input must not be duplicated by separate nodes/pipelines. This
// process-wide atomic is only used at construction/drop, never in the hot path.
static COLLECTING: AtomicBool = AtomicBool::new(false);

struct Lease;

impl Lease {
    fn acquire() -> Result<Self, otel_arrow_dfe_config::error::Error> {
        let _ = COLLECTING
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error:
                    "another winperfcounters receiver already collects this host in this process"
                        .to_owned(),
            })?;
        Ok(Self)
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        COLLECTING.store(false, Ordering::Release);
    }
}

struct WinPerfCountersReceiver {
    config: Config,
    lease: Arc<Lease>,
}

#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Receiver)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
/// Registers the Windows performance-counter local receiver factory.
pub static WINPERFCOUNTERS_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: WINPERFCOUNTERS_RECEIVER_URN,
    create: |pipeline, node, node_config, receiver_config, _capabilities| {
        if pipeline.num_cores() > 1 {
            return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: "host-wide winperfcounters collection requires a one-core source pipeline"
                    .to_owned(),
            });
        }
        let config = Config::from_json(&node_config.config)?;
        let receiver = WinPerfCountersReceiver {
            config,
            lease: Arc::new(Lease::acquire()?),
        };
        Ok(ReceiverWrapper::local(
            receiver,
            node,
            node_config,
            receiver_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: |value| Config::from_json(value).map(|_| ()),
};

impl WinPerfCountersReceiver {
    async fn collect_and_send(
        &self,
        effect_handler: &local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let failure = |error: String| Error::ReceiverError {
            receiver: effect_handler.receiver_id(),
            kind: ReceiverErrorKind::Other,
            error,
            source_detail: String::new(),
        };
        let mut interval = tokio::time::interval(self.config.collection_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            let _ = interval.tick().await;
            // Blocking PDH calls must not stall the thread-per-core runtime.
            // The Arc retains exclusivity even if shutdown drops this future
            // while an uncancellable Windows call is still completing.
            let lease = Arc::clone(&self.lease);
            let sample = tokio::task::spawn_blocking(move || {
                let _lease = lease;
                pdh::collect()
            })
            .await
            .map_err(|err| failure(format!("PDH worker failed: {err}")))?
            .map_err(|err| failure(err.to_string()))?;
            let records = into_otap(sample).map_err(|err| failure(err.to_string()))?;
            let pdata = OtapPdata::new(Context::default(), records.into());
            effect_handler
                .send_message_with_source_node(pdata)
                .await
                .map_err(Error::from)?;
        }
    }
}

#[async_trait(?Send)]
impl local::Receiver<OtapPdata> for WinPerfCountersReceiver {
    async fn start(
        self: Box<Self>,
        mut ctrl_msg_recv: local::ControlChannel<OtapPdata>,
        effect_handler: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let collect_and_send = self.collect_and_send(&effect_handler);
        tokio::pin!(collect_and_send);
        loop {
            tokio::select! {
                biased;
                msg = ctrl_msg_recv.recv() => match msg {
                    Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                        effect_handler.notify_receiver_drained().await?;
                        return Ok(TerminalState::new::<[MetricSetSnapshot; 0]>(deadline, []));
                    }
                    Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                        return Ok(TerminalState::new::<[MetricSetSnapshot; 0]>(deadline, []));
                    }
                    Err(err) => return Err(Error::ChannelRecvError(err)),
                    _ => {}
                },
                result = &mut collect_and_send => return result,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: A receiver stops while its blocking worker still holds the collection lease.
    /// Guarantees: Duplicate collection stays rejected until the final owner releases the lease.
    #[test]
    fn lease_covers_outstanding_worker() {
        let receiver = Arc::new(Lease::acquire().unwrap());
        let worker = Arc::clone(&receiver);
        assert!(Lease::acquire().is_err());
        drop(receiver);
        assert!(Lease::acquire().is_err());
        drop(worker);
        let next_receiver = Lease::acquire().unwrap();
        drop(next_receiver);
    }
}
