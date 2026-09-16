// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows-only, single-core receiver for exact and calculated performance gauges.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = WINDOWSPERFCOUNTERS_RECEIVER_URN,
    target = "otel.receiver.windowsperfcounters",
);

mod metrics;
mod pdh;

use crate::receivers::windowsperfcounters::{Config, into_otap};
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
use otel_arrow_dfe_telemetry::metrics::MetricSet;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::time::MissedTickBehavior;

/// Factory identity for the Windows performance-counter receiver.
pub const WINDOWSPERFCOUNTERS_RECEIVER_URN: &str = "urn:otel:receiver:windowsperfcounters";

// Host-wide input must not be duplicated by separate nodes/pipelines. This
// process-wide atomic is only used at construction/drop, never in the hot path.
static COLLECTING: AtomicBool = AtomicBool::new(false);

pub(super) struct Lease;

#[cfg(test)]
static TEST_LEASE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

impl Lease {
    fn acquire() -> Result<Self, otel_arrow_dfe_config::error::Error> {
        let _ = COLLECTING
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error:
                    "another windowsperfcounters receiver already collects this host in this process"
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

struct WindowsPerfCountersReceiver {
    config: Config,
    worker: pdh::Worker,
    metrics: Rc<RefCell<MetricSet<metrics::WindowsPerfCountersMetrics>>>,
}

#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Receiver)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
/// Registers the Windows performance-counter local receiver factory.
pub static WINDOWSPERFCOUNTERS_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: WINDOWSPERFCOUNTERS_RECEIVER_URN,
    create: |pipeline, node, node_config, receiver_config, _capabilities| {
        if pipeline.num_cores() > 1 {
            return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error:
                    "host-wide windowsperfcounters collection requires a one-core source pipeline"
                        .to_owned(),
            });
        }
        let config = Config::from_json(&node_config.config)?;
        let mut metrics = pipeline.register_metrics::<metrics::WindowsPerfCountersMetrics>();
        metrics.configured_exact.set(
            config
                .counters
                .iter()
                .filter(|counter| !counter.path.contains('*'))
                .count() as u64,
        );
        metrics.configured_wildcard.set(
            config
                .counters
                .iter()
                .filter(|counter| counter.path.contains('*'))
                .count() as u64,
        );
        let lease = Arc::new(Lease::acquire()?);
        let worker = pdh::Worker::start(
            config.counters.clone(),
            config.wildcard_refresh_interval(),
            config.max_instances_per_wildcard,
            config.max_expanded_counters,
            lease,
        )
        .map_err(
            |err| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: format!("PDH runtime initialization failed: {err}"),
            },
        )?;
        let receiver = WindowsPerfCountersReceiver {
            config,
            worker,
            metrics: Rc::new(RefCell::new(metrics)),
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

impl WindowsPerfCountersReceiver {
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
            let scrape_start = Instant::now();
            let sample = match self.worker.collect().await {
                Ok(sample) => {
                    let mut metrics = self.metrics.borrow_mut();
                    metrics.scrapes.add(1);
                    metrics
                        .scrape_duration
                        .record(scrape_start.elapsed().as_secs_f64());
                    metrics.apply(&sample.diagnostics);
                    drop(metrics);
                    for overflow in &sample.overflows {
                        let path_template = &self.config.counters[overflow.counter_index].path;
                        otel_arrow_dfe_telemetry::otel_warn!(
                            "windowsperfcounters.instance_limit_exceeded",
                            path_template = path_template,
                            reason = overflow.reason,
                            discovered = overflow.discovered as u64,
                            retained = overflow.retained as u64,
                            omitted = overflow.omitted as u64
                        );
                    }
                    if sample.diagnostics.instances_added > 0
                        || sample.diagnostics.instances_removed > 0
                    {
                        otel_arrow_dfe_telemetry::otel_info!(
                            "windowsperfcounters.instances_changed",
                            added = sample.diagnostics.instances_added,
                            removed = sample.diagnostics.instances_removed,
                            active = sample.diagnostics.active_expanded_counters as u64
                        );
                    }
                    if sample.diagnostics.retry_attempts > 0
                        || sample.diagnostics.retry_recoveries > 0
                        || sample.diagnostics.query_rebuild_attempts > 0
                    {
                        otel_arrow_dfe_telemetry::otel_info!(
                            "windowsperfcounters.recovery",
                            retry_attempts = sample.diagnostics.retry_attempts,
                            retry_recoveries = sample.diagnostics.retry_recoveries,
                            query_rebuild_attempts = sample.diagnostics.query_rebuild_attempts,
                            query_rebuild_recoveries = sample.diagnostics.query_rebuild_recoveries
                        );
                    }
                    sample
                }
                Err(err) if err.is_collection_failure() => {
                    let mut metrics = self.metrics.borrow_mut();
                    metrics.scrape_failures.add(1);
                    metrics
                        .scrape_duration
                        .record(scrape_start.elapsed().as_secs_f64());
                    otel_arrow_dfe_telemetry::otel_warn!(
                        "windowsperfcounters.scrape_failed",
                        error = %err
                    );
                    continue;
                }
                Err(err) => return Err(failure(err.to_string())),
            };
            for counter_failure in &sample.failures {
                let path_template = &self.config.counters[counter_failure.counter_index].path;
                otel_arrow_dfe_telemetry::otel_warn!(
                    "windowsperfcounters.counter_failed",
                    path_template = path_template,
                    reason = counter_failure.reason,
                    error = counter_failure.error
                );
            }
            let Some(records) =
                into_otap(&self.config.counters, sample).map_err(|err| failure(err.to_string()))?
            else {
                continue;
            };
            let pdata = OtapPdata::new(Context::default(), records.into());
            effect_handler
                .send_message_with_source_node(pdata)
                .await
                .map_err(Error::from)?;
        }
    }
}

#[async_trait(?Send)]
impl local::Receiver<OtapPdata> for WindowsPerfCountersReceiver {
    async fn start(
        mut self: Box<Self>,
        mut ctrl_msg_recv: local::ControlChannel<OtapPdata>,
        effect_handler: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        enum Exit {
            Control { deadline: Instant, drained: bool },
            Collection(Result<TerminalState, Error>),
        }

        let exit = {
            let collect_and_send = self.collect_and_send(&effect_handler);
            tokio::pin!(collect_and_send);
            loop {
                tokio::select! {
                    biased;
                    msg = ctrl_msg_recv.recv() => match msg {
                        Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                            break Exit::Control { deadline, drained: true };
                        }
                        Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                            break Exit::Control { deadline, drained: false };
                        }
                        Ok(NodeControlMsg::CollectTelemetry { mut metrics_reporter }) => {
                            let mut metrics = self.metrics.borrow_mut();
                            let _ = metrics_reporter.report(&mut metrics);
                        }
                        Err(err) => {
                            break Exit::Collection(Err(Error::ChannelRecvError(err)));
                        }
                        _ => {}
                    },
                    result = &mut collect_and_send => break Exit::Collection(result),
                }
            }
        };

        let (deadline, drained, collection_result) = match exit {
            Exit::Control { deadline, drained } => (deadline, drained, None),
            Exit::Collection(result) => {
                (Instant::now() + Duration::from_secs(5), false, Some(result))
            }
        };
        match self.worker.shutdown(deadline).await {
            Ok(true) => {}
            Ok(false) => {
                otel_arrow_dfe_telemetry::otel_warn!(
                    "windowsperfcounters.shutdown_timeout",
                    "PDH worker still owns its query and will close it when the active call returns"
                );
            }
            Err(err) => {
                return Err(Error::ReceiverError {
                    receiver: effect_handler.receiver_id(),
                    kind: ReceiverErrorKind::Other,
                    error: err.to_string(),
                    source_detail: String::new(),
                });
            }
        }
        if let Some(result) = collection_result {
            return result;
        }
        if drained {
            effect_handler.notify_receiver_drained().await?;
        }
        let snapshot = self.metrics.borrow().snapshot();
        Ok(TerminalState::new(deadline, [snapshot]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: A receiver stops while its blocking worker still holds the collection lease.
    /// Guarantees: Duplicate collection stays rejected until the final owner releases the lease.
    #[tokio::test(flavor = "current_thread")]
    async fn lease_covers_outstanding_worker() {
        let _serial = TEST_LEASE_LOCK.lock().await;
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
