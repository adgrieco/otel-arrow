// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows-only, single-core receiver for exact performance counters.

use super::config::RuntimeConfig;
use super::metrics::WindowsPerfCountersReceiverMetrics;
use super::otap_builder::into_otap;
use super::pdh;
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
use otel_arrow_dfe_telemetry::common_attributes::Outcome;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::time::{MissedTickBehavior, timeout};

/// Factory identity for the Windows performance-counter receiver.
pub const WINDOWSPERFCOUNTERS_RECEIVER_URN: &str = "urn:otel:receiver:windowsperfcounters";

const WORKER_SHUTDOWN_MAX_WAIT: Duration = Duration::from_secs(1);
const PIPELINE_COMPLETION_RESERVE: Duration = Duration::from_millis(500);

struct WindowsPerfCountersReceiver {
    config: RuntimeConfig,
    node: String,
    metrics: Rc<RefCell<WindowsPerfCountersReceiverMetrics>>,
}

fn worker_shutdown_deadline(now: Instant, pipeline_deadline: Instant) -> Instant {
    let available = pipeline_deadline.saturating_duration_since(now);
    now + available
        .saturating_sub(PIPELINE_COMPLETION_RESERVE)
        .min(WORKER_SHUTDOWN_MAX_WAIT)
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
        let config = RuntimeConfig::from_json(&node_config.config)?;
        let receiver = WindowsPerfCountersReceiver {
            config,
            node: pipeline.node_id().as_ref().to_owned(),
            metrics: Rc::new(RefCell::new(WindowsPerfCountersReceiverMetrics::register(
                &pipeline,
            ))),
        };
        Ok(ReceiverWrapper::local(
            receiver,
            node,
            node_config,
            receiver_config,
        ))
    },
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: |value| RuntimeConfig::from_json(value).map(|_| ()),
};

impl WindowsPerfCountersReceiver {
    async fn collect_and_send(
        &self,
        worker: &pdh::Worker,
        effect_handler: &local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let failure = |error: String| Error::ReceiverError {
            receiver: effect_handler.receiver_id(),
            kind: ReceiverErrorKind::Other,
            error,
            source_detail: String::new(),
        };
        tokio::time::sleep(self.config.initial_delay).await;
        let mut interval = tokio::time::interval(self.config.collection_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut scrape_failed = false;
        let mut counter_failed = vec![false; self.config.counters.len()];
        loop {
            let _ = interval.tick().await;
            let sample = match timeout(self.config.collection_interval, worker.collect()).await {
                Ok(Ok(sample)) => {
                    self.metrics.borrow_mut().record_scrape(Outcome::Success);
                    scrape_failed = false;
                    sample
                }
                Ok(Err(err)) if err.is_collection_failure() => {
                    let mut metrics = self.metrics.borrow_mut();
                    metrics.record_scrape(Outcome::Failure);
                    if err.is_overrun() {
                        metrics.health.scrape_overruns.inc();
                    }
                    drop(metrics);
                    if !scrape_failed {
                        otel_warn!(
                            "otelcol.node.windowsperfcounters.scrape.fail",
                            node = self.node.as_str(),
                            error = %err
                        );
                        scrape_failed = true;
                    }
                    continue;
                }
                Ok(Err(err)) => {
                    self.metrics.borrow_mut().record_scrape(Outcome::Failure);
                    return Err(failure(err.to_string()));
                }
                Err(_) => {
                    let mut metrics = self.metrics.borrow_mut();
                    metrics.record_scrape(Outcome::Failure);
                    metrics.health.scrape_overruns.inc();
                    drop(metrics);
                    if !scrape_failed {
                        otel_warn!(
                            "otelcol.node.windowsperfcounters.scrape.timeout",
                            node = self.node.as_str(),
                            timeout_seconds = self.config.collection_interval.as_secs_f64()
                        );
                        scrape_failed = true;
                    }
                    continue;
                }
            };
            for point in &sample.points {
                let Some(failed) = counter_failed.get_mut(point.counter_index) else {
                    return Err(failure(format!(
                        "sample references missing configured counter {}",
                        point.counter_index
                    )));
                };
                *failed = false;
            }
            self.metrics
                .borrow_mut()
                .health
                .failed_counter_values
                .add(sample.failures.len() as u64);
            for counter_failure in &sample.failures {
                let Some(counter) = self.config.counters.get(counter_failure.counter_index) else {
                    return Err(failure(format!(
                        "sample failure references missing configured counter {}",
                        counter_failure.counter_index
                    )));
                };
                let failed = &mut counter_failed[counter_failure.counter_index];
                if !*failed {
                    otel_warn!(
                        "otelcol.node.windowsperfcounters.counter.fail",
                        node = self.node.as_str(),
                        path = counter.path.as_str(),
                        error = counter_failure.error.as_str()
                    );
                    *failed = true;
                }
            }
            let Some(records) =
                into_otap(&self.config.counters, sample).map_err(|err| failure(err.to_string()))?
            else {
                continue;
            };
            effect_handler
                .send_message_with_source_node(OtapPdata::new(Context::default(), records.into()))
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

        let worker_start = pdh::Worker::start(self.config.counters.clone(), self.node.clone());
        tokio::pin!(worker_start);
        let mut worker = loop {
            tokio::select! {
                biased;
                msg = ctrl_msg_recv.recv() => match msg {
                    Ok(NodeControlMsg::CollectTelemetry { mut metrics_reporter }) => {
                        let mut metrics = self.metrics.borrow_mut();
                        let _ = metrics.report(&mut metrics_reporter);
                    }
                    Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                        effect_handler.notify_receiver_drained().await?;
                        let snapshots = self.metrics.borrow_mut().terminal_snapshots();
                        return Ok(TerminalState::new(deadline, snapshots));
                    }
                    Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                        let snapshots = self.metrics.borrow_mut().terminal_snapshots();
                        return Ok(TerminalState::new(deadline, snapshots));
                    }
                    Err(err) => return Err(Error::ChannelRecvError(err)),
                    _ => {}
                },
                result = &mut worker_start => break result.map_err(|err| Error::ReceiverError {
                    receiver: effect_handler.receiver_id(),
                    kind: ReceiverErrorKind::Other,
                    error: "failed to initialize the Windows PDH query".to_owned(),
                    source_detail: err.to_string(),
                })?,
            }
        };
        let exit = {
            let collect_and_send = self.collect_and_send(&worker, &effect_handler);
            tokio::pin!(collect_and_send);
            loop {
                tokio::select! {
                    biased;
                    msg = ctrl_msg_recv.recv() => match msg {
                        Ok(NodeControlMsg::CollectTelemetry { mut metrics_reporter }) => {
                            let mut metrics = self.metrics.borrow_mut();
                            let _ = metrics.report(&mut metrics_reporter);
                        }
                        Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                            break Exit::Control { deadline, drained: true };
                        }
                        Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                            break Exit::Control { deadline, drained: false };
                        }
                        Err(err) => return Err(Error::ChannelRecvError(err)),
                        _ => {}
                    },
                    result = &mut collect_and_send => break Exit::Collection(result),
                }
            }
        };

        match exit {
            Exit::Collection(result) => result,
            Exit::Control { deadline, drained } => {
                if drained {
                    effect_handler.notify_receiver_drained().await?;
                }
                let shutdown_deadline = worker_shutdown_deadline(Instant::now(), deadline);
                if !worker.shutdown(shutdown_deadline).await.map_err(|err| {
                    Error::ReceiverError {
                        receiver: effect_handler.receiver_id(),
                        kind: ReceiverErrorKind::Other,
                        error: err.to_string(),
                        source_detail: String::new(),
                    }
                })? {
                    otel_warn!(
                        "otelcol.node.windowsperfcounters.shutdown.timeout",
                        node = self.node.as_str()
                    );
                }
                let snapshots = self.metrics.borrow_mut().terminal_snapshots();
                Ok(TerminalState::new(deadline, snapshots))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::windowsperfcounters_receiver::config::{CounterConfig, MetricKind};
    use otel_arrow_dfe_config::node::NodeUserConfig;
    use otel_arrow_dfe_engine::testing::receiver::TestRuntime;
    use otel_arrow_dfe_engine::testing::{test_node, test_pipeline_ctx};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// Scenario: Pipeline shutdown has less or more time than the worker maximum.
    /// Guarantees: Worker cleanup preserves the completion reserve and never exceeds one second.
    #[test]
    fn bounds_worker_shutdown_deadline() {
        let now = Instant::now();
        assert_eq!(
            worker_shutdown_deadline(now, now + Duration::from_millis(400)),
            now
        );
        assert_eq!(
            worker_shutdown_deadline(now, now + Duration::from_secs(5)),
            now + WORKER_SHUTDOWN_MAX_WAIT
        );
    }

    fn run_live_lifecycle(drain: bool) {
        let test_runtime = TestRuntime::<OtapPdata>::new();
        let (pipeline, _) = test_pipeline_ctx();
        let receiver = WindowsPerfCountersReceiver {
            config: RuntimeConfig {
                counters: vec![CounterConfig {
                    path: r"\Memory\Available Bytes".to_owned(),
                    name: "windows.memory.available".to_owned(),
                    unit: "By".to_owned(),
                    description: "Available physical memory.".to_owned(),
                    kind: MetricKind::Gauge,
                    attributes: BTreeMap::new(),
                    scale_power10: 0,
                }],
                collection_interval: Duration::from_secs(1),
                initial_delay: Duration::ZERO,
            },
            node: "windowsperfcounters".to_owned(),
            metrics: Rc::new(RefCell::new(WindowsPerfCountersReceiverMetrics::register(
                &pipeline,
            ))),
        };
        let receiver = ReceiverWrapper::local(
            receiver,
            test_node("windowsperfcounters"),
            Arc::new(NodeUserConfig::new_receiver_config(
                WINDOWSPERFCOUNTERS_RECEIVER_URN,
            )),
            test_runtime.config(),
        );

        test_runtime
            .set_receiver(receiver)
            .run_test(move |ctx| async move {
                ctx.sleep(Duration::from_secs(1)).await;
                let deadline = Instant::now() + Duration::from_secs(2);
                if drain {
                    ctx.send_control_msg(NodeControlMsg::DrainIngress {
                        deadline,
                        reason: "test drain".to_owned(),
                    })
                    .await
                    .unwrap();
                } else {
                    ctx.send_shutdown(deadline, "test shutdown").await.unwrap();
                }
            })
            .run_validation(|_| async {});
    }

    /// Scenario: The engine requests ingress drain and shutdown from a live receiver.
    /// Guarantees: Both lifecycle paths stop the PDH worker and complete before their deadlines.
    #[test]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    fn completes_drain_and_shutdown_through_engine_harness() {
        run_live_lifecycle(true);
        run_live_lifecycle(false);
    }
}
