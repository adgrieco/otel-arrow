// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows-only, single-core receiver for exact and calculated performance metrics.

use super::config::Config;
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
use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::time::{MissedTickBehavior, timeout};

/// Factory identity for the Windows performance-counter receiver.
pub const WINDOWSPERFCOUNTERS_RECEIVER_URN: &str = "urn:otel:receiver:windowsperfcounters";

const WORKER_SHUTDOWN_MAX_WAIT: Duration = Duration::from_secs(1);
const PIPELINE_COMPLETION_RESERVE: Duration = Duration::from_millis(500);

struct WindowsPerfCountersReceiver {
    config: Config,
    node: String,
    metrics: Rc<RefCell<WindowsPerfCountersReceiverMetrics>>,
}

async fn wait_initial_delay(delay: Duration) {
    tokio::time::sleep(delay).await;
}

fn worker_shutdown_deadline(now: Instant, pipeline_deadline: Instant) -> Instant {
    let available = pipeline_deadline.saturating_duration_since(now);
    let worker_budget = available
        .saturating_sub(PIPELINE_COMPLETION_RESERVE)
        .min(WORKER_SHUTDOWN_MAX_WAIT);
    now + worker_budget
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
        let mut metrics = WindowsPerfCountersReceiverMetrics::register(&pipeline);
        metrics.health.configured_exact.set(
            config
                .counters
                .iter()
                .filter(|counter| !counter.path.contains('*'))
                .count() as u64,
        );
        metrics.health.configured_wildcard.set(
            config
                .counters
                .iter()
                .filter(|counter| counter.path.contains('*'))
                .count() as u64,
        );
        let receiver = WindowsPerfCountersReceiver {
            config,
            node: node.name.to_string(),
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
        worker: &pdh::Worker,
        effect_handler: &local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let failure = |error: String| Error::ReceiverError {
            receiver: effect_handler.receiver_id(),
            kind: ReceiverErrorKind::Other,
            error,
            source_detail: String::new(),
        };
        wait_initial_delay(self.config.initial_delay).await;
        let mut interval = tokio::time::interval(self.config.collection_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut scrape_failed = false;
        let mut active_counter_failures = BTreeSet::new();
        loop {
            let _ = interval.tick().await;
            let scrape_start = Instant::now();
            let sample = match timeout(self.config.collection_interval, worker.collect()).await {
                Ok(Ok(sample)) => {
                    let mut metrics = self.metrics.borrow_mut();
                    metrics.record_scrape(Outcome::Success);
                    metrics.health.scrapes.add(1);
                    metrics
                        .health
                        .scrape_duration
                        .record(scrape_start.elapsed().as_secs_f64());
                    metrics.health.apply(&sample.diagnostics);
                    metrics
                        .health
                        .failed_counter_values
                        .add(sample.failures.len() as u64);
                    drop(metrics);
                    scrape_failed = false;
                    for overflow in &sample.overflows {
                        let path_template = &self.config.counters[overflow.counter_index].path;
                        otel_arrow_dfe_telemetry::otel_warn!(
                            "otelcol.node.windowsperfcounters.instance_limit.exceeded",
                            node = self.node.as_str(),
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
                            "otelcol.node.windowsperfcounters.instances.change",
                            node = self.node.as_str(),
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
                            "otelcol.node.windowsperfcounters.recovery",
                            node = self.node.as_str(),
                            retry_attempts = sample.diagnostics.retry_attempts,
                            retry_recoveries = sample.diagnostics.retry_recoveries,
                            query_rebuild_attempts = sample.diagnostics.query_rebuild_attempts,
                            query_rebuild_recoveries = sample.diagnostics.query_rebuild_recoveries
                        );
                    }
                    sample
                }
                Ok(Err(err)) if err.is_collection_failure() => {
                    let mut metrics = self.metrics.borrow_mut();
                    metrics.record_scrape(Outcome::Failure);
                    metrics.health.scrape_failures.add(1);
                    metrics
                        .health
                        .scrape_duration
                        .record(scrape_start.elapsed().as_secs_f64());
                    if err.is_overrun() {
                        metrics.health.scrape_overruns.inc();
                    }
                    drop(metrics);
                    if !scrape_failed {
                        otel_arrow_dfe_telemetry::otel_warn!(
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
                    metrics.health.scrape_failures.inc();
                    metrics.health.scrape_overruns.inc();
                    metrics
                        .health
                        .scrape_duration
                        .record(scrape_start.elapsed().as_secs_f64());
                    drop(metrics);
                    if !scrape_failed {
                        otel_arrow_dfe_telemetry::otel_warn!(
                            "otelcol.node.windowsperfcounters.scrape.timeout",
                            node = self.node.as_str(),
                            timeout_seconds = self.config.collection_interval.as_secs_f64()
                        );
                        scrape_failed = true;
                    }
                    continue;
                }
            };
            let mut current_counter_failures = BTreeSet::new();
            for counter_failure in &sample.failures {
                let Some(counter) = self.config.counters.get(counter_failure.counter_index) else {
                    return Err(failure(format!(
                        "sample failure references missing configured counter {}",
                        counter_failure.counter_index
                    )));
                };
                let key = (counter_failure.counter_index, counter_failure.reason);
                if !active_counter_failures.contains(&key) {
                    otel_arrow_dfe_telemetry::otel_warn!(
                        "otelcol.node.windowsperfcounters.counter.fail",
                        node = self.node.as_str(),
                        path_template = counter.path.as_str(),
                        reason = counter_failure.reason,
                        error = counter_failure.error.as_str()
                    );
                }
                let _ = current_counter_failures.insert(key);
            }
            active_counter_failures = current_counter_failures;
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

        let worker_start = pdh::Worker::start(
            self.config.counters.clone(),
            self.config.wildcard_refresh_interval(),
            self.config.max_instances_per_wildcard,
            self.config.max_expanded_counters,
            self.node.clone(),
        );
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
                        Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                            break Exit::Control { deadline, drained: true };
                        }
                        Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                            break Exit::Control { deadline, drained: false };
                        }
                        Ok(NodeControlMsg::CollectTelemetry { mut metrics_reporter }) => {
                            let mut metrics = self.metrics.borrow_mut();
                            let _ = metrics.report(&mut metrics_reporter);
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
        let worker_deadline = worker_shutdown_deadline(Instant::now(), deadline);
        match worker.shutdown(worker_deadline).await {
            Ok(true) => {}
            Ok(false) => {
                otel_arrow_dfe_telemetry::otel_warn!(
                    "otelcol.node.windowsperfcounters.shutdown.timeout",
                    node = self.node.as_str()
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
        let snapshots = self.metrics.borrow_mut().terminal_snapshots();
        Ok(TerminalState::new(deadline, snapshots))
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

    /// Scenario: Collection has a nonzero initial delay before its first request.
    /// Guarantees: The delay remains pending until the configured duration has fully elapsed.
    #[tokio::test(start_paused = true)]
    async fn initial_delay_gates_first_collection() {
        let delay = Duration::from_secs(10);
        let wait = wait_initial_delay(delay);
        tokio::pin!(wait);
        tokio::select! {
            biased;
            () = &mut wait => panic!("initial delay completed before time advanced"),
            () = tokio::task::yield_now() => {}
        }

        tokio::time::advance(delay - Duration::from_nanos(1)).await;
        tokio::select! {
            biased;
            () = &mut wait => panic!("initial delay completed before the full duration"),
            () = tokio::task::yield_now() => {}
        }
        tokio::time::advance(Duration::from_nanos(1)).await;
        wait.await;
    }

    /// Scenario: Native worker cleanup competes with receiver and pipeline teardown for one deadline.
    /// Guarantees: Worker waiting is capped and preserves time for pipeline completion.
    #[test]
    fn worker_shutdown_preserves_pipeline_completion_budget() {
        let now = Instant::now();

        assert_eq!(
            worker_shutdown_deadline(now, now + Duration::from_secs(15)),
            now + WORKER_SHUTDOWN_MAX_WAIT
        );
        assert_eq!(
            worker_shutdown_deadline(now, now + Duration::from_millis(750)),
            now + Duration::from_millis(250)
        );
        assert_eq!(
            worker_shutdown_deadline(now, now + Duration::from_millis(250)),
            now
        );
    }

    fn run_live_lifecycle(drain: bool) {
        let test_runtime = TestRuntime::<OtapPdata>::new();
        let (pipeline, _) = test_pipeline_ctx();
        let receiver = WindowsPerfCountersReceiver {
            config: Config {
                counters: vec![CounterConfig {
                    path: r"\Memory\Available Bytes".to_owned(),
                    name: "windows.memory.available".to_owned(),
                    unit: "By".to_owned(),
                    description: Arc::from("Available physical memory."),
                    metric_kind: MetricKind::Gauge,
                    attributes: Arc::new(BTreeMap::new()),
                    excluded_aggregation_instance: None,
                    scale_power10: 0,
                }],
                collection_interval: Duration::from_secs(1),
                initial_delay: Duration::ZERO,
                wildcard_refresh_interval: None,
                max_instances_per_wildcard: 256,
                max_expanded_counters: 4_096,
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
