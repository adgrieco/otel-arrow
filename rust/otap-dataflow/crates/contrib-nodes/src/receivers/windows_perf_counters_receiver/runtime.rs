// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows-only, single-core receiver for exact and calculated performance metrics.

use super::config::Config;
use super::metrics::WindowsPerfCountersReceiverMetrics;
use super::model::{Sample, SampleFailure};
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
use std::future::Future;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tokio::time::{MissedTickBehavior, timeout};

/// Factory identity for the Windows performance-counter receiver.
pub const WINDOWSPERFCOUNTERS_RECEIVER_URN: &str = "urn:otel:receiver:windowsperfcounters";

const WORKER_SHUTDOWN_MAX_WAIT: Duration = Duration::from_secs(1);
const PIPELINE_COMPLETION_RESERVE: Duration = Duration::from_millis(500);

fn failed_counter_value_count(failures: &[SampleFailure]) -> u64 {
    u64::try_from(
        failures
            .iter()
            .filter(|failure| !failure.diagnostic_only)
            .count(),
    )
    .unwrap_or(u64::MAX)
}

fn receiver_error(
    effect_handler: &local::EffectHandler<OtapPdata>,
    error: impl Into<String>,
    source_detail: impl Into<String>,
) -> Error {
    Error::ReceiverError {
        receiver: effect_handler.receiver_id(),
        kind: ReceiverErrorKind::Other,
        error: error.into(),
        source_detail: source_detail.into(),
    }
}

#[derive(Default)]
struct FailureLogState {
    scrape_failed: bool,
    active_counter_failures: BTreeSet<(usize, &'static str)>,
}

impl FailureLogState {
    fn record_success(&mut self) {
        self.scrape_failed = false;
    }

    fn log_scrape_failure(&mut self, node: &str, error: &pdh::Error) {
        if !self.scrape_failed {
            otel_arrow_dfe_telemetry::otel_warn!(
                "otelcol.node.windowsperfcounters.scrape.fail",
                node = node,
                error = %error
            );
            self.scrape_failed = true;
        }
    }

    fn log_scrape_timeout(&mut self, node: &str, timeout: Duration) {
        if !self.scrape_failed {
            otel_arrow_dfe_telemetry::otel_warn!(
                "otelcol.node.windowsperfcounters.scrape.timeout",
                node = node,
                timeout_seconds = timeout.as_secs_f64()
            );
            self.scrape_failed = true;
        }
    }

    fn log_counter_failures(
        &mut self,
        receiver: &WindowsPerfCountersReceiver,
        sample: &Sample,
        effect_handler: &local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let mut current = BTreeSet::new();
        for failure in &sample.metadata.failures {
            let Some(counter) = receiver.config.counters.get(failure.counter_index) else {
                return Err(receiver_error(
                    effect_handler,
                    format!(
                        "sample failure references missing configured counter {}",
                        failure.counter_index
                    ),
                    "",
                ));
            };
            let key = (failure.counter_index, failure.reason);
            if !self.active_counter_failures.contains(&key) {
                otel_arrow_dfe_telemetry::otel_warn!(
                    "otelcol.node.windowsperfcounters.counter.fail",
                    node = receiver.node.as_str(),
                    path_template = counter.path.as_str(),
                    reason = failure.reason,
                    error = failure.error.as_str()
                );
            }
            let _ = current.insert(key);
        }
        self.active_counter_failures = current;
        Ok(())
    }
}

struct ControlExit {
    deadline: Instant,
    drained: bool,
}

struct WindowsPerfCountersReceiver {
    config: Config,
    node: String,
    metrics: Rc<RefCell<WindowsPerfCountersReceiverMetrics>>,
}

async fn wait_initial_delay(delay: Duration) {
    tokio::time::sleep(delay).await;
}

async fn with_optional_timeout<F>(
    duration: Option<Duration>,
    future: F,
) -> Result<F::Output, tokio::time::error::Elapsed>
where
    F: Future,
{
    match duration {
        Some(duration) => timeout(duration, future).await,
        None => Ok(future.await),
    }
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
                .filter(|counter| !counter.instance_config.is_wildcard())
                .count() as u64,
        );
        metrics.health.configured_wildcard.set(
            config
                .counters
                .iter()
                .filter(|counter| counter.instance_config.is_wildcard())
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
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: |value| Config::from_json(value).map(|_| ()),
};

impl WindowsPerfCountersReceiver {
    fn record_completed_scrape(&self, scrape_start: Instant, succeeded: bool, overrun: bool) {
        let mut metrics = self.metrics.borrow_mut();
        metrics.record_scrape(if succeeded {
            Outcome::Success
        } else {
            Outcome::Failure
        });
        if succeeded {
            metrics.health.scrapes.add(1);
        } else {
            metrics.health.scrape_failures.add(1);
        }
        if overrun {
            metrics.health.scrape_overruns.inc();
        }
        metrics
            .health
            .scrape_duration
            .record(scrape_start.elapsed().as_secs_f64());
    }

    fn record_successful_sample(&self, scrape_start: Instant, sample: &Sample) {
        self.record_completed_scrape(scrape_start, true, false);
        let mut metrics = self.metrics.borrow_mut();
        metrics.health.apply(&sample.metadata.diagnostics);
        metrics
            .health
            .failed_counter_values
            .add(failed_counter_value_count(&sample.metadata.failures));
    }

    fn log_sample_events(&self, sample: &Sample) {
        for overflow in &sample.metadata.overflows {
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
        if sample.metadata.diagnostics.instances_added > 0
            || sample.metadata.diagnostics.instances_removed > 0
        {
            otel_arrow_dfe_telemetry::otel_info!(
                "otelcol.node.windowsperfcounters.instances.change",
                node = self.node.as_str(),
                added = sample.metadata.diagnostics.instances_added,
                removed = sample.metadata.diagnostics.instances_removed,
                active = sample.metadata.diagnostics.active_expanded_counters as u64
            );
        }
        if sample.metadata.diagnostics.retry_attempts > 0
            || sample.metadata.diagnostics.retry_recoveries > 0
            || sample.metadata.diagnostics.query_rebuild_attempts > 0
            || sample.metadata.diagnostics.query_recreation_failures > 0
        {
            otel_arrow_dfe_telemetry::otel_info!(
                "otelcol.node.windowsperfcounters.recovery",
                node = self.node.as_str(),
                retry_attempts = sample.metadata.diagnostics.retry_attempts,
                retry_recoveries = sample.metadata.diagnostics.retry_recoveries,
                query_rebuild_attempts = sample.metadata.diagnostics.query_rebuild_attempts,
                query_rebuild_recoveries = sample.metadata.diagnostics.query_rebuild_recoveries,
                query_recreation_attempts = sample.metadata.diagnostics.query_recreation_attempts,
                query_recreation_failures = sample.metadata.diagnostics.query_recreation_failures
            );
        }
    }

    fn handle_control_message(&self, message: NodeControlMsg<OtapPdata>) -> Option<ControlExit> {
        match message {
            NodeControlMsg::CollectTelemetry {
                mut metrics_reporter,
            } => {
                let mut metrics = self.metrics.borrow_mut();
                let _ = metrics.report(&mut metrics_reporter);
                None
            }
            NodeControlMsg::DrainIngress { deadline, .. } => Some(ControlExit {
                deadline,
                drained: true,
            }),
            NodeControlMsg::Shutdown { deadline, .. } => Some(ControlExit {
                deadline,
                drained: false,
            }),
            _ => None,
        }
    }

    async fn collect_and_send(
        &self,
        worker: &pdh::Worker,
        effect_handler: &local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        wait_initial_delay(self.config.initial_delay).await;
        let mut interval = tokio::time::interval(self.config.collection_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut failure_logs = FailureLogState::default();
        loop {
            let _ = interval.tick().await;
            let scrape_start = Instant::now();
            let sample = match with_optional_timeout(self.config.timeout, worker.collect()).await {
                Ok(Ok(sample)) => {
                    self.record_successful_sample(scrape_start, &sample);
                    failure_logs.record_success();
                    sample
                }
                Ok(Err(err)) if err.is_collection_failure() => {
                    self.record_completed_scrape(scrape_start, false, err.is_overrun());
                    failure_logs.log_scrape_failure(&self.node, &err);
                    continue;
                }
                Ok(Err(err)) => {
                    self.metrics.borrow_mut().record_scrape(Outcome::Failure);
                    return Err(receiver_error(effect_handler, err.to_string(), ""));
                }
                Err(_) => {
                    self.record_completed_scrape(scrape_start, false, true);
                    failure_logs
                        .log_scrape_timeout(&self.node, self.config.timeout.unwrap_or_default());
                    continue;
                }
            };
            self.log_sample_events(&sample);
            failure_logs.log_counter_failures(self, &sample, effect_handler)?;
            let Some(records) = into_otap(&self.config.counters, sample)
                .map_err(|err| receiver_error(effect_handler, err.to_string(), ""))?
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
            Control(ControlExit),
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
                msg = ctrl_msg_recv.recv() => {
                    let message = msg.map_err(Error::ChannelRecvError)?;
                    if let Some(exit) = self.handle_control_message(message) {
                        if exit.drained {
                            effect_handler.notify_receiver_drained().await?;
                        }
                        let snapshots = self.metrics.borrow_mut().terminal_snapshots();
                        return Ok(TerminalState::new(exit.deadline, snapshots));
                    }
                },
                result = &mut worker_start => break result.map_err(|err| {
                    receiver_error(
                        &effect_handler,
                        "failed to initialize the Windows PDH query",
                        err.to_string(),
                    )
                })?,
            }
        };
        let exit = {
            let collect_and_send = self.collect_and_send(&worker, &effect_handler);
            tokio::pin!(collect_and_send);
            loop {
                tokio::select! {
                    biased;
                    msg = ctrl_msg_recv.recv() => {
                        match msg {
                            Ok(message) => {
                                if let Some(exit) = self.handle_control_message(message) {
                                    break Exit::Control(exit);
                                }
                            }
                            Err(err) => {
                                break Exit::Collection(Err(Error::ChannelRecvError(err)));
                            }
                        }
                    },
                    result = &mut collect_and_send => break Exit::Collection(result),
                }
            }
        };

        let (deadline, drained, collection_result) = match exit {
            Exit::Control(exit) => (exit.deadline, exit.drained, None),
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
                return Err(receiver_error(&effect_handler, err.to_string(), ""));
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
    use crate::receivers::windows_perf_counters_receiver::config::{
        CounterConfig, InstanceConfig, MetricKind,
    };
    use otel_arrow_dfe_config::node::NodeUserConfig;
    use otel_arrow_dfe_engine::testing::receiver::TestRuntime;
    use otel_arrow_dfe_engine::testing::{test_node, test_pipeline_ctx};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    /// Scenario: A sample contains one value failure and one query-level log entry.
    /// Guarantees: Only the actual counter-value omission contributes to failed-value metrics.
    #[test]
    fn diagnostic_only_failures_do_not_count_as_failed_values() {
        let failures = vec![
            SampleFailure {
                counter_index: 0,
                reason: "counter CStatus",
                error: "PDH_INVALID_DATA".to_owned(),
                diagnostic_only: false,
            },
            SampleFailure {
                counter_index: 1,
                reason: "PdhCollectQueryData",
                error: "PDH status 0x00000001".to_owned(),
                diagnostic_only: true,
            },
        ];

        assert_eq!(failed_counter_value_count(&failures), 1);
    }

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
                    instance_config: InstanceConfig::None,
                    scale_power10: 0,
                    recreate_query: false,
                    source_mapping_index: 0,
                }],
                collection_interval: Duration::from_secs(1),
                initial_delay: Duration::ZERO,
                timeout: Some(Duration::from_secs(1)),
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

    /// Scenario: Scrape timeout is enabled or explicitly disabled.
    /// Guarantees: Enabled deadlines expire while disabled deadlines await the same future to completion.
    #[tokio::test(start_paused = true)]
    async fn optional_timeout_controls_scrape_deadline() {
        let timed = with_optional_timeout(
            Some(Duration::from_secs(1)),
            tokio::time::sleep(Duration::from_secs(2)),
        )
        .await;
        assert!(timed.is_err());

        let untimed = with_optional_timeout(None, async { 42 }).await;
        assert_eq!(untimed.unwrap(), 42);
    }
}
