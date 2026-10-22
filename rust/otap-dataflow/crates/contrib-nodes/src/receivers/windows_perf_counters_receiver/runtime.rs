// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows-only, single-core receiver for exact and calculated performance metrics.

use super::config::Config;
use super::metrics::WindowsPerfCountersReceiverMetrics;
use super::model::{Sample, SampleFailure};
use super::otap_builder::{ProjectionInvariantError, project_sample, validate_member_indexes};
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
use tokio::time::MissedTickBehavior;

#[cfg(test)]
use tokio::time::timeout;

/// Factory identity for the Windows performance-counter receiver.
pub const WINDOWSPERFCOUNTERS_RECEIVER_URN: &str = "urn:otel:receiver:windows_perf_counters";

const WORKER_SHUTDOWN_MAX_WAIT: Duration = Duration::from_secs(1);
const PIPELINE_COMPLETION_RESERVE: Duration = Duration::from_millis(500);

fn next_future_collection_tick(
    scheduled_tick: tokio::time::Instant,
    now: tokio::time::Instant,
    period: Duration,
) -> Option<tokio::time::Instant> {
    if period.is_zero() {
        return None;
    }
    let elapsed = now.saturating_duration_since(scheduled_tick);
    if elapsed < period {
        return None;
    }
    let mut periods = elapsed.as_nanos() / period.as_nanos() + 1;
    let mut next = scheduled_tick;
    while periods > 0 {
        let step = u32::try_from(periods.min(u128::from(u32::MAX))).unwrap_or(u32::MAX);
        next += period
            .checked_mul(step)
            .expect("collection cadence duration must fit");
        periods -= u128::from(step);
    }
    Some(next)
}

fn failed_counter_value_count(failures: &[SampleFailure]) -> u64 {
    u64::try_from(
        failures
            .iter()
            .filter(|failure| !failure.diagnostic_only)
            .filter(|failure| {
                matches!(
                    failure.reason,
                    "PdhGetRawCounterValue"
                        | "PdhGetFormattedCounterValue"
                        | "raw counter CStatus"
                        | "formatted counter CStatus"
                        | "counter CStatus"
                        | "calculation"
                        | "scale"
                )
            })
            .map(|failure| (failure.counter_index, &failure.path))
            .collect::<BTreeSet<_>>()
            .len(),
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
    scrape_failure_active: bool,
    scrape_timeout_active: bool,
}

impl FailureLogState {
    fn record_completion(&mut self, on_time: bool) {
        if on_time {
            self.scrape_timeout_active = false;
        }
    }

    fn record_success(&mut self) {
        self.scrape_failure_active = false;
    }

    fn log_scrape_failure(&mut self, node: &str, error: &pdh::Error) {
        if !self.scrape_failure_active {
            otel_arrow_dfe_telemetry::otel_warn!(
                "windows_perf_counters_receiver.scrape.fail",
                node = node,
                error = %error
            );
            self.scrape_failure_active = true;
        }
    }

    fn log_scrape_timeout(&mut self, node: &str, timeout: Duration) {
        if !self.scrape_timeout_active {
            otel_arrow_dfe_telemetry::otel_warn!(
                "windows_perf_counters_receiver.scrape.timeout",
                node = node,
                timeout_seconds = timeout.as_secs_f64()
            );
            self.scrape_timeout_active = true;
        }
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

#[derive(Debug)]
enum CollectionLoopError {
    Native(pdh::Error),
    Projection(ProjectionInvariantError),
    Send(Error),
}

async fn wait_initial_delay(delay: Duration) {
    tokio::time::sleep(delay).await;
}

async fn initialize_and_signal_ready<F, T, E, S>(startup: F, signal_ready: S) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
    S: FnOnce(),
{
    let initialized = startup.await?;
    signal_ready();
    Ok(initialized)
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
                    "host-wide windows_perf_counters collection requires a one-core source pipeline"
                        .to_owned(),
            });
        }
        let config = Config::from_json(&node_config.config)?;
        config.emit_warnings();
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
        Ok(
            ReceiverWrapper::local(receiver, node, node_config, receiver_config)
                .with_startup_readiness(),
        )
    },
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: |value| Config::from_json(value).map(|_| ()),
};

impl WindowsPerfCountersReceiver {
    fn record_completed_scrape(&self, collection_duration: Duration, succeeded: bool) {
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
        metrics
            .health
            .scrape_duration
            .record(collection_duration.as_secs_f64());
    }

    fn record_sample_diagnostics(&self, sample: &Sample) {
        let mut metrics = self.metrics.borrow_mut();
        metrics.health.apply(&sample.metadata.diagnostics);
        metrics
            .health
            .counter_read_failures
            .add(failed_counter_value_count(&sample.metadata.failures));
    }

    fn record_successful_sample(&self, collection_duration: Duration, sample: &Sample) {
        self.record_completed_scrape(collection_duration, true);
        self.record_sample_diagnostics(sample);
    }

    fn record_recovery_only_sample(&self, collection_duration: Duration, sample: &Sample) {
        let mut metrics = self.metrics.borrow_mut();
        metrics.record_scrape(Outcome::Failure);
        metrics
            .health
            .scrape_duration
            .record(collection_duration.as_secs_f64());
        drop(metrics);
        self.record_sample_diagnostics(sample);
    }

    async fn process_collected_sample<F, Fut>(
        &self,
        sample: Sample,
        collection_duration: Duration,
        overrun: bool,
        failure_logs: &mut FailureLogState,
        send: F,
    ) -> Result<(), CollectionLoopError>
    where
        F: FnOnce(OtapPdata) -> Fut,
        Fut: Future<Output = Result<(), Error>>,
    {
        // No-query maintenance uses zero as an absent observation timestamp.
        let projection = if sample.timestamp_unix_nano == 0
            && !sample.metadata.diagnostics.any_query_collected
            && sample.points.is_empty()
        {
            validate_member_indexes(&self.config.counters, &sample).map(|()| None)
        } else {
            project_sample(&self.config.counters, &sample)
        };
        let projection = match projection {
            Ok(projection) => projection,
            Err(err) => {
                self.metrics.borrow_mut().record_scrape(Outcome::Failure);
                return Err(CollectionLoopError::Projection(err));
            }
        };
        failure_logs.record_completion(!overrun);
        if sample.metadata.diagnostics.any_query_collected {
            self.record_successful_sample(collection_duration, &sample);
            failure_logs.record_success();
        } else {
            self.record_recovery_only_sample(collection_duration, &sample);
        }
        let Some(records) = projection else {
            return Ok(());
        };
        send(OtapPdata::new(Context::default(), records.into()))
            .await
            .map_err(CollectionLoopError::Send)?;
        Ok(())
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

    async fn collect_and_send_with<C, CFut, S, SFut>(
        &self,
        mut collect: C,
        mut send: S,
    ) -> Result<TerminalState, CollectionLoopError>
    where
        C: FnMut() -> CFut,
        CFut: Future<Output = Result<Sample, pdh::Error>>,
        S: FnMut(OtapPdata) -> SFut,
        SFut: Future<Output = Result<(), Error>>,
    {
        wait_initial_delay(self.config.initial_delay).await;
        let mut interval = tokio::time::interval(self.config.collection_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut failure_logs = FailureLogState::default();
        let mut previous_tick = None;
        loop {
            if let Some(scheduled_tick) = previous_tick
                && let Some(next_tick) = next_future_collection_tick(
                    scheduled_tick,
                    tokio::time::Instant::now(),
                    self.config.collection_interval,
                )
            {
                interval.reset_at(next_tick);
            }
            let scheduled_tick = interval.tick().await;
            previous_tick = Some(scheduled_tick);
            let scrape_start = tokio::time::Instant::now();
            let collection = collect();
            tokio::pin!(collection);
            let threshold = self.config.scrape_timeout_threshold();
            let (result, mut overrun) = tokio::select! {
                biased;
                result = &mut collection => {
                    (result, false)
                }
                _ = tokio::time::sleep_until(scrape_start + threshold) => {
                    self.metrics.borrow_mut().health.scrape_overruns.inc();
                    failure_logs.log_scrape_timeout(&self.node, threshold);
                    (collection.await, true)
                }
            };
            let collection_duration = scrape_start.elapsed();
            if !overrun
                && collection_duration > threshold
                && !matches!(&result, Err(pdh::Error::WorkerBusy))
            {
                self.metrics.borrow_mut().health.scrape_overruns.inc();
                failure_logs.log_scrape_timeout(&self.node, threshold);
                overrun = true;
            }
            let sample = match result {
                Ok(sample) => sample,
                Err(pdh::Error::WorkerBusy) => {
                    self.metrics.borrow_mut().health.worker_busy.inc();
                    continue;
                }
                Err(err) if err.is_collection_failure() => {
                    self.record_completed_scrape(collection_duration, false);
                    failure_logs.record_completion(!overrun);
                    failure_logs.log_scrape_failure(&self.node, &err);
                    continue;
                }
                Err(err) => {
                    self.metrics.borrow_mut().record_scrape(Outcome::Failure);
                    return Err(CollectionLoopError::Native(err));
                }
            };
            self.process_collected_sample(
                sample,
                collection_duration,
                overrun,
                &mut failure_logs,
                &mut send,
            )
            .await?;
        }
    }

    async fn collect_and_send(
        &self,
        worker: &pdh::Worker,
        effect_handler: &local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        self.collect_and_send_with(
            || worker.collect(),
            |pdata| async {
                effect_handler
                    .send_message_with_source_node(pdata)
                    .await
                    .map_err(Error::from)
            },
        )
        .await
        .map_err(|error| match error {
            CollectionLoopError::Native(error) => {
                receiver_error(effect_handler, error.to_string(), "")
            }
            CollectionLoopError::Projection(error) => {
                receiver_error(effect_handler, error.to_string(), "")
            }
            CollectionLoopError::Send(error) => error,
        })
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

        let worker_start = initialize_and_signal_ready(
            async {
                let worker = pdh::Worker::start(
                    self.config.counters.clone(),
                    pdh::QueryCadence::new(
                        self.config.query_retry_cap(),
                        self.config.wildcard_refresh_interval(),
                    ),
                    self.config.max_instances_per_wildcard,
                    self.config.max_expanded_counters,
                    self.node.clone(),
                )
                .await?;
                {
                    let mut metrics = self.metrics.borrow_mut();
                    metrics
                        .health
                        .query_pending
                        .set(u64::from(worker.startup.pending));
                    metrics
                        .health
                        .active_expanded
                        .set(worker.startup.active_expanded as u64);
                }
                otel_arrow_dfe_telemetry::otel_info!(
                    "windows_perf_counters_receiver.ready",
                    node = self.node.as_str(),
                    query_pending = worker.startup.pending,
                    active_expanded = worker.startup.active_expanded as u64
                );
                Ok::<_, pdh::Error>(worker)
            },
            || effect_handler.signal_ready(),
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
                    otel_arrow_dfe_telemetry::otel_error!(
                        "windows_perf_counters_receiver.startup.fail",
                        node = self.node.as_str(),
                        error = %err
                    );
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
                    "windows_perf_counters_receiver.shutdown.timeout",
                    node = self.node.as_str()
                );
            }
            Err(err) => {
                otel_arrow_dfe_telemetry::otel_warn!(
                    "windows_perf_counters_receiver.shutdown.fail",
                    node = self.node.as_str(),
                    error = %err
                );
                if let Some(Err(primary)) = collection_result {
                    return Err(primary);
                }
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
#[path = "runtime_tests.rs"]
mod tests;
