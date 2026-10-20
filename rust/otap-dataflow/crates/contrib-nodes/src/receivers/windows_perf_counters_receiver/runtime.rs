// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows-only, single-core receiver for exact and calculated performance metrics.

use super::config::Config;
use super::metrics::WindowsPerfCountersReceiverMetrics;
use super::model::{MonotonicPublication, Sample, SampleFailure, SampleValue};
use super::otap_builder::{ProjectionInvariantError, project_sample};
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
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::MissedTickBehavior;

#[cfg(test)]
use tokio::time::timeout;

/// Factory identity for the Windows performance-counter receiver.
pub const WINDOWSPERFCOUNTERS_RECEIVER_URN: &str = "urn:otel:receiver:windowsperfcounters";

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
    scrape_failure_active: bool,
    scrape_timeout_active: bool,
    active_counter_failures: BTreeSet<(usize, Arc<str>, &'static str)>,
}

impl FailureLogState {
    fn record_success(&mut self, on_time: bool) {
        self.scrape_failure_active = false;
        if on_time {
            self.scrape_timeout_active = false;
        }
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

    fn log_counter_failures(
        &mut self,
        receiver: &WindowsPerfCountersReceiver,
        sample: &Sample,
    ) -> Result<(), usize> {
        for point in &sample.points {
            if !matches!(point.value, SampleValue::Value(_)) {
                continue;
            }
            let Some(counter) = receiver.config.counters.get(point.counter_index) else {
                return Err(point.counter_index);
            };
            self.active_counter_failures
                .retain(|(counter_index, path, _)| {
                    *counter_index != point.counter_index
                        || (path.as_ref() != point.path.as_ref()
                            && path.as_ref() != counter.path.as_str())
                });
        }
        for failure in &sample.metadata.failures {
            let Some(counter) = receiver.config.counters.get(failure.counter_index) else {
                return Err(failure.counter_index);
            };
            let key = (
                failure.counter_index,
                Arc::clone(&failure.path),
                failure.reason,
            );
            if !self.active_counter_failures.contains(&key) {
                otel_arrow_dfe_telemetry::otel_warn!(
                    "windows_perf_counters_receiver.counter.fail",
                    node = receiver.node.as_str(),
                    path_template = counter.path.as_str(),
                    reason = failure.reason,
                    error = failure.error.as_str()
                );
            }
            let _ = self.active_counter_failures.insert(key);
        }
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

#[derive(Debug)]
enum CollectionLoopError {
    Native(pdh::Error),
    Projection(ProjectionInvariantError),
    MissingCounter(usize),
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
                    "host-wide windowsperfcounters collection requires a one-core source pipeline"
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
            .failed_counter_values
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

    fn log_sample_events(&self, sample: &Sample) {
        for overflow in &sample.metadata.overflows {
            let path_template = &self.config.counters[overflow.counter_index].path;
            otel_arrow_dfe_telemetry::otel_warn!(
                "windows_perf_counters_receiver.instance_limit.exceeded",
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
                "windows_perf_counters_receiver.instances.change",
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
                "windows_perf_counters_receiver.recovery.change",
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

    async fn process_collected_sample<F, Fut>(
        &self,
        sample: Sample,
        collection_duration: Duration,
        overrun: bool,
        failure_logs: &mut FailureLogState,
        send: F,
    ) -> Result<Vec<MonotonicPublication>, CollectionLoopError>
    where
        F: FnOnce(OtapPdata) -> Fut,
        Fut: Future<Output = Result<(), Error>>,
    {
        let projection = match project_sample(&self.config.counters, &sample) {
            Ok(projection) => projection,
            Err(err) => {
                self.record_completed_scrape(collection_duration, false);
                return Err(CollectionLoopError::Projection(err));
            }
        };
        if sample.metadata.diagnostics.any_query_collected {
            self.record_successful_sample(collection_duration, &sample);
            failure_logs.record_success(!overrun);
        } else {
            self.record_recovery_only_sample(collection_duration, &sample);
        }
        self.log_sample_events(&sample);
        failure_logs
            .log_counter_failures(self, &sample)
            .map_err(CollectionLoopError::MissingCounter)?;
        let Some(records) = projection.records else {
            return Ok(Vec::new());
        };
        send(OtapPdata::new(Context::default(), records.into()))
            .await
            .map_err(CollectionLoopError::Send)?;
        Ok(projection.monotonic_publications)
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

    async fn collect_and_send_with<C, CFut, S, SFut, P, PFut>(
        &self,
        mut collect: C,
        mut send: S,
        mut publish: P,
    ) -> Result<TerminalState, CollectionLoopError>
    where
        C: FnMut() -> CFut,
        CFut: Future<Output = Result<Sample, pdh::Error>>,
        S: FnMut(OtapPdata) -> SFut,
        SFut: Future<Output = Result<(), Error>>,
        P: FnMut(Vec<MonotonicPublication>) -> PFut,
        PFut: Future<Output = Result<(), pdh::Error>>,
    {
        wait_initial_delay(self.config.initial_delay).await;
        let mut interval = tokio::time::interval(self.config.collection_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut failure_logs = FailureLogState::default();
        loop {
            let scheduled_tick = interval.tick().await;
            let scrape_start = Instant::now();
            let collection = collect();
            tokio::pin!(collection);
            let threshold = self.config.scrape_timeout_threshold();
            let (result, overrun) = tokio::select! {
                biased;
                result = &mut collection => {
                    (result, false)
                }
                _ = tokio::time::sleep_until(
                    tokio::time::Instant::from_std(scrape_start + threshold)
                ) => {
                    self.metrics.borrow_mut().health.scrape_overruns.inc();
                    failure_logs.log_scrape_timeout(&self.node, threshold);
                    (collection.await, true)
                }
            };
            if let Some(next_tick) = next_future_collection_tick(
                scheduled_tick,
                tokio::time::Instant::now(),
                self.config.collection_interval,
            ) {
                interval.reset_at(next_tick);
            }
            let collection_duration = scrape_start.elapsed();
            let sample = match result {
                Ok(sample) => sample,
                Err(pdh::Error::WorkerBusy) => {
                    self.metrics.borrow_mut().health.worker_busy.inc();
                    continue;
                }
                Err(err) if err.is_collection_failure() => {
                    self.record_completed_scrape(collection_duration, false);
                    failure_logs.log_scrape_failure(&self.node, &err);
                    continue;
                }
                Err(err) => {
                    self.metrics.borrow_mut().record_scrape(Outcome::Failure);
                    return Err(CollectionLoopError::Native(err));
                }
            };
            let publications = self
                .process_collected_sample(
                    sample,
                    collection_duration,
                    overrun,
                    &mut failure_logs,
                    &mut send,
                )
                .await?;
            match publish(publications).await {
                Ok(()) => {}
                Err(pdh::Error::WorkerBusy) => {
                    self.metrics.borrow_mut().health.worker_busy.inc();
                }
                Err(error) => return Err(CollectionLoopError::Native(error)),
            }
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
            |publications| worker.publish_monotonic_starts(publications),
        )
        .await
        .map_err(|error| match error {
            CollectionLoopError::Native(error) => {
                receiver_error(effect_handler, error.to_string(), "")
            }
            CollectionLoopError::Projection(error) => {
                receiver_error(effect_handler, error.to_string(), "")
            }
            CollectionLoopError::MissingCounter(counter_index) => receiver_error(
                effect_handler,
                format!("sample failure references missing configured counter {counter_index}"),
                "",
            ),
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
            pdh::Worker::start_with_cadence(
                self.config.counters.clone(),
                pdh::QueryCadence::new(
                    self.config.query_retry_cap(),
                    self.config.wildcard_refresh_interval(),
                ),
                self.config.max_instances_per_wildcard,
                self.config.max_expanded_counters,
                self.node.clone(),
            ),
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
        Aggregation, CounterConfig, InstanceConfig, MetricKind,
    };
    use crate::receivers::windows_perf_counters_receiver::model::{
        MonotonicPublicationCandidate, Number, PendingMetadata, SampleDiagnostics, SamplePoint,
        SampleValue,
    };
    use arrow::array::TimestampNanosecondArray;
    use otel_arrow_dfe_config::node::NodeUserConfig;
    use otel_arrow_dfe_engine::capability::registry::Capabilities;
    use otel_arrow_dfe_engine::testing::receiver::TestRuntime;
    use otel_arrow_dfe_engine::testing::{test_node, test_pipeline_ctx};
    use otel_arrow_dfe_otap::OTAP_PIPELINE_FACTORY;
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
    use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::any_value;
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::metric;
    use otel_arrow_dfe_pdata::{OtlpProtoBytes, TryIntoWithOptions};
    use otel_arrow_dfe_pdata_codec::OtapPayload;
    use otel_arrow_dfe_pdata_codec::PayloadData;
    use prost::Message;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn test_counter(attributes: BTreeMap<String, String>) -> CounterConfig {
        CounterConfig {
            path: r"\Memory\Available Bytes".to_owned(),
            name: Arc::from("windows.memory.available"),
            unit: Arc::from("By"),
            description: Arc::from("Available physical memory."),
            metric_kind: MetricKind::Gauge,
            attributes: Arc::new(attributes),
            instance_config: InstanceConfig::None,
            scale_power10: 0,
            recreate_query: false,
            source_mapping_index: 0,
        }
    }

    fn test_receiver(counters: Vec<CounterConfig>) -> WindowsPerfCountersReceiver {
        let (pipeline, _) = test_pipeline_ctx();
        WindowsPerfCountersReceiver {
            config: Config {
                counters,
                collection_interval: Duration::from_secs(1),
                initial_delay: Duration::ZERO,
                timeout: Some(Duration::from_secs(1)),
                wildcard_refresh_interval: None,
                max_instances_per_wildcard: 256,
                max_expanded_counters: 4_096,
                warnings: Vec::new(),
            },
            node: "windowsperfcounters".to_owned(),
            metrics: Rc::new(RefCell::new(WindowsPerfCountersReceiverMetrics::register(
                &pipeline,
            ))),
        }
    }

    fn sample_point(counter_index: usize, timestamp: i64, value: Number) -> SamplePoint {
        SamplePoint {
            counter_index,
            path: Arc::from(r"\Memory\Available Bytes"),
            instance: None,
            emit_instance_attribute: false,
            timestamp_unix_nano: timestamp,
            start_time_unix_nano: None,
            monotonic_publication: None,
            value: SampleValue::Value(value),
        }
    }

    fn collected_metadata() -> PendingMetadata {
        PendingMetadata {
            diagnostics: SampleDiagnostics {
                any_query_collected: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Scenario: A sample contains one value failure and one query-level log entry.
    /// Guarantees: Only the actual counter-value omission contributes to failed-value metrics.
    #[test]
    fn diagnostic_only_failures_do_not_count_as_failed_values() {
        let failures = vec![
            SampleFailure {
                counter_index: 0,
                path: Arc::from(r"\Memory\Available Bytes"),
                reason: "counter CStatus",
                error: "PDH_INVALID_DATA".to_owned(),
                diagnostic_only: false,
            },
            SampleFailure {
                counter_index: 1,
                path: Arc::from(r"\Memory\Committed Bytes"),
                reason: "PdhCollectQueryData",
                error: "PDH status 0x00000001".to_owned(),
                diagnostic_only: true,
            },
        ];

        assert_eq!(failed_counter_value_count(&failures), 1);
    }

    /// Scenario: One partial sample contains a failed query open and the next sample is empty only because that query remains in recovery.
    /// Guarantees: The partial sample succeeds, recovery-only counts as an attempt failure, and only the failed open increments query-failure health.
    #[tokio::test]
    async fn failed_open_and_recovery_only_sample_have_distinct_accounting() {
        let mut unavailable = test_counter(BTreeMap::new());
        unavailable.path = r"\Unavailable\Value".to_owned();
        unavailable.name = Arc::from("windows.unavailable");
        let receiver = test_receiver(vec![test_counter(BTreeMap::new()), unavailable]);
        let mut failure_logs = FailureLogState::default();
        let sends = Rc::new(std::cell::Cell::new(0_usize));

        let failed_open = Sample {
            timestamp_unix_nano: 100,
            points: vec![sample_point(0, 100, Number::Integer(42))],
            metadata: PendingMetadata {
                diagnostics: SampleDiagnostics {
                    any_query_collected: true,
                    scrape_failures: 1,
                    ..Default::default()
                },
                failures: vec![SampleFailure {
                    counter_index: 1,
                    path: Arc::from(r"\Unavailable\Value"),
                    reason: "PdhOpenQueryW",
                    error: "PDH status 0x00000001".to_owned(),
                    diagnostic_only: false,
                }],
                overflows: Vec::new(),
            },
        };
        let sent = Rc::clone(&sends);
        let publications = receiver
            .process_collected_sample(
                failed_open,
                Duration::from_millis(1),
                false,
                &mut failure_logs,
                move |_| {
                    sent.set(sent.get() + 1);
                    async { Ok(()) }
                },
            )
            .await
            .unwrap();
        assert!(publications.is_empty());

        let recovery_only = Sample {
            timestamp_unix_nano: 110,
            points: Vec::new(),
            metadata: PendingMetadata {
                failures: vec![SampleFailure {
                    counter_index: 1,
                    path: Arc::from(r"\Unavailable\Value"),
                    reason: "query_recovery_pending",
                    error: "recovery deferred".to_owned(),
                    diagnostic_only: true,
                }],
                ..Default::default()
            },
        };
        let publications = receiver
            .process_collected_sample(
                recovery_only,
                Duration::from_millis(1),
                false,
                &mut failure_logs,
                |_| async { panic!("empty recovery sample must not be sent") },
            )
            .await
            .unwrap();
        assert!(publications.is_empty());

        let metrics = receiver.metrics.borrow();
        assert_eq!(sends.get(), 1);
        assert_eq!(metrics.scrape_attempts(Outcome::Success), 1);
        assert_eq!(metrics.scrape_attempts(Outcome::Failure), 1);
        assert_eq!(metrics.health.scrapes.get(), 1);
        assert_eq!(metrics.health.scrape_failures.get(), 1);
        assert_eq!(metrics.health.failed_counter_values.get(), 1);
    }

    /// Scenario: A concrete counter fails, is absent and omitted on later scrapes, then produces a valid value before failing again.
    /// Guarantees: Warning suppression persists until real value recovery and re-arms only after that recovery.
    #[test]
    fn counter_failure_suppression_requires_valid_recovery() {
        let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        let path = Arc::<str>::from(r"\Memory\Available Bytes");
        let failure = SampleFailure {
            counter_index: 0,
            path: Arc::clone(&path),
            reason: "counter CStatus",
            error: "PDH_INVALID_DATA".to_owned(),
            diagnostic_only: false,
        };
        let mut state = FailureLogState::default();
        let mut failed = Sample::empty(10);
        failed.metadata.failures.push(failure.clone());
        state.log_counter_failures(&receiver, &failed).unwrap();
        assert_eq!(state.active_counter_failures.len(), 1);

        state
            .log_counter_failures(&receiver, &Sample::empty(20))
            .unwrap();
        assert_eq!(state.active_counter_failures.len(), 1);

        let omitted = Sample {
            timestamp_unix_nano: 30,
            points: vec![SamplePoint {
                counter_index: 0,
                path: Arc::clone(&path),
                instance: None,
                emit_instance_attribute: false,
                timestamp_unix_nano: 30,
                start_time_unix_nano: None,
                monotonic_publication: None,
                value: SampleValue::NoObservation,
            }],
            metadata: collected_metadata(),
        };
        state.log_counter_failures(&receiver, &omitted).unwrap();
        assert_eq!(state.active_counter_failures.len(), 1);

        let recovered = Sample {
            timestamp_unix_nano: 40,
            points: vec![sample_point(0, 40, Number::Integer(42))],
            metadata: collected_metadata(),
        };
        state.log_counter_failures(&receiver, &recovered).unwrap();
        assert!(state.active_counter_failures.is_empty());

        state.log_counter_failures(&receiver, &failed).unwrap();
        assert_eq!(state.active_counter_failures.len(), 1);
    }

    /// Scenario: A timeout warning is followed by a distinct native collection failure before any successful scrape.
    /// Guarantees: Timeout suppression cannot hide the first real failure warning in the same unsuccessful episode.
    #[test]
    fn timeout_and_failure_warning_states_are_independent() {
        let mut state = FailureLogState::default();
        state.log_scrape_timeout("test", Duration::from_secs(1));
        assert!(state.scrape_timeout_active);
        assert!(!state.scrape_failure_active);

        state.log_scrape_failure(
            "test",
            &pdh::Error::Pdh {
                operation: pdh::PdhOperation::CollectQueryData,
                path: "<query>".to_owned(),
                status: 1,
            },
        );
        assert!(state.scrape_timeout_active);
        assert!(state.scrape_failure_active);
    }

    /// Scenario: A timeout warning is followed first by a late success and then by an on-time success.
    /// Guarantees: Late success keeps warning suppression armed, while the first on-time success re-arms the warning.
    #[test]
    fn timeout_warning_rearms_only_after_on_time_success() {
        let mut state = FailureLogState::default();
        state.log_scrape_timeout("test", Duration::from_secs(1));

        state.record_success(false);
        assert!(state.scrape_timeout_active);

        state.record_success(true);
        assert!(!state.scrape_timeout_active);
    }

    /// Scenario: The initial PDH pass remains pending before it eventually succeeds.
    /// Guarantees: Pipeline readiness stays blocked until native initialization completes, so downstream nodes cannot start against an unprimed receiver.
    #[tokio::test]
    async fn startup_readiness_waits_for_initial_pass() {
        let ready = Rc::new(std::cell::Cell::new(false));
        let signal = Rc::clone(&ready);
        let (release, pending) = tokio::sync::oneshot::channel::<()>();
        let startup = initialize_and_signal_ready(
            async move {
                pending.await.expect("release initial pass");
                Ok::<_, &'static str>(42)
            },
            move || signal.set(true),
        );
        tokio::pin!(startup);

        tokio::select! {
            biased;
            result = &mut startup => panic!("startup completed early: {result:?}"),
            () = tokio::task::yield_now() => {}
        }
        assert!(!ready.get());

        release.send(()).expect("release startup");
        assert_eq!(startup.await.unwrap(), 42);
        assert!(ready.get());
    }

    /// Scenario: Native initialization fails before the worker can enter its collection loop.
    /// Guarantees: A failed receiver never reports readiness, allowing the pipeline to surface startup failure instead of advertising a false healthy state.
    #[tokio::test]
    async fn startup_failure_does_not_signal_readiness() {
        let ready = Rc::new(std::cell::Cell::new(false));
        let signal = Rc::clone(&ready);
        let result = initialize_and_signal_ready(
            async { Err::<(), _>("initial PDH pass failed") },
            move || signal.set(true),
        )
        .await;

        assert_eq!(result.unwrap_err(), "initial PDH pass failed");
        assert!(!ready.get());
    }

    /// Scenario: A collected sample references a counter index outside the validated configuration.
    /// Guarantees: Runtime processing returns a fatal invariant and records a failed scrape instead of silently dropping structurally corrupt telemetry.
    #[tokio::test]
    async fn runtime_terminates_on_projection_invariant() {
        let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        let sent = Rc::new(RefCell::new(Vec::new()));
        let capture = Rc::clone(&sent);
        let result = receiver
            .process_collected_sample(
                Sample {
                    timestamp_unix_nano: 10,
                    points: vec![sample_point(1, 10, Number::Integer(42))],
                    metadata: collected_metadata(),
                },
                Duration::ZERO,
                false,
                &mut FailureLogState::default(),
                move |pdata| async move {
                    capture.borrow_mut().push(pdata);
                    Ok(())
                },
            )
            .await;

        assert!(matches!(result, Err(CollectionLoopError::Projection(_))));
        assert!(sent.borrow().is_empty());
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.scrape_attempts(Outcome::Success), 0);
        assert_eq!(metrics.scrape_attempts(Outcome::Failure), 1);
        assert_eq!(metrics.health.scrapes.get(), 0);
        assert_eq!(metrics.health.scrape_failures.get(), 1);
        assert_eq!(metrics.health.counter_read_failures.get(), 0);
        assert_eq!(metrics.health.failed_counter_values.get(), 0);
    }

    /// Scenario: Projection and downstream delivery occur after a two-second native collection.
    /// Guarantees: Scrape duration records only the supplied native collection interval.
    #[tokio::test(start_paused = true)]
    async fn scrape_duration_excludes_projection_and_delivery() {
        let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        let publications = receiver
            .process_collected_sample(
                Sample {
                    timestamp_unix_nano: 10,
                    points: vec![sample_point(0, 10, Number::Integer(42))],
                    metadata: collected_metadata(),
                },
                Duration::from_secs(2),
                false,
                &mut FailureLogState::default(),
                |_| async {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    Ok(())
                },
            )
            .await
            .unwrap();

        assert!(publications.is_empty());
        let distribution = receiver.metrics.borrow().health.scrape_duration.get();
        assert_eq!(distribution.summary(), (1, 2.0, 2.0, 2.0));
    }

    /// Scenario: Two collections each finish after the cadence while timeout is unset.
    /// Guarantees: Late scrapes retain their native timestamps, count as successful overruns, and collection proceeds to the next scrape instead of stalling after the first.
    #[tokio::test]
    async fn no_timeout_delivers_late_scrapes_and_continues() {
        let mut receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        receiver.config.timeout = None;
        receiver.config.collection_interval = Duration::from_millis(10);
        let collect_calls = Rc::new(std::cell::Cell::new(0_usize));
        let collect_state = Rc::clone(&collect_calls);
        let collect = move || {
            let call = collect_state.get();
            collect_state.set(call + 1);
            async move {
                if call >= 2 {
                    return std::future::pending::<Result<Sample, pdh::Error>>().await;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
                let timestamp = if call == 0 { 100 } else { 200 };
                Ok(Sample {
                    timestamp_unix_nano: timestamp,
                    points: vec![sample_point(0, timestamp, Number::Integer(call as i64))],
                    metadata: collected_metadata(),
                })
            }
        };
        let sent = Rc::new(RefCell::new(Vec::new()));
        let capture = Rc::clone(&sent);
        let run = receiver.collect_and_send_with(
            collect,
            move |pdata| {
                let capture = Rc::clone(&capture);
                async move {
                    capture.borrow_mut().push(pdata);
                    Ok(())
                }
            },
            |_| async { Ok(()) },
        );
        tokio::pin!(run);

        let wait_for_following_scrape = async {
            while collect_calls.get() < 3 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::select! {
            biased;
            _ = &mut run => panic!("collection loop stopped after a late scrape"),
            result = timeout(Duration::from_secs(1), wait_for_following_scrape) => {
                result.expect("following scrape did not start");
            }
        }

        assert_eq!(collect_calls.get(), 3);
        assert_eq!(sent.borrow().len(), 2);
        let timestamps = sent
            .borrow()
            .iter()
            .cloned()
            .map(|pdata| {
                let (_, payload) = pdata.into_parts();
                let PayloadData::OtapRecords(records) = payload.into_data() else {
                    panic!("expected native OTAP records");
                };
                records
                    .get(ArrowPayloadType::NumberDataPoints)
                    .expect("number data points")
                    .column_by_name("time_unix_nano")
                    .expect("point timestamps")
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("nanosecond timestamp array")
                    .value(0)
            })
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![100, 200]);

        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.scrape_attempts(Outcome::Success), 2);
        assert_eq!(metrics.scrape_attempts(Outcome::Failure), 0);
        assert_eq!(metrics.health.scrapes.get(), 2);
        assert_eq!(metrics.health.scrape_failures.get(), 0);
        assert_eq!(metrics.health.scrape_overruns.get(), 2);
    }

    /// Scenario: Delivery succeeds, but the worker command queue is busy when the monotonic start acknowledgement is published.
    /// Guarantees: The rejected acknowledgement is counted without changing scrape outcome or stopping collection.
    #[tokio::test]
    async fn worker_busy_after_delivery_does_not_stop_collection() {
        let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        let collect_calls = Rc::new(std::cell::Cell::new(0_usize));
        let collect_state = Rc::clone(&collect_calls);
        let result = receiver
            .collect_and_send_with(
                move || {
                    let call = collect_state.get();
                    collect_state.set(call + 1);
                    async move {
                        if call == 0 {
                            Ok(Sample {
                                timestamp_unix_nano: 100,
                                points: vec![sample_point(0, 100, Number::Integer(1))],
                                metadata: collected_metadata(),
                            })
                        } else {
                            Err(pdh::Error::WorkerStopped)
                        }
                    }
                },
                |_| async { Ok(()) },
                |_| async { Err(pdh::Error::WorkerBusy) },
            )
            .await;

        assert!(matches!(
            result,
            Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
        ));
        assert_eq!(collect_calls.get(), 2);
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.health.worker_busy.get(), 1);
        assert_eq!(metrics.health.scrape_overruns.get(), 0);
        assert_eq!(metrics.health.scrape_failures.get(), 0);
    }

    /// Scenario: A native collection request is rejected because the capacity-one worker queue is busy.
    /// Guarantees: Worker backpressure counts as neither an overrun nor a failed scrape.
    #[tokio::test]
    async fn worker_busy_collection_is_not_an_overrun() {
        let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        let collect_calls = Rc::new(std::cell::Cell::new(0_usize));
        let collect_state = Rc::clone(&collect_calls);
        let result = receiver
            .collect_and_send_with(
                move || {
                    let call = collect_state.get();
                    collect_state.set(call + 1);
                    async move {
                        if call == 0 {
                            Err(pdh::Error::WorkerBusy)
                        } else {
                            Err(pdh::Error::WorkerStopped)
                        }
                    }
                },
                |_| async { Ok(()) },
                |_| async { Ok(()) },
            )
            .await;

        assert!(matches!(
            result,
            Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
        ));
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.scrape_attempts(Outcome::Failure), 1);
        assert_eq!(metrics.health.scrape_failures.get(), 0);
        assert_eq!(metrics.health.scrape_overruns.get(), 0);
        assert_eq!(metrics.health.worker_busy.get(), 1);
    }

    /// Scenario: Native collection reports an impossible internal sample invariant.
    /// Guarantees: Invariant corruption terminates the receiver instead of being retried as an ordinary scrape failure.
    #[tokio::test]
    async fn invalid_sample_collection_is_fatal() {
        let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        let result = receiver
            .collect_and_send_with(
                || async { Err(pdh::Error::InvalidSample("injected invariant")) },
                |_| async { Ok(()) },
                |_| async { Ok(()) },
            )
            .await;

        assert!(matches!(
            result,
            Err(CollectionLoopError::Native(pdh::Error::InvalidSample(
                "injected invariant"
            )))
        ));
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.scrape_attempts(Outcome::Failure), 1);
        assert_eq!(metrics.health.scrape_failures.get(), 0);
    }

    /// Scenario: Delivery succeeds, but the worker has stopped before the monotonic start acknowledgement can be published.
    /// Guarantees: A stopped worker remains fatal because no future collection can succeed.
    #[tokio::test]
    async fn worker_stopped_after_delivery_stops_collection() {
        let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        let collect_calls = Rc::new(std::cell::Cell::new(0_usize));
        let collect_state = Rc::clone(&collect_calls);
        let result = receiver
            .collect_and_send_with(
                move || {
                    collect_state.set(collect_state.get() + 1);
                    async {
                        Ok(Sample {
                            timestamp_unix_nano: 100,
                            points: vec![sample_point(0, 100, Number::Integer(1))],
                            metadata: collected_metadata(),
                        })
                    }
                },
                |_| async { Ok(()) },
                |_| async { Err(pdh::Error::WorkerStopped) },
            )
            .await;

        assert!(matches!(
            result,
            Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
        ));
        assert_eq!(collect_calls.get(), 1);
    }

    /// Scenario: A monotonic collection finishes after its configured timeout threshold.
    /// Guarantees: The late scrape is delivered and acknowledged with its native timestamp, counts as a successful overrun, and does not start a concurrent scrape.
    #[tokio::test]
    async fn timeout_retains_and_publishes_late_monotonic_start() {
        let mut counter = test_counter(BTreeMap::new());
        counter.metric_kind = MetricKind::Sum {
            aggregation: Aggregation::Cumulative,
            monotonic: true,
        };
        let mut receiver = test_receiver(vec![counter]);
        receiver.config.timeout = Some(Duration::from_millis(10));
        receiver.config.collection_interval = Duration::from_millis(10);

        let published_start = Rc::new(std::cell::Cell::new(None::<i64>));
        let collect_published_start = Rc::clone(&published_start);
        let collect_calls = Rc::new(std::cell::Cell::new(0_usize));
        let collect_state = Rc::clone(&collect_calls);
        let collect = move || {
            let call = collect_state.get();
            collect_state.set(call + 1);
            let start = Rc::clone(&collect_published_start);
            async move {
                if call >= 1 {
                    return std::future::pending::<Result<Sample, pdh::Error>>().await;
                }
                let timestamp = 100;
                let sample = Sample {
                    timestamp_unix_nano: timestamp,
                    points: vec![SamplePoint {
                        counter_index: 0,
                        path: Arc::from(r"\Memory\Available Bytes"),
                        instance: None,
                        emit_instance_attribute: false,
                        timestamp_unix_nano: timestamp,
                        start_time_unix_nano: Some(start.get().unwrap_or(timestamp)),
                        monotonic_publication: Some(MonotonicPublicationCandidate::Ready(
                            MonotonicPublication {
                                counter_index: 0,
                                path: Arc::from(r"\Memory\Available Bytes"),
                                query_generation: 0,
                                timestamp_unix_nano: timestamp,
                            },
                        )),
                        value: SampleValue::Value(Number::Integer(call as i64 + 1)),
                    }],
                    metadata: collected_metadata(),
                };
                if call == 0 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(sample)
            }
        };
        let sent = Rc::new(RefCell::new(Vec::new()));
        let capture = Rc::clone(&sent);
        let publish_start = Rc::clone(&published_start);
        let run = receiver.collect_and_send_with(
            collect,
            move |pdata| {
                let capture = Rc::clone(&capture);
                async move {
                    capture.borrow_mut().push(pdata);
                    Ok(())
                }
            },
            move |publications| {
                let publish_start = Rc::clone(&publish_start);
                async move {
                    assert_eq!(publications.len(), 1);
                    publish_start.set(Some(publications[0].timestamp_unix_nano));
                    Ok(())
                }
            },
        );
        tokio::pin!(run);

        let wait_for_following_scrape = async {
            while collect_calls.get() < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::select! {
            biased;
            _ = &mut run => panic!("collection loop stopped after late delivery"),
            result = timeout(Duration::from_secs(1), wait_for_following_scrape) => {
                result.expect("following scrape did not start");
            }
        }

        assert_eq!(sent.borrow().len(), 1);
        let pdata = sent.borrow_mut().pop().unwrap();
        let (_, payload) = pdata.into_parts();
        let PayloadData::OtapRecords(records) = payload.into_data() else {
            panic!("expected native OTAP records");
        };
        let points = records
            .get(ArrowPayloadType::NumberDataPoints)
            .expect("number data points");
        for column in ["start_time_unix_nano", "time_unix_nano"] {
            assert_eq!(
                points
                    .column_by_name(column)
                    .expect(column)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("nanosecond timestamp array")
                    .value(0),
                100
            );
        }
        assert_eq!(published_start.get(), Some(100));
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.scrape_attempts(Outcome::Success), 1);
        assert_eq!(metrics.scrape_attempts(Outcome::Failure), 0);
        assert_eq!(metrics.health.scrapes.get(), 1);
        assert_eq!(metrics.health.scrape_failures.get(), 0);
        assert_eq!(metrics.health.scrape_overruns.get(), 1);
    }

    /// Scenario: A scrape remains blocked after its configured timeout threshold and completes later.
    /// Guarantees: Overrun telemetry increments at threshold crossing before completion or delivery, then remains single-counted.
    #[tokio::test]
    async fn timeout_records_overrun_before_late_scrape_completes() {
        let mut receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        receiver.config.timeout = Some(Duration::from_millis(10));
        receiver.config.collection_interval = Duration::from_secs(1);
        let release = Rc::new(std::cell::Cell::new(false));
        let collect_release = Rc::clone(&release);
        let collect_calls = Rc::new(std::cell::Cell::new(0_usize));
        let collect_state = Rc::clone(&collect_calls);
        let sent = Rc::new(RefCell::new(Vec::new()));
        let capture = Rc::clone(&sent);
        let run = receiver.collect_and_send_with(
            move || {
                let call = collect_state.get();
                collect_state.set(call + 1);
                let release = Rc::clone(&collect_release);
                async move {
                    if call > 0 {
                        return std::future::pending::<Result<Sample, pdh::Error>>().await;
                    }
                    while !release.get() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Ok(Sample {
                        timestamp_unix_nano: 100,
                        points: vec![sample_point(0, 100, Number::Integer(1))],
                        metadata: collected_metadata(),
                    })
                }
            },
            move |pdata| {
                let capture = Rc::clone(&capture);
                async move {
                    capture.borrow_mut().push(pdata);
                    Ok(())
                }
            },
            |_| async { Ok(()) },
        );
        tokio::pin!(run);

        let wait_for_overrun = async {
            while receiver.metrics.borrow().health.scrape_overruns.get() == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::select! {
            biased;
            _ = &mut run => panic!("collection loop stopped before late scrape release"),
            result = timeout(Duration::from_secs(1), wait_for_overrun) => {
                result.expect("overrun was not recorded at the threshold");
            }
        }
        assert!(sent.borrow().is_empty());
        assert_eq!(receiver.metrics.borrow().health.scrapes.get(), 0);

        release.set(true);
        let wait_for_delivery = async {
            while sent.borrow().is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::select! {
            biased;
            _ = &mut run => panic!("collection loop stopped after late scrape release"),
            result = timeout(Duration::from_secs(1), wait_for_delivery) => {
                result.expect("late scrape was not delivered");
            }
        }
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.health.scrapes.get(), 1);
        assert_eq!(metrics.health.scrape_overruns.get(), 1);
    }

    /// Scenario: A positive timeout is longer than the collection interval and the scrape completes between them.
    /// Guarantees: The explicit timeout takes precedence, so the successful scrape is on time and does not count as an overrun.
    #[tokio::test]
    async fn positive_timeout_takes_precedence_over_collection_interval() {
        let mut receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        receiver.config.timeout = Some(Duration::from_millis(200));
        receiver.config.collection_interval = Duration::from_millis(5);
        let calls = Rc::new(std::cell::Cell::new(0_usize));
        let collect_calls = Rc::clone(&calls);
        let result = receiver
            .collect_and_send_with(
                move || {
                    let call = collect_calls.get();
                    collect_calls.set(call + 1);
                    async move {
                        if call == 0 {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                            Ok(Sample {
                                timestamp_unix_nano: 100,
                                points: vec![sample_point(0, 100, Number::Integer(1))],
                                metadata: collected_metadata(),
                            })
                        } else {
                            Err(pdh::Error::WorkerStopped)
                        }
                    }
                },
                |_| async { Ok(()) },
                |_| async { Ok(()) },
            )
            .await;

        assert!(matches!(
            result,
            Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
        ));
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.health.scrapes.get(), 1);
        assert_eq!(metrics.health.scrape_overruns.get(), 0);
    }

    /// Scenario: Native collection completes at the same instant as the configured timeout threshold.
    /// Guarantees: Completion at the boundary is on time and does not increment overrun telemetry.
    #[tokio::test(start_paused = true)]
    async fn completion_at_timeout_threshold_is_on_time() {
        let mut receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        receiver.config.timeout = Some(Duration::from_millis(10));
        receiver.config.collection_interval = Duration::from_millis(10);
        let calls = Rc::new(std::cell::Cell::new(0_usize));
        let collect_calls = Rc::clone(&calls);
        let result = receiver
            .collect_and_send_with(
                move || {
                    let call = collect_calls.get();
                    collect_calls.set(call + 1);
                    async move {
                        if call == 0 {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            Ok(Sample {
                                timestamp_unix_nano: 100,
                                points: vec![sample_point(0, 100, Number::Integer(1))],
                                metadata: collected_metadata(),
                            })
                        } else {
                            Err(pdh::Error::WorkerStopped)
                        }
                    }
                },
                |_| async { Ok(()) },
                |_| async { Ok(()) },
            )
            .await;

        assert!(matches!(
            result,
            Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
        ));
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.health.scrapes.get(), 1);
        assert_eq!(metrics.health.scrape_overruns.get(), 0);
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
                    name: Arc::from("windows.memory.available"),
                    unit: Arc::from("By"),
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
                warnings: Vec::new(),
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

    /// Scenario: The registered factory starts a live receiver with exact, aggregate, wildcard, and unavailable peer mappings.
    /// Guarantees: The engine handoff decodes to Memory, _Total, and startup-expanded metrics while the unavailable peer is omitted.
    #[test]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    fn live_factory_collects_decodes_and_hands_off_metrics() {
        let test_runtime = TestRuntime::<OtapPdata>::new();
        let (pipeline, _) = test_pipeline_ctx();
        let factory = OTAP_PIPELINE_FACTORY
            .get_receiver_factory_map()
            .get(WINDOWSPERFCOUNTERS_RECEIVER_URN)
            .expect("registered windowsperfcounters factory");
        let mut node_config = NodeUserConfig::new_receiver_config(WINDOWSPERFCOUNTERS_RECEIVER_URN);
        node_config.config = serde_json::json!({
            "metrics": {
                "windows.memory.available": {
                    "unit": "By",
                    "gauge": {}
                },
                "windows.processor.time": {
                    "unit": "%",
                    "gauge": {}
                },
                "windows.process.id": {
                    "unit": "1",
                    "gauge": {}
                },
                "windows.memory.missing": {
                    "unit": "By",
                    "gauge": {}
                }
            },
            "perfcounters": [
                {
                    "object": "Memory",
                    "counters": [{
                        "name": "Available Bytes",
                        "metric": "windows.memory.available"
                    }]
                },
                {
                    "object": "Processor",
                    "instances": ["_Total"],
                    "counters": [{
                        "name": "% Processor Time",
                        "metric": "windows.processor.time"
                    }]
                },
                {
                    "object": "Process",
                    "instances": ["*"],
                    "counters": [{
                        "name": "ID Process",
                        "metric": "windows.process.id"
                    }]
                },
                {
                    "object": "Memory",
                    "counters": [{
                        "name": "otel-arrow-nonexistent-counter",
                        "metric": "windows.memory.missing"
                    }]
                }
            ],
            "collection_interval": "10s",
            "initial_delay": "500ms",
            "timeout": "5s",
            "wildcard_refresh_interval": "30s",
            "max_instances_per_wildcard": 256,
            "max_expanded_counters": 512
        });
        let receiver = (factory.create)(
            pipeline,
            test_node(test_runtime.config().name.clone()),
            Arc::new(node_config),
            test_runtime.config(),
            &Capabilities::empty(),
        )
        .expect("factory should create the live receiver");

        test_runtime
            .set_receiver(receiver)
            .run_test(|ctx| async move {
                ctx.sleep(Duration::from_millis(1_500)).await;
                ctx.send_shutdown(
                    Instant::now() + Duration::from_secs(3),
                    "live factory handoff complete",
                )
                .await
                .unwrap();
            })
            .run_validation(|mut ctx| async move {
                let pdata = ctx
                    .recv()
                    .await
                    .expect("receiver should hand off one batch");
                let (_, payload) = pdata.into_parts();
                let PayloadData::OtapRecords(records) = payload.into_data() else {
                    panic!("factory handoff must contain decoded OTAP records");
                };
                let bytes: OtlpProtoBytes = OtapPayload::from_otap(records)
                    .try_into_with_default()
                    .expect("OTAP records should decode to OTLP");
                let request = ExportMetricsServiceRequest::decode(bytes.as_bytes())
                    .expect("decoded OTLP metrics request");
                let metrics = request
                    .resource_metrics
                    .iter()
                    .flat_map(|resource| &resource.scope_metrics)
                    .flat_map(|scope| &scope.metrics)
                    .collect::<Vec<_>>();
                let metric = |name: &str| metrics.iter().find(|metric| metric.name == name);

                let memory = metric("windows.memory.available").expect("Memory metric");
                let Some(metric::Data::Gauge(memory)) = &memory.data else {
                    panic!("Memory metric must be a Gauge");
                };
                assert_eq!(memory.data_points.len(), 1);

                let processor = metric("windows.processor.time").expect("_Total metric");
                let Some(metric::Data::Gauge(processor)) = &processor.data else {
                    panic!("_Total metric must be a Gauge");
                };
                assert_eq!(processor.data_points.len(), 1);
                let aggregate = processor.data_points[0]
                    .attributes
                    .iter()
                    .find(|attribute| attribute.key == "windows.perf_counter.instance")
                    .expect("_Total instance identity");
                assert!(matches!(
                    aggregate.value.as_ref().and_then(|value| value.value.as_ref()),
                    Some(any_value::Value::StringValue(value)) if value == "_Total"
                ));

                let process = metric("windows.process.id").expect("expanded Process metric");
                let Some(metric::Data::Gauge(process)) = &process.data else {
                    panic!("expanded Process metric must be a Gauge");
                };
                assert!(!process.data_points.is_empty());
                assert!(process.data_points.iter().all(|point| {
                    point
                        .attributes
                        .iter()
                        .any(|attribute| attribute.key == "windows.perf_counter.instance")
                }));
                assert!(metric("windows.memory.missing").is_none());
            });
    }

    /// Scenario: The engine requests ingress drain and shutdown from a live receiver.
    /// Guarantees: Both lifecycle paths stop the PDH worker and complete before their deadlines.
    #[test]
    #[ignore = "requires live Windows performance counters; run explicitly with `-- --ignored`"]
    fn completes_drain_and_shutdown_through_engine_harness() {
        run_live_lifecycle(true);
        run_live_lifecycle(false);
    }

    async fn loop_collection_start_offsets(
        period: Duration,
        timeout: Option<Duration>,
        first_collection_duration: Duration,
    ) -> Vec<Duration> {
        let mut receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        receiver.config.collection_interval = period;
        receiver.config.timeout = timeout;
        let origin = tokio::time::Instant::now();
        let starts = Rc::new(RefCell::new(Vec::new()));
        let collect_starts = Rc::clone(&starts);
        let result = receiver
            .collect_and_send_with(
                move || {
                    let call = collect_starts.borrow().len();
                    collect_starts
                        .borrow_mut()
                        .push(tokio::time::Instant::now().duration_since(origin));
                    async move {
                        if call > 0 {
                            return Err(pdh::Error::WorkerStopped);
                        }
                        tokio::time::sleep(first_collection_duration).await;
                        Ok(Sample {
                            timestamp_unix_nano: 100,
                            points: vec![sample_point(0, 100, Number::Integer(1))],
                            metadata: PendingMetadata {
                                diagnostics: SampleDiagnostics {
                                    any_query_collected: true,
                                    ..Default::default()
                                },
                                ..Default::default()
                            },
                        })
                    }
                },
                |_| async { Ok(()) },
                |_| async { Ok(()) },
            )
            .await;
        assert!(matches!(
            result,
            Err(CollectionLoopError::Native(pdh::Error::WorkerStopped))
        ));
        starts.borrow().clone()
    }

    /// Scenario: Timeout is omitted and one collection spans multiple original cadence points.
    /// Guarantees: The loop skips elapsed ticks and starts next at the first future point on the original schedule.
    #[tokio::test(start_paused = true)]
    async fn no_timeout_skips_to_next_original_cadence_point() {
        assert_eq!(
            loop_collection_start_offsets(
                Duration::from_millis(10),
                None,
                Duration::from_millis(25),
            )
            .await,
            [Duration::ZERO, Duration::from_millis(30)]
        );
    }

    /// Scenario: Timeout exceeds the period and collection finishes after multiple scheduled ticks but before timeout.
    /// Guarantees: Missed cadence is detected independently of timeout telemetry and no immediate catch-up collection runs.
    #[tokio::test(start_paused = true)]
    async fn long_timeout_still_skips_missed_collection_ticks() {
        assert_eq!(
            loop_collection_start_offsets(
                Duration::from_millis(10),
                Some(Duration::from_millis(50)),
                Duration::from_millis(25),
            )
            .await,
            [Duration::ZERO, Duration::from_millis(30)]
        );
    }

    /// Scenario: Timeout is shorter than the period and collection crosses timeout but finishes before the next scheduled tick.
    /// Guarantees: Overrun telemetry does not move or drop the still-future original cadence point.
    #[tokio::test(start_paused = true)]
    async fn short_timeout_preserves_unmissed_collection_tick() {
        assert_eq!(
            loop_collection_start_offsets(
                Duration::from_millis(20),
                Some(Duration::from_millis(5)),
                Duration::from_millis(10),
            )
            .await,
            [Duration::ZERO, Duration::from_millis(20)]
        );
    }
}
