// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Windows-only, single-core receiver for exact and calculated performance metrics.

use super::config::Config;
use super::metrics::WindowsPerfCountersReceiverMetrics;
use super::model::{MonotonicPublication, Sample, SampleFailure};
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

fn append_projection_failures(sample: &mut Sample, failures: Vec<SampleFailure>) {
    sample.metadata.diagnostics.counter_read_failures = sample
        .metadata
        .diagnostics
        .counter_read_failures
        .saturating_add(u64::try_from(failures.len()).unwrap_or(u64::MAX));
    sample.metadata.failures.extend(failures);
}

fn scrape_overran(collection_interval: Duration, elapsed: Duration) -> bool {
    elapsed > collection_interval
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
                "windows_perf_counters_receiver.scrape.fail",
                node = node,
                error = %error
            );
            self.scrape_failed = true;
        }
    }

    fn log_scrape_timeout(&mut self, node: &str, timeout: Duration) {
        if !self.scrape_failed {
            otel_arrow_dfe_telemetry::otel_warn!(
                "windows_perf_counters_receiver.scrape.timeout",
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
    ) -> Result<(), usize> {
        let mut current = BTreeSet::new();
        for failure in &sample.metadata.failures {
            let Some(counter) = receiver.config.counters.get(failure.counter_index) else {
                return Err(failure.counter_index);
            };
            let key = (failure.counter_index, failure.reason);
            if !self.active_counter_failures.contains(&key) {
                otel_arrow_dfe_telemetry::otel_warn!(
                    "windows_perf_counters_receiver.counter.fail",
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

#[derive(Debug)]
enum ProcessSampleError {
    Projection(ProjectionInvariantError),
    MissingCounter(usize),
    Send(Error),
}

#[derive(Debug)]
enum CollectionLoopError {
    Native(pdh::Error),
    Sample(ProcessSampleError),
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

    fn record_sample_diagnostics(&self, sample: &Sample) {
        let mut metrics = self.metrics.borrow_mut();
        metrics.health.apply(&sample.metadata.diagnostics);
        metrics
            .health
            .failed_counter_values
            .add(failed_counter_value_count(&sample.metadata.failures));
    }

    fn record_successful_sample(&self, scrape_start: Instant, sample: &Sample, overrun: bool) {
        self.record_completed_scrape(scrape_start, true, overrun);
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
        mut sample: Sample,
        scrape_start: Instant,
        overrun: bool,
        failure_logs: &mut FailureLogState,
        send: F,
    ) -> Result<Vec<MonotonicPublication>, ProcessSampleError>
    where
        F: FnOnce(OtapPdata) -> Fut,
        Fut: Future<Output = Result<(), Error>>,
    {
        let projection = match project_sample(&self.config.counters, &sample) {
            Ok(projection) => projection,
            Err(err) => {
                self.record_completed_scrape(scrape_start, false, overrun);
                return Err(ProcessSampleError::Projection(err));
            }
        };
        append_projection_failures(&mut sample, projection.point_failures);
        self.record_successful_sample(scrape_start, &sample, overrun);
        failure_logs.record_success();
        self.log_sample_events(&sample);
        failure_logs
            .log_counter_failures(self, &sample)
            .map_err(ProcessSampleError::MissingCounter)?;
        let Some(records) = projection.records else {
            return Ok(Vec::new());
        };
        send(OtapPdata::new(Context::default(), records.into()))
            .await
            .map_err(ProcessSampleError::Send)?;
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
            let _ = interval.tick().await;
            let scrape_start = Instant::now();
            let sample = match with_optional_timeout(self.config.timeout, collect()).await {
                Ok(Ok(sample)) => sample,
                Ok(Err(err)) if err.is_collection_failure() => {
                    let overrun = err.is_overrun()
                        || scrape_overran(self.config.collection_interval, scrape_start.elapsed());
                    self.record_completed_scrape(scrape_start, false, overrun);
                    failure_logs.log_scrape_failure(&self.node, &err);
                    continue;
                }
                Ok(Err(err)) => {
                    self.metrics.borrow_mut().record_scrape(Outcome::Failure);
                    return Err(CollectionLoopError::Native(err));
                }
                Err(_) => {
                    self.record_completed_scrape(scrape_start, false, true);
                    failure_logs
                        .log_scrape_timeout(&self.node, self.config.timeout.unwrap_or_default());
                    continue;
                }
            };
            let overrun = scrape_overran(self.config.collection_interval, scrape_start.elapsed());
            let publications = self
                .process_collected_sample(
                    sample,
                    scrape_start,
                    overrun,
                    &mut failure_logs,
                    &mut send,
                )
                .await
                .map_err(CollectionLoopError::Sample)?;
            publish(publications)
                .await
                .map_err(CollectionLoopError::Native)?;
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
            CollectionLoopError::Sample(ProcessSampleError::Projection(error)) => {
                receiver_error(effect_handler, error.to_string(), "")
            }
            CollectionLoopError::Sample(ProcessSampleError::MissingCounter(counter_index)) => {
                receiver_error(
                    effect_handler,
                    format!("sample failure references missing configured counter {counter_index}"),
                    "",
                )
            }
            CollectionLoopError::Sample(ProcessSampleError::Send(error)) => error,
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
        Number, SamplePoint, SampleValue,
    };
    use arrow::array::TimestampNanosecondArray;
    use arrow::util::display::array_value_to_string;
    use otel_arrow_dfe_config::node::NodeUserConfig;
    use otel_arrow_dfe_engine::testing::receiver::TestRuntime;
    use otel_arrow_dfe_engine::testing::{test_node, test_pipeline_ctx};
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
    use otel_arrow_dfe_pdata_codec::PayloadData;
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
            monotonic_publication_generation: None,
            value: SampleValue::Value(value),
        }
    }

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

    /// Scenario: Projection omits two invalid points after native collection already reported one read failure.
    /// Guarantees: Health telemetry stays truthful when native and projection failures share one scrape.
    #[test]
    fn projection_failures_extend_sample_accounting() {
        let mut sample = Sample::empty(1);
        sample.metadata.diagnostics.counter_read_failures = 1;
        sample.metadata.failures.push(SampleFailure {
            counter_index: 0,
            reason: "counter CStatus",
            error: "PDH_INVALID_DATA".to_owned(),
            diagnostic_only: false,
        });

        append_projection_failures(
            &mut sample,
            vec![
                SampleFailure {
                    counter_index: 1,
                    reason: "projection.point",
                    error: "invalid timestamp".to_owned(),
                    diagnostic_only: false,
                },
                SampleFailure {
                    counter_index: 2,
                    reason: "projection.point",
                    error: "invalid start time".to_owned(),
                    diagnostic_only: false,
                },
            ],
        );

        assert_eq!(sample.metadata.diagnostics.counter_read_failures, 3);
        assert_eq!(sample.metadata.failures.len(), 3);
        assert_eq!(failed_counter_value_count(&sample.metadata.failures), 3);
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

    /// Scenario: One collected scrape contains a healthy point beside a point with an invalid timestamp.
    /// Guarantees: Runtime delivery keeps the healthy value and only its attributes, preventing one bad provider value from corrupting the Arrow batch or health accounting.
    #[tokio::test]
    async fn runtime_delivers_healthy_peer_without_orphan_attributes() {
        let receiver = test_receiver(vec![test_counter(BTreeMap::from([(
            "role".to_owned(),
            "primary".to_owned(),
        )]))]);
        let sent = Rc::new(RefCell::new(Vec::new()));
        let capture = Rc::clone(&sent);
        let _ = receiver
            .process_collected_sample(
                Sample {
                    timestamp_unix_nano: 10,
                    points: vec![
                        sample_point(0, 10, Number::Integer(42)),
                        sample_point(0, 0, Number::Integer(99)),
                    ],
                    metadata: Default::default(),
                },
                Instant::now(),
                false,
                &mut FailureLogState::default(),
                move |pdata| async move {
                    capture.borrow_mut().push(pdata);
                    Ok(())
                },
            )
            .await
            .expect("healthy peer should be delivered");

        let pdata = sent.borrow_mut().pop().expect("one batch should be sent");
        let (_, payload) = pdata.into_parts();
        let PayloadData::OtapRecords(records) = payload.into_data() else {
            panic!("expected native OTAP records");
        };
        let points = records
            .get(ArrowPayloadType::NumberDataPoints)
            .expect("number data points");
        assert_eq!(points.num_rows(), 1);
        assert_eq!(
            array_value_to_string(points.column_by_name("int_value").unwrap().as_ref(), 0).unwrap(),
            "42"
        );
        let attributes = records
            .get(ArrowPayloadType::NumberDpAttrs)
            .expect("number point attributes");
        let parent_ids = attributes.column_by_name("parent_id").unwrap();
        let keys = attributes.column_by_name("key").unwrap();
        let values = attributes.column_by_name("str").unwrap();
        let decoded = (0..attributes.num_rows())
            .map(|index| {
                (
                    array_value_to_string(parent_ids.as_ref(), index).unwrap(),
                    array_value_to_string(keys.as_ref(), index).unwrap(),
                    array_value_to_string(values.as_ref(), index).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            decoded,
            vec![
                ("0".to_owned(), "role".to_owned(), "primary".to_owned()),
                (
                    "0".to_owned(),
                    "windows.perf_counter.path".to_owned(),
                    r"\Memory\Available Bytes".to_owned()
                )
            ]
        );

        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.scrape_attempts(Outcome::Success), 1);
        assert_eq!(metrics.scrape_attempts(Outcome::Failure), 0);
        assert_eq!(metrics.health.scrapes.get(), 1);
        assert_eq!(metrics.health.scrape_failures.get(), 0);
        assert_eq!(metrics.health.counter_read_failures.get(), 1);
        assert_eq!(metrics.health.failed_counter_values.get(), 1);
    }

    /// Scenario: A scrape has only an expected invalid point and the following scrape is valid.
    /// Guarantees: Expected point rejection cannot create a hidden batch-failure state that suppresses the next delivery or distorts cumulative health.
    #[tokio::test]
    async fn runtime_continues_after_expected_point_rejection() {
        let receiver = test_receiver(vec![test_counter(BTreeMap::new())]);
        let sent = Rc::new(RefCell::new(Vec::new()));
        let mut failure_logs = FailureLogState::default();

        for (timestamp, value) in [(0, 99), (20, 42)] {
            let capture = Rc::clone(&sent);
            let _ = receiver
                .process_collected_sample(
                    Sample {
                        timestamp_unix_nano: if timestamp == 0 { 10 } else { timestamp },
                        points: vec![sample_point(0, timestamp, Number::Integer(value))],
                        metadata: Default::default(),
                    },
                    Instant::now(),
                    false,
                    &mut failure_logs,
                    move |pdata| async move {
                        capture.borrow_mut().push(pdata);
                        Ok(())
                    },
                )
                .await
                .expect("expected point rejection should not terminate collection");

            let metrics = receiver.metrics.borrow();
            if timestamp == 0 {
                assert!(sent.borrow().is_empty());
                assert_eq!(metrics.health.scrapes.get(), 1);
                assert_eq!(metrics.health.counter_read_failures.get(), 1);
                assert_eq!(metrics.health.failed_counter_values.get(), 1);
            }
        }

        assert_eq!(sent.borrow().len(), 1);
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.scrape_attempts(Outcome::Success), 2);
        assert_eq!(metrics.scrape_attempts(Outcome::Failure), 0);
        assert_eq!(metrics.health.scrapes.get(), 2);
        assert_eq!(metrics.health.scrape_failures.get(), 0);
        assert_eq!(metrics.health.counter_read_failures.get(), 1);
        assert_eq!(metrics.health.failed_counter_values.get(), 1);
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
                    metadata: Default::default(),
                },
                Instant::now(),
                false,
                &mut FailureLogState::default(),
                move |pdata| async move {
                    capture.borrow_mut().push(pdata);
                    Ok(())
                },
            )
            .await;

        assert!(matches!(result, Err(ProcessSampleError::Projection(_))));
        assert!(sent.borrow().is_empty());
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.scrape_attempts(Outcome::Success), 0);
        assert_eq!(metrics.scrape_attempts(Outcome::Failure), 1);
        assert_eq!(metrics.health.scrapes.get(), 0);
        assert_eq!(metrics.health.scrape_failures.get(), 1);
        assert_eq!(metrics.health.counter_read_failures.get(), 0);
        assert_eq!(metrics.health.failed_counter_values.get(), 0);
    }

    /// Scenario: Two collections each finish after the cadence while timeout is disabled.
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
                    metadata: Default::default(),
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

    /// Scenario: A positive timeout discards one monotonic candidate before the next scrape is projected, sent, and acknowledged.
    /// Guarantees: Runtime acknowledges only the delivered point, whose unknown cumulative start equals its own timestamp rather than the discarded scrape's timestamp.
    #[tokio::test]
    async fn timeout_does_not_publish_discarded_monotonic_start() {
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
                if call >= 2 {
                    return std::future::pending::<Result<Sample, pdh::Error>>().await;
                }
                let timestamp = if call == 0 { 100 } else { 200 };
                let sample = Sample {
                    timestamp_unix_nano: timestamp,
                    points: vec![SamplePoint {
                        counter_index: 0,
                        path: Arc::from(r"\Memory\Available Bytes"),
                        instance: None,
                        emit_instance_attribute: false,
                        timestamp_unix_nano: timestamp,
                        start_time_unix_nano: Some(start.get().unwrap_or(timestamp)),
                        monotonic_publication_generation: Some(0),
                        value: SampleValue::Value(Number::Integer(call as i64 + 1)),
                    }],
                    metadata: Default::default(),
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
            while collect_calls.get() < 3 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::select! {
            biased;
            _ = &mut run => panic!("collection loop stopped after timeout recovery"),
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
                200
            );
        }
        assert_eq!(published_start.get(), Some(200));
        let metrics = receiver.metrics.borrow();
        assert_eq!(metrics.scrape_attempts(Outcome::Success), 1);
        assert_eq!(metrics.scrape_attempts(Outcome::Failure), 1);
        assert_eq!(metrics.health.scrapes.get(), 1);
        assert_eq!(metrics.health.scrape_failures.get(), 1);
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

    /// Scenario: Scrape discard is explicitly enabled or left disabled.
    /// Guarantees: Default late data is retained while a positive deadline can intentionally discard it.
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

    /// Scenario: Collection finishes before, exactly at, or after its scheduled cadence.
    /// Guarantees: Only genuinely late completion increments overrun telemetry without turning success into failure.
    #[test]
    fn classifies_collection_cadence_overrun() {
        let interval = Duration::from_secs(1);

        assert!(!scrape_overran(
            interval,
            interval - Duration::from_nanos(1)
        ));
        assert!(!scrape_overran(interval, interval));
        assert!(scrape_overran(interval, interval + Duration::from_nanos(1)));
    }

    /// Scenario: One collection spans several scheduled interval ticks.
    /// Guarantees: One immediate tick follows the overrun, then cadence resumes without a burst of every missed scrape.
    #[tokio::test(start_paused = true)]
    async fn missed_collection_ticks_are_skipped() {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let _ = interval.tick().await;

        tokio::time::advance(Duration::from_millis(3_500)).await;
        let _ = interval.tick().await;
        let next = interval.tick();
        tokio::pin!(next);
        tokio::select! {
            biased;
            _ = &mut next => panic!("missed ticks queued an immediate extra scrape"),
            () = tokio::task::yield_now() => {}
        }

        tokio::time::advance(Duration::from_millis(499)).await;
        tokio::select! {
            biased;
            _ = &mut next => panic!("next scrape ran before the cadence boundary"),
            () = tokio::task::yield_now() => {}
        }
        tokio::time::advance(Duration::from_millis(1)).await;
        let _ = next.await;
    }
}
