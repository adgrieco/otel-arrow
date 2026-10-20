// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::super::config::{Aggregation, CounterConfig, InstanceConfig, MetricKind};
use super::super::model::{
    ExpansionOverflow, InstanceIdentity, MonotonicPublication, MonotonicPublicationCandidate,
    Number, PendingMetadata, Sample, SampleDiagnostics, SampleFailure, SampleValue,
    merge_diagnostics, scale_integer,
};
use super::super::{
    TEST_PDH_LOCK,
    config::Config,
    otap_builder::{into_otap, project_sample},
};
use super::counter_type::*;
use super::expansion::*;
use super::native::NativePdh;
use super::query::*;
use super::timing::*;
use super::worker::*;
use super::*;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

fn counter(path: &str, name: &str) -> CounterConfig {
    CounterConfig {
        path: path.to_owned(),
        name: Arc::from(name),
        unit: Arc::from("By"),
        description: Arc::from(format!("Description for {name}.")),
        metric_kind: MetricKind::Gauge,
        attributes: Arc::new(BTreeMap::new()),
        instance_config: InstanceConfig::None,
        scale_power10: 0,
        recreate_query: false,
        source_mapping_index: 0,
    }
}

fn monotonic_sum(path: &str, name: &str, unit: &str) -> CounterConfig {
    CounterConfig {
        metric_kind: MetricKind::Sum {
            aggregation: Aggregation::Cumulative,
            monotonic: true,
        },
        unit: Arc::from(unit),
        ..counter(path, name)
    }
}

fn delta_sum(path: &str, name: &str, monotonic: bool) -> CounterConfig {
    CounterConfig {
        metric_kind: MetricKind::Sum {
            aggregation: Aggregation::Delta,
            monotonic,
        },
        unit: Arc::from("{operation}"),
        ..counter(path, name)
    }
}

fn query_counts() -> (usize, usize) {
    (
        QUERY_OPENS.load(Ordering::Relaxed),
        QUERY_CLOSES.load(Ordering::Relaxed),
    )
}

struct FakeCounter {
    path: String,
    native_type: u32,
    raw: Option<i64>,
    multi_count: u32,
    formatted: Option<Number>,
    raw_error: bool,
    formatted_error: bool,
}

fn fake_counter(path: &str, formatted: Number) -> FakeCounter {
    FakeCounter {
        path: path.to_owned(),
        native_type: 0x0001_0100,
        raw: None,
        multi_count: 1,
        formatted: Some(formatted),
        raw_error: false,
        formatted_error: false,
    }
}

struct FakeState {
    counters: Vec<FakeCounter>,
    added: Vec<String>,
    calls: FakeNativeCalls,
    collect_has_data: bool,
    collect_error: bool,
    collect_status: Option<u32>,
    timestamp: i64,
    timestamp_error: bool,
    remove_error: bool,
    expand_error: bool,
    expand_errors: BTreeSet<String>,
    stop_after_expand: Option<Arc<AtomicBool>>,
    stop_after_add: Option<Arc<AtomicBool>>,
    removed: Vec<usize>,
    expansions: BTreeMap<String, Vec<String>>,
}

#[derive(Default)]
struct FakeNativeCalls {
    add: usize,
    counter_type: usize,
    collect: usize,
    raw: usize,
    formatted: usize,
    timestamp: usize,
    expand: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FakeCounterToken {
    index: usize,
    generation: u64,
}

struct FakePdh {
    state: Rc<RefCell<FakeState>>,
    generations: Vec<u64>,
    active: BTreeSet<FakeCounterToken>,
}

impl FakePdh {
    fn new(state: Rc<RefCell<FakeState>>) -> Self {
        let counter_count = state.borrow().counters.len();
        Self {
            state,
            generations: vec![0; counter_count],
            active: BTreeSet::new(),
        }
    }

    fn counter_index(&self, counter: &FakeCounterToken, path: &str) -> Result<usize, Error> {
        if self.active.contains(counter) {
            Ok(counter.index)
        } else {
            Err(Error::InvalidCounterToken {
                path: path.to_owned(),
            })
        }
    }
}

impl PdhApi for FakePdh {
    type Counter = FakeCounterToken;

    fn check_running(&self) -> Result<(), Error> {
        let state = self.state.borrow();
        if state
            .stop_after_expand
            .iter()
            .chain(state.stop_after_add.iter())
            .any(|stop| stop.load(Ordering::Acquire))
        {
            Err(Error::WorkerInitStopped)
        } else {
            Ok(())
        }
    }

    fn add(&mut self, path: &str) -> Result<Self::Counter, Error> {
        let mut state = self.state.borrow_mut();
        state.calls.add += 1;
        let counter = state
            .counters
            .iter()
            .position(|counter| counter.path == path)
            .ok_or_else(|| Error::Pdh {
                operation: PdhOperation::AddEnglishCounter,
                path: path.to_owned(),
                status: 1,
            })?;
        state.added.push(path.to_owned());
        if let Some(stop) = &state.stop_after_add {
            stop.store(true, Ordering::Release);
        }
        if counter >= self.generations.len() {
            self.generations.resize(counter + 1, 0);
        }
        self.generations[counter] = self.generations[counter].wrapping_add(1);
        let generation = self.generations[counter];
        let token = FakeCounterToken {
            index: counter,
            generation,
        };
        let _ = self.active.insert(token);
        Ok(token)
    }

    fn remove(&mut self, counter: &Self::Counter, path: &str) -> Result<(), Error> {
        let index = self.counter_index(counter, path)?;
        let mut state = self.state.borrow_mut();
        state.removed.push(index);
        if state.remove_error {
            Err(Error::Pdh {
                operation: PdhOperation::RemoveCounter,
                path: state.counters[index].path.clone(),
                status: 1,
            })
        } else {
            let _ = self.active.remove(counter);
            Ok(())
        }
    }

    fn counter_type(&mut self, counter: &Self::Counter, path: &str) -> Result<u32, Error> {
        let index = self.counter_index(counter, path)?;
        let mut state = self.state.borrow_mut();
        state.calls.counter_type += 1;
        Ok(state.counters[index].native_type)
    }

    fn collect(&mut self) -> Result<bool, Error> {
        let mut state = self.state.borrow_mut();
        state.calls.collect += 1;
        if state.collect_error || state.collect_status.is_some() {
            Err(Error::Pdh {
                operation: PdhOperation::CollectQueryData,
                path: "<query>".to_owned(),
                status: state.collect_status.unwrap_or(1),
            })
        } else {
            Ok(state.collect_has_data)
        }
    }

    fn raw(&mut self, counter: &Self::Counter, path: &str) -> Result<Option<RawSample>, Error> {
        let index = self.counter_index(counter, path)?;
        let mut state = self.state.borrow_mut();
        state.calls.raw += 1;
        let counter = &state.counters[index];
        if counter.raw_error {
            Err(Error::Pdh {
                operation: PdhOperation::GetRawCounterValue,
                path: path.to_owned(),
                status: 1,
            })
        } else {
            Ok(counter.raw.map(|second_value| RawSample {
                second_value,
                multi_count: counter.multi_count,
            }))
        }
    }

    fn formatted(
        &mut self,
        counter: &Self::Counter,
        path: &str,
        format: ValueFormat,
    ) -> Result<Option<Number>, Error> {
        let index = self.counter_index(counter, path)?;
        let mut state = self.state.borrow_mut();
        state.calls.formatted += 1;
        let counter = &state.counters[index];
        if counter.formatted_error {
            return Err(Error::CounterData {
                operation: PdhOperation::CounterDataStatus,
                path: path.to_owned(),
                status: 1,
            });
        }
        if let Some(number) = counter.formatted {
            assert!(matches!(
                (format, number),
                (ValueFormat::Integer, Number::Integer(_))
                    | (ValueFormat::Double, Number::Double(_))
            ));
        }
        Ok(counter.formatted)
    }

    fn timestamp(&mut self) -> Result<i64, Error> {
        let mut state = self.state.borrow_mut();
        state.calls.timestamp += 1;
        if state.timestamp_error {
            Err(Error::InvalidSample("test timestamp failure"))
        } else {
            Ok(state.timestamp)
        }
    }

    fn expand(&mut self, path: &str) -> Result<Vec<String>, Error> {
        let mut state = self.state.borrow_mut();
        state.calls.expand += 1;
        if let Some(stop) = &state.stop_after_expand {
            stop.store(true, Ordering::Release);
        }
        if state.expand_error || state.expand_errors.contains(path) {
            return Err(Error::Pdh {
                operation: PdhOperation::ExpandWildcardPath,
                path: path.to_owned(),
                status: 1,
            });
        }
        Ok(state.expansions.get(path).cloned().unwrap_or_default())
    }

    fn parse_instance(&mut self, path: &str) -> Result<InstanceIdentity, Error> {
        NativePdh::parse_instance_path(path)
    }
}

struct RecoveringPrimePdh {
    collect_calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl PdhApi for RecoveringPrimePdh {
    type Counter = ();

    fn add(&mut self, _path: &str) -> Result<Self::Counter, Error> {
        Ok(())
    }

    fn remove(&mut self, _counter: &Self::Counter, _path: &str) -> Result<(), Error> {
        Ok(())
    }

    fn counter_type(&mut self, _counter: &Self::Counter, _path: &str) -> Result<u32, Error> {
        Ok(0x2041_0500)
    }

    fn collect(&mut self) -> Result<bool, Error> {
        if self.collect_calls.fetch_add(1, Ordering::Relaxed) == 0 {
            Err(Error::Pdh {
                operation: PdhOperation::CollectQueryData,
                path: "<query>".to_owned(),
                status: 1,
            })
        } else {
            Ok(true)
        }
    }

    fn raw(&mut self, _counter: &Self::Counter, _path: &str) -> Result<Option<RawSample>, Error> {
        Ok(Some(RawSample {
            second_value: i64::try_from(self.collect_calls.load(Ordering::Relaxed))
                .unwrap_or(i64::MAX),
            multi_count: 1,
        }))
    }

    fn formatted(
        &mut self,
        _counter: &Self::Counter,
        _path: &str,
        _format: ValueFormat,
    ) -> Result<Option<Number>, Error> {
        Ok(Some(Number::Double(1.0)))
    }

    fn timestamp(&mut self) -> Result<i64, Error> {
        Ok(100 + i64::try_from(self.collect_calls.load(Ordering::Relaxed)).unwrap_or(i64::MAX))
    }

    fn expand(&mut self, _path: &str) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }

    fn parse_instance(&mut self, path: &str) -> Result<InstanceIdentity, Error> {
        Err(Error::InvalidCounterToken {
            path: path.to_owned(),
        })
    }
}

struct RebuildPrimePdh {
    fail_collect: Arc<AtomicBool>,
    collect_calls: usize,
}

impl PdhApi for RebuildPrimePdh {
    type Counter = ();

    fn add(&mut self, _path: &str) -> Result<Self::Counter, Error> {
        Ok(())
    }

    fn remove(&mut self, _counter: &Self::Counter, _path: &str) -> Result<(), Error> {
        Ok(())
    }

    fn counter_type(&mut self, _counter: &Self::Counter, _path: &str) -> Result<u32, Error> {
        Ok(0x2041_0500)
    }

    fn collect(&mut self) -> Result<bool, Error> {
        if self.fail_collect.load(Ordering::Relaxed) {
            return Err(Error::Pdh {
                operation: PdhOperation::CollectQueryData,
                path: "<query>".to_owned(),
                status: 1,
            });
        }
        self.collect_calls += 1;
        Ok(true)
    }

    fn raw(&mut self, _counter: &Self::Counter, _path: &str) -> Result<Option<RawSample>, Error> {
        Ok(Some(RawSample {
            second_value: i64::try_from(self.collect_calls).unwrap_or(i64::MAX) * 10,
            multi_count: 1,
        }))
    }

    fn formatted(
        &mut self,
        _counter: &Self::Counter,
        _path: &str,
        _format: ValueFormat,
    ) -> Result<Option<Number>, Error> {
        Ok(Some(Number::Double(self.collect_calls as f64)))
    }

    fn timestamp(&mut self) -> Result<i64, Error> {
        Ok(100 + i64::try_from(self.collect_calls).unwrap_or(i64::MAX) * 10)
    }

    fn expand(&mut self, _path: &str) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }

    fn parse_instance(&mut self, path: &str) -> Result<InstanceIdentity, Error> {
        Err(Error::InvalidCounterToken {
            path: path.to_owned(),
        })
    }
}

fn fake_query(
    configs: Vec<CounterConfig>,
    counters: Vec<FakeCounter>,
) -> (Rc<RefCell<FakeState>>, Query<FakePdh>) {
    fake_query_with_cadences(
        configs,
        counters,
        Duration::from_secs(30),
        Duration::from_secs(30),
    )
}

fn fake_query_with_cadences(
    configs: Vec<CounterConfig>,
    counters: Vec<FakeCounter>,
    exact_retry_cap: Duration,
    wildcard_refresh_interval: Duration,
) -> (Rc<RefCell<FakeState>>, Query<FakePdh>) {
    let state = fake_state(counters);
    let query = Query::open_with_seed(
        FakePdh::new(Rc::clone(&state)),
        configs,
        exact_retry_cap,
        wildcard_refresh_interval,
        256,
        4_096,
        None,
    )
    .unwrap();
    (state, query)
}

fn fake_state_value(counters: Vec<FakeCounter>) -> FakeState {
    FakeState {
        counters,
        added: Vec::new(),
        calls: FakeNativeCalls::default(),
        collect_has_data: true,
        collect_error: false,
        collect_status: None,
        timestamp: 100,
        timestamp_error: false,
        remove_error: false,
        expand_error: false,
        expand_errors: BTreeSet::new(),
        stop_after_expand: None,
        stop_after_add: None,
        removed: Vec::new(),
        expansions: BTreeMap::new(),
    }
}

fn fake_state(counters: Vec<FakeCounter>) -> Rc<RefCell<FakeState>> {
    Rc::new(RefCell::new(fake_state_value(counters)))
}

fn fake_query_with_seed(
    configs: Vec<CounterConfig>,
    counters: Vec<FakeCounter>,
    exact_retry_cap: Duration,
    wildcard_refresh_interval: Duration,
    max_native_counters: usize,
    seed: Option<&QueryRecreationSeed>,
) -> (Rc<RefCell<FakeState>>, Query<FakePdh>) {
    let state = fake_state(counters);
    let query = Query::open_managed_with_seed(
        FakePdh::new(Rc::clone(&state)),
        configs,
        exact_retry_cap,
        wildcard_refresh_interval,
        256,
        max_native_counters,
        seed,
    )
    .unwrap();
    (state, query)
}

fn advance_fake_queries(states: &Rc<RefCell<Vec<Rc<RefCell<FakeState>>>>>) {
    for state in states.borrow().iter() {
        let mut state = state.borrow_mut();
        state.timestamp += 10;
        for counter in &mut state.counters {
            if let Some(raw) = &mut counter.raw {
                *raw += 10;
            }
        }
    }
}

fn collected_monotonic_publications(sample: &Sample) -> Vec<MonotonicPublication> {
    sample
        .points
        .iter()
        .filter_map(|point| match &point.monotonic_publication {
            Some(MonotonicPublicationCandidate::Ready(publication)) => Some(publication.clone()),
            Some(MonotonicPublicationCandidate::Pending) | None => None,
        })
        .collect()
}

/// Scenario: A fake PDH add receives one concrete English wildcard expansion result.
/// Guarantees: Expanded members use the same English-add adapter route as exact configured paths.
#[test]
fn fake_pdh_records_english_expanded_add() {
    let path = r"\Process(worker)\Private Bytes";
    let state = fake_state(vec![FakeCounter {
        path: path.to_owned(),
        native_type: 0x0001_0100,
        raw: None,
        multi_count: 1,
        formatted: Some(Number::Integer(1)),
        raw_error: false,
        formatted_error: false,
    }]);
    let mut api = FakePdh::new(Rc::clone(&state));

    let _ = api.add(path).unwrap();

    assert_eq!(state.borrow().added, [path.to_owned()]);
}

/// Scenario: A fake PDH token is used after removal and after a replacement add.
/// Guarantees: Removed generations stay invalid while the replacement token remains usable.
#[test]
fn fake_pdh_rejects_removed_tokens() {
    let path = r"\Memory\Available Bytes";
    let state = fake_state(vec![FakeCounter {
        path: path.to_owned(),
        native_type: 0x0001_0100,
        raw: None,
        multi_count: 1,
        formatted: Some(Number::Integer(1)),
        raw_error: false,
        formatted_error: false,
    }]);
    let mut api = FakePdh::new(state);
    let removed = api.add(path).unwrap();
    api.remove(&removed, path).unwrap();
    let replacement = api.add(path).unwrap();

    assert!(matches!(
        api.counter_type(&removed, path),
        Err(Error::InvalidCounterToken { .. })
    ));
    assert_eq!(api.counter_type(&replacement, path).unwrap(), 0x0001_0100);
}

mod live;
mod query;
mod timing;
mod worker;
