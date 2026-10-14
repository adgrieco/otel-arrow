// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::super::config::{Aggregation, CounterConfig, MetricKind};
use super::super::model::{
    ExpansionOverflow, InstanceIdentity, Number, Sample, SampleDiagnostics, SampleFailure,
    SampleValue, merge_diagnostics, merge_sample_metadata, scale_integer,
};
use super::super::{TEST_PDH_LOCK, config::Config, otap_builder::into_otap};
use super::counter_type::*;
use super::expansion::*;
use super::native::NativePdh;
use super::query::*;
use super::timing::*;
use super::worker::*;
use super::*;
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
        name: name.to_owned(),
        unit: "By".to_owned(),
        description: Arc::from(format!("Description for {name}.")),
        metric_kind: MetricKind::Gauge,
        attributes: Arc::new(BTreeMap::new()),
        has_instance: false,
        emit_instance_attribute: false,
        excluded_aggregation_instance: None,
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
        unit: unit.to_owned(),
        ..counter(path, name)
    }
}

fn delta_sum(path: &str, name: &str, monotonic: bool) -> CounterConfig {
    CounterConfig {
        metric_kind: MetricKind::Sum {
            aggregation: Aggregation::Delta,
            monotonic,
        },
        unit: "{operation}".to_owned(),
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

struct FakeState {
    counters: Vec<FakeCounter>,
    added: Vec<String>,
    collect_has_data: bool,
    collect_error: bool,
    timestamp: i64,
    timestamp_error: bool,
    remove_error: bool,
    removed: Vec<usize>,
    expansions: BTreeMap<String, Vec<String>>,
}

struct FakePdh(Rc<RefCell<FakeState>>);

impl PdhApi for FakePdh {
    type Counter = usize;

    fn add(&mut self, path: &str, _localized: bool) -> Result<Self::Counter, Error> {
        let mut state = self.0.borrow_mut();
        let counter = state
            .counters
            .iter()
            .position(|counter| counter.path == path)
            .ok_or_else(|| Error::Pdh {
                operation: "PdhAddEnglishCounterW",
                path: path.to_owned(),
                status: 1,
            })?;
        state.added.push(path.to_owned());
        Ok(counter)
    }

    fn remove(&mut self, counter: &Self::Counter, _path: &str) -> Result<(), Error> {
        let mut state = self.0.borrow_mut();
        state.removed.push(*counter);
        if state.remove_error {
            Err(Error::Pdh {
                operation: "PdhRemoveCounter",
                path: state.counters[*counter].path.clone(),
                status: 1,
            })
        } else {
            Ok(())
        }
    }

    fn counter_type(&mut self, counter: &Self::Counter, _path: &str) -> Result<u32, Error> {
        Ok(self.0.borrow().counters[*counter].native_type)
    }

    fn collect(&mut self) -> Result<bool, Error> {
        let state = self.0.borrow();
        if state.collect_error {
            Err(Error::Pdh {
                operation: "PdhCollectQueryData",
                path: "<query>".to_owned(),
                status: 1,
            })
        } else {
            Ok(state.collect_has_data)
        }
    }

    fn raw(&mut self, counter: &Self::Counter, path: &str) -> Result<Option<RawSample>, Error> {
        let state = self.0.borrow();
        let counter = &state.counters[*counter];
        if counter.raw_error {
            Err(Error::Pdh {
                operation: "PdhGetRawCounterValue",
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
        let state = self.0.borrow();
        let counter = &state.counters[*counter];
        if counter.formatted_error {
            return Err(Error::CounterData {
                operation: "counter CStatus",
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
        let state = self.0.borrow();
        if state.timestamp_error {
            Err(Error::InvalidSample("test timestamp failure"))
        } else {
            Ok(state.timestamp)
        }
    }

    fn expand(&mut self, path: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .0
            .borrow()
            .expansions
            .get(path)
            .cloned()
            .unwrap_or_default())
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

    fn add(&mut self, _path: &str, _localized: bool) -> Result<Self::Counter, Error> {
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
                operation: "PdhCollectQueryData",
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

    fn add(&mut self, _path: &str, _localized: bool) -> Result<Self::Counter, Error> {
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
                operation: "PdhCollectQueryData",
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
    let state = Rc::new(RefCell::new(FakeState {
        counters,
        added: Vec::new(),
        collect_has_data: true,
        collect_error: false,
        timestamp: 100,
        timestamp_error: false,
        remove_error: false,
        removed: Vec::new(),
        expansions: BTreeMap::new(),
    }));
    let query = Query::open_with(
        FakePdh(Rc::clone(&state)),
        configs,
        Duration::from_secs(30),
        256,
        4_096,
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

mod live;
mod query;
mod timing;
mod worker;
