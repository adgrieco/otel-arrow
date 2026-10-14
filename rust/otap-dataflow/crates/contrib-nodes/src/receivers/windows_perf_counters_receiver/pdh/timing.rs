// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::super::model::Number;
use super::error::Error;

pub(super) fn positive_timestamp(timestamp: i64) -> Result<i64, Error> {
    if timestamp > 0 {
        Ok(timestamp)
    } else {
        Err(Error::InvalidSample("timestamp must be positive"))
    }
}

pub(super) fn cumulative_start_time(
    start_time_unix_nano: &mut i64,
    previous_timestamp_unix_nano: &mut i64,
    timestamp_unix_nano: i64,
) -> i64 {
    if timestamp_unix_nano < *previous_timestamp_unix_nano {
        *start_time_unix_nano = timestamp_unix_nano;
    }
    *previous_timestamp_unix_nano = timestamp_unix_nano;
    *start_time_unix_nano
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct MonotonicSequence {
    pub(super) start_time_unix_nano: Option<i64>,
    pub(super) previous_value: Option<i64>,
    pub(super) previous_timestamp_unix_nano: Option<i64>,
}

pub(super) fn monotonic_start_time(
    path: &str,
    sequence: &mut MonotonicSequence,
    value: i64,
    timestamp_unix_nano: i64,
) -> Result<i64, Error> {
    if value < 0 {
        return Err(Error::Calculation {
            path: path.to_owned(),
            message: format!("monotonic Sum value must be non-negative, got {value}"),
        });
    }
    if sequence.start_time_unix_nano.is_none()
        || sequence.previous_value.is_none()
        || sequence
            .previous_value
            .is_some_and(|previous| value < previous)
        || sequence
            .previous_timestamp_unix_nano
            .is_some_and(|previous| timestamp_unix_nano < previous)
    {
        sequence.start_time_unix_nano = Some(timestamp_unix_nano);
    }
    sequence.previous_value = Some(value);
    sequence.previous_timestamp_unix_nano = Some(timestamp_unix_nano);
    sequence.start_time_unix_nano.ok_or(Error::InvalidSample(
        "monotonic Sum start time was not initialized",
    ))
}

pub(super) fn reset_monotonic_sequences_on_query_rollback<'a>(
    previous_query_timestamp_unix_nano: i64,
    timestamp_unix_nano: i64,
    sequences: impl Iterator<Item = &'a mut MonotonicSequence>,
) {
    if timestamp_unix_nano < previous_query_timestamp_unix_nano {
        for sequence in sequences {
            *sequence = MonotonicSequence::default();
        }
    }
}

pub(super) fn advance_collection_time<'a>(
    start_time_unix_nano: &mut i64,
    previous_timestamp_unix_nano: &mut i64,
    timestamp_unix_nano: i64,
    sequences: impl Iterator<Item = &'a mut MonotonicSequence>,
) -> i64 {
    reset_monotonic_sequences_on_query_rollback(
        *previous_timestamp_unix_nano,
        timestamp_unix_nano,
        sequences,
    );
    cumulative_start_time(
        start_time_unix_nano,
        previous_timestamp_unix_nano,
        timestamp_unix_nano,
    )
}

pub(super) fn delta_interval_start(
    path: &str,
    baseline_unix_nano: &mut Option<i64>,
    number: Number,
    monotonic: bool,
    timestamp_unix_nano: i64,
) -> Result<Option<i64>, Error> {
    let Number::Double(value) = number else {
        return Err(Error::InvalidSample(
            "delta Sum was not backed by a formatted double value",
        ));
    };
    let previous = baseline_unix_nano.replace(timestamp_unix_nano);
    let Some(previous) = previous else {
        return Ok(None);
    };
    if timestamp_unix_nano <= previous || (monotonic && value < 0.0) {
        return Ok(None);
    }
    if !value.is_finite() {
        return Err(Error::Calculation {
            path: path.to_owned(),
            message: "delta Sum value must be finite".to_owned(),
        });
    }
    Ok(Some(previous))
}
