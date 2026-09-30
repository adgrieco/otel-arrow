// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Values shared by configuration, PDH collection, and OTAP projection.

/// A numeric performance-counter value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Number {
    /// An exact signed integer.
    Integer(i64),
    /// A finite calculated or fractionally scaled value.
    Double(f64),
}

/// One configured counter's state for a collection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum SampleValue {
    /// A ready numeric value.
    Value(Number),
    /// A valid base did not advance because no operations were observed.
    NoObservation,
}

/// One exact counter point in a collection.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SamplePoint {
    /// Index of the configured counter that defines metric metadata.
    pub(super) counter_index: usize,
    /// Ready value or expected omission.
    pub(super) value: SampleValue,
}

/// One counter-local failure omitted from an otherwise successful collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SampleFailure {
    /// Index of the configured exact path.
    pub(super) counter_index: usize,
    /// Native status or calculation detail.
    pub(super) error: String,
}

/// A successful point-in-time PDH reading, independent of Windows handles.
#[derive(Debug, Clone)]
pub(super) struct Sample {
    /// Start of the current cumulative sequence.
    pub(super) start_time_unix_nano: i64,
    /// Collection time as nanoseconds since the Unix epoch.
    pub(super) timestamp_unix_nano: i64,
    /// Exact points keyed by configured counter.
    pub(super) points: Vec<SamplePoint>,
    /// Counter-local failures that did not suppress healthy points.
    pub(super) failures: Vec<SampleFailure>,
}

pub(super) fn scale_integer(value: i64, power10: i32) -> Result<Number, String> {
    match power10.cmp(&0) {
        std::cmp::Ordering::Equal => Ok(Number::Integer(value)),
        std::cmp::Ordering::Greater => {
            let factor = 10_i64
                .checked_pow(power10 as u32)
                .ok_or_else(|| format!("10^{power10} exceeds i64"))?;
            value
                .checked_mul(factor)
                .map(Number::Integer)
                .ok_or_else(|| format!("{value} * 10^{power10} exceeds i64"))
        }
        std::cmp::Ordering::Less => {
            let divisor = 10_i64
                .checked_pow(power10.unsigned_abs())
                .ok_or_else(|| format!("10^{power10} exceeds i64"))?;
            let divides_exactly = value % divisor == 0;
            let integer_to_convert = if divides_exactly {
                value / divisor
            } else {
                value
            };
            let converted = integer_to_convert as f64;
            if converted as i128 != i128::from(integer_to_convert) {
                return Err(format!(
                    "{integer_to_convert} cannot be converted to f64 without losing integer precision"
                ));
            }
            if divides_exactly {
                Ok(Number::Double(converted))
            } else {
                scale_double(converted, power10)
            }
        }
    }
}

pub(super) fn scale_double(value: f64, power10: i32) -> Result<Number, String> {
    let exponent = power10
        .checked_abs()
        .ok_or_else(|| format!("10^{power10} exceeds supported exponent range"))?;
    let scaled = if power10 < 0 {
        value / 10_f64.powi(exponent)
    } else {
        value * 10_f64.powi(exponent)
    };
    let operation = if power10 < 0 { "/" } else { "*" };
    if !scaled.is_finite() {
        return Err(format!("{value} {operation} 10^{} is not finite", exponent));
    }
    if value != 0.0 && scaled == 0.0 {
        return Err(format!(
            "{value} {operation} 10^{} underflows f64",
            exponent
        ));
    }
    Ok(Number::Double(scaled))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: Integer and calculated values use supported decimal scaling.
    /// Guarantees: Exact nonnegative scales remain integers and invalid floating results fail.
    #[test]
    fn scales_values_safely() {
        assert_eq!(scale_integer(42, 3).unwrap(), Number::Integer(42_000));
        assert_eq!(scale_integer(42, -1).unwrap(), Number::Double(4.2));
        assert_eq!(scale_integer(3, -1).unwrap(), Number::Double(0.3));
        assert_eq!(scale_integer(7, -1).unwrap(), Number::Double(0.7));
        assert_eq!(
            scale_integer(86_580_518_913, -6).unwrap(),
            Number::Double(86_580.518_913)
        );
        assert!(scale_integer(i64::MAX, 1).is_err());
        assert!(scale_integer(9_007_199_254_740_993, -1).is_err());
        assert!(scale_double(f64::MAX, 18).is_err());
    }
}
