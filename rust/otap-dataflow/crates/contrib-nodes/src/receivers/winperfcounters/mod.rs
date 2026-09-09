// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Portable performance-counter configuration and OTAP gauge projection.

mod config;
mod otap_builder;

pub use config::{Config, CounterConfig};
pub use otap_builder::into_otap;

/// A numeric performance-counter value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Number {
    /// An exact signed integer.
    Integer(i64),
    /// A finite calculated or fractionally scaled value.
    Double(f64),
}

/// One configured counter's state for a collection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SampleValue {
    /// A ready numeric value.
    Value(Number),
    /// A two-sample counter is in its bounded first-scrape warm-up.
    Warming,
    /// A valid base did not advance because no operations were observed.
    NoObservation,
}

/// A successful point-in-time PDH reading, independent of Windows handles.
#[derive(Debug, Clone)]
pub struct Sample {
    /// Collection time as nanoseconds since the Unix epoch.
    pub timestamp_unix_nano: i64,
    /// Values and expected omissions in configured order.
    pub values: Vec<SampleValue>,
}

pub(crate) fn scale_integer(value: i64, power10: i32) -> Result<Number, String> {
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

pub(crate) fn scale_double(value: f64, power10: i32) -> Result<Number, String> {
    let scaled = value * 10_f64.powi(power10);
    if !scaled.is_finite() {
        return Err(format!("{value} * 10^{power10} is not finite"));
    }
    if value != 0.0 && scaled == 0.0 {
        return Err(format!("{value} * 10^{power10} underflows f64"));
    }
    Ok(Number::Double(scaled))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: Direct integers use zero, positive, and negative decimal scaling.
    /// Guarantees: Nonnegative exact results stay integers and every negative scale returns double.
    #[test]
    fn scales_direct_values_without_unnecessary_float_conversion() {
        assert_eq!(scale_integer(42, 0).unwrap(), Number::Integer(42));
        assert_eq!(scale_integer(42, 3).unwrap(), Number::Integer(42_000));
        assert_eq!(
            scale_integer(9, 18).unwrap(),
            Number::Integer(9_000_000_000_000_000_000)
        );
        assert!(scale_integer(10, 18).is_err());
        assert_eq!(scale_integer(42, -1).unwrap(), Number::Double(4.2));
        assert_eq!(
            scale_integer(1_000_000_000_000_000_000, -18).unwrap(),
            Number::Double(1.0)
        );
    }

    /// Scenario: Scaling reaches i64 and f64 precision boundaries.
    /// Guarantees: Exact integer division is used when safe; overflow and precision-losing conversion are rejected.
    #[test]
    fn rejects_unsafe_integer_scaling() {
        assert!(scale_integer(i64::MAX, 1).is_err());
        assert!(scale_integer(9_007_199_254_740_993, -1).is_err());
        assert!(scale_integer(i64::MAX, -1).is_err());
        assert!(scale_integer(9_007_199_254_740_992, -1).is_ok());
        assert!(scale_integer(i64::MIN, -1).is_ok());
        assert_eq!(
            scale_integer(9_007_199_254_740_990, -1).unwrap(),
            Number::Double(900_719_925_474_099.0)
        );
        assert_eq!(
            scale_integer(-9_007_199_254_740_990, -1).unwrap(),
            Number::Double(-900_719_925_474_099.0)
        );
        assert!(scale_integer(90_071_992_547_409_930, -1).is_err());
        assert_eq!(scale_integer(0, -18).unwrap(), Number::Double(0.0));
    }

    /// Scenario: A calculated value or scale produces a non-finite or underflowed result.
    /// Guarantees: NaN, infinity, and nonzero values rounded to zero are rejected.
    #[test]
    fn rejects_non_finite_double_scaling() {
        assert!(scale_double(f64::NAN, 0).is_err());
        assert!(scale_double(f64::MAX, 18).is_err());
        assert!(scale_double(f64::MIN_POSITIVE, -18).is_err());
        assert_eq!(scale_double(12.5, -1).unwrap(), Number::Double(1.25));
        assert_eq!(scale_double(0.0, -18).unwrap(), Number::Double(0.0));
    }
}
