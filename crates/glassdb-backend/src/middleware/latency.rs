//! Validated provider-latency distributions.

use rand::Rng;
use rand_distr::{Distribution, StandardNormal};

/// A lognormal distribution parameterized by its arithmetic mean and standard
/// deviation in one caller-selected unit.
#[derive(Debug, Clone, Copy)]
pub struct Lognormal {
    mu: f64,
    sigma: f64,
}

/// Invalid arithmetic parameters for a [`Lognormal`] distribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum LognormalError {
    /// The mean was negative or non-finite.
    #[error("mean must be finite and non-negative")]
    InvalidMean,
    /// The standard deviation was negative or non-finite.
    #[error("standard deviation must be finite and non-negative")]
    InvalidStandardDeviation,
    /// A zero mean was paired with a positive standard deviation.
    #[error("a zero mean requires a zero standard deviation")]
    PositiveDeviationWithZeroMean,
    /// Finite inputs produced parameters outside the representable range.
    #[error("mean and standard deviation produce unrepresentable parameters")]
    UnrepresentableParameters,
}

impl Lognormal {
    /// Builds a distribution whose samples use the same unit as `mean` and
    /// `standard_deviation`.
    pub fn new(mean: f64, standard_deviation: f64) -> Result<Self, LognormalError> {
        if !mean.is_finite() || mean < 0.0 {
            return Err(LognormalError::InvalidMean);
        }
        if !standard_deviation.is_finite() || standard_deviation < 0.0 {
            return Err(LognormalError::InvalidStandardDeviation);
        }
        if mean == 0.0 {
            if standard_deviation > 0.0 {
                return Err(LognormalError::PositiveDeviationWithZeroMean);
            }
            return Ok(Self {
                mu: f64::NEG_INFINITY,
                sigma: 0.0,
            });
        }

        // Convert arithmetic mean/deviation to the underlying normal
        // parameters. Keeping this operation order stable also keeps seeded
        // distribution vectors stable across callers.
        let relative_deviation = standard_deviation / mean;
        let variance = (relative_deviation * relative_deviation + 1.0).ln();
        let mu = mean.ln() - 0.5 * variance;
        let sigma = variance.sqrt();
        if !mu.is_finite() || !sigma.is_finite() {
            return Err(LognormalError::UnrepresentableParameters);
        }
        Ok(Self { mu, sigma })
    }

    /// Samples from the distribution using `uniform` for random bits.
    pub fn sample<R: Rng + ?Sized>(&self, uniform: &mut R) -> f64 {
        let normal: f64 = StandardNormal.sample(uniform);
        (normal * self.sigma + self.mu).exp()
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    use super::*;

    #[test]
    fn seeded_samples_cover_variable_and_degenerate_distributions() {
        let mut uniform = StdRng::seed_from_u64(0xF2_5B);
        let distributions = [
            Lognormal::new(0.0, 0.0).unwrap(),
            Lognormal::new(50.0, 0.0).unwrap(),
            Lognormal::new(57.0, 7.0).unwrap(),
            Lognormal::new(22.0, 9.0).unwrap(),
        ];
        let samples: Vec<_> = distributions
            .iter()
            .map(|distribution| distribution.sample(&mut uniform).to_bits())
            .collect();

        assert_eq!(
            samples,
            [
                0x0000_0000_0000_0000,
                0x4048_ffff_ffff_ffff,
                0x4049_2429_5902_901a,
                0x4035_5f55_55ba_23e6,
            ]
        );
        assert_eq!(uniform.random::<u64>(), 0x0fbd_427c_7a48_38ae);
    }

    #[test]
    fn invalid_parameters_are_rejected() {
        let cases = [
            (-1.0, 0.0, LognormalError::InvalidMean),
            (f64::NAN, 0.0, LognormalError::InvalidMean),
            (f64::INFINITY, 0.0, LognormalError::InvalidMean),
            (1.0, -1.0, LognormalError::InvalidStandardDeviation),
            (1.0, f64::NAN, LognormalError::InvalidStandardDeviation),
            (1.0, f64::INFINITY, LognormalError::InvalidStandardDeviation),
            (0.0, 1.0, LognormalError::PositiveDeviationWithZeroMean),
            (
                f64::MIN_POSITIVE,
                f64::MAX,
                LognormalError::UnrepresentableParameters,
            ),
        ];

        for (mean, standard_deviation, expected) in cases {
            assert_eq!(
                Lognormal::new(mean, standard_deviation).unwrap_err(),
                expected
            );
        }
    }
}
