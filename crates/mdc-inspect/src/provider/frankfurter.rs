//! Frankfurter dated reference rates, using the provider's default hourly cadence.

use std::time::Duration;

use mdc_core::Owned;
use mdc_providers::frankfurter::{FrankfurterConfig, FrankfurterConnector};
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{ProviderRun, SessionAdapter, SessionBounds};

/// Adapter for reference-rate-only Frankfurter sessions.
#[derive(Debug, Default)]
pub struct FrankfurterAdapter {
    rest_url: Option<Url>,
}

impl FrankfurterAdapter {
    const REQUESTED_DEPTH: usize = 1;

    /// Bounds owned by the Frankfurter adapter and quote configuration.
    pub(crate) fn session_bounds() -> SessionBounds {
        SessionBounds {
            max_symbols: mdc_providers::frankfurter::FRANKFURTER_CAPABILITIES
                .bounds
                .max_symbols_per_connection,
            projector_depth: Self::REQUESTED_DEPTH,
            affected_connection: 1,
        }
    }

    /// Uses production endpoints and the default one-hour poll interval.
    pub fn new() -> Self {
        Self::default()
    }

    /// Redirects REST to a mock and shortens polling to 50 ms for offline tests.
    pub fn with_endpoints(rest_url: Url) -> Self {
        Self {
            rest_url: Some(rest_url),
        }
    }
}

impl SessionAdapter for FrankfurterAdapter {
    fn bounds(&self) -> SessionBounds {
        Self::session_bounds()
    }

    fn validate(&self, config: &SessionConfig) -> Result<(), Error> {
        let _ = self.prepare(config)?;
        Ok(())
    }

    fn start(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        Ok((self.prepare(config)?)(sink, session_token))
    }
}

impl FrankfurterAdapter {
    fn prepare(
        &self,
        config: &SessionConfig,
    ) -> Result<impl FnOnce(LossIntolerantSender, CancellationToken) -> ProviderRun, Error> {
        config.validate()?;
        let base = config
            .base
            .as_ref()
            .ok_or_else(|| Error::Config("Frankfurter requires a base currency".to_string()))?;
        let mut builder = FrankfurterConfig::builder()
            .base_currency(base.to_string())
            .quote_currencies(config.symbols.iter().map(ToString::to_string).collect());
        if let Some(url) = &self.rest_url {
            builder = builder
                .base_url(url.as_str())
                .poll_interval(Duration::from_millis(50));
        }
        let provider_config = builder
            .build()
            .map_err(|error| Error::provider_config("Frankfurter/ECB", error))?;
        let connector = FrankfurterConnector::builder(provider_config)
            .build()
            .map_err(|error| Error::provider_config("Frankfurter/ECB", error))?;
        Ok(move |sink, session_token| {
            let (handle, join) = connector.run(Owned::new(sink), session_token);
            ProviderRun::spawned(join, handle)
        })
    }
}
