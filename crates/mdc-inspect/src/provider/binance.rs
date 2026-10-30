//! Binance configuration, builder integration, and connector-future
//! adaptation for the inspector session (PRD 31 §3.3, §3.10 requirement 155).
//!
//! The adapter uses only existing public contracts: the builder owns the
//! sink (wrapping the loss-intolerant sender in `mdc_core::Owned`), `build`
//! returns the connector plus a `ConnectorHandle`, and `run` consumes the
//! connector under a cancellation token. No provider control is exposed.

use mdc_providers::binance::{BinanceConfig, BinanceConnectorBuilder};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{
    MAX_RETAINED_BOOK_DEPTH, ProviderFuture, ProviderRun, SessionAdapter, SessionBounds,
    validation_sink,
};
use mdc_sinks::loss_intolerant::LossIntolerantSender;

/// Session adapter for Binance Spot.
pub struct BinanceAdapter {
    ws_url: Option<Url>,
    rest_url: Option<Url>,
    #[cfg(test)]
    test_resync_cooldown: Option<Duration>,
}

impl BinanceAdapter {
    /// Bounds owned by the Binance adapter, independent of the REST snapshot depth.
    pub(crate) fn session_bounds() -> SessionBounds {
        SessionBounds {
            max_symbols: mdc_providers::binance::BINANCE_CAPABILITIES
                .bounds
                .max_symbols_per_connection,
            projector_depth: MAX_RETAINED_BOOK_DEPTH,
            affected_connection: 1,
        }
    }

    /// Production adapter using provider defaults (`BinanceConfig::default`,
    /// requirement 28).
    pub fn new() -> Self {
        Self {
            ws_url: None,
            rest_url: None,
            #[cfg(test)]
            test_resync_cooldown: None,
        }
    }

    /// Test adapter redirecting endpoints to localhost mocks and shortening
    /// retry timers so mock-driven resyncs run in milliseconds instead of
    /// the production cooldowns (requirement 157).
    #[cfg(test)]
    pub fn with_endpoints(ws_url: Url, rest_url: Url) -> Self {
        Self::with_test_timing(ws_url, rest_url, Duration::from_millis(10))
    }

    /// Test adapter with an explicit resync cooldown, so fault-window tests
    /// can hold the unavailable window open long enough to observe it.
    #[cfg(test)]
    pub fn with_test_timing(ws_url: Url, rest_url: Url, resync_cooldown: Duration) -> Self {
        Self {
            ws_url: Some(ws_url),
            rest_url: Some(rest_url),
            test_resync_cooldown: Some(resync_cooldown),
        }
    }

    /// Builds the provider configuration: defaults with the inspected
    /// stream switches — depth on, book ticker and trades off, so both the
    /// book and the projected top of book come from one integrity-controlled
    /// L2 source (requirements 29–30).
    fn build_config(&self) -> BinanceConfig {
        let mut config = BinanceConfig::default();
        config.streams.depth = true;
        config.streams.book_ticker = false;
        config.streams.trade = false;
        config.symbols_per_connection = Self::session_bounds().max_symbols;
        if let Some(url) = &self.ws_url {
            config.ws_base_url = url.clone();
        }
        if let Some(url) = &self.rest_url {
            config.rest_base_url = url.clone();
        }
        if self.ws_url.is_some() || self.rest_url.is_some() {
            // Mock-driven tests: millisecond retry timers (test seam only).
            config.initial_backoff = Duration::from_millis(20);
            #[cfg(test)]
            {
                config.snapshot_retry_cooldown = self
                    .test_resync_cooldown
                    .unwrap_or(Duration::from_millis(10));
            }
        }
        config
    }
}

impl Default for BinanceAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionAdapter for BinanceAdapter {
    fn validate(&self, config: &SessionConfig) -> Result<(), Error> {
        let _ = self.prepare(config, validation_sink(Self::session_bounds()))?;
        Ok(())
    }

    fn bounds(&self) -> SessionBounds {
        Self::session_bounds()
    }

    fn start(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        Ok((self.prepare(config, sink)?)(session_token))
    }
}

impl BinanceAdapter {
    fn prepare(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
    ) -> Result<impl FnOnce(CancellationToken) -> ProviderRun, Error> {
        config.validate()?;
        let symbols = config.symbols.clone();
        let (connector, handle) = BinanceConnectorBuilder::new(self.build_config())
            .symbols(symbols)
            .sink(sink)
            .build()
            .map_err(|error| Error::provider_config("Binance Spot", error))?;
        Ok(move |session_token| {
            let future: ProviderFuture = Box::pin(async move {
                let _ownership = handle;
                connector.run(session_token).await
            });
            ProviderRun::Future(future)
        })
    }
}

#[cfg(test)]
mod tests;
