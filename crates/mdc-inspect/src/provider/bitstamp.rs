//! Bitstamp depth-only connector integration with mocked REST recovery.

use std::time::Duration;

use mdc_providers::bitstamp::{BitstampConfig, BitstampConnectorBuilder};
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{
    ProviderFuture, ProviderRun, SessionAdapter, SessionBounds, validation_sink,
};

/// Session adapter for Bitstamp.
pub struct BitstampAdapter {
    ws_url: Option<Url>,
    rest_url: Option<Url>,
}

impl BitstampAdapter {
    const REQUESTED_DEPTH: usize = 100;

    /// Bounds and depth owned by the Bitstamp adapter.
    pub(crate) fn session_bounds() -> SessionBounds {
        SessionBounds {
            max_symbols: mdc_providers::bitstamp::BITSTAMP_CAPABILITIES
                .bounds
                .max_symbols_per_connection,
            projector_depth: Self::REQUESTED_DEPTH,
            affected_connection: 1,
        }
    }

    /// Uses the provider's production endpoints and retry timing.
    pub fn new() -> Self {
        Self {
            ws_url: None,
            rest_url: None,
        }
    }

    /// Redirects WebSocket and REST transport to mock endpoints with
    /// millisecond snapshot-retry timing (test seam only).
    pub fn with_endpoints(ws_url: Url, rest_url: Url) -> Self {
        Self {
            ws_url: Some(ws_url),
            rest_url: Some(rest_url),
        }
    }

    fn build_config(&self) -> BitstampConfig {
        let mut config = BitstampConfig {
            streams: mdc_providers::bitstamp::BitstampStreams {
                depth: true,
                trades: false,
            },
            ..BitstampConfig::default()
        };
        config.symbols_per_connection = Self::session_bounds().max_symbols;
        if let Some(url) = &self.ws_url {
            config.ws_url = url.clone();
            config.snapshot_retry_cooldown = Duration::from_millis(20);
        }
        if let Some(url) = &self.rest_url {
            config.rest_url = url.clone();
        }
        config
    }
}

impl Default for BitstampAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionAdapter for BitstampAdapter {
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

impl BitstampAdapter {
    fn prepare(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
    ) -> Result<impl FnOnce(CancellationToken) -> ProviderRun, Error> {
        config.validate()?;
        let mut builder = BitstampConnectorBuilder::new(self.build_config());
        for (index, symbol) in config.symbols.iter().enumerate() {
            builder = builder.symbol_with_tag(symbol.clone(), index as mdc_core::SymbolTag);
        }
        let (connector, handle) = builder
            .sink(sink)
            .build()
            .map_err(|error| Error::provider_config("Bitstamp", error))?;
        Ok(move |session_token| {
            let future: ProviderFuture = Box::pin(async move {
                let _ownership = handle;
                connector.run(session_token).await
            });
            ProviderRun::Future(future)
        })
    }
}
