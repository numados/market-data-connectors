//! Bitfinex depth-only integration, owning the unspawned connector future.

use std::time::Duration;

use mdc_providers::bitfinex::BitfinexConnectorBuilder;
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{
    MAX_RETAINED_BOOK_DEPTH, ProviderFuture, ProviderRun, SessionAdapter, SessionBounds,
};

/// Session adapter for Bitfinex.
pub struct BitfinexAdapter {
    ws_url: Option<Url>,
}

impl BitfinexAdapter {
    const REQUESTED_DEPTH: usize = 250;

    /// Bounds and depth owned by the Bitfinex adapter.
    pub(crate) fn session_bounds() -> SessionBounds {
        SessionBounds {
            max_symbols: mdc_providers::bitfinex::BITFINEX_CAPABILITIES
                .bounds
                .max_symbols_per_connection,
            projector_depth: MAX_RETAINED_BOOK_DEPTH,
            affected_connection: 1,
        }
    }

    /// Uses the provider's production endpoints and retry timing.
    pub fn new() -> Self {
        Self { ws_url: None }
    }

    /// Redirects WebSocket transport to a mock with millisecond retry timing.
    /// Bitfinex does not use the REST endpoint.
    pub fn with_endpoints(ws_url: Url, _rest_url: Url) -> Self {
        Self {
            ws_url: Some(ws_url),
        }
    }
}

impl Default for BitfinexAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionAdapter for BitfinexAdapter {
    fn validate(&self, config: &SessionConfig) -> Result<(), Error> {
        let _ = self.prepare(config)?;
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
        Ok((self.prepare(config)?)(sink, session_token))
    }
}

impl BitfinexAdapter {
    fn prepare(
        &self,
        config: &SessionConfig,
    ) -> Result<impl FnOnce(LossIntolerantSender, CancellationToken) -> ProviderRun, Error> {
        config.validate()?;
        let mut builder = BitfinexConnectorBuilder::new()
            .enable_depth(true)
            .enable_trades(false)
            .orderbook_depth(Self::REQUESTED_DEPTH)
            .map_err(|error| Error::provider_config("Bitfinex", error))?;
        for (index, symbol) in config.symbols.iter().enumerate() {
            builder = builder.symbol_with_tag(symbol.clone(), index as mdc_core::SymbolTag);
        }
        if let Some(url) = &self.ws_url {
            builder = builder
                .ws_url(url.to_string())
                .resubscribe_timeout(Duration::from_millis(500))
                .quarantine_duration(Duration::from_millis(20));
        }
        let connector = builder
            .build()
            .map_err(|error| Error::provider_config("Bitfinex", error))?;
        Ok(move |sink, session_token| {
            let (handle, run) = connector.run(mdc_core::Owned::new(sink), session_token);
            let future: ProviderFuture = Box::pin(async move {
                let _ownership = handle;
                run.await
            });
            ProviderRun::Future(future)
        })
    }
}
