//! Bitfinex depth-only integration, owning the unspawned connector future.

use std::time::Duration;

use mdc_providers::bitfinex::BitfinexConnectorBuilder;
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{ProviderFuture, ProviderRun, SessionAdapter};

/// Session adapter for Bitfinex.
pub struct BitfinexAdapter {
    ws_url: Option<Url>,
}

impl BitfinexAdapter {
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
    fn start(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        let mut builder = BitfinexConnectorBuilder::new()
            .enable_depth(true)
            .enable_trades(false)
            .orderbook_depth(250)
            .map_err(|error| Error::Session(format!("Bitfinex: {error}")))?;
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
            .map_err(|error| Error::Session(format!("Bitfinex: {error}")))?;
        let (handle, run) = connector.run(mdc_core::Owned::new(sink), session_token);
        let future: ProviderFuture = Box::pin(async move {
            let _ownership = handle;
            run.await
        });
        Ok(ProviderRun {
            future,
            abort_guards: Vec::new(),
        })
    }
}
