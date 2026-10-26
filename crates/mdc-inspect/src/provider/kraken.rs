//! Kraken depth-only connector integration and owned session lifecycle.

use std::time::Duration;

use mdc_providers::kraken::{KrakenConfig, KrakenConnectorBuilder};
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{ProviderFuture, ProviderRun, SessionAdapter};

/// Session adapter for Kraken.
pub struct KrakenAdapter {
    ws_url: Option<Url>,
}

impl KrakenAdapter {
    /// Uses the provider's production endpoints and retry timing.
    pub fn new() -> Self {
        Self { ws_url: None }
    }

    /// Redirects transport to mock endpoints with millisecond retry timing.
    /// The REST endpoint is unused by WebSocket-only providers.
    pub fn with_endpoints(ws_url: Url, _rest_url: Url) -> Self {
        Self {
            ws_url: Some(ws_url),
        }
    }

    fn build_config(&self) -> KrakenConfig {
        let mut config = KrakenConfig {
            depth: 1000,
            streams: mdc_providers::kraken::KrakenStreams {
                depth: true,
                trade: false,
            },
            ..KrakenConfig::default()
        };
        if let Some(url) = &self.ws_url {
            config.ws_base_url = url.clone();
            config.initial_backoff = Duration::from_millis(20);
            config.max_backoff = Duration::from_millis(100);
            config.resubscribe_timeout = Duration::from_millis(500);
            config.quarantine_duration = Duration::from_millis(20);
        }
        config
    }
}

impl Default for KrakenAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionAdapter for KrakenAdapter {
    fn start(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        let mut builder = KrakenConnectorBuilder::new(self.build_config());
        for (index, symbol) in config.symbols.iter().enumerate() {
            builder = builder.symbol_with_tag(symbol.clone(), index as mdc_core::SymbolTag);
        }
        let (connector, handle) = builder
            .sink(sink)
            .build()
            .map_err(|error| Error::Session(format!("Kraken: {error}")))?;
        let future: ProviderFuture = Box::pin(async move {
            let _ownership = handle;
            connector.run(session_token).await
        });
        Ok(ProviderRun::Future(future))
    }
}
