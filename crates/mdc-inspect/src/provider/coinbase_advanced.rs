//! Coinbase Advanced depth-only connector integration and owned session lifecycle.

use std::time::Duration;

use mdc_providers::coinbase_advanced::{CoinbaseAdvancedConfig, CoinbaseAdvancedConnectorBuilder};
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{ProviderFuture, ProviderRun, SessionAdapter};

/// Session adapter for Coinbase Advanced.
pub struct CoinbaseAdvancedAdapter {
    ws_url: Option<Url>,
}

impl CoinbaseAdvancedAdapter {
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

    fn build_config(&self) -> CoinbaseAdvancedConfig {
        let mut config = CoinbaseAdvancedConfig {
            streams: mdc_providers::coinbase_advanced::CoinbaseAdvancedStreams {
                level2: true,
                heartbeats: true,
                market_trades: false,
            },
            ..CoinbaseAdvancedConfig::default()
        };
        if let Some(url) = &self.ws_url {
            config.ws_url = url.clone();
            config.resync_timeout = Duration::from_millis(20);
        }
        config
    }
}

impl Default for CoinbaseAdvancedAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionAdapter for CoinbaseAdvancedAdapter {
    fn start(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        let mut builder = CoinbaseAdvancedConnectorBuilder::new(self.build_config());
        for (index, symbol) in config.symbols.iter().enumerate() {
            builder = builder.symbol_with_tag(symbol.clone(), index as mdc_core::SymbolTag);
        }
        let (connector, handle) = builder
            .sink(mdc_core::Owned::new(sink))
            .build()
            .map_err(|error| Error::Session(format!("Coinbase Advanced: {error}")))?;
        let future: ProviderFuture = Box::pin(async move {
            let _ownership = handle;
            connector.run(session_token).await
        });
        Ok(ProviderRun {
            future,
            abort_guards: Vec::new(),
        })
    }
}
