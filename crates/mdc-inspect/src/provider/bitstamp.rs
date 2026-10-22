//! Bitstamp depth-only connector integration with mocked REST recovery.

use std::time::Duration;

use mdc_providers::bitstamp::{BitstampConfig, BitstampConnectorBuilder};
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{ProviderFuture, ProviderRun, SessionAdapter};

/// Session adapter for Bitstamp.
pub struct BitstampAdapter {
    ws_url: Option<Url>,
    rest_url: Option<Url>,
}

impl BitstampAdapter {
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
    fn start(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
        session_token: CancellationToken,
    ) -> Result<ProviderRun, Error> {
        let mut builder = BitstampConnectorBuilder::new(self.build_config());
        for (index, symbol) in config.symbols.iter().enumerate() {
            builder = builder.symbol_with_tag(symbol.clone(), index as mdc_core::SymbolTag);
        }
        let (connector, handle) = builder
            .sink(sink)
            .build()
            .map_err(|error| Error::Session(format!("Bitstamp: {error}")))?;
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
