//! LMAX Digital depth-only connector integration and owned session lifecycle.

use std::time::Duration;

use mdc_providers::lmax::{LmaxConfig, LmaxConnectorBuilder};
use mdc_sinks::loss_intolerant::LossIntolerantSender;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::SessionConfig;
use crate::error::Error;
use crate::provider::{
    ProviderFuture, ProviderRun, SessionAdapter, SessionBounds, validation_sink,
};

/// Session adapter for LMAX Digital.
pub struct LmaxAdapter {
    ws_url: Option<Url>,
}

impl LmaxAdapter {
    const REQUESTED_DEPTH: usize = 20;

    /// Bounds and limits owned by the LMAX adapter.
    pub(crate) fn session_bounds() -> SessionBounds {
        SessionBounds {
            max_symbols: mdc_providers::lmax::connector::LMAX_CAPABILITIES
                .bounds
                .max_symbols_per_connection,
            projector_depth: Self::REQUESTED_DEPTH,
            affected_connection: 1,
        }
    }

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

    fn build_config(&self) -> LmaxConfig {
        let mut config = LmaxConfig {
            symbols_per_connection: Self::session_bounds().max_symbols,
            streams: mdc_providers::lmax::LmaxStreams {
                book: true,
                trades: false,
            },
            ..LmaxConfig::default()
        };
        if let Some(url) = &self.ws_url {
            config.ws_url = url.clone();
            config.control_pacing.initial_delay = Duration::from_millis(10);
            config.control_pacing.min_command_interval = Duration::from_millis(10);
            config.control_pacing.too_many_requests_backoff = Duration::from_millis(20);
            config.control_pacing.max_backoff = Duration::from_millis(100);
        }
        config
    }
}

impl Default for LmaxAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionAdapter for LmaxAdapter {
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

impl LmaxAdapter {
    fn prepare(
        &self,
        config: &SessionConfig,
        sink: LossIntolerantSender,
    ) -> Result<impl FnOnce(CancellationToken) -> ProviderRun, Error> {
        config.validate()?;
        let mut builder = LmaxConnectorBuilder::new(self.build_config());
        for (index, symbol) in config.symbols.iter().enumerate() {
            builder = builder.symbol_with_tag(symbol.clone(), index as mdc_core::SymbolTag);
        }
        let (connector, handle) = builder
            .sink(sink)
            .build()
            .map_err(|error| Error::provider_config("LMAX Digital", error))?;
        Ok(move |session_token| {
            let future: ProviderFuture = Box::pin(async move {
                let _ownership = handle;
                connector.run(session_token).await
            });
            ProviderRun::Future(future)
        })
    }
}
