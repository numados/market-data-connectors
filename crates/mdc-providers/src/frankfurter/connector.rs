//! High-level connector handle and builder for Frankfurter ECB reference rates.

use mdc_core::StreamingSink;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::frankfurter::config::{ConfigError, FrankfurterConfig};
use crate::frankfurter::connection::run_connection_task;
use crate::frankfurter::control::ConnectorHandle;
use crate::frankfurter::rest::FrankfurterRestClient;

/// Market data connector instance for Frankfurter ECB reference rate REST polling.
#[derive(Debug)]
pub struct FrankfurterConnector {
    config: FrankfurterConfig,
    client: Option<reqwest::Client>,
}

impl FrankfurterConnector {
    /// Creates a builder for initializing a `FrankfurterConnector`.
    pub fn builder(config: FrankfurterConfig) -> FrankfurterConnectorBuilder {
        FrankfurterConnectorBuilder::new(config)
    }

    /// Returns a reference to the connector configuration.
    pub fn config(&self) -> &FrankfurterConfig {
        &self.config
    }

    /// Spawns the connector polling loop on the runtime, returning a control handle and join handle.
    pub fn run<S: StreamingSink + Send + 'static>(
        self,
        sink: S,
        cancel_token: CancellationToken,
    ) -> (ConnectorHandle, tokio::task::JoinHandle<()>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(cmd_tx);

        let rest_client = match self.client {
            Some(c) => FrankfurterRestClient::with_client(c, &self.config),
            None => match FrankfurterRestClient::new(&self.config) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(venue = "frankfurter_ecb", error = %e, "failed to initialize REST client");
                    return (handle, tokio::spawn(async {}));
                }
            },
        };

        let join_handle = tokio::spawn(run_connection_task(
            self.config,
            rest_client,
            sink,
            cmd_rx,
            cancel_token,
        ));

        (handle, join_handle)
    }
}

/// Builder for constructing and validating a [`FrankfurterConnector`].
#[derive(Debug)]
pub struct FrankfurterConnectorBuilder {
    config: FrankfurterConfig,
    client: Option<reqwest::Client>,
}

impl FrankfurterConnectorBuilder {
    /// Creates a new builder for the given config.
    pub fn new(config: FrankfurterConfig) -> Self {
        Self {
            config,
            client: None,
        }
    }

    /// Configures an injected `reqwest::Client` (e.g. for mock testing or custom pooling).
    pub fn with_client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Validates configuration and builds the [`FrankfurterConnector`].
    pub fn build(self) -> Result<FrankfurterConnector, ConfigError> {
        self.config.validate()?;
        Ok(FrankfurterConnector {
            config: self.config,
            client: self.client,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_valid_config() {
        let config = FrankfurterConfig::default();
        let connector = FrankfurterConnector::builder(config).build().unwrap();
        assert_eq!(connector.config().base_currency, "EUR");
        assert_eq!(connector.config().quote_currencies.len(), 2);
    }

    #[test]
    fn test_builder_invalid_config_fails() {
        let mut config = FrankfurterConfig::default();
        config.quote_currencies.clear();
        let err = FrankfurterConnector::builder(config).build().unwrap_err();
        assert_eq!(err, ConfigError::MissingQuotes);
    }
}
