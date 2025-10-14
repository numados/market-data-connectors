//! High-level connector handle and builder for VWD FX REST polling.

use mdc_core::{RunId, StreamingSink};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::vwd::config::{ConfigError, VwdConfig};
use crate::vwd::connection::run_connection_task;
use crate::vwd::control::ConnectorHandle;

/// Market data connector instance for VWD FX REST polling.
#[derive(Debug)]
pub struct VwdConnector {
    config: VwdConfig,
    run_id: Option<RunId>,
}

impl VwdConnector {
    /// Creates a builder for initializing a `VwdConnector`.
    pub fn builder(config: VwdConfig) -> VwdConnectorBuilder {
        VwdConnectorBuilder::new(config)
    }

    /// Returns a reference to the connector's configuration.
    pub fn config(&self) -> &VwdConfig {
        &self.config
    }

    /// Spawns the connector polling loop on the runtime, returning a control handle and join handle.
    pub fn run<S: StreamingSink + Send + 'static>(
        self,
        sink: S,
        cancel_token: CancellationToken,
    ) -> (ConnectorHandle, tokio::task::JoinHandle<()>) {
        let run_id = self.run_id.unwrap_or_else(|| {
            let now = crate::now_nanos().0 as u128;
            RunId::from_nanos(now)
        });
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let handle = ConnectorHandle::new(cmd_tx);
        let join_handle = tokio::spawn(run_connection_task(
            run_id,
            self.config,
            sink,
            cmd_rx,
            cancel_token,
        ));
        (handle, join_handle)
    }
}

/// Builder for constructing and validating a [`VwdConnector`].
#[derive(Debug)]
pub struct VwdConnectorBuilder {
    config: VwdConfig,
    run_id: Option<RunId>,
}

impl VwdConnectorBuilder {
    /// Creates a new builder for the given config.
    pub fn new(config: VwdConfig) -> Self {
        Self {
            config,
            run_id: None,
        }
    }

    /// Sets the runtime execution run ID.
    pub fn run_id(mut self, run_id: RunId) -> Self {
        self.run_id = Some(run_id);
        self
    }

    /// Validates the configuration and builds the [`VwdConnector`].
    pub fn build(self) -> Result<VwdConnector, ConfigError> {
        self.config.validate()?;
        Ok(VwdConnector {
            config: self.config,
            run_id: self.run_id,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::vwd::config::{VwdCredentials, VwdSymbol};

    #[test]
    fn test_builder_missing_credentials_fails() {
        let config = VwdConfig {
            base_url: url::Url::parse("https://rest-interface.vwd.com").unwrap(),
            account_path: secrecy::SecretString::new("".into()),
            credentials: VwdCredentials::new("", ""),
            poll_interval: std::time::Duration::from_secs(5),
            request_timeout: std::time::Duration::from_secs(10),
            indicative_size: rust_decimal::Decimal::from(100),
            significant_digits: 6,
            symbols: vec![VwdSymbol::direct("EURUSD")],
            emit_book_ticker: false,
        };

        let err = VwdConnector::builder(config).build().unwrap_err();
        assert_eq!(err, ConfigError::MissingCredentials);
    }

    #[test]
    fn test_builder_valid_config_succeeds() {
        let config = VwdConfig::builder()
            .account_path("acct")
            .credentials(VwdCredentials::new("user", "pass"))
            .add_symbol(VwdSymbol::direct("EURUSD"))
            .build()
            .unwrap();

        let connector = VwdConnector::builder(config).build().unwrap();
        assert_eq!(connector.config().symbols.len(), 1);
    }
}
