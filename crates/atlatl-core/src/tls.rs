//! rustls with ring; server certs are checked against the OS trust store (rustls-platform-verifier).

use std::sync::{Arc, Once};

use rustls::ClientConfig;
use rustls_platform_verifier::BuilderVerifierExt;

/// Installs ring as the process-wide rustls provider; reqwest needs one with `rustls-no-provider`.
pub fn install_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// The builder every HTTP client starts from.
#[allow(clippy::disallowed_methods)]
pub fn http_client() -> reqwest::ClientBuilder {
    install_provider();
    reqwest::Client::builder()
}

/// For clients that bring their own transport (libsql).
pub fn client_config() -> Result<ClientConfig, rustls::Error> {
    Ok(
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()?
            .with_platform_verifier()?
            .with_no_client_auth(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_with_platform_verifier() {
        assert!(http_client().build().is_ok());
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    #[test]
    fn install_is_idempotent() {
        install_provider();
        install_provider();
        assert!(http_client().build().is_ok());
    }

    #[test]
    fn client_config_builds() {
        let c = client_config().expect("config");
        assert!(c.alpn_protocols.is_empty());
    }
}
