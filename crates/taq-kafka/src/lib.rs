use std::{env, fmt, fs, path::PathBuf};

use anyhow::{bail, Context, Result};
use rdkafka::config::ClientConfig;

const PROTOCOL_ENV: &str = "TAQ_KAFKA_SECURITY_PROTOCOL";
const MECHANISM_ENV: &str = "TAQ_KAFKA_SASL_MECHANISM";
const USERNAME_ENV: &str = "TAQ_KAFKA_SASL_USERNAME";
const PASSWORD_ENV: &str = "TAQ_KAFKA_SASL_PASSWORD";
const PASSWORD_FILE_ENV: &str = "TAQ_KAFKA_SASL_PASSWORD_FILE";
const CA_ENV: &str = "TAQ_KAFKA_TLS_CA_FILE";
const CERT_ENV: &str = "TAQ_KAFKA_TLS_CERT_FILE";
const KEY_ENV: &str = "TAQ_KAFKA_TLS_KEY_FILE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Protocol {
    Plaintext,
    Ssl,
    SaslSsl,
}

impl Protocol {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "PLAINTEXT" => Ok(Self::Plaintext),
            "SSL" => Ok(Self::Ssl),
            "SASL_SSL" => Ok(Self::SaslSsl),
            _ => bail!("{PROTOCOL_ENV} must be PLAINTEXT, SSL, or SASL_SSL"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Plaintext => "plaintext",
            Self::Ssl => "ssl",
            Self::SaslSsl => "sasl_ssl",
        }
    }
}

/// Shared Kafka transport and authentication policy. Secret values are never
/// included in its Debug output or in validation errors.
#[derive(Clone)]
pub struct KafkaSecurityConfig {
    protocol: Protocol,
    sasl_mechanism: Option<String>,
    sasl_username: Option<String>,
    sasl_password: Option<String>,
    ca_file: Option<PathBuf>,
    cert_file: Option<PathBuf>,
    key_file: Option<PathBuf>,
}

impl fmt::Debug for KafkaSecurityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaSecurityConfig")
            .field("protocol", &self.protocol.as_str())
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username.as_ref().map(|_| "[REDACTED]"))
            .field("sasl_password", &self.sasl_password.as_ref().map(|_| "[REDACTED]"))
            .field("ca_file", &self.ca_file)
            .field("cert_file", &self.cert_file)
            .field("key_file", &self.key_file)
            .finish()
    }
}

impl KafkaSecurityConfig {
    /// Returns whether this profile requires TLS plus SASL authentication.
    pub fn uses_sasl_ssl(&self) -> bool {
        self.protocol == Protocol::SaslSsl
    }

    /// Identifies credentials that must never be accepted by the public API guard.
    pub fn uses_development_credentials(&self) -> bool {
        const PLACEHOLDERS: &[&str] = &["taq", "taq_dev", "default", "admin", "user", "password", "changeme", "example"];
        self.sasl_username
            .as_deref()
            .map(str::to_ascii_lowercase)
            .is_some_and(|value| PLACEHOLDERS.contains(&value.as_str()))
            || self
                .sasl_password
                .as_deref()
                .map(str::to_ascii_lowercase)
                .is_some_and(|value| PLACEHOLDERS.contains(&value.as_str()))
    }

    /// Reads and validates the shared Kafka security environment variables.
    pub fn from_env() -> Result<Self> {
        let protocol = read_optional_env(PROTOCOL_ENV)?;
        let mechanism = read_optional_env(MECHANISM_ENV)?;
        let username = read_optional_env(USERNAME_ENV)?;
        let password = resolve_sasl_password(
            read_optional_env(PASSWORD_ENV)?,
            read_optional_env(PASSWORD_FILE_ENV)?,
        )?;
        let ca_file = read_optional_env(CA_ENV)?;
        let cert_file = read_optional_env(CERT_ENV)?;
        let key_file = read_optional_env(KEY_ENV)?;
        Self::from_values(
            protocol.as_deref(),
            mechanism.as_deref(),
            username.as_deref(),
            password.as_deref(),
            ca_file.as_deref(),
            cert_file.as_deref(),
            key_file.as_deref(),
        )
    }

    fn from_values(
        protocol: Option<&str>,
        mechanism: Option<&str>,
        username: Option<&str>,
        password: Option<&str>,
        ca_file: Option<&str>,
        cert_file: Option<&str>,
        key_file: Option<&str>,
    ) -> Result<Self> {
        let protocol = Protocol::parse(protocol.unwrap_or(""))?;
        let mechanism = nonempty(mechanism, MECHANISM_ENV)?;
        let username = nonempty(username, USERNAME_ENV)?;
        let password = nonempty(password, PASSWORD_ENV)?;
        let ca_file = path_value(ca_file, CA_ENV)?;
        let cert_file = path_value(cert_file, CERT_ENV)?;
        let key_file = path_value(key_file, KEY_ENV)?;

        match protocol {
            Protocol::Plaintext => {
                reject_config(mechanism.is_some(), MECHANISM_ENV)?;
                reject_config(username.is_some(), USERNAME_ENV)?;
                reject_config(password.is_some(), PASSWORD_ENV)?;
                reject_config(ca_file.is_some(), CA_ENV)?;
                reject_config(cert_file.is_some(), CERT_ENV)?;
                reject_config(key_file.is_some(), KEY_ENV)?;
            }
            Protocol::Ssl => {
                reject_config(mechanism.is_some(), MECHANISM_ENV)?;
                reject_config(username.is_some(), USERNAME_ENV)?;
                reject_config(password.is_some(), PASSWORD_ENV)?;
                if ca_file.is_none() {
                    bail!("{CA_ENV} is required for SSL");
                }
            }
            Protocol::SaslSsl => {
                if !matches!(mechanism.as_deref(), Some("PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512")) {
                    bail!("{MECHANISM_ENV} must be PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512");
                }
                if username.is_none() {
                    bail!("{USERNAME_ENV} is required for SASL_SSL");
                }
                if password.is_none() {
                    bail!("{PASSWORD_ENV} is required for SASL_SSL");
                }
                if ca_file.is_none() {
                    bail!("{CA_ENV} is required for SASL_SSL");
                }
            }
        }

        if cert_file.is_some() != key_file.is_some() {
            bail!("{CERT_ENV} and {KEY_ENV} must be configured together");
        }
        if protocol == Protocol::Plaintext && (cert_file.is_some() || key_file.is_some()) {
            bail!("client certificate files require SSL or SASL_SSL");
        }

        for (path, variable) in [
            (ca_file.as_ref(), CA_ENV),
            (cert_file.as_ref(), CERT_ENV),
            (key_file.as_ref(), KEY_ENV),
        ] {
            if let Some(path) = path {
                let metadata = fs::metadata(path)
                    .with_context(|| format!("{variable} file is unavailable"))?;
                if !metadata.is_file() {
                    bail!("{variable} must point to a file");
                }
                fs::File::open(path)
                    .with_context(|| format!("{variable} file is not readable"))?;
            }
        }

        Ok(Self {
            protocol,
            sasl_mechanism: mechanism,
            sasl_username: username,
            sasl_password: password,
            ca_file,
            cert_file,
            key_file,
        })
    }

    /// Applies transport and authentication settings. Callers retain ownership
    /// of bootstrap servers, timeouts, consumer groups, and other client options.
    pub fn apply(&self, client: &mut ClientConfig) -> Result<()> {
        client
            .set("security.protocol", self.protocol.as_str())
            .set("ssl.endpoint.identification.algorithm", "https");

        if let Some(path) = &self.ca_file {
            client.set("ssl.ca.location", path.to_string_lossy().into_owned());
        }
        if let (Some(cert), Some(key)) = (&self.cert_file, &self.key_file) {
            client
                .set("ssl.certificate.location", cert.to_string_lossy().into_owned())
                .set("ssl.key.location", key.to_string_lossy().into_owned());
        }
        if self.protocol == Protocol::SaslSsl {
            client
                .set("sasl.mechanism", self.sasl_mechanism.as_deref().expect("validated SASL mechanism"))
                .set("sasl.username", self.sasl_username.as_deref().expect("validated SASL username"))
                .set("sasl.password", self.sasl_password.as_deref().expect("validated SASL password"));
        }
        Ok(())
    }
}

fn nonempty(value: Option<&str>, variable: &str) -> Result<Option<String>> {
    match value {
        Some(value) if value.trim().is_empty() => bail!("{variable} must not be empty"),
        Some(value) => Ok(Some(value.to_owned())),
        None => Ok(None),
    }
}

fn read_optional_env(variable: &str) -> Result<Option<String>> {
    read_optional_env_value(variable, env::var(variable))
}

fn read_optional_env_value(
    variable: &str,
    value: std::result::Result<String, env::VarError>,
) -> Result<Option<String>> {
    match value {
        Ok(value) => Ok(normalize_optional_env_value(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            bail!("{variable} must contain valid Unicode")
        }
    }
}

fn normalize_optional_env_value(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn resolve_sasl_password(
    password: Option<String>,
    password_file: Option<String>,
) -> Result<Option<String>> {
    match (password, password_file) {
        (Some(_), Some(_)) => {
            bail!("configure solo una de {PASSWORD_ENV} o {PASSWORD_FILE_ENV}")
        }
        (Some(password), None) => Ok(Some(password)),
        (None, None) => Ok(None),
        (None, Some(path)) => {
            let path = PathBuf::from(path);
            let metadata = fs::metadata(&path)
                .with_context(|| format!("{PASSWORD_FILE_ENV} file is unavailable"))?;
            if !metadata.is_file() {
                bail!("{PASSWORD_FILE_ENV} must point to a file");
            }
            let contents = fs::read_to_string(&path)
                .with_context(|| format!("{PASSWORD_FILE_ENV} is not readable text"))?;
            let password = contents
                .trim_end_matches(|character| matches!(character, '\r' | '\n'))
                .to_owned();
            if password.trim().is_empty() {
                bail!("{PASSWORD_FILE_ENV} must contain a nonempty password");
            }
            Ok(Some(password))
        }
    }
}

fn path_value(value: Option<&str>, variable: &str) -> Result<Option<PathBuf>> {
    nonempty(value, variable).map(|value| value.map(PathBuf::from))
}

fn reject_config(present: bool, variable: &str) -> Result<()> {
    if present {
        bail!("{variable} is not allowed with the selected security protocol");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(protocol: &str) -> Result<KafkaSecurityConfig> {
        KafkaSecurityConfig::from_values(
            Some(protocol),
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    #[test]
    fn accepts_only_explicit_plaintext_without_other_security_options() {
        assert!(config("PLAINTEXT").is_ok());
        assert!(config("plaintext").is_err());
        assert!(config("").is_err());
        assert!(KafkaSecurityConfig::from_values(
            Some("PLAINTEXT"), Some("PLAIN"), None, None, None, None, None
        ).is_err());
    }

    #[test]
    fn optional_environment_values_treat_empty_strings_as_absent() {
        assert_eq!(normalize_optional_env_value(String::new()), None);
        assert_eq!(normalize_optional_env_value("  ".to_owned()), None);
        assert_eq!(normalize_optional_env_value("PLAIN".to_owned()), Some("PLAIN".to_owned()));
        assert!(KafkaSecurityConfig::from_values(
            Some("SASL_SSL"), Some("PLAIN"), Some(""), Some(""), Some("ca.pem"), None, None
        ).is_err());
    }

    #[test]
    fn non_unicode_environment_values_fail_without_echoing_contents() {
        let error = read_optional_env_value(
            PASSWORD_ENV,
            Err(env::VarError::NotUnicode("secret bytes".into())),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains(PASSWORD_ENV));
        assert!(!error.contains("secret bytes"));
    }

    #[test]
    fn loads_sasl_password_from_file_and_preserves_non_newline_whitespace() {
        let path = std::env::temp_dir().join(format!(
            "taq-kafka-secret-{}.txt",
            std::process::id()
        ));
        fs::write(&path, b" secret with spaces \r\n").unwrap();
        let path = path.to_string_lossy().into_owned();
        let password = resolve_sasl_password(None, Some(path)).unwrap();
        assert_eq!(password.as_deref(), Some(" secret with spaces "));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_multiple_or_empty_sasl_password_sources() {
        assert!(resolve_sasl_password(Some("one".to_owned()), Some("two".to_owned())).is_err());
        assert!(resolve_sasl_password(None, Some(String::new())).is_err());
        let path = std::env::temp_dir().join(format!(
            "taq-kafka-empty-secret-{}.txt",
            std::process::id()
        ));
        fs::write(&path, b"   \r\n").unwrap();
        let path = path.to_string_lossy().into_owned();
        assert!(resolve_sasl_password(None, Some(path.clone())).is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn requires_ca_for_ssl_and_sasl_credentials_for_sasl_ssl() {
        assert!(config("SSL").is_err());
        assert!(config("SASL_SSL").is_err());
        assert!(KafkaSecurityConfig::from_values(
            Some("SASL_SSL"), Some("GSSAPI"), Some("user"), Some("secret"), Some("missing.pem"), None, None
        ).is_err());
    }

    #[test]
    fn accepts_ssl_profiles_with_an_existing_readable_ca_file() {
        let manifest = env!("CARGO_MANIFEST_DIR");
        let ca = PathBuf::from(manifest).join("Cargo.toml");
        let ca = ca.to_string_lossy();
        assert!(KafkaSecurityConfig::from_values(
            Some("SSL"), None, None, None, Some(&ca), None, None
        ).is_ok());
        assert!(KafkaSecurityConfig::from_values(
            Some("SASL_SSL"), Some("SCRAM-SHA-512"), Some("user"), Some("secret"), Some(&ca), None, None
        ).is_ok());
    }

    #[test]
    fn rejects_partial_client_certificate_pair() {
        assert!(KafkaSecurityConfig::from_values(
            Some("SSL"), None, None, None, Some("missing-ca.pem"), Some("cert.pem"), None
        ).is_err());
    }

    #[test]
    fn debug_redacts_sasl_password() {
        let config = KafkaSecurityConfig {
            protocol: Protocol::SaslSsl,
            sasl_mechanism: Some("PLAIN".to_owned()),
            sasl_username: Some("alice".to_owned()),
            sasl_password: Some("sensitive-secret".to_owned()),
            ca_file: Some(PathBuf::from("ca.pem")),
            cert_file: None,
            key_file: None,
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains("sensitive-secret"));
        assert!(debug.contains("REDACTED"));
    }

    #[test]
    fn identifies_obvious_development_credentials_for_public_guard() {
        let ca = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let ca = ca.to_string_lossy();
        let config = KafkaSecurityConfig::from_values(
            Some("SASL_SSL"),
            Some("SCRAM-SHA-512"),
            Some("admin"),
            Some("safe-secret-value"),
            Some(&ca),
            None,
            None,
        )
        .unwrap();
        assert!(config.uses_development_credentials());
    }
}
