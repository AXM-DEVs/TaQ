use std::time::Duration;
use std::{fs::File, io::BufReader, path::Path, sync::Arc};

use anyhow::{Context, Result};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use serde_json::Value;
use taq_ingest::SourceCursorStore;
use tokio_postgres::config::SslMode;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;

pub type PgPool = Pool;
pub use deadpool_postgres::Client as PgClient;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PgTlsMode {
    Disable,
    VerifyFull,
}

impl PgTlsMode {
    pub fn from_value(value: Option<&str>) -> Result<Self> {
        match value {
            Some("disable") => Ok(Self::Disable),
            Some("verify-full") => Ok(Self::VerifyFull),
            None => anyhow::bail!("TAQ_PG_TLS_MODE es obligatoria"),
            Some("") => anyhow::bail!("TAQ_PG_TLS_MODE no puede estar vacío"),
            Some(_) => anyhow::bail!("TAQ_PG_TLS_MODE debe ser disable o verify-full"),
        }
    }

    pub fn from_env() -> Result<Self> {
        let value = std::env::var_os("TAQ_PG_TLS_MODE")
            .ok_or_else(|| anyhow::anyhow!("TAQ_PG_TLS_MODE es obligatoria"))?;
        let value = value
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("TAQ_PG_TLS_MODE debe ser texto Unicode"))?;
        Self::from_value(Some(value))
    }
}

pub fn pg_tls_config(ca_file: Option<&Path>) -> Result<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    if let Some(path) = ca_file {
        let file = File::open(path)
            .with_context(|| format!("no se pudo abrir TAQ_PG_TLS_CA_FILE ({})", path.display()))?;
        let mut reader = BufReader::new(file);
        let certificates = rustls_pemfile::certs(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .with_context(|| format!("PEM inválido en TAQ_PG_TLS_CA_FILE ({})", path.display()))?;
        anyhow::ensure!(
            !certificates.is_empty(),
            "TAQ_PG_TLS_CA_FILE no contiene certificados PEM ({})",
            path.display()
        );
        for certificate in certificates {
            roots.add(certificate).with_context(|| {
                format!("certificado inválido en TAQ_PG_TLS_CA_FILE ({})", path.display())
            })?;
        }
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

#[derive(Clone, Copy, Debug)]
pub struct PgPoolConfig {
    max_size: usize,
    acquire_timeout: Duration,
}

impl PgPoolConfig {
    pub fn from_values(max_size: Option<&str>, acquire_timeout_secs: Option<&str>) -> Result<Self> {
        let max_size = max_size.unwrap_or("16").parse::<usize>()?;
        anyhow::ensure!(
            (1..=64).contains(&max_size),
            "TAQ_PG_POOL_MAX_SIZE debe estar entre 1 y 64"
        );
        let acquire_timeout_secs = acquire_timeout_secs.unwrap_or("5").parse::<u64>()?;
        anyhow::ensure!(
            (1..=60).contains(&acquire_timeout_secs),
            "TAQ_PG_POOL_ACQUIRE_TIMEOUT_SECS debe estar entre 1 y 60"
        );
        Ok(Self {
            max_size,
            acquire_timeout: Duration::from_secs(acquire_timeout_secs),
        })
    }

    pub fn max_size(self) -> usize {
        self.max_size
    }

    pub fn acquire_timeout(self) -> Duration {
        self.acquire_timeout
    }
}

pub fn create_pg_pool(
    database_url: &str,
    config: PgPoolConfig,
    tls_mode: PgTlsMode,
    ca_file: Option<&Path>,
) -> Result<PgPool> {
    let mut pg_config = database_url.parse()?;
    let manager = match tls_mode {
        PgTlsMode::Disable => {
            pg_config.ssl_mode(SslMode::Disable);
            Manager::from_config(
                pg_config,
                NoTls,
                ManagerConfig { recycling_method: RecyclingMethod::Fast },
            )
        }
        PgTlsMode::VerifyFull => {
            // tokio-postgres has no VerifyFull mode. Require enforces TLS while
            // MakeRustlsConnect validates both the certificate chain and hostname.
            pg_config.ssl_mode(SslMode::Require);
            Manager::from_config(
                pg_config,
                MakeRustlsConnect::new(pg_tls_config(ca_file)?),
                ManagerConfig { recycling_method: RecyclingMethod::Fast },
            )
        }
    };
    Ok(Pool::builder(manager)
        .max_size(config.max_size())
        .wait_timeout(Some(config.acquire_timeout()))
        .create_timeout(Some(config.acquire_timeout()))
        .runtime(Runtime::Tokio1)
        .build()?)
}

#[cfg(test)]
mod pool_config_tests {
    use super::{PgPoolConfig, PgTlsMode};
    use std::{path::Path, time::Duration};

    #[test]
    fn defaults_to_bounded_pool_and_acquire_timeout() {
        let config = PgPoolConfig::from_values(None, None).unwrap();
        assert_eq!(config.max_size(), 16);
        assert_eq!(config.acquire_timeout(), Duration::from_secs(5));
    }

    #[test]
    fn accepts_valid_pool_limits() {
        let config = PgPoolConfig::from_values(Some("8"), Some("12")).unwrap();
        assert_eq!(config.max_size(), 8);
        assert_eq!(config.acquire_timeout(), Duration::from_secs(12));
    }

    #[test]
    fn rejects_invalid_or_unbounded_pool_limits() {
        for value in ["0", "65", "invalid"] {
            assert!(PgPoolConfig::from_values(Some(value), None).is_err());
        }
        for value in ["0", "61", "invalid"] {
            assert!(PgPoolConfig::from_values(None, Some(value)).is_err());
        }
    }

    #[test]
    fn parses_tls_modes_strictly() {
        assert_eq!(PgTlsMode::from_value(Some("disable")).unwrap(), PgTlsMode::Disable);
        assert_eq!(PgTlsMode::from_value(Some("verify-full")).unwrap(), PgTlsMode::VerifyFull);
        assert!(PgTlsMode::from_value(None).is_err());
        for value in ["require", "prefer", "unknown", ""] {
            assert!(PgTlsMode::from_value(Some(value)).is_err());
        }
    }

    #[test]
    fn loads_webpki_roots_and_rejects_empty_or_invalid_pem() {
        assert!(super::pg_tls_config(None).is_ok());
        // Public ISRG Root X1, selected from the local certificate store by
        // SHA1 thumbprint CABD2A79A1076A31F21D253635CB039D4329A5E8.
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/isrg-root-x1.pem");
        assert!(super::pg_tls_config(Some(&fixture)).is_ok());
        let dir = std::env::temp_dir().join(format!("taq-pg-ca-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty.pem");
        std::fs::write(&empty, []).unwrap();
        assert!(super::pg_tls_config(Some(&empty)).is_err());
        let invalid = dir.join("invalid.pem");
        std::fs::write(&invalid, b"-----BEGIN CERTIFICATE-----\nnot-a-cert\n-----END CERTIFICATE-----\n").unwrap();
        assert!(super::pg_tls_config(Some(&invalid)).is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[derive(Clone)]
pub struct PgCursorStore {
    pool: PgPool,
}

impl PgCursorStore {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let config = PgPoolConfig::from_values(
            std::env::var("TAQ_PG_POOL_MAX_SIZE").ok().as_deref(),
            std::env::var("TAQ_PG_POOL_ACQUIRE_TIMEOUT_SECS")
                .ok()
                .as_deref(),
        )?;
        let tls_mode = PgTlsMode::from_env()?;
        let ca_file = std::env::var_os("TAQ_PG_TLS_CA_FILE")
            .filter(|value| !value.is_empty())
            .map(std::path::PathBuf::from);
        let pool = create_pg_pool(database_url, config, tls_mode, ca_file.as_deref())
            .context("no se pudo crear el pool PostgreSQL para checkpoints de fuentes")?;
        let client = pool
            .get()
            .await
            .context("no se pudo conectar a PostgreSQL para checkpoints de fuentes")?;
        client
            .query(
                "SELECT source_name, cursor FROM public.taq_source_cursors WHERE FALSE",
                &[],
            )
            .await
            .context("no existe public.taq_source_cursors; aplique 0001_source_cursors.sql")?;
        client
            .query(
                "SELECT source_name, native_id FROM public.taq_source_events_seen WHERE FALSE",
                &[],
            )
            .await
            .context("no existe public.taq_source_events_seen; aplique 0001_source_cursors.sql")?;
        drop(client);
        let cleanup_pool = pool.clone();
        tokio::spawn(async move {
            let mut cleanup = tokio::time::interval(std::time::Duration::from_secs(86_400));
            cleanup.tick().await;
            loop {
                cleanup.tick().await;
                let result = async {
                    let client = cleanup_pool.get().await?;
                    client.execute(
                        "DELETE FROM public.taq_source_events_seen WHERE seen_at < now() - interval '30 days'",
                        &[],
                    ).await?;
                    Ok::<_, anyhow::Error>(())
                }.await;
                if let Err(error) = result {
                    tracing::warn!(%error, "falló la limpieza de identidades de fuentes antiguas");
                }
            }
        });
        Ok(Self { pool })
    }
}

impl SourceCursorStore for PgCursorStore {
    fn load<'a>(
        &'a self,
        source: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<Option<Value>, String>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let client = self.pool.get().await.map_err(|error| {
                format!("no se pudo obtener conexión del pool PostgreSQL: {error}")
            })?;
            let row = client
                .query_opt(
                    "SELECT cursor FROM public.taq_source_cursors WHERE source_name = $1",
                    &[&source],
                )
                .await
                .map_err(|error| format!("consulta del cursor PostgreSQL falló: {error}"))?;
            Ok(row.map(|row| row.get::<_, Value>(0)))
        })
    }

    fn save<'a>(
        &'a self,
        source: &'a str,
        cursor: &'a Value,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::result::Result<(), String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let client = self.pool.get().await.map_err(|error| {
                format!("no se pudo obtener conexión del pool PostgreSQL: {error}")
            })?;
            client
                .execute(
                    "INSERT INTO public.taq_source_cursors (source_name, cursor, updated_at) \
                     VALUES ($1, $2, now()) \
                     ON CONFLICT (source_name) DO UPDATE \
                     SET cursor = EXCLUDED.cursor, updated_at = EXCLUDED.updated_at",
                    &[&source, cursor],
                )
                .await
                .map_err(|error| format!("escritura del cursor PostgreSQL falló: {error}"))?;
            Ok(())
        })
    }

    fn is_seen<'a>(
        &'a self,
        source: &'a str,
        native_id: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::result::Result<bool, String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let client = self.pool.get().await.map_err(|error| {
                format!("no se pudo obtener conexión del pool PostgreSQL: {error}")
            })?;
            let row = client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM public.taq_source_events_seen \
                     WHERE source_name = $1 AND native_id = $2)",
                    &[&source, &native_id],
                )
                .await
                .map_err(|error| format!("consulta de identidad vista falló: {error}"))?;
            Ok(row.get(0))
        })
    }

    fn acknowledge<'a>(
        &'a self,
        source: &'a str,
        native_id: &'a str,
        cursor: Option<&'a Value>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::result::Result<(), String>> + Send + 'a>,
    > {
        Box::pin(async move {
            let client = self.pool.get().await.map_err(|error| {
                format!("no se pudo obtener conexión del pool PostgreSQL: {error}")
            })?;
            client
                .execute(
                    "WITH seen AS (\
                         INSERT INTO public.taq_source_events_seen (source_name, native_id) \
                         VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING 1\
                     ) \
                     INSERT INTO public.taq_source_cursors (source_name, cursor, updated_at) \
                     SELECT $1, $3, now() WHERE $3 IS NOT NULL AND (SELECT count(*) FROM seen) >= 0 \
                     ON CONFLICT (source_name) DO UPDATE \
                     SET cursor = EXCLUDED.cursor, updated_at = EXCLUDED.updated_at",
                    &[&source, &native_id, &cursor],
                )
                .await
                .map_err(|error| format!("confirmación del evento en PostgreSQL falló: {error}"))?;
            Ok(())
        })
    }
}
