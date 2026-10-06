use std::{
    collections::HashSet,
    env,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Extension, Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use chrono::{DateTime, Utc};
use rdkafka::{
    ClientConfig,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use taq_core::Event;
use taq_observability::Telemetry;
use taq_observability::global_delivery_metrics;
use taq_state::{PgClient, PgPool, PgPoolConfig, create_pg_pool};
use tracing::{error, info};

#[derive(Clone)]
struct AppState {
    db: PgPool,
    context_ready: Arc<AtomicBool>,
}

impl AppState {
    async fn connection(&self) -> Result<PgClient, ApiError> {
        self.db
            .get()
            .await
            .map_err(|error| internal_error("postgres_pool_acquire", error))
    }
}

#[derive(Clone)]
struct Auth {
    tenant_id: String,
    principal: String,
    scopes: Vec<String>,
}

#[derive(Debug)]
struct ApiError(StatusCode, &'static str);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

fn internal_error(context: &str, error: impl std::fmt::Display) -> ApiError {
    error!(%error, context, "falló operación de API");
    ApiError(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let database_url = env::var("TAQ_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://taq:taq_dev@127.0.0.1:5432/taq".to_owned());
    let pool_config = PgPoolConfig::from_values(
        env::var("TAQ_PG_POOL_MAX_SIZE").ok().as_deref(),
        env::var("TAQ_PG_POOL_ACQUIRE_TIMEOUT_SECS").ok().as_deref(),
    )?;
    let pool = create_pg_pool(&database_url, pool_config)?;
    let db = pool.get().await?;
    db.query("SELECT 1 FROM public.taq_api_keys LIMIT 0", &[])
        .await?;
    if let Some((tenant_id, principal, scopes, create_tenant, ttl_seconds)) = provisioning_args()? {
        let tenant = if create_tenant {
            let row = db.query_one("INSERT INTO public.taq_tenants(tenant_id,name) VALUES(gen_random_uuid(),$1) RETURNING tenant_id::text", &[&tenant_id]).await?;
            row.get::<_, String>(0)
        } else {
            tenant_id.clone()
        };
        let secret = format!("taq_{}", random_hex());
        let digest = blake3::hash(secret.as_bytes());
        let key_id:String=db.query_one("INSERT INTO public.taq_api_keys(key_hash,tenant_id,principal,scopes,expires_at) VALUES($1,$2::uuid,$3,$4,CASE WHEN $5::bigint IS NULL THEN NULL ELSE now()+($5 * interval '1 second') END) RETURNING key_id::text",
            &[&&digest.as_bytes()[..], &tenant, &principal, &scopes, &ttl_seconds]).await?.get(0);
        println!("tenant_id={tenant}");
        println!("key_id={key_id}");
        println!("principal={principal}");
        println!("scopes={}", scopes.join(","));
        println!("api_key={secret}");
        println!("Guarda la clave ahora; TaQ solo almacena su hash.");
        return Ok(());
    }
    drop(db);
    for variable in [
        "TAQ_ACTION_DISPATCH_ENABLED",
        "TAQ_API_PUBLICLY_ROUTABLE",
        "TAQ_API_TLS_TERMINATED_UPSTREAM",
    ] {
        if env::var(variable).is_ok_and(|value| value != "true" && value != "false") {
            anyhow::bail!("{variable} solo acepta true o false");
        }
    }
    let telemetry = Telemetry::init("taq-api")?;
    let context_ready = Arc::new(AtomicBool::new(false));
    let state = AppState {
        db: pool,
        context_ready: context_ready.clone(),
    };
    let public = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready));
    let protected = Router::new()
        .route(
            "/v1/watchlist/entities",
            get(list_entities).post(create_entity),
        )
        .route("/v1/findings", get(list_findings).post(create_finding))
        .route(
            "/v1/findings/{finding_id}/status",
            post(update_finding_status),
        )
        .route("/v1/findings/{finding_id}/actions", post(create_action))
        .route("/v1/findings/{finding_id}/feedback", post(create_feedback))
        .route("/v1/actions/{action_id}/decision", post(decide_action))
        .route("/v1/actions", get(list_actions))
        .route("/v1/rules", get(list_rules).post(create_rule))
        .route("/v1/rules/{rule_id}/evaluations", post(evaluate_rule))
        .route("/v1/rules/{rule_id}/promotion", post(promote_rule))
        .route("/v1/rule-datasets", post(register_dataset))
        .route("/v1/guardrails", post(set_guardrails))
        .route("/v1/action-policies", post(create_action_policy))
        .route("/v1/api-keys/{key_id}", delete(revoke_api_key))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .layer(DefaultBodyLimit::max(1_048_576));
    let context_db = state.db.clone();
    let expiry_db = state.db.clone();
    let dispatch_db = state.db.clone();
    let app = public.merge(protected).with_state(state);
    let bind: SocketAddr = env::var("TAQ_API_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_owned())
        .parse()?;
    if env::var("TAQ_API_PUBLICLY_ROUTABLE").as_deref() == Ok("true")
        && env::var("TAQ_API_TLS_TERMINATED_UPSTREAM").as_deref() != Ok("true")
    {
        anyhow::bail!(
            "API públicamente enrutable requiere TLS terminado en proxy de confianza y TAQ_API_TLS_TERMINATED_UPSTREAM=true"
        );
    }
    let listener = tokio::net::TcpListener::bind(bind).await?;
    info!(%bind, "taq-api escuchando");
    let broker = env::var("TAQ_KAFKA_BROKER").unwrap_or_else(|_| "localhost:19092".to_owned());
    let worker_ready = context_ready.clone();
    let context_broker = broker.clone();
    let context_task = tokio::spawn(async move {
        retry_task("consumer de contexto", || {
            worker_ready.store(false, Ordering::SeqCst);
            run_context_consumer(context_db.clone(), &context_broker, worker_ready.clone())
        })
        .await
    });
    let expiry_task = tokio::spawn(async move {
        retry_task("expiración de acciones", || {
            expire_actions_loop(expiry_db.clone())
        })
        .await
    });
    let dispatch_task = tokio::spawn(async move {
        retry_task("dispatcher de acciones", || {
            dispatch_actions_loop(dispatch_db.clone(), &broker)
        })
        .await
    });
    let server_result: anyhow::Result<()> = tokio::select! {
        result = axum::serve(listener, app) => result.map_err(Into::into),
        result = context_task => match result {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        },
        result = expiry_task => match result {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        },
        result = dispatch_task => match result {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        },
    };
    let telemetry_result = telemetry.shutdown().await;
    server_result?;
    telemetry_result?;
    Ok(())
}

async fn retry_task<F, Fut>(name: &'static str, mut run: F) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    loop {
        match run().await {
            Ok(()) => tracing::warn!(task = name, "tarea de fondo terminó; se reiniciará"),
            Err(error) => {
                tracing::error!(%error, task = name, "tarea de fondo falló; se reintentará")
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

type ProvisioningArgs = (String, String, Vec<String>, bool, Option<i64>);

fn provisioning_args() -> anyhow::Result<Option<ProvisioningArgs>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("--provision-tenant") if (4..=5).contains(&args.len()) => {
            let ttl = parse_key_ttl(args.get(4))?;
            Ok(Some((
                args[1].clone(),
                args[2].clone(),
                parse_scopes(&args[3])?,
                true,
                ttl,
            )))
        }
        Some("--create-key") if (4..=5).contains(&args.len()) => {
            let ttl = parse_key_ttl(args.get(4))?;
            Ok(Some((
                args[1].clone(),
                args[2].clone(),
                parse_scopes(&args[3])?,
                false,
                ttl,
            )))
        }
        None => Ok(None),
        _ => anyhow::bail!(
            "uso: taq-api [--provision-tenant NOMBRE PRINCIPAL SCOPES [TTL_SEGUNDOS] | --create-key TENANT_UUID PRINCIPAL SCOPES [TTL_SEGUNDOS]]; scopes separados por coma, TTL opcional entre 60 y 31536000"
        ),
    }
}

fn parse_key_ttl(value: Option<&String>) -> anyhow::Result<Option<i64>> {
    let Some(value) = value else { return Ok(None) };
    let ttl = value
        .parse::<i64>()
        .map_err(|_| anyhow::anyhow!("TTL de API key debe ser entero"))?;
    if !(60..=31_536_000).contains(&ttl) {
        anyhow::bail!("TTL de API key debe estar entre 60 y 31536000 segundos");
    }
    Ok(Some(ttl))
}

fn parse_scopes(value: &str) -> anyhow::Result<Vec<String>> {
    let scopes = value
        .split(',')
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if scopes.is_empty()
        || scopes
            .iter()
            .any(|scope| scope.len() > 64 || scope.chars().any(char::is_whitespace))
    {
        anyhow::bail!("cada scope debe ser un token no vacío de máximo 64 caracteres");
    }
    Ok(scopes)
}

fn random_hex() -> String {
    rand::random::<[u8; 32]>()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn run_context_consumer(
    db: PgPool,
    brokers: &str,
    ready: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "taq-context-v1")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    consumer.subscribe(&["events.raw"])?;
    let dead_letter: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()?;
    consumer.fetch_metadata(Some("events.raw"), std::time::Duration::from_secs(10))?;
    ready.store(true, Ordering::SeqCst);
    info!("consumer contextualizador suscrito a events.raw");
    let mut aliases = Vec::new();
    let mut refresh_at = tokio::time::Instant::now();

    loop {
        let message = consumer.recv().await?;
        let Some(payload) = message.payload() else {
            consumer.commit_message(&message, CommitMode::Sync)?;
            continue;
        };
        let event: Event = match serde_json::from_slice(payload) {
            Ok(event) => event,
            Err(error) => {
                global_delivery_metrics().record_context_failure();
                let key = format!("{}:{}", message.partition(), message.offset());
                let envelope = serde_json::to_vec(&json!({
                    "schema_version":1,
                    "reason":"invalid_event_json",
                    "detail":error.to_string(),
                    "source_topic":"events.raw",
                    "partition":message.partition(),
                    "offset":message.offset(),
                    "payload_bytes":payload.len(),
                    "payload_hash":blake3::hash(payload).to_hex().to_string(),
                }))?;
                dead_letter
                    .send(
                        FutureRecord::to("events.context_dead_letter")
                            .key(&key)
                            .payload(&envelope),
                        std::time::Duration::from_secs(5),
                    )
                    .await
                    .map_err(|(error, _)| {
                        anyhow::anyhow!("falló el ACK de dead-letter de contexto: {error}")
                    })?;
                consumer.commit_message(&message, CommitMode::Sync)?;
                continue;
            }
        };
        let lag_records = if message.offset() % 100 == 0 {
            consumer
                .fetch_watermarks(
                    "events.raw",
                    message.partition(),
                    std::time::Duration::from_millis(100),
                )
                .ok()
                .map(|(_, high)| {
                    (high.max(0) as u64).saturating_sub(message.offset().max(0) as u64)
                })
        } else {
            None
        };
        if tokio::time::Instant::now() >= refresh_at {
            aliases = load_watchlist_cache(&db).await?;
            refresh_at = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        }
        let findings = match contextualize_event(&db, &event, &aliases).await {
            Ok(findings) => findings,
            Err(error) => {
                global_delivery_metrics().record_context_failure();
                return Err(error);
            }
        };
        global_delivery_metrics().record_context_event(findings as u64, lag_records);
        consumer.commit_message(&message, CommitMode::Sync)?;
    }
}

async fn expire_actions_loop(db: PgPool) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        let mut connection = db.get().await?;
        let tx = connection.transaction().await?;
        tx.execute(
            "WITH expired AS (\
                 UPDATE public.taq_actions SET state='expired',updated_at=now() \
                 WHERE state IN ('pending','approved') AND expires_at <= now() \
                 RETURNING tenant_id,action_id\
             ) \
             INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) \
             SELECT tenant_id,'system','action.expired','action',action_id::text,'{}'::jsonb FROM expired",
            &[],
        ).await?;
        tx.execute(
            "UPDATE public.taq_action_outbox o SET state='failed',claimed_at=NULL,last_error='action_expired' \
             FROM public.taq_actions a WHERE a.tenant_id=o.tenant_id AND a.action_id=o.action_id \
               AND a.state='expired' AND o.state IN ('pending','claimed')",
            &[],
        ).await?;
        tx.commit().await?;
        drop(connection);
        db.get().await?.execute(
            "UPDATE public.taq_action_outbox o SET state=CASE WHEN a.state='expired' THEN 'failed' ELSE 'pending' END, \
             last_error=CASE WHEN a.state='expired' THEN 'action_expired' ELSE o.last_error END, \
             claimed_at=NULL,available_at=now() FROM public.taq_actions a \
             WHERE a.tenant_id=o.tenant_id AND a.action_id=o.action_id AND o.state='claimed' \
               AND o.claimed_at < now()-interval '2 minutes' AND a.state IN ('approved','expired')",
            &[],
        ).await?;
    }
}

async fn dispatch_actions_loop(db: PgPool, brokers: &str) -> anyhow::Result<()> {
    if env::var("TAQ_ACTION_DISPATCH_ENABLED").as_deref() != Ok("true") {
        info!("dispatcher de acciones deshabilitado por kill switch");
        std::future::pending::<()>().await;
    }
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "10000")
        .create()?;
    loop {
        let mut connection = db.get().await?;
        let tx = connection.transaction().await?;
        let row=tx.query_opt(
            "SELECT o.tenant_id::text,o.action_id::text,o.payload,o.attempts \
             FROM public.taq_action_outbox o JOIN public.taq_actions a \
               ON a.tenant_id=o.tenant_id AND a.action_id=o.action_id \
             WHERE o.state='pending' AND o.available_at<=now() AND a.state='approved' AND a.expires_at>now() \
             ORDER BY o.available_at,o.tenant_id,o.action_id FOR UPDATE OF o SKIP LOCKED LIMIT 1",
            &[],
        ).await?;
        let Some(row) = row else {
            tx.commit().await?;
            drop(connection);
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        };
        let tenant_id: String = row.get(0);
        let action_id: String = row.get(1);
        let payload: Value = row.get(2);
        let attempts: i32 = row.get::<_, i32>(3).saturating_add(1);
        tx.execute("UPDATE public.taq_action_outbox SET state='claimed',attempts=$3,claimed_at=now(),last_error=NULL WHERE tenant_id=$1::uuid AND action_id=$2::uuid",&[&tenant_id,&action_id,&attempts]).await?;
        tx.commit().await?;
        drop(connection);

        let bytes = serde_json::to_vec(&payload)?;
        let delivery = producer
            .send(
                FutureRecord::to("actions.approved")
                    .key(&action_id)
                    .payload(&bytes),
                std::time::Duration::from_secs(5),
            )
            .await;
        match delivery {
            Ok(_) => {
                let mut connection = db.get().await?;
                let tx = connection.transaction().await?;
                tx.execute("UPDATE public.taq_action_outbox SET state='delivered',delivered_at=now(),claimed_at=NULL WHERE tenant_id=$1::uuid AND action_id=$2::uuid AND state='claimed'",&[&tenant_id,&action_id]).await?;
                tx.execute("UPDATE public.taq_actions SET state='queued',updated_at=now() WHERE tenant_id=$1::uuid AND action_id=$2::uuid AND state='approved'",&[&tenant_id,&action_id]).await?;
                tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,'system','action.queued','action',$2,'{}'::jsonb)",&[&tenant_id,&action_id]).await?;
                tx.commit().await?;
                drop(connection);
            }
            Err((error, _)) => {
                let terminal = attempts >= 20;
                let delay_secs = 2_i32.saturating_pow(attempts.min(8) as u32).min(300);
                let state = if terminal { "failed" } else { "pending" };
                if terminal {
                    let mut connection = db.get().await?;
                    let tx = connection.transaction().await?;
                    tx.execute("UPDATE public.taq_action_outbox SET state='failed',available_at=now()+($3 * interval '1 second'),claimed_at=NULL,last_error=$4 WHERE tenant_id=$1::uuid AND action_id=$2::uuid AND state='claimed'",&[&tenant_id,&action_id,&delay_secs,&error.to_string()]).await?;
                    tx.execute("UPDATE public.taq_actions SET state='failed',updated_at=now() WHERE tenant_id=$1::uuid AND action_id=$2::uuid AND state='approved'",&[&tenant_id,&action_id]).await?;
                    tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,'system','action.dispatch_failed','action',$2,$3)",&[&tenant_id,&action_id,&json!({"attempts":attempts,"reason":"broker_delivery_failed"})]).await?;
                    tx.commit().await?;
                    error!(%action_id,attempts,"acción agotó los reintentos al topic interno");
                } else {
                    db.get().await?.execute("UPDATE public.taq_action_outbox SET state=$3,available_at=now()+($4 * interval '1 second'),claimed_at=NULL,last_error=$5 WHERE tenant_id=$1::uuid AND action_id=$2::uuid AND state='claimed'",&[&tenant_id,&action_id,&state,&delay_secs,&error.to_string()]).await?;
                    tracing::warn!(%action_id,attempts,retry_in_seconds=delay_secs,"falló el envío del outbox; se reintentará");
                }
            }
        }
    }
}

struct ContextMatch {
    entity_id: String,
    entity_type: String,
    version: i64,
    match_kind: String,
}

struct WatchAlias {
    tenant_id: String,
    entity_id: String,
    entity_type: String,
    version: i64,
    normalized_alias: String,
    alias_kind: String,
}

async fn load_watchlist_cache(db: &PgPool) -> anyhow::Result<Vec<WatchAlias>> {
    let connection = db.get().await?;
    let rows=connection.query(
        "SELECT e.tenant_id::text,e.entity_id::text,e.entity_type, \
                (SELECT MAX(w.version) FROM public.taq_watchlist_versions w WHERE w.tenant_id=e.tenant_id), \
                a.normalized_alias,a.alias_kind \
         FROM public.taq_tenants t JOIN public.taq_entities e ON e.tenant_id=t.tenant_id \
         JOIN public.taq_entity_aliases a ON a.tenant_id=e.tenant_id AND a.entity_id=e.entity_id \
         WHERE t.active AND e.enabled \
         ORDER BY e.tenant_id,e.entity_id,a.alias_kind,a.normalized_alias LIMIT 100001",
        &[],
    ).await?;
    if rows.len() > 100_000 {
        anyhow::bail!("watchlist excede límite de cache de 100000 alias");
    }
    Ok(rows
        .into_iter()
        .map(|row| WatchAlias {
            tenant_id: row.get(0),
            entity_id: row.get(1),
            entity_type: row.get(2),
            version: row.get(3),
            normalized_alias: row.get(4),
            alias_kind: row.get(5),
        })
        .collect())
}

async fn contextualize_event(
    db: &PgPool,
    event: &Event,
    aliases: &[WatchAlias],
) -> anyhow::Result<usize> {
    let mut findings_created = 0_usize;
    let serialized = serde_json::to_vec(event)?;
    let event_hash = blake3::hash(&serialized).to_hex().to_string();
    let source_url = ["link", "url"]
        .iter()
        .find_map(|key| {
            event
                .payload
                .get(*key)
                .and_then(Value::as_str)
                .filter(|value| value.starts_with("http://") || value.starts_with("https://"))
        })
        .map(str::to_owned);
    let searchable = searchable_event_content(event);

    let mut tenant_matches = std::collections::BTreeMap::<String, Vec<ContextMatch>>::new();
    let mut negative_matches = std::collections::HashSet::<(String, String)>::new();
    for alias in aliases {
        let stored_match_kind = if alias.alias_kind == "exact" {
            "canonical"
        } else {
            alias.alias_kind.as_str()
        };
        let found = event_matches_alias(
            event,
            &alias.entity_type,
            &alias.alias_kind,
            &alias.normalized_alias,
            source_url.as_deref(),
            &searchable,
        );
        if found {
            if alias.alias_kind == "negative" {
                negative_matches.insert((alias.tenant_id.clone(), alias.entity_id.clone()));
                continue;
            }
            let matches = tenant_matches.entry(alias.tenant_id.clone()).or_default();
            if !matches
                .iter()
                .any(|matched| matched.entity_id == alias.entity_id)
            {
                matches.push(ContextMatch {
                    entity_id: alias.entity_id.clone(),
                    entity_type: alias.entity_type.clone(),
                    version: alias.version,
                    match_kind: stored_match_kind.to_owned(),
                });
            }
        }
    }
    for (tenant_id, matches) in tenant_matches {
        let matches = matches
            .into_iter()
            .filter(|matched| {
                !negative_matches.contains(&(tenant_id.clone(), matched.entity_id.clone()))
            })
            .collect::<Vec<_>>();
        if matches.is_empty() {
            continue;
        }
        let mut connection = db.get().await?;
        let tx = connection.transaction().await?;
        for matched in matches {
            let evidence =
                json!({"event_id":event.id,"source_url":source_url,"event_hash":event_hash});
            let context_match_kind = if matched.match_kind == "exact" {
                "canonical"
            } else {
                &matched.match_kind
            };
            tx.execute(
                "INSERT INTO public.taq_contextualized_events(tenant_id,event_id,entity_id,context_version,match_kind,evidence) \
                 VALUES($1::uuid,$2,$3::uuid,$4,$5,$6) ON CONFLICT DO NOTHING",
                &[&tenant_id,&event.id,&matched.entity_id,&matched.version,&context_match_kind,&evidence],
            ).await?;
            let rules = tx
                .query(
                    "SELECT rule_id,version,definition FROM public.taq_rule_versions \
                 WHERE tenant_id=$1::uuid AND state='active' ORDER BY rule_id,version",
                    &[&tenant_id],
                )
                .await?;
            for rule in rules {
                let rule_id: String = rule.get(0);
                let rule_version: i64 = rule.get(1);
                let definition: Value = rule.get(2);
                let required_match = definition
                    .get("match_kind")
                    .and_then(Value::as_str)
                    .unwrap_or("any");
                if required_match != "any" && required_match != matched.match_kind {
                    continue;
                }
                let severity = definition
                    .get("severity")
                    .and_then(Value::as_str)
                    .unwrap_or("medium");
                if !["info", "low", "medium", "high", "critical"].contains(&severity) {
                    continue;
                }
                let score = definition
                    .get("score")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.5);
                if !(0.0..=1.0).contains(&score) {
                    continue;
                }
                let finding_type = definition
                    .get("finding_type")
                    .and_then(Value::as_str)
                    .unwrap_or(&matched.entity_type);
                let explanation = definition
                    .get("explanation")
                    .and_then(Value::as_str)
                    .unwrap_or(
                        "Coincidencia exacta de entidad vigilada; requiere revisión humana.",
                    );
                let idempotency_key = blake3::hash(
                    format!(
                        "{}|{}|{}|{}|{}",
                        tenant_id, matched.entity_id, rule_id, rule_version, event.id
                    )
                    .as_bytes(),
                )
                .to_hex()
                .to_string();
                let rule_evidence =
                    json!([{"event_id":event.id,"source_url":source_url,"event_hash":event_hash}]);
                let finding_material = serde_json::to_vec(
                    &json!({"entity_id":matched.entity_id,"rule_id":rule_id,"rule_version":rule_version,"finding_type":finding_type,"severity":severity,"score":score,"explanation":explanation,"evidence":rule_evidence}),
                )?;
                let request_hash = blake3::hash(&finding_material);
                let inserted = tx.execute(
                    "INSERT INTO public.taq_findings(tenant_id,finding_id,idempotency_key,request_hash,entity_id,rule_id,rule_version,finding_type,severity,score,explanation,evidence) \
                     VALUES($1::uuid,gen_random_uuid(),$2,$3,$4::uuid,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (tenant_id,idempotency_key) DO NOTHING",
                    &[&tenant_id,&idempotency_key,&&request_hash.as_bytes()[..],&matched.entity_id,&rule_id,&rule_version,&finding_type,&severity,&score,&explanation,&rule_evidence],
                ).await?;
                findings_created += inserted as usize;
            }
        }
        tx.commit().await?;
    }
    Ok(findings_created)
}

fn searchable_event_content(event: &Event) -> String {
    let mut values = event.entity_ids.clone();
    if let Some(object) = event.payload.as_object() {
        for key in [
            "title",
            "description",
            "text",
            "content",
            "name",
            "link",
            "url",
        ] {
            if let Some(value) = object.get(key).and_then(Value::as_str) {
                values.push(value.to_lowercase());
            }
        }
    }
    values.join(" ").to_lowercase()
}

fn event_matches_alias(
    event: &Event,
    entity_type: &str,
    alias_kind: &str,
    normalized_alias: &str,
    source_url: Option<&str>,
    searchable: &str,
) -> bool {
    if alias_kind == "domain" || entity_type == "domain" {
        source_url.and_then(domain_from_url).is_some_and(|host| {
            host == normalized_alias || host.ends_with(&format!(".{normalized_alias}"))
        })
    } else {
        exact_phrase_match(searchable, normalized_alias)
            || event
                .entity_ids
                .iter()
                .any(|value| value.to_lowercase() == normalized_alias)
    }
}

fn exact_phrase_match(content: &str, phrase: &str) -> bool {
    if phrase.is_empty() {
        return false;
    }
    content.match_indices(phrase).any(|(start, matched)| {
        let end = start + matched.len();
        let left_boundary = content[..start]
            .chars()
            .next_back()
            .is_none_or(|c| !c.is_alphanumeric());
        let right_boundary = content[end..]
            .chars()
            .next()
            .is_none_or(|c| !c.is_alphanumeric());
        left_boundary && right_boundary
    })
}

fn domain_from_url(value: &str) -> Option<String> {
    let parsed = url::Url::parse(value).ok()?;
    let host = parsed.host_str()?;
    idna::domain_to_ascii(host)
        .ok()
        .map(|host| host.to_ascii_lowercase())
}

async fn authenticate(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    global_delivery_metrics().record_api_request();
    let Some(token) = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| value.len() >= 32 && value.len() <= 512)
    else {
        return ApiError(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };
    let hash = blake3::hash(token.as_bytes());
    let client = match state.connection().await {
        Ok(client) => client,
        Err(error) => return error.into_response(),
    };
    let row = match client.query_opt(
        "SELECT k.tenant_id::text, k.principal, k.scopes, k.requests_per_minute FROM public.taq_api_keys k \
         JOIN public.taq_tenants t ON t.tenant_id = k.tenant_id \
         WHERE k.key_hash = $1 AND k.active AND t.active \
           AND (k.expires_at IS NULL OR k.expires_at > now())",
        &[&&hash.as_bytes()[..]],
    ).await {
        Ok(row) => row,
        Err(error) => return internal_error("autenticación", error).into_response(),
    };
    let Some(row) = row else {
        return ApiError(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    };
    let requests_per_minute: i32 = row.get(3);
    let count: i32 = match client
        .query_one(
            "INSERT INTO public.taq_api_usage(key_hash,window_start,request_count) \
         VALUES($1,date_trunc('minute',now()),1) \
         ON CONFLICT(key_hash) DO UPDATE SET \
           request_count=CASE WHEN taq_api_usage.window_start=EXCLUDED.window_start \
             THEN LEAST(taq_api_usage.request_count+1,$2+1) ELSE 1 END, \
           window_start=EXCLUDED.window_start RETURNING request_count",
            &[
                &&hash.as_bytes()[..] as &(dyn tokio_postgres::types::ToSql + Sync),
                &requests_per_minute,
            ],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(error) => return internal_error("cuota API", error).into_response(),
    };
    if count > requests_per_minute {
        global_delivery_metrics().record_api_rate_limited();
        return ApiError(StatusCode::TOO_MANY_REQUESTS, "rate_limit_exceeded").into_response();
    }
    request.extensions_mut().insert(Auth {
        tenant_id: row.get(0),
        principal: row.get(1),
        scopes: row.get(2),
    });
    next.run(request).await
}

async fn live() -> Json<Value> {
    Json(json!({"status":"live"}))
}

async fn ready(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    state
        .connection()
        .await?
        .query_one("SELECT 1", &[])
        .await
        .map_err(|error| internal_error("readiness PostgreSQL", error))?;
    if !state.context_ready.load(Ordering::SeqCst) {
        return Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "context_consumer_not_ready",
        ));
    }
    Ok(Json(json!({"status":"ready"})))
}

fn require_scope(auth: &Auth, scope: &str) -> Result<(), ApiError> {
    if !auth
        .scopes
        .iter()
        .any(|allowed| allowed == scope || allowed == "*")
    {
        return Err(ApiError(StatusCode::FORBIDDEN, "forbidden"));
    }
    Ok(())
}

#[derive(Serialize)]
struct EntityRow {
    entity_id: String,
    entity_type: String,
    canonical_name: String,
    version: i64,
    aliases: Vec<String>,
}

async fn list_entities(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
) -> Result<Json<Vec<EntityRow>>, ApiError> {
    require_scope(&auth, "watchlist:read")?;
    let rows = state
        .connection()
        .await?
        .query(
            "SELECT e.entity_id::text, e.entity_type, e.canonical_name, e.version, \
         COALESCE(array_agg(a.alias) FILTER (WHERE a.alias IS NOT NULL), ARRAY[]::text[]) \
         FROM public.taq_entities e LEFT JOIN public.taq_entity_aliases a \
           ON a.tenant_id=e.tenant_id AND a.entity_id=e.entity_id \
         WHERE e.tenant_id=$1::uuid AND e.enabled \
         GROUP BY e.tenant_id, e.entity_id ORDER BY e.canonical_name LIMIT 500",
            &[&auth.tenant_id],
        )
        .await
        .map_err(|error| internal_error("listar watchlist", error))?;
    Ok(Json(
        rows.into_iter()
            .map(|row| EntityRow {
                entity_id: row.get(0),
                entity_type: row.get(1),
                canonical_name: row.get(2),
                version: row.get(3),
                aliases: row.get(4),
            })
            .collect(),
    ))
}

#[derive(Deserialize)]
struct CreateEntity {
    idempotency_key: String,
    entity_type: String,
    canonical_name: String,
    aliases: Vec<String>,
    #[serde(default)]
    negative_aliases: Vec<String>,
    reason: String,
}

async fn create_entity(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Json(input): Json<CreateEntity>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_scope(&auth, "watchlist:write")?;
    if !["brand", "domain", "organization"].contains(&input.entity_type.as_str())
        || input.canonical_name.trim().is_empty()
        || input.reason.trim().is_empty()
        || input.reason.len() > 2000
        || input.aliases.len() > 100
        || input.negative_aliases.len() > 100
        || input.idempotency_key.trim().is_empty()
        || input.idempotency_key.len() > 256
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_entity"));
    }
    let canonical = normalize_watch_value(&input.entity_type, &input.canonical_name)?;
    let mut normalized_aliases = std::collections::BTreeMap::<(String, String), String>::new();
    for alias in input
        .aliases
        .iter()
        .cloned()
        .chain(std::iter::once(input.canonical_name.clone()))
    {
        let normalized = normalize_watch_value(&input.entity_type, &alias)?;
        let kind = if input.entity_type == "domain" {
            "domain"
        } else if normalized == canonical {
            "exact"
        } else {
            "alias"
        };
        normalized_aliases
            .entry((normalized, kind.to_owned()))
            .or_insert(alias);
    }
    for alias in &input.negative_aliases {
        let normalized = normalize_watch_value(&input.entity_type, alias)?;
        normalized_aliases
            .entry((normalized, "negative".to_owned()))
            .or_insert(alias.clone());
    }
    let request_material=serde_json::to_vec(&json!({"type":input.entity_type,"canonical":canonical,"aliases":normalized_aliases.keys().map(|(normalized,kind)|json!([normalized,kind])).collect::<Vec<_>>(),"reason":input.reason}))
        .map_err(|_|ApiError(StatusCode::BAD_REQUEST,"invalid_entity"))?;
    let request_hash = blake3::hash(&request_material);
    let mut db = state.connection().await?;
    let tx = db
        .transaction()
        .await
        .map_err(|error| internal_error("iniciar transacción de watchlist", error))?;
    tx.query_opt(
        "SELECT tenant_id FROM public.taq_tenants WHERE tenant_id=$1::uuid FOR UPDATE",
        &[&auth.tenant_id],
    )
    .await
    .map_err(|error| internal_error("bloquear tenant", error))?
    .ok_or(ApiError(StatusCode::UNAUTHORIZED, "unauthorized"))?;
    if let Some(existing)=tx.query_opt("SELECT entity_id::text,version,request_hash FROM public.taq_entities WHERE tenant_id=$1::uuid AND idempotency_key=$2",&[&auth.tenant_id,&input.idempotency_key])
        .await.map_err(|error|internal_error("consultar idempotencia de watchlist",error))? {
        let existing_hash:Vec<u8>=existing.get(2);
        if existing_hash.as_slice()!=request_hash.as_bytes() { return Err(ApiError(StatusCode::CONFLICT,"idempotency_key_conflict")); }
        tx.rollback().await.map_err(|error|internal_error("cerrar transacción idempotente",error))?;
        return Ok((StatusCode::OK,Json(json!({"entity_id":existing.get::<_,String>(0),"version":existing.get::<_,i64>(1),"idempotent":true}))));
    }
    let row = tx.query_one("SELECT COALESCE(MAX(version),0)+1 FROM public.taq_watchlist_versions WHERE tenant_id=$1::uuid", &[&auth.tenant_id])
        .await.map_err(|error| internal_error("crear versión de watchlist", error))?;
    let version: i64 = row.get(0);
    tx.execute("INSERT INTO public.taq_watchlist_versions(tenant_id,version,created_by,change_reason) VALUES($1::uuid,$2,$3,$4)",
        &[&auth.tenant_id, &version, &"api-key", &input.reason])
        .await.map_err(|error| internal_error("guardar versión de watchlist", error))?;
    let row = tx.query_one("INSERT INTO public.taq_entities(tenant_id,entity_id,idempotency_key,request_hash,entity_type,canonical_name,version) VALUES($1::uuid,gen_random_uuid(),$2,$3,$4,$5,$6) RETURNING entity_id::text",
        &[&auth.tenant_id, &input.idempotency_key, &&request_hash.as_bytes()[..], &input.entity_type, &canonical, &version])
        .await.map_err(|error| internal_error("crear entidad", error))?;
    let entity_id: String = row.get(0);
    for ((normalized, kind), alias) in normalized_aliases {
        tx.execute("INSERT INTO public.taq_entity_aliases(tenant_id,entity_id,alias,normalized_alias,alias_kind) VALUES($1::uuid,$2::uuid,$3,$4,$5)",
            &[&auth.tenant_id, &entity_id, &alias, &normalized, &kind])
            .await.map_err(|error| internal_error("guardar alias", error))?;
    }
    tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'watchlist.entity.create','entity',$3,$4)",
        &[&auth.tenant_id, &auth.principal, &entity_id, &json!({"version":version,"type":input.entity_type})])
        .await.map_err(|error| internal_error("auditar entidad", error))?;
    tx.commit()
        .await
        .map_err(|error| internal_error("confirmar watchlist", error))?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"entity_id":entity_id,"version":version,"canonical_name":canonical})),
    ))
}

fn normalize_watch_value(entity_type: &str, value: &str) -> Result<String, ApiError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 253 || value.chars().any(char::is_control) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_watchlist_value"));
    }
    if entity_type == "domain" {
        let domain = value.trim_end_matches('.');
        let ascii = idna::domain_to_ascii(domain)
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_domain"))?
            .to_ascii_lowercase();
        if ascii.is_empty()
            || ascii
                .split('.')
                .any(|label| label.is_empty() || label.len() > 63)
        {
            return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_domain"));
        }
        Ok(ascii)
    } else {
        Ok(value.to_lowercase())
    }
}

#[derive(Deserialize)]
struct Pagination {
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn list_findings(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Query(page): Query<Pagination>,
) -> Result<Json<Vec<Value>>, ApiError> {
    require_scope(&auth, "findings:read")?;
    let limit = page.limit.unwrap_or(50).clamp(1, 100);
    let offset = page.offset.unwrap_or(0).max(0);
    let rows = state.connection().await?.query("SELECT jsonb_build_object('finding_id',finding_id,'entity_id',entity_id,'type',finding_type,'severity',severity,'score',score,'explanation',explanation,'evidence',evidence,'status',status,'rule_id',rule_id,'rule_version',rule_version,'created_at',created_at) FROM public.taq_findings WHERE tenant_id=$1::uuid ORDER BY created_at DESC LIMIT $2 OFFSET $3", &[&auth.tenant_id,&limit,&offset])
        .await.map_err(|error| internal_error("listar hallazgos", error))?;
    Ok(Json(rows.into_iter().map(|row| row.get(0)).collect()))
}

#[derive(Deserialize)]
struct CreateFinding {
    entity_id: String,
    rule_id: String,
    rule_version: i64,
    finding_type: String,
    severity: String,
    score: f64,
    explanation: String,
    evidence: Value,
    idempotency_key: String,
}

async fn create_finding(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Json(input): Json<CreateFinding>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_scope(&auth, "findings:write")?;
    if input.score.is_nan()
        || !(0.0..=1.0).contains(&input.score)
        || input.explanation.trim().is_empty()
        || input.explanation.len() > 2000
        || input.finding_type.trim().is_empty()
        || input.finding_type.len() > 128
        || !valid_uuid(&input.entity_id)
        || !validate_evidence(&input.evidence)
        || input.idempotency_key.is_empty()
        || input.idempotency_key.len() > 256
        || !["info", "low", "medium", "high", "critical"].contains(&input.severity.as_str())
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_finding"));
    }
    let material=serde_json::to_vec(&json!({"entity_id":input.entity_id,"rule_id":input.rule_id,"rule_version":input.rule_version,"finding_type":input.finding_type,"severity":input.severity,"score":input.score,"explanation":input.explanation,"evidence":input.evidence}))
        .map_err(|_|ApiError(StatusCode::BAD_REQUEST,"invalid_finding"))?;
    let request_hash = blake3::hash(&material);
    let inserted=state.connection().await?.query_opt("INSERT INTO public.taq_findings(tenant_id,finding_id,idempotency_key,request_hash,entity_id,rule_id,rule_version,finding_type,severity,score,explanation,evidence) SELECT $1::uuid,gen_random_uuid(),$2,$3,$4::uuid,$5,$6,$7,$8,$9,$10,$11 FROM public.taq_rule_versions r WHERE r.tenant_id=$1::uuid AND r.rule_id=$5 AND r.version=$6 AND r.state='active' ON CONFLICT (tenant_id,idempotency_key) DO NOTHING RETURNING finding_id::text", &[&auth.tenant_id,&input.idempotency_key,&&request_hash.as_bytes()[..],&input.entity_id,&input.rule_id,&input.rule_version,&input.finding_type,&input.severity,&input.score,&input.explanation,&input.evidence])
        .await.map_err(|error| internal_error("crear hallazgo", error))?;
    let (finding_id, idempotent) = if let Some(row) = inserted {
        (row.get::<_, String>(0), false)
    } else {
        let row=state.connection().await?.query_opt("SELECT finding_id::text,request_hash FROM public.taq_findings WHERE tenant_id=$1::uuid AND idempotency_key=$2",&[&auth.tenant_id,&input.idempotency_key])
            .await.map_err(|error|internal_error("consultar hallazgo idempotente",error))?
            .ok_or(ApiError(StatusCode::UNPROCESSABLE_ENTITY,"entity_or_active_rule_not_found"))?;
        let stored: Vec<u8> = row.get(1);
        if stored.as_slice() != request_hash.as_bytes() {
            return Err(ApiError(StatusCode::CONFLICT, "idempotency_key_conflict"));
        }
        (row.get(0), true)
    };
    Ok((
        if idempotent {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(json!({"finding_id":finding_id,"idempotent":idempotent})),
    ))
}

fn valid_uuid(value: &str) -> bool {
    value.len() == 36
        && value.chars().enumerate().all(|(i, ch)| {
            if [8, 13, 18, 23].contains(&i) {
                ch == '-'
            } else {
                ch.is_ascii_hexdigit()
            }
        })
}

fn validate_evidence(evidence: &Value) -> bool {
    let Some(items) = evidence.as_array() else {
        return false;
    };
    if items.is_empty() || items.len() > 100 {
        return false;
    }
    let Ok(encoded) = serde_json::to_vec(evidence) else {
        return false;
    };
    if encoded.len() > 64 * 1024 {
        return false;
    }
    items.iter().all(|item| {
        let Some(object) = item.as_object() else {
            return false;
        };
        let valid_keys = object
            .keys()
            .all(|key| ["event_id", "source_url", "event_hash"].contains(&key.as_str()));
        let has_event = object
            .get("event_id")
            .and_then(Value::as_str)
            .is_some_and(|v| !v.trim().is_empty() && v.len() <= 256);
        let valid_url = object.get("source_url").is_none_or(|value| {
            value.is_null()
                || value.as_str().is_some_and(|url| {
                    url.len() <= 2048 && (url.starts_with("https://") || url.starts_with("http://"))
                })
        });
        let valid_hash = object.get("event_hash").is_none_or(|value| {
            value.is_null()
                || value.as_str().is_some_and(|hash| {
                    hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
        });
        valid_keys && has_event && valid_url && valid_hash
    })
}

#[derive(Deserialize)]
struct FeedbackInput {
    idempotency_key: String,
    label: String,
    reason: String,
}

async fn create_feedback(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(finding_id): Path<String>,
    Json(input): Json<FeedbackInput>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_scope(&auth, "feedback:write")?;
    if !valid_uuid(&finding_id) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_finding_id"));
    }
    if !["correct", "false_positive", "missed", "needs_review"].contains(&input.label.as_str())
        || input.reason.trim().is_empty()
        || input.reason.len() > 2000
        || input.idempotency_key.trim().is_empty()
        || input.idempotency_key.len() > 256
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_feedback"));
    }
    let mut db = state.connection().await?;
    let tx = db
        .transaction()
        .await
        .map_err(|error| internal_error("iniciar feedback", error))?;
    let row = tx.query_opt("SELECT jsonb_build_object('finding_id',finding_id,'entity_id',entity_id,'rule_id',rule_id,'rule_version',rule_version,'evidence',evidence,'status',status),rule_id,rule_version FROM public.taq_findings WHERE tenant_id=$1::uuid AND finding_id=$2::uuid", &[&auth.tenant_id,&finding_id])
        .await.map_err(|error| internal_error("leer hallazgo para feedback", error))?
        .ok_or(ApiError(StatusCode::NOT_FOUND,"finding_not_found"))?;
    let snapshot: Value = row.get(0);
    let rule_id: String = row.get(1);
    let rule_version: i64 = row.get(2);
    let material=serde_json::to_vec(&json!({"finding_id":finding_id,"label":input.label,"reason":input.reason,"author":auth.principal}))
        .map_err(|_|ApiError(StatusCode::BAD_REQUEST,"invalid_feedback"))?;
    let request_hash = blake3::hash(&material);
    let inserted=tx.query_opt("INSERT INTO public.taq_feedback(tenant_id,feedback_id,idempotency_key,request_hash,finding_id,finding_snapshot,label,reason,author,rule_id,rule_version) VALUES($1::uuid,gen_random_uuid(),$2,$3,$4::uuid,$5,$6,$7,$8,$9,$10) ON CONFLICT(tenant_id,idempotency_key) DO NOTHING RETURNING feedback_id::text", &[&auth.tenant_id,&input.idempotency_key,&&request_hash.as_bytes()[..],&finding_id,&snapshot,&input.label,&input.reason,&auth.principal,&rule_id,&rule_version])
        .await.map_err(|error| internal_error("guardar feedback", error))?;
    let (feedback_id, was_inserted) = if let Some(row) = inserted {
        (row.get::<_, String>(0), true)
    } else {
        let row=tx.query_one("SELECT feedback_id::text,request_hash FROM public.taq_feedback WHERE tenant_id=$1::uuid AND idempotency_key=$2",&[&auth.tenant_id,&input.idempotency_key])
            .await.map_err(|error|internal_error("consultar feedback idempotente",error))?;
        let stored: Vec<u8> = row.get(1);
        if stored.as_slice() != request_hash.as_bytes() {
            return Err(ApiError(StatusCode::CONFLICT, "idempotency_key_conflict"));
        }
        (row.get(0), false)
    };
    if !was_inserted {
        tx.rollback()
            .await
            .map_err(|error| internal_error("cerrar feedback repetido", error))?;
        return Ok((
            StatusCode::OK,
            Json(
                json!({"feedback_id":feedback_id,"idempotent":true,"promotion":"manual_review_required"}),
            ),
        ));
    }
    tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'finding.feedback','feedback',$3,$4)",&[&auth.tenant_id,&auth.principal,&feedback_id,&json!({"finding_id":finding_id,"label":input.label})])
        .await.map_err(|error|internal_error("auditar feedback",error))?;
    tx.commit()
        .await
        .map_err(|error| internal_error("confirmar feedback", error))?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"feedback_id":feedback_id,"promotion":"manual_review_required"})),
    ))
}

#[derive(Deserialize)]
struct FindingStatus {
    status: String,
}

async fn update_finding_status(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(finding_id): Path<String>,
    Json(input): Json<FindingStatus>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&auth, "findings:review")?;
    if !valid_uuid(&finding_id) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_finding_id"));
    }
    if !["confirmed", "false_positive", "dismissed"].contains(&input.status.as_str()) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_finding_status"));
    }
    let mut db = state.connection().await?;
    let tx = db
        .transaction()
        .await
        .map_err(|error| internal_error("iniciar revisión de hallazgo", error))?;
    let count=tx.execute("UPDATE public.taq_findings SET status=$3,updated_at=now() WHERE tenant_id=$1::uuid AND finding_id=$2::uuid",&[&auth.tenant_id,&finding_id,&input.status])
        .await.map_err(|error|internal_error("actualizar estado de hallazgo",error))?;
    if count == 0 {
        return Err(ApiError(StatusCode::NOT_FOUND, "finding_not_found"));
    }
    tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'finding.status','finding',$3,$4)",&[&auth.tenant_id,&auth.principal,&finding_id,&json!({"status":input.status})])
        .await.map_err(|error|internal_error("auditar revisión de hallazgo",error))?;
    tx.commit()
        .await
        .map_err(|error| internal_error("confirmar revisión", error))?;
    Ok(Json(json!({"finding_id":finding_id,"status":input.status})))
}

#[derive(Deserialize)]
struct CreateRule {
    rule_id: String,
    definition: Value,
}

async fn create_rule(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Json(input): Json<CreateRule>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_scope(&auth, "rules:write")?;
    if input.rule_id.trim().is_empty() || input.rule_id.len() > 128 || !input.definition.is_object()
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_rule"));
    }
    if let Some(match_kind) = input.definition.get("match_kind").and_then(Value::as_str)
        && !["any", "canonical", "alias", "domain"].contains(&match_kind)
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_rule_match_kind"));
    }
    if input
        .definition
        .get("severity")
        .and_then(Value::as_str)
        .is_some_and(|severity| !["info", "low", "medium", "high", "critical"].contains(&severity))
        || input
            .definition
            .get("score")
            .and_then(Value::as_f64)
            .is_some_and(|score| !score.is_finite() || !(0.0..=1.0).contains(&score))
        || input
            .definition
            .get("finding_type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.is_empty() || kind.len() > 128)
        || input
            .definition
            .get("explanation")
            .and_then(Value::as_str)
            .is_some_and(|text| text.is_empty() || text.len() > 2000)
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_rule_definition"));
    }
    let mut db = state.connection().await?;
    let tx = db
        .transaction()
        .await
        .map_err(|error| internal_error("iniciar versión de regla", error))?;
    tx.query_opt(
        "SELECT tenant_id FROM public.taq_tenants WHERE tenant_id=$1::uuid FOR UPDATE",
        &[&auth.tenant_id],
    )
    .await
    .map_err(|error| internal_error("bloquear tenant para regla", error))?
    .ok_or(ApiError(StatusCode::UNAUTHORIZED, "unauthorized"))?;
    let version: i64 = tx.query_one("SELECT COALESCE(MAX(version),0)+1 FROM public.taq_rule_versions WHERE tenant_id=$1::uuid AND rule_id=$2", &[&auth.tenant_id,&input.rule_id])
        .await.map_err(|error| internal_error("obtener versión de regla", error))?.get(0);
    tx.execute("INSERT INTO public.taq_rule_versions(tenant_id,rule_id,version,definition,state,created_by) VALUES($1::uuid,$2,$3,$4,'candidate',$5)",
        &[&auth.tenant_id,&input.rule_id,&version,&input.definition,&auth.principal])
        .await.map_err(|error| internal_error("guardar regla candidata", error))?;
    tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'rule.create','rule',$3,$4)",
        &[&auth.tenant_id,&auth.principal,&input.rule_id,&json!({"version":version,"state":"candidate"})])
        .await.map_err(|error| internal_error("auditar creación de regla", error))?;
    tx.commit()
        .await
        .map_err(|error| internal_error("confirmar regla", error))?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"rule_id":input.rule_id,"version":version,"state":"candidate"})),
    ))
}

async fn list_rules(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Query(page): Query<Pagination>,
) -> Result<Json<Vec<Value>>, ApiError> {
    require_scope(&auth, "rules:read")?;
    let limit = page.limit.unwrap_or(50).clamp(1, 100);
    let offset = page.offset.unwrap_or(0).max(0);
    let rows=state.connection().await?.query(
        "SELECT jsonb_build_object('rule_id',rule_id,'version',version,'definition',definition,'state',state,'created_by',created_by,'created_at',created_at) \
         FROM public.taq_rule_versions WHERE tenant_id=$1::uuid ORDER BY rule_id,version DESC LIMIT $2 OFFSET $3",
        &[&auth.tenant_id,&limit,&offset],
    ).await.map_err(|error|internal_error("listar versiones de reglas",error))?;
    Ok(Json(rows.into_iter().map(|row| row.get(0)).collect()))
}

#[derive(Clone, Deserialize, Serialize)]
struct DatasetAlias {
    value: String,
    kind: String,
}

#[derive(Clone, Deserialize, Serialize)]
struct DatasetSample {
    split: String,
    label: String,
    event: Event,
    watchlist_version: i64,
    entity_id: String,
    entity_type: String,
    aliases: Vec<DatasetAlias>,
}

const RULE_DATASET_FORMAT_VERSION: &str = "taq_rule_dataset_v1";

fn rule_dataset_hash(samples: &[DatasetSample]) -> Result<String, ApiError> {
    let mut material = RULE_DATASET_FORMAT_VERSION.as_bytes().to_vec();
    material.push(0);
    material.extend(
        serde_json::to_vec(samples)
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_dataset_manifest"))?,
    );
    Ok(blake3::hash(&material).to_hex().to_string())
}

fn normalize_dataset_order(samples: &mut [DatasetSample]) {
    for sample in samples.iter_mut() {
        sample.aliases.sort_by(|left, right| {
            left.kind
                .cmp(&right.kind)
                .then_with(|| left.value.cmp(&right.value))
        });
    }
    samples.sort_by(|left, right| {
        left.split
            .cmp(&right.split)
            .then_with(|| left.event.id.cmp(&right.event.id))
            .then_with(|| left.entity_id.cmp(&right.entity_id))
            .then_with(|| left.label.cmp(&right.label))
    });
}

fn calculate_rule_metrics(definition: &Value, samples: &[DatasetSample], split: &str) -> Value {
    let required_match = definition
        .get("match_kind")
        .and_then(Value::as_str)
        .unwrap_or("any");
    let mut tp = 0_u64;
    let mut fp = 0_u64;
    let mut fn_ = 0_u64;
    let mut tn = 0_u64;
    for sample in samples.iter().filter(|sample| sample.split == split) {
        let source_url = ["link", "url"].iter().find_map(|key| {
            sample
                .event
                .payload
                .get(*key)
                .and_then(Value::as_str)
                .filter(|value| value.starts_with("http://") || value.starts_with("https://"))
        });
        let searchable = searchable_event_content(&sample.event);
        let mut matched_kind = None;
        let mut negative_match = false;
        for alias in &sample.aliases {
            let found = event_matches_alias(
                &sample.event,
                &sample.entity_type,
                &alias.kind,
                &alias.value,
                source_url,
                &searchable,
            );
            if !found {
                continue;
            }
            if alias.kind == "negative" {
                negative_match = true;
                continue;
            }
            if matched_kind.is_none() {
                matched_kind = Some(if alias.kind == "exact" {
                    "canonical"
                } else {
                    alias.kind.as_str()
                });
            }
        }
        let predicted = !negative_match
            && matched_kind.is_some_and(|kind| required_match == "any" || required_match == kind);
        let expected = sample.label == "alertable";
        match (predicted, expected) {
            (true, true) => tp += 1,
            (true, false) => fp += 1,
            (false, true) => fn_ += 1,
            (false, false) => tn += 1,
        }
    }
    let count = tp + fp + fn_ + tn;
    let precision = if tp + fp == 0 {
        0.0
    } else {
        tp as f64 / (tp + fp) as f64
    };
    let recall = if tp + fn_ == 0 {
        0.0
    } else {
        tp as f64 / (tp + fn_) as f64
    };
    let false_positives_per_1000 = if count == 0 {
        0.0
    } else {
        fp as f64 * 1000.0 / count as f64
    };
    json!({"tp":tp,"fp":fp,"fn":fn_,"tn":tn,"sample_count":count,"precision":precision,"recall":recall,"false_positives_per_1000":false_positives_per_1000})
}

#[derive(Deserialize)]
struct RegisterDataset {
    dataset_version: String,
    samples: Vec<DatasetSample>,
}

async fn register_dataset(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Json(input): Json<RegisterDataset>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_scope(&auth, "rules:dataset:approve")?;
    if input.dataset_version.trim().is_empty()
        || input.dataset_version.len() > 128
        || input.samples.is_empty()
        || input.samples.len() > 1000
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "invalid_dataset_manifest",
        ));
    }
    let mut samples = input.samples;
    let mut unique_samples = HashSet::new();
    let mut split_labels = std::collections::BTreeMap::<String, HashSet<String>>::new();
    for sample in &mut samples {
        if !["gold", "temporal_holdout"].contains(&sample.split.as_str())
            || !["alertable", "not_alertable"].contains(&sample.label.as_str())
            || !valid_uuid(&sample.entity_id)
            || sample.watchlist_version <= 0
            || !["brand", "domain", "organization"].contains(&sample.entity_type.as_str())
            || sample.event.id.trim().is_empty()
            || sample.event.id.len() > 256
            || sample.event.source.trim().is_empty()
            || sample.event.source.len() > 128
            || sample.aliases.is_empty()
            || sample.aliases.len() > 100
        {
            return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_dataset_sample"));
        }
        if !unique_samples.insert((
            sample.split.clone(),
            sample.event.id.clone(),
            sample.entity_id.clone(),
        )) {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "duplicate_dataset_sample",
            ));
        }
        split_labels
            .entry(sample.split.clone())
            .or_default()
            .insert(sample.label.clone());
        for alias in &mut sample.aliases {
            if !["exact", "alias", "domain", "negative"].contains(&alias.kind.as_str()) {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    "invalid_dataset_alias_kind",
                ));
            }
            alias.value = normalize_watch_value(
                if alias.kind == "domain" || sample.entity_type == "domain" {
                    "domain"
                } else {
                    &sample.entity_type
                },
                &alias.value,
            )?;
        }
    }
    if ["gold", "temporal_holdout"].iter().any(|split| {
        split_labels
            .get(*split)
            .is_none_or(|labels| labels.len() != 2)
    }) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "dataset_requires_both_labels_in_each_split",
        ));
    }
    normalize_dataset_order(&mut samples);
    let entity_ids = samples
        .iter()
        .map(|sample| sample.entity_id.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let entities=state.connection().await?.query("SELECT entity_id::text,entity_type FROM public.taq_entities WHERE tenant_id=$1::uuid AND entity_id::text=ANY($2)",&[&auth.tenant_id,&entity_ids])
        .await.map_err(|error|internal_error("verificar entidades del dataset",error))?;
    let entity_types = entities
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<std::collections::HashMap<_, _>>();
    let watchlist_versions = samples
        .iter()
        .map(|sample| sample.watchlist_version)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let known_versions=state.connection().await?.query("SELECT version FROM public.taq_watchlist_versions WHERE tenant_id=$1::uuid AND version=ANY($2)",&[&auth.tenant_id,&watchlist_versions])
        .await.map_err(|error|internal_error("verificar versiones de watchlist",error))?.into_iter().map(|row|row.get::<_,i64>(0)).collect::<HashSet<_>>();
    if samples.iter().any(|sample| {
        entity_types.get(&sample.entity_id) != Some(&sample.entity_type)
            || !known_versions.contains(&sample.watchlist_version)
    }) {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "dataset_entity_or_watchlist_version_not_found",
        ));
    }
    let manifest_hash = rule_dataset_hash(&samples)?;
    let sample_count = samples.len() as i64;
    let mut db = state.connection().await?;
    let tx = db
        .transaction()
        .await
        .map_err(|error| internal_error("iniciar registro de dataset", error))?;
    let insert_result=tx.query_opt(
        "INSERT INTO public.taq_rule_datasets(tenant_id,dataset_version,format_version,manifest_hash,sample_count,approved_by) \
         VALUES($1::uuid,$2,$3,$4,$5,$6) ON CONFLICT(tenant_id,dataset_version) DO NOTHING RETURNING dataset_version",
        &[&auth.tenant_id,&input.dataset_version,&RULE_DATASET_FORMAT_VERSION,&manifest_hash,&sample_count,&auth.principal],
    ).await;
    let inserted = match insert_result {
        Ok(row) => row.is_some(),
        Err(error) if error.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) => {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "dataset_manifest_already_registered",
            ));
        }
        Err(error) => return Err(internal_error("registrar dataset de evaluación", error)),
    };
    if inserted {
        for (index, sample) in samples.iter().enumerate() {
            let content = serde_json::to_value(sample)
                .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid_dataset_sample"))?;
            tx.execute(
                "INSERT INTO public.taq_rule_dataset_samples(tenant_id,dataset_version,sample_index,split,label,sample) VALUES($1::uuid,$2,$3,$4,$5,$6)",
                &[&auth.tenant_id,&input.dataset_version,&(index as i32),&sample.split,&sample.label,&content],
            ).await.map_err(|error|internal_error("guardar muestra inmutable",error))?;
        }
        tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'rule.dataset.register','rule_dataset',$3,$4)",
            &[&auth.tenant_id,&auth.principal,&input.dataset_version,&json!({"manifest_hash":manifest_hash,"sample_count":sample_count})])
            .await.map_err(|error|internal_error("auditar registro de dataset",error))?;
    } else {
        let existing=tx.query_one("SELECT d.format_version,d.manifest_hash,d.sample_count,(SELECT count(*) FROM public.taq_rule_dataset_samples s WHERE s.tenant_id=d.tenant_id AND s.dataset_version=d.dataset_version) FROM public.taq_rule_datasets d WHERE d.tenant_id=$1::uuid AND d.dataset_version=$2 FOR SHARE",&[&auth.tenant_id,&input.dataset_version])
            .await.map_err(|error|internal_error("leer manifest existente",error))?;
        let old_format: String = existing.get(0);
        let old_hash: String = existing.get(1);
        let old_count: i64 = existing.get(2);
        let stored_count: i64 = existing.get(3);
        if old_hash != manifest_hash || old_count != sample_count {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "dataset_version_is_immutable",
            ));
        }
        if old_format != RULE_DATASET_FORMAT_VERSION || stored_count != old_count {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "dataset_content_missing_register_new_version",
            ));
        }
    }
    let approved_by: String = if inserted {
        auth.principal.clone()
    } else {
        tx.query_one("SELECT approved_by FROM public.taq_rule_datasets WHERE tenant_id=$1::uuid AND dataset_version=$2",&[&auth.tenant_id,&input.dataset_version])
            .await.map_err(|error|internal_error("leer aprobador del dataset",error))?.get(0)
    };
    tx.commit()
        .await
        .map_err(|error| internal_error("confirmar dataset", error))?;
    let status = if inserted {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(
            json!({"dataset_version":input.dataset_version,"manifest_hash":manifest_hash,"sample_count":sample_count,"approved_by":approved_by,"idempotent":!inserted}),
        ),
    ))
}

#[derive(Deserialize)]
struct Guardrails {
    min_precision: f64,
    min_recall: f64,
    max_false_positives_per_1000: f64,
}

async fn set_guardrails(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Json(input): Json<Guardrails>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&auth, "rules:guardrails:write")?;
    if !input.min_precision.is_finite()
        || !input.min_recall.is_finite()
        || !input.max_false_positives_per_1000.is_finite()
        || !(0.0..=1.0).contains(&input.min_precision)
        || !(0.0..=1.0).contains(&input.min_recall)
        || input.max_false_positives_per_1000 < 0.0
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_guardrails"));
    }
    let mut db = state.connection().await?;
    let tx = db
        .transaction()
        .await
        .map_err(|error| internal_error("iniciar guardrails", error))?;
    tx.query_opt(
        "SELECT tenant_id FROM public.taq_tenants WHERE tenant_id=$1::uuid FOR UPDATE",
        &[&auth.tenant_id],
    )
    .await
    .map_err(|error| internal_error("bloquear tenant para guardrails", error))?
    .ok_or(ApiError(StatusCode::UNAUTHORIZED, "unauthorized"))?;
    let version:i64=tx.query_one("SELECT COALESCE(MAX(version),0)+1 FROM public.taq_rule_guardrail_history WHERE tenant_id=$1::uuid",&[&auth.tenant_id]).await
        .map_err(|error|internal_error("calcular versión de guardrails",error))?.get(0);
    tx.execute("INSERT INTO public.taq_rule_guardrails(tenant_id,version,min_precision,min_recall,max_false_positives_per_1000,approved_by) VALUES($1::uuid,$2,$3,$4,$5,$6) ON CONFLICT(tenant_id) DO UPDATE SET version=EXCLUDED.version,min_precision=EXCLUDED.min_precision,min_recall=EXCLUDED.min_recall,max_false_positives_per_1000=EXCLUDED.max_false_positives_per_1000,approved_by=EXCLUDED.approved_by,updated_at=now()",
        &[&auth.tenant_id,&version,&input.min_precision,&input.min_recall,&input.max_false_positives_per_1000,&auth.principal]).await
        .map_err(|error|internal_error("actualizar guardrails",error))?;
    tx.execute("INSERT INTO public.taq_rule_guardrail_history(tenant_id,version,min_precision,min_recall,max_false_positives_per_1000,approved_by) VALUES($1::uuid,$2,$3,$4,$5,$6)",&[&auth.tenant_id,&version,&input.min_precision,&input.min_recall,&input.max_false_positives_per_1000,&auth.principal]).await
        .map_err(|error|internal_error("guardar historia de guardrails",error))?;
    tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'rule.guardrails','guardrails',$3,$4)",&[&auth.tenant_id,&auth.principal,&version.to_string(),&json!({"min_precision":input.min_precision,"min_recall":input.min_recall,"max_false_positives_per_1000":input.max_false_positives_per_1000})]).await
        .map_err(|error|internal_error("auditar guardrails",error))?;
    tx.commit()
        .await
        .map_err(|error| internal_error("confirmar guardrails", error))?;
    Ok(Json(
        json!({"version":version,"approved_by":auth.principal}),
    ))
}

#[derive(Deserialize)]
struct EvaluationRequest {
    version: i64,
    dataset_version: String,
}

async fn evaluate_rule(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(rule_id): Path<String>,
    Json(input): Json<EvaluationRequest>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&auth, "rules:evaluate")?;
    if input.version <= 0
        || input.dataset_version.trim().is_empty()
        || input.dataset_version.len() > 128
    {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "invalid_evaluation_request",
        ));
    }
    let rule=state.connection().await?.query_opt("SELECT definition,state FROM public.taq_rule_versions WHERE tenant_id=$1::uuid AND rule_id=$2 AND version=$3",&[&auth.tenant_id,&rule_id,&input.version])
        .await.map_err(|error|internal_error("leer regla candidata",error))?.ok_or(ApiError(StatusCode::NOT_FOUND,"rule_not_found"))?;
    let definition: Value = rule.get(0);
    let rule_state: String = rule.get(1);
    if !["candidate", "shadow"].contains(&rule_state.as_str()) {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "rule_not_evaluable_in_current_state",
        ));
    }
    let dataset=state.connection().await?.query_opt("SELECT format_version,manifest_hash,sample_count FROM public.taq_rule_datasets WHERE tenant_id=$1::uuid AND dataset_version=$2",&[&auth.tenant_id,&input.dataset_version])
        .await.map_err(|error|internal_error("leer dataset inmutable",error))?.ok_or(ApiError(StatusCode::NOT_FOUND,"dataset_not_found"))?;
    let format_version: String = dataset.get(0);
    if format_version != RULE_DATASET_FORMAT_VERSION {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "dataset_content_missing_register_new_version",
        ));
    }
    let expected_hash: String = dataset.get(1);
    let expected_count: i64 = dataset.get(2);
    let rows=state.connection().await?.query("SELECT sample FROM public.taq_rule_dataset_samples WHERE tenant_id=$1::uuid AND dataset_version=$2 ORDER BY sample_index",&[&auth.tenant_id,&input.dataset_version])
        .await.map_err(|error|internal_error("leer muestras del dataset",error))?;
    if rows.len() as i64 != expected_count {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "dataset_integrity_check_failed",
        ));
    }
    let samples = rows
        .into_iter()
        .map(|row| row.get::<_, Value>(0))
        .map(serde_json::from_value::<DatasetSample>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ApiError(StatusCode::CONFLICT, "dataset_integrity_check_failed"))?;
    let actual_hash = rule_dataset_hash(&samples)
        .map_err(|_| ApiError(StatusCode::CONFLICT, "dataset_integrity_check_failed"))?;
    if actual_hash != expected_hash {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "dataset_integrity_check_failed",
        ));
    }
    let guardrails=state.connection().await?.query_opt("SELECT version,min_precision,min_recall,max_false_positives_per_1000 FROM public.taq_rule_guardrails WHERE tenant_id=$1::uuid",&[&auth.tenant_id])
        .await.map_err(|error|internal_error("leer guardrails vigentes",error))?.ok_or(ApiError(StatusCode::PRECONDITION_FAILED,"guardrails_required"))?;
    let guardrail_version: i64 = guardrails.get(0);
    let min_precision: f64 = guardrails.get(1);
    let min_recall: f64 = guardrails.get(2);
    let max_fp: f64 = guardrails.get(3);
    let mut results = Vec::new();
    let mut metrics_by_split = std::collections::BTreeMap::<String, Value>::new();
    for split in ["gold", "temporal_holdout"] {
        let metrics = calculate_rule_metrics(&definition, &samples, split);
        metrics_by_split.insert(split.to_owned(), metrics.clone());
        let precision = metrics["precision"].as_f64().unwrap_or(0.0);
        let recall = metrics["recall"].as_f64().unwrap_or(0.0);
        let false_positives_per_1000 = metrics["false_positives_per_1000"]
            .as_f64()
            .unwrap_or(f64::INFINITY);
        let passed = precision >= min_precision
            && recall >= min_recall
            && false_positives_per_1000 <= max_fp;
        let mut connection = state.connection().await?;
        let tx = connection
            .transaction()
            .await
            .map_err(|error| internal_error("iniciar persistencia de evaluación", error))?;
        tx.execute("INSERT INTO public.taq_rule_evaluations(tenant_id,candidate_rule_id,candidate_version,dataset_version,split,metrics,passed,guardrail_version,evaluated_by,evaluation_method) VALUES($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,'taq_runner_v1') ON CONFLICT DO NOTHING",
            &[&auth.tenant_id,&rule_id,&input.version,&input.dataset_version,&split,&metrics,&passed,&guardrail_version,&auth.principal])
            .await.map_err(|error|internal_error("guardar evaluación reproducible",error))?;
        tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'rule.evaluate','rule',$3,$4)",&[&auth.tenant_id,&auth.principal,&rule_id,&json!({"version":input.version,"dataset_version":input.dataset_version,"split":split,"guardrail_version":guardrail_version,"passed":passed})])
            .await.map_err(|error|internal_error("auditar evaluación reproducible",error))?;
        tx.commit()
            .await
            .map_err(|error| internal_error("confirmar evaluación reproducible", error))?;
        results.push(json!({"split":split,"metrics":metrics,"passed":passed,"guardrail_version":guardrail_version}));
    }
    let gold = &metrics_by_split["gold"];
    let holdout = &metrics_by_split["temporal_holdout"];
    let drift = json!({
        "precision_delta":holdout["precision"].as_f64().unwrap_or(0.0)-gold["precision"].as_f64().unwrap_or(0.0),
        "recall_delta":holdout["recall"].as_f64().unwrap_or(0.0)-gold["recall"].as_f64().unwrap_or(0.0),
        "false_positives_per_1000_delta":holdout["false_positives_per_1000"].as_f64().unwrap_or(0.0)-gold["false_positives_per_1000"].as_f64().unwrap_or(0.0),
        "confusion_counts_delta":{"tp":holdout["tp"].as_i64().unwrap_or(0)-gold["tp"].as_i64().unwrap_or(0),"fp":holdout["fp"].as_i64().unwrap_or(0)-gold["fp"].as_i64().unwrap_or(0),"fn":holdout["fn"].as_i64().unwrap_or(0)-gold["fn"].as_i64().unwrap_or(0),"tn":holdout["tn"].as_i64().unwrap_or(0)-gold["tn"].as_i64().unwrap_or(0)}
    });
    Ok(Json(
        json!({"rule_id":rule_id,"version":input.version,"dataset_version":input.dataset_version,"manifest_hash":expected_hash,"evaluation_method":"taq_runner_v1","results":results,"temporal_drift":drift}),
    ))
}

#[derive(Deserialize)]
struct Promotion {
    version: i64,
    target: String,
}

async fn promote_rule(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(rule_id): Path<String>,
    Json(input): Json<Promotion>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&auth, "rules:promote")?;
    if !["shadow", "active", "rejected", "retired"].contains(&input.target.as_str()) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_rule_state"));
    }
    let mut db = state.connection().await?;
    let tx = db
        .transaction()
        .await
        .map_err(|error| internal_error("iniciar promoción de regla", error))?;
    let row=tx.query_opt("SELECT state FROM public.taq_rule_versions WHERE tenant_id=$1::uuid AND rule_id=$2 AND version=$3 FOR UPDATE",&[&auth.tenant_id,&rule_id,&input.version])
        .await.map_err(|error|internal_error("leer regla a promover",error))?.ok_or(ApiError(StatusCode::NOT_FOUND,"rule_not_found"))?;
    let current: String = row.get(0);
    let allowed = matches!(
        (current.as_str(), input.target.as_str()),
        ("candidate", "shadow")
            | ("shadow", "active")
            | ("retired", "active")
            | ("candidate", "rejected")
            | ("shadow", "retired")
            | ("active", "retired")
    );
    if !allowed {
        return Err(ApiError(StatusCode::CONFLICT, "invalid_rule_transition"));
    }
    if input.target == "shadow" || input.target == "active" {
        let ready:bool=tx.query_one(
            "SELECT EXISTS(SELECT 1 FROM public.taq_rule_evaluations e JOIN public.taq_rule_guardrails g ON g.tenant_id=e.tenant_id AND g.version=e.guardrail_version \
             WHERE e.tenant_id=$1::uuid AND e.candidate_rule_id=$2 AND e.candidate_version=$3 AND e.passed AND e.evaluation_method='taq_runner_v1' \
             GROUP BY e.dataset_version,e.guardrail_version HAVING count(DISTINCT e.split)=2)",
            &[&auth.tenant_id,&rule_id,&input.version],
        ).await.map_err(|error|internal_error("comprobar evidencia de evaluación",error))?.get(0);
        if !ready {
            return Err(ApiError(
                StatusCode::PRECONDITION_FAILED,
                "current_gold_and_temporal_holdout_pass_required",
            ));
        }
    }
    if input.target == "active" {
        tx.execute("UPDATE public.taq_rule_versions SET state='retired' WHERE tenant_id=$1::uuid AND rule_id=$2 AND state='active'",&[&auth.tenant_id,&rule_id])
            .await.map_err(|error|internal_error("retirar versión activa anterior",error))?;
    }
    tx.execute("UPDATE public.taq_rule_versions SET state=$4 WHERE tenant_id=$1::uuid AND rule_id=$2 AND version=$3",&[&auth.tenant_id,&rule_id,&input.version,&input.target])
        .await.map_err(|error|internal_error("actualizar estado de regla",error))?;
    tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'rule.promote','rule',$3,$4)",&[&auth.tenant_id,&auth.principal,&rule_id,&json!({"version":input.version,"from":current,"to":input.target})])
        .await.map_err(|error|internal_error("auditar promoción",error))?;
    tx.commit()
        .await
        .map_err(|error| internal_error("confirmar promoción", error))?;
    Ok(Json(
        json!({"rule_id":rule_id,"version":input.version,"state":input.target}),
    ))
}

#[derive(Deserialize)]
struct ActionPolicy {
    policy_id: String,
    definition: Value,
}

async fn create_action_policy(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Json(input): Json<ActionPolicy>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_scope(&auth, "actions:policy:write")?;
    if input.policy_id.trim().is_empty()
        || input.policy_id.len() > 128
        || !input.definition.is_object()
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_action_policy"));
    }
    let mut db = state.connection().await?;
    let tx = db
        .transaction()
        .await
        .map_err(|error| internal_error("iniciar política", error))?;
    tx.query_opt(
        "SELECT tenant_id FROM public.taq_tenants WHERE tenant_id=$1::uuid FOR UPDATE",
        &[&auth.tenant_id],
    )
    .await
    .map_err(|error| internal_error("bloquear tenant para política", error))?
    .ok_or(ApiError(StatusCode::UNAUTHORIZED, "unauthorized"))?;
    let version:i64=tx.query_one("SELECT COALESCE(MAX(version),0)+1 FROM public.taq_action_policies WHERE tenant_id=$1::uuid AND policy_id=$2",&[&auth.tenant_id,&input.policy_id]).await
        .map_err(|error|internal_error("calcular versión de política",error))?.get(0);
    tx.execute("UPDATE public.taq_action_policies SET active=FALSE WHERE tenant_id=$1::uuid AND policy_id=$2 AND active",&[&auth.tenant_id,&input.policy_id]).await
        .map_err(|error|internal_error("desactivar política previa",error))?;
    tx.execute("INSERT INTO public.taq_action_policies(tenant_id,policy_id,version,definition,active,created_by) VALUES($1::uuid,$2,$3,$4,TRUE,$5)",&[&auth.tenant_id,&input.policy_id,&version,&input.definition,&auth.principal]).await
        .map_err(|error|internal_error("crear política",error))?;
    tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'action.policy.create','action_policy',$3,$4)",&[&auth.tenant_id,&auth.principal,&input.policy_id,&json!({"version":version,"external_execution":false})]).await
        .map_err(|error|internal_error("auditar política",error))?;
    tx.commit()
        .await
        .map_err(|error| internal_error("confirmar política", error))?;
    Ok((
        StatusCode::CREATED,
        Json(
            json!({"policy_id":input.policy_id,"version":version,"active":true,"external_execution":false}),
        ),
    ))
}

#[derive(Deserialize)]
struct Decision {
    decision: String,
}

#[derive(Deserialize)]
struct CreateAction {
    policy_id: String,
    idempotency_key: String,
    expires_in_seconds: Option<i64>,
}

async fn create_action(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(finding_id): Path<String>,
    Json(input): Json<CreateAction>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    require_scope(&auth, "actions:request")?;
    if !valid_uuid(&finding_id) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_finding_id"));
    }
    let ttl = input.expires_in_seconds.unwrap_or(86_400);
    if input.idempotency_key.is_empty()
        || input.idempotency_key.len() > 256
        || !(60..=604_800).contains(&ttl)
    {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_action_request"));
    }
    let mut db = state.connection().await?;
    let tx = db
        .transaction()
        .await
        .map_err(|error| internal_error("iniciar solicitud de acción", error))?;
    let material=serde_json::to_vec(&json!({"finding_id":finding_id,"policy_id":input.policy_id,"expires_in_seconds":ttl,"requested_by":auth.principal}))
        .map_err(|_|ApiError(StatusCode::BAD_REQUEST,"invalid_action_request"))?;
    let request_hash = blake3::hash(&material);
    let existing=tx.query_opt("SELECT action_id::text,state,expires_at,request_hash FROM public.taq_actions WHERE tenant_id=$1::uuid AND idempotency_key=$2",&[&auth.tenant_id,&input.idempotency_key])
        .await.map_err(|error|internal_error("consultar acción idempotente",error))?;
    let (action_id, status, expires_at, was_inserted) = if let Some(row) = existing {
        let stored: Vec<u8> = row.get(3);
        if stored.as_slice() != request_hash.as_bytes() {
            return Err(ApiError(StatusCode::CONFLICT, "idempotency_key_conflict"));
        }
        (
            row.get::<_, String>(0),
            row.get::<_, String>(1),
            row.get::<_, std::time::SystemTime>(2),
            false,
        )
    } else {
        let row = tx.query_opt(
        "INSERT INTO public.taq_actions(tenant_id,action_id,finding_id,policy_id,policy_version,idempotency_key,request_hash,requested_by,expires_at) \
         SELECT $1::uuid,gen_random_uuid(),f.finding_id,p.policy_id,p.version,$4,$5,$6,now()+($7 * interval '1 second') \
         FROM public.taq_findings f JOIN public.taq_action_policies p ON p.tenant_id=f.tenant_id \
           AND p.policy_id=$3 AND p.active \
         WHERE f.tenant_id=$1::uuid AND f.finding_id=$2::uuid \
         ON CONFLICT (tenant_id,idempotency_key) DO NOTHING \
         RETURNING action_id::text,state,expires_at",
        &[&auth.tenant_id,&finding_id,&input.policy_id,&input.idempotency_key,&&request_hash.as_bytes()[..],&auth.principal,&ttl],
      ).await.map_err(|error| internal_error("solicitar acción", error))?;
        let (action_id, status, expires_at, was_inserted) = if let Some(row) = row {
            (
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, std::time::SystemTime>(2),
                true,
            )
        } else {
            let row=tx.query_opt("SELECT action_id::text,state,expires_at,request_hash FROM public.taq_actions WHERE tenant_id=$1::uuid AND idempotency_key=$2",&[&auth.tenant_id,&input.idempotency_key])
              .await.map_err(|error|internal_error("resolver acción idempotente concurrente",error))?
              .ok_or(ApiError(StatusCode::UNPROCESSABLE_ENTITY,"finding_or_active_policy_not_found"))?;
            let stored: Vec<u8> = row.get(3);
            if stored.as_slice() != request_hash.as_bytes() {
                return Err(ApiError(StatusCode::CONFLICT, "idempotency_key_conflict"));
            }
            (
                row.get::<_, String>(0),
                row.get::<_, String>(1),
                row.get::<_, std::time::SystemTime>(2),
                false,
            )
        };
        (action_id, status, expires_at, was_inserted)
    };
    if was_inserted {
        tx.execute(
            "INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'action.request','action',$3,$4)",
            &[&auth.tenant_id,&auth.principal,&action_id,&json!({"policy_id":input.policy_id})],
        ).await.map_err(|error| internal_error("auditar solicitud de acción", error))?;
    }
    tx.commit()
        .await
        .map_err(|error| internal_error("confirmar solicitud de acción", error))?;
    let expires_at = DateTime::<Utc>::from(expires_at).to_rfc3339();
    Ok((
        if was_inserted {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(
            json!({"action_id":action_id,"state":status,"expires_at":expires_at,"requires_approval":true,"idempotent":!was_inserted}),
        ),
    ))
}

async fn decide_action(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(action_id): Path<String>,
    Json(input): Json<Decision>,
) -> Result<Json<Value>, ApiError> {
    require_scope(&auth, "actions:approve")?;
    if !valid_uuid(&action_id) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_action_id"));
    }
    if !["approve", "reject"].contains(&input.decision.as_str()) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_decision"));
    }
    let mut db = state.connection().await?;
    let tx = db
        .transaction()
        .await
        .map_err(|error| internal_error("iniciar decisión", error))?;
    let row = tx.query_opt("SELECT state, expires_at > now(), expires_at, requested_by, finding_id::text, policy_id, policy_version FROM public.taq_actions WHERE tenant_id=$1::uuid AND action_id=$2::uuid FOR UPDATE", &[&auth.tenant_id,&action_id])
        .await.map_err(|error| internal_error("leer acción", error))?.ok_or(ApiError(StatusCode::NOT_FOUND,"action_not_found"))?;
    let current: String = row.get(0);
    let unexpired: bool = row.get(1);
    let expires_at: std::time::SystemTime = row.get(2);
    let requested_by: String = row.get(3);
    let finding_id: String = row.get(4);
    let policy_id: String = row.get(5);
    let policy_version: i64 = row.get(6);
    if current == "pending" && !unexpired {
        tx.execute("UPDATE public.taq_actions SET state='expired',updated_at=now() WHERE tenant_id=$1::uuid AND action_id=$2::uuid",&[&auth.tenant_id,&action_id])
            .await.map_err(|error|internal_error("expirar acción vencida",error))?;
        tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'action.expired','action',$3,'{}'::jsonb)",&[&auth.tenant_id,&auth.principal,&action_id])
            .await.map_err(|error|internal_error("auditar expiración",error))?;
        tx.commit()
            .await
            .map_err(|error| internal_error("confirmar expiración", error))?;
        return Err(ApiError(
            StatusCode::CONFLICT,
            "action_not_pending_or_expired",
        ));
    }
    if current != "pending" {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "action_not_pending_or_expired",
        ));
    }
    if requested_by == auth.principal {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "requester_cannot_approve_own_action",
        ));
    }
    let next = if input.decision == "approve" {
        "approved"
    } else {
        "rejected"
    };
    tx.execute("UPDATE public.taq_actions SET state=$3,approved_by=$4,updated_at=now() WHERE tenant_id=$1::uuid AND action_id=$2::uuid", &[&auth.tenant_id,&action_id,&next,&auth.principal])
        .await.map_err(|error| internal_error("actualizar acción", error))?;
    if next == "approved" {
        let payload = json!({"tenant_id":auth.tenant_id,"action_id":action_id,"finding_id":finding_id,"policy_id":policy_id,"policy_version":policy_version,"approved_by":auth.principal,"expires_at":DateTime::<Utc>::from(expires_at).to_rfc3339(),"idempotency_key":action_id});
        tx.execute("INSERT INTO public.taq_action_outbox(tenant_id,action_id,payload) VALUES($1::uuid,$2::uuid,$3) ON CONFLICT DO NOTHING", &[&auth.tenant_id,&action_id,&payload])
            .await.map_err(|error| internal_error("crear outbox de acción", error))?;
    }
    tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,$3,'action',$4,$5)", &[&auth.tenant_id,&auth.principal,&format!("action.{next}"),&action_id,&json!({"external_execution":false})])
        .await.map_err(|error| internal_error("auditar decisión", error))?;
    tx.commit()
        .await
        .map_err(|error| internal_error("confirmar decisión", error))?;
    Ok(Json(
        json!({"action_id":action_id,"state":next,"external_execution":false}),
    ))
}

#[derive(Deserialize)]
struct ActionFilter {
    state: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

async fn list_actions(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Query(filter): Query<ActionFilter>,
) -> Result<Json<Vec<Value>>, ApiError> {
    require_scope(&auth, "actions:read")?;
    let limit = filter.limit.unwrap_or(50).clamp(1, 100);
    let offset = filter.offset.unwrap_or(0).max(0);
    if filter.state.as_deref().is_some_and(|value| {
        ![
            "pending",
            "approved",
            "rejected",
            "expired",
            "queued",
            "completed",
            "failed",
        ]
        .contains(&value)
    }) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_action_state"));
    }
    let rows=state.connection().await?.query(
        "SELECT jsonb_build_object('action_id',action_id,'finding_id',finding_id,'policy_id',policy_id,'policy_version',policy_version,'state',CASE WHEN state='pending' AND expires_at<=now() THEN 'expired' ELSE state END,'requested_by',requested_by,'approved_by',approved_by,'expires_at',expires_at,'created_at',created_at) \
         FROM public.taq_actions WHERE tenant_id=$1::uuid AND ($2::text IS NULL OR (CASE WHEN state='pending' AND expires_at<=now() THEN 'expired' ELSE state END)=$2) \
         ORDER BY created_at DESC LIMIT $3 OFFSET $4",
        &[&auth.tenant_id,&filter.state,&limit,&offset],
    ).await.map_err(|error|internal_error("listar acciones",error))?;
    Ok(Json(rows.into_iter().map(|row| row.get(0)).collect()))
}

async fn revoke_api_key(
    State(state): State<AppState>,
    Extension(auth): Extension<Auth>,
    Path(key_id): Path<String>,
) -> Result<StatusCode, ApiError> {
    require_scope(&auth, "api_keys:admin")?;
    if !valid_uuid(&key_id) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid_key_id"));
    }
    let mut db = state.connection().await?;
    let tx = db
        .transaction()
        .await
        .map_err(|error| internal_error("iniciar revocación de API key", error))?;
    let count=tx.execute("UPDATE public.taq_api_keys SET active=FALSE WHERE tenant_id=$1::uuid AND key_id=$2::uuid",&[&auth.tenant_id,&key_id])
        .await.map_err(|error|internal_error("revocar API key",error))?;
    if count == 0 {
        return Err(ApiError(StatusCode::NOT_FOUND, "api_key_not_found"));
    }
    tx.execute("INSERT INTO public.taq_audit_log(tenant_id,actor,operation,object_type,object_id,details) VALUES($1::uuid,$2,'api_key.revoke','api_key',$3,'{}'::jsonb)",&[&auth.tenant_id,&auth.principal,&key_id])
        .await.map_err(|error|internal_error("auditar revocación",error))?;
    tx.commit()
        .await
        .map_err(|error| internal_error("confirmar revocación", error))?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod rule_dataset_tests {
    use super::*;
    use taq_core::TimestampQuality;

    fn sample(
        id: &str,
        split: &str,
        label: &str,
        entity_type: &str,
        payload: Value,
        aliases: &[(&str, &str)],
    ) -> DatasetSample {
        DatasetSample {
            split: split.to_owned(),
            label: label.to_owned(),
            event: Event {
                schema_version: 2,
                id: id.to_owned(),
                source: "fixture".to_owned(),
                kind: "mention".to_owned(),
                entity_ids: Vec::new(),
                occurred_at_ms: 1,
                occurred_at_quality: TimestampQuality::Source,
                ingested_at_ms: 2,
                payload,
            },
            watchlist_version: 1,
            entity_id: "11111111-1111-4111-8111-111111111111".to_owned(),
            entity_type: entity_type.to_owned(),
            aliases: aliases
                .iter()
                .map(|(kind, value)| DatasetAlias {
                    kind: (*kind).to_owned(),
                    value: (*value).to_owned(),
                })
                .collect(),
        }
    }

    #[test]
    fn normalized_manifest_hash_is_independent_of_sample_and_alias_order() {
        let mut first = vec![
            sample(
                "event-b",
                "gold",
                "alertable",
                "brand",
                json!({"title":"Acme"}),
                &[("alias", "ACME Inc"), ("exact", "Acme")],
            ),
            sample(
                "event-a",
                "temporal_holdout",
                "not_alertable",
                "brand",
                json!({"title":"Other"}),
                &[("exact", "Acme")],
            ),
        ];
        let mut reordered = first.iter().cloned().rev().collect::<Vec<_>>();
        reordered[1].aliases.reverse();
        normalize_dataset_order(&mut first);
        normalize_dataset_order(&mut reordered);

        assert_eq!(
            rule_dataset_hash(&first).unwrap(),
            rule_dataset_hash(&reordered).unwrap()
        );
    }

    #[test]
    fn metrics_apply_positive_and_negative_aliases_and_count_confusion_matrix() {
        let samples = vec![
            sample(
                "tp",
                "gold",
                "alertable",
                "brand",
                json!({"title":"Acme announces a launch"}),
                &[("exact", "acme")],
            ),
            sample(
                "tn-negative",
                "gold",
                "not_alertable",
                "brand",
                json!({"title":"Acme parody account"}),
                &[("exact", "acme"), ("negative", "parody")],
            ),
            sample(
                "tn-unmatched",
                "gold",
                "not_alertable",
                "brand",
                json!({"title":"Unrelated story"}),
                &[("exact", "acme")],
            ),
            sample(
                "fn",
                "gold",
                "alertable",
                "brand",
                json!({"title":"Unmatched story"}),
                &[("exact", "acme")],
            ),
        ];
        let metrics = calculate_rule_metrics(&json!({"match_kind":"canonical"}), &samples, "gold");
        assert_eq!(metrics["tp"], 1);
        assert_eq!(metrics["fp"], 0);
        assert_eq!(metrics["fn"], 1);
        assert_eq!(metrics["tn"], 2);
        assert_eq!(metrics["precision"], 1.0);
        assert_eq!(metrics["recall"], 0.5);
    }

    #[test]
    fn domain_match_accepts_subdomains_of_the_watched_domain() {
        let samples = vec![sample(
            "domain-match",
            "gold",
            "alertable",
            "domain",
            json!({"url":"https://news.example.com/story"}),
            &[("domain", "example.com")],
        )];
        let metrics = calculate_rule_metrics(&json!({"match_kind":"domain"}), &samples, "gold");
        assert_eq!(metrics["tp"], 1);
        assert_eq!(metrics["fn"], 0);
    }
}
