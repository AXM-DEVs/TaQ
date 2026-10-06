use std::env;

use serde_json::{Value, json};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let base = env::var("TAQ_API_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let key = env::var("TAQ_API_KEY").map_err(|_| "define TAQ_API_KEY en el entorno")?;
    let client = reqwest::Client::new();
    let (method, path, body): (&str, String, Option<Value>) = match args.as_slice() {
        [group, command] if group == "watchlist" && command == "list" => {
            ("GET", "/v1/watchlist/entities".into(), None)
        }
        [group, command] if group == "findings" && command == "list" => {
            ("GET", "/v1/findings".into(), None)
        }
        [
            group,
            command,
            entity_type,
            name,
            reason,
            aliases,
            idempotency_key,
        ] if group == "watchlist" && command == "add" => (
            "POST",
            "/v1/watchlist/entities".into(),
            Some(json!({
                "idempotency_key":idempotency_key,
                "entity_type":entity_type,
                "canonical_name":name,
                "aliases":aliases.split(',').filter(|value| !value.is_empty()).collect::<Vec<_>>(),
                "reason":reason
            })),
        ),
        [group, command, finding_id, policy_id, idempotency_key]
            if group == "action" && command == "request" =>
        {
            (
                "POST",
                format!("/v1/findings/{finding_id}/actions"),
                Some(json!({"policy_id":policy_id,"idempotency_key":idempotency_key})),
            )
        }
        [group, command, action_id, decision] if group == "action" && command == "decide" => (
            "POST",
            format!("/v1/actions/{action_id}/decision"),
            Some(json!({"decision":decision})),
        ),
        [group, finding_id, label, reason, idempotency_key] if group == "feedback" => (
            "POST",
            format!("/v1/findings/{finding_id}/feedback"),
            Some(json!({"idempotency_key":idempotency_key,"label":label,"reason":reason})),
        ),
        [group, method, path, body]
            if group == "api"
                && ["GET", "POST", "DELETE"].contains(&method.as_str())
                && (path.starts_with("/v1/") || path.starts_with("/health/"))
                && !path.contains("..") =>
        {
            (
                method,
                path.clone(),
                if method == "GET" || method == "DELETE" {
                    None
                } else {
                    Some(serde_json::from_str(body)?)
                },
            )
        }
        _ => {
            eprintln!(
                "uso:\n  taqctl watchlist list\n  taqctl watchlist add <brand|domain|organization> <name> <reason> <alias1,alias2> <idempotency_key>\n  taqctl findings list\n  taqctl action request <finding_id> <policy_id> <idempotency_key>\n  taqctl action decide <action_id> <approve|reject>\n  taqctl feedback <finding_id> <correct|false_positive|missed|needs_review> <reason> <idempotency_key>\n  taqctl api <GET|POST|DELETE> </v1/...> [json]"
            );
            std::process::exit(2);
        }
    };
    let mut request = match method {
        "GET" => client.get(format!("{base}{path}")),
        "DELETE" => client.delete(format!("{base}{path}")),
        _ => client.post(format!("{base}{path}")),
    }
    .bearer_auth(key);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    println!("{text}");
    if !status.is_success() {
        std::process::exit(1);
    }
    Ok(())
}
