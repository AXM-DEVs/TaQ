set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]
set dotenv-load

compose := "docker compose --env-file .env -f deploy/docker-compose.yml"

up:
    {{compose}} up -d --wait

down:
    {{compose}} down

loadgen rate duration:
    cargo run --release -p taq-loadgen -- --rate {{rate}} --duration {{duration}} --entities 100 --duplicate-ratio 0

verify:
    {{compose}} exec -T clickhouse clickhouse-client --user="$env:CLICKHOUSE_USER" --password="$env:CLICKHOUSE_PASSWORD" --query "SELECT count() AS count, uniqExact(id) AS unique_ids, quantileExact(0.99)(toUnixTimestamp64Milli(inserted_at) - ingested_at_ms) AS p99_ingest_ms FROM taq.events FORMAT PrettyCompact"

test:
    cargo test --workspace

lint:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
