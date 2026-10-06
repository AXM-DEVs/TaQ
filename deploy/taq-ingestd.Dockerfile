FROM rust:1.98.1-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends cmake pkg-config build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --locked --release -p taq-ingestd -p taq-dlq-replay

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/taq-ingestd /usr/local/bin/taq-ingestd
COPY --from=build /src/target/release/taq-dlq-replay /usr/local/bin/taq-dlq-replay
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/taq-ingestd"]
