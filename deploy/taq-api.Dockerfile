FROM rust:1.98.1-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends cmake pkg-config build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --locked --release --bins -p taq-api

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /src/target/release/taq-api /usr/local/bin/taq-api
COPY --from=build /src/target/release/taqctl /usr/local/bin/taqctl
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/taq-api"]
