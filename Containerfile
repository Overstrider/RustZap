FROM docker.io/library/rust:1 AS chef
WORKDIR /app

FROM chef AS builder
COPY Cargo.toml Cargo.lock* rust-toolchain.toml ./
COPY src ./src
COPY migrations ./migrations
RUN cargo build --release --features external-integrations

FROM docker.io/library/debian:trixie-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /nonexistent --shell /usr/sbin/nologin rustzap \
    && mkdir -p /data/rustzap /data/rustzap/wa-sessions /var/log/rustzap \
    && chown -R rustzap:rustzap /data/rustzap /var/log/rustzap
WORKDIR /app
COPY --from=builder /app/target/release/rustzap /usr/local/bin/rustzap
COPY migrations ./migrations
USER rustzap
EXPOSE 8167
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8167/ready || exit 1
CMD ["rustzap"]
