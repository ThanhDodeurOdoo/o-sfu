FROM rust:1.92.0-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY cluster ./cluster
COPY model ./model
COPY core ./core
COPY protocol ./protocol
COPY router ./router
COPY rfc ./rfc
COPY telemetry ./telemetry
COPY tests ./tests
COPY src ./src

RUN cargo build --release --locked -p o-sfu

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --home-dir /srv/o-sfu --shell /usr/sbin/nologin osfu

WORKDIR /srv/o-sfu

COPY --from=builder /app/target/release/o-sfu /usr/local/bin/o-sfu
COPY --from=builder /app/target/release/o-sfu-control-plane /usr/local/bin/o-sfu-control-plane

ENV BIND_ADDRESS=0.0.0.0:8070
ENV CONTROL_PLANE_BIND_ADDRESS=0.0.0.0:8071
ENV PROXY=false
ENV RUST_LOG=info

EXPOSE 8070
EXPOSE 8071
EXPOSE 40000-49999/udp

USER osfu

CMD ["o-sfu"]
