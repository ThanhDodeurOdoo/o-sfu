FROM rust:1.97.0-bookworm@sha256:8fa55b2f3ddf97471ab6a767bfa3f37e6bad0986ba823e75fea57e2a2a5c3073 AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tests ./tests
COPY src ./src

RUN cargo build --release --locked -p o-sfu --bin o-sfu

FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241 AS runtime

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

RUN useradd --system --create-home --home-dir /srv/o-sfu --shell /usr/sbin/nologin osfu

WORKDIR /srv/o-sfu

COPY --from=builder /app/target/release/o-sfu /usr/local/bin/o-sfu

ENV BIND_ADDRESS=0.0.0.0:8070
ENV PROXY=false
ENV RUST_LOG=info

EXPOSE 8070
EXPOSE 40000-49999/udp

USER osfu

CMD ["o-sfu"]
