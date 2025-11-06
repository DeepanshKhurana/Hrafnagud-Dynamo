# syntax=docker/dockerfile:1.7

FROM rocker/r-ver:latest

ENV DEBIAN_FRONTEND=noninteractive TZ=Asia/Kolkata

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git build-essential pkg-config \
    libssl-dev libcurl4-gnutls-dev libxml2-dev libpq-dev \
    libv8-dev libsodium-dev libuv1-dev zlib1g-dev cmake libgit2-dev \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /tmp/faucet.tar.gz \
      https://codeload.github.com/ixpantia/faucet/tar.gz/main \
 && tar -xzf /tmp/faucet.tar.gz -C /tmp \
 && curl -fsSL https://sh.rustup.rs | sh -s -- -y \
 && . "$HOME/.cargo/env" \
 && cd /tmp/faucet-* \
 && cargo install --path . --locked \
 && install -m 0755 "$HOME/.cargo/bin/faucet" /usr/local/bin/faucet \
 && rm -rf /tmp/faucet.tar.gz /tmp/faucet-* "$HOME/.cargo"

RUN mkdir -p /usr/local/Hrafnagud-Dynamo
COPY . /usr/local/Hrafnagud-Dynamo
WORKDIR /usr/local/Hrafnagud-Dynamo

RUN R -e "renv::restore()"

EXPOSE 8008
CMD ["faucet","--host","0.0.0.0:8008","start","--dir","."]
