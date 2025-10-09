# syntax=docker/dockerfile:1.7

FROM alpine/git:latest AS gh
RUN apk add --no-cache openssh-client
RUN --mount=type=ssh \
    mkdir -p /root/.ssh && \
    ssh-keyscan -t rsa,ed25519 github.com >> /root/.ssh/known_hosts && \
    git clone --depth 1 https://github.com/DeepanshKhurana/supabaseR.git /tmp/supabaseR && \
    git clone --depth 1 https://github.com/DeepanshKhurana/ical.git /tmp/ical && \
    git clone --branch feat/ssl-friendly-postgres --depth 1 https://github.com/DeepanshKhurana/faucet.git /tmp/faucet

FROM rocker/r-ver:latest AS app
ENV RENV_DOWNLOAD_METHOD=libcurl \
    RENV_CONFIG_REPOS_OVERRIDE=https://cloud.r-project.org \
    DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Kolkata

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git build-essential pkg-config \
    libssl-dev libcurl4-gnutls-dev libxml2-dev libpq-dev \
    libv8-dev libsodium-dev libuv1-dev zlib1g-dev \
    cmake libgit2-dev && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/local/Hrafnagud-Dynamo
COPY . /usr/local/Hrafnagud-Dynamo
COPY --from=gh /tmp/supabaseR /tmp/supabaseR
COPY --from=gh /tmp/ical /tmp/ical
COPY --from=gh /tmp/faucet /tmp/faucet

WORKDIR /usr/local/Hrafnagud-Dynamo

RUN R -e "options(repos=Sys.getenv('RENV_CONFIG_REPOS_OVERRIDE')); install.packages(c('renv','remotes'))"
RUN R -e "source('.Rprofile'); renv::restore(prompt = FALSE)"
RUN R -e "remotes::install_local('/tmp/supabaseR', dependencies=TRUE, upgrade='never')"
RUN R -e "remotes::install_local('/tmp/ical', dependencies=TRUE, upgrade='never')"

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH=/root/.cargo/bin:${PATH}
RUN . $HOME/.cargo/env && cargo install --path /tmp/faucet --locked && \
    install -m 0755 /root/.cargo/bin/faucet /usr/local/bin/faucet && \
    rm -rf /tmp/supabaseR /tmp/ical /tmp/faucet

EXPOSE 8008
CMD ["faucet","--host","0.0.0.0:8008","start","--dir","."]
