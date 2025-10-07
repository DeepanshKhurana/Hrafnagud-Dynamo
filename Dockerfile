# syntax=docker/dockerfile:1.7

FROM rocker/r-ver AS build

ENV TZ=Asia/Kolkata

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    libcurl4-gnutls-dev \
    libxml2-dev \
    libpq-dev \
    libv8-dev \
    libsodium-dev \
    libuv1-dev \
    zlib1g-dev \
    git && \
    rm -rf /var/lib/apt/lists/*

RUN R -q -e "install.packages('renv', repos='https://cloud.r-project.org')"

WORKDIR /usr/local/Hrafnagud-Dynamo
COPY . /usr/local/Hrafnagud-Dynamo

RUN R -q -e "source('.Rprofile')"

RUN R -q -e "renv::restore(prompt = FALSE)"

RUN R -q -e "install.packages('remotes', repos='https://cloud.r-project.org')"

RUN git clone https://github.com/DeepanshKhurana/supabaseR.git /tmp/supabaseR && \
    git clone https://github.com/DeepanshKhurana/ical.git /tmp/ical

RUN R -q -e "remotes::install_local('/tmp/supabaseR', upgrade = 'never'); remotes::install_local('/tmp/ical', upgrade = 'never')"

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH=/root/.cargo/bin:${PATH}

RUN . $HOME/.cargo/env; rustc --version && cargo --version

RUN git clone https://github.com/DeepanshKhurana/faucet.git /tmp/faucet && \
    cd /tmp/faucet && git checkout feat/ssl-friendly-postgres && \
    cargo install --path . && install -m 0755 /root/.cargo/bin/faucet /usr/local/bin/faucet



FROM alpine/git:latest AS creds

RUN apk add --no-cache openssh-client

RUN mkdir -p /root/.ssh && \
    ssh-keyscan -t rsa,ed25519 github.com >> /root/.ssh/known_hosts

RUN --mount=type=ssh \
    git clone git@github.com:DeepanshKhurana/Hrafnagud-Creds.git /tmp/Hrafnagud-Creds



FROM build

COPY --from=creds /tmp/Hrafnagud-Creds /tmp/Hrafnagud-Creds

RUN mkdir -p /usr/local/Hrafnagud-Dynamo && \
    cp /tmp/Hrafnagud-Creds/creds.txt /usr/local/Hrafnagud-Dynamo/.Renviron && \
    cp /tmp/Hrafnagud-Creds/ebenezer_service_account.json /usr/local/Hrafnagud-Dynamo/.service_account && \
    cp /tmp/Hrafnagud-Creds/supabase.crt /usr/local/Hrafnagud-Dynamo/supabase.crt && \
    printf '\n' >> /usr/local/Hrafnagud-Dynamo/.Renviron && \
    cat /tmp/Hrafnagud-Creds/api.txt >> /usr/local/Hrafnagud-Dynamo/.Renviron && \
    rm -rf /tmp/Hrafnagud-Creds /tmp/supabaseR /tmp/ical /tmp/faucet

EXPOSE 8008

CMD ["faucet","--host","0.0.0.0:8008","start","--dir","."]
