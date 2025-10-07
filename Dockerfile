# syntax=docker/dockerfile:1.7
FROM rocker/r-ver AS api

RUN R -e "install.packages('renv', repos='https://cloud.r-project.org')"
ENV RENV_PATHS_CACHE=/renv/cache
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
    git

COPY . /usr/local/Hrafnagud-Dynamo/
WORKDIR /usr/local/Hrafnagud-Dynamo/

RUN R -e "source('.Rprofile')"
RUN R -e "renv::install('git::git@github.com:petermeissner/ical.git')"
RUN R -e "renv::install('git::git@github.com:DeepanshKhurana/supabaseR.git')"
RUN --mount=type=cache,target=/renv/cache,id=renv-cache \
    R -e "renv::restore(prompt = FALSE)"

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN . "$HOME/.cargo/env"; rustc --version && cargo --version

RUN git clone https://github.com/DeepanshKhurana/faucet.git /tmp/faucet \
    && cd /tmp/faucet \
    && git checkout feat/ssl-friendly-postgres \
    && cargo install --path .


FROM alpine:3.20 AS creds
RUN rm -rf /tmp/Hrafnagud-Creds
RUN apk add --no-cache git openssh-client
RUN mkdir -p -m 0700 /root/.ssh && \
    ssh-keyscan -t rsa,ed25519 github.com >> /root/.ssh/known_hosts

RUN --mount=type=ssh \
    git clone git@github.com:DeepanshKhurana/Hrafnagud-Creds.git /tmp/Hrafnagud-Creds

COPY --from=creds /tmp/Hrafnagud-Creds /tmp/Hrafnagud-Creds
RUN mkdir -p /usr/local/Hrafnagud-Dynamo && \
    touch /usr/local/Hrafnagud-Dynamo/.Renviron && \
    touch /usr/local/Hrafnagud-Dynamo/.Renviron && \
    cp /tmp/Hrafnagud-Creds/creds.txt /usr/local/Hrafnagud-Dynamo/.Renviron && \
    cp /tmp/Hrafnagud-Creds/ebenezer_service_account.json /usr/local/Hrafnagud-Dynamo/.service_account && \
    cp /tmp/Hrafnagud-Creds/supabase.crt /usr/local/Hrafnagud-Dynamo/supabase.crt && \
    echo "" >> /usr/local/Hrafnagud-Dynamo/.Renviron && \
    cat /tmp/Hrafnagud-Creds/api.txt >> /usr/local/Hrafnagud-Dynamo/.Renviron && \
    rm -rf /tmp/Hrafnagud-Creds

EXPOSE 8008

CMD ["faucet","--host","0.0.0.0:8008","start","--dir","."]
