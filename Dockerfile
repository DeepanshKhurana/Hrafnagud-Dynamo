FROM rocker/r-ver

RUN R -e "install.packages('renv', repos='https://cloud.r-project.org')"

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
 && curl -fsSL https://supabase-downloads.s3-ap-southeast-1.amazonaws.com/prod/ssl/prod-ca-2021.crt -o /usr/local/share/ca-certificates/supabase-prod-ca-2021.crt \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN . "$HOME/.cargo/env"; rustc --version && cargo --version
RUN . "$HOME/.cargo/env"; cargo install faucet-server

COPY . /usr/local/Hrafnagud-Dynamo/
WORKDIR /usr/local/Hrafnagud-Dynamo/

RUN R -e "source('.Rprofile')"
RUN R -e "renv::restore()"

EXPOSE 8008

CMD ["bash", "-lc", "faucet --host 0.0.0.0:8008 start --dir ."]
