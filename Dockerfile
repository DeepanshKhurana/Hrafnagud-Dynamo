FROM rocker/r-ver

RUN R -e "install.packages('renv')"

RUN apt-get update && apt-get install -y \
    libcurl4-gnutls-dev \
    libssl-dev \
    libxml2-dev \
    libpq-dev \
    libv8-dev \
    libsodium-dev

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

RUN cargo install faucet-server

COPY . /usr/local/Hrafnagud-Dynamo/

WORKDIR /usr/local/Hrafnagud-Dynamo/

RUN R -e "source('.Rprofile')"

RUN R -e "renv::restore()"

EXPOSE 8008

CMD ["R", "-e", "source('/usr/local/Hrafnagud-Dynamo/entrypoint.R')"]
