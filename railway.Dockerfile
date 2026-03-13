FROM rust:1.88-bookworm AS chef
ARG TARGETPLATFORM=linux/amd64
WORKDIR /build
RUN apt-get update && apt-get install -y libpq-dev jq
RUN ARCH=$(echo $TARGETPLATFORM | sed -e 's/\//_/g') && \
    if [ "$ARCH" = "linux_amd64" ]; then \
    ARCH="linux_x86_64"; \
    fi && \
    LINK=$(curl -s https://api.github.com/repos/streamingfast/substreams/releases/latest | jq -r ".assets[] | select(.name | contains(\"$ARCH\")) | .browser_download_url") && \
    echo ARCH: $ARCH, LINK: $LINK && \
    curl -L $LINK | tar zxf - -C /usr/local/bin/
RUN cargo install cargo-chef
COPY rust-toolchain.toml .
RUN rustup set profile minimal && rustup install

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --package tycho-indexer --release --recipe-path recipe.json
COPY . .
RUN ./stable-build.sh

FROM debian:bookworm
WORKDIR /opt/tycho-indexer

# Runtime binary and config files.
COPY --from=build /build/target/release/tycho-indexer ./tycho-indexer
COPY --from=build /build/extractors*.yaml ./

# Railway does not mount the local repo like docker-compose does, so if you want
# local .spkg files instead of S3-backed retrieval they must be baked into the image.
# In this repo the Railway-specific packages live under railway/substreams.
COPY --from=build /build/railway/substreams ./substreams

RUN apt-get update && apt-get install -y curl libpq-dev libcurl4 && rm -rf /var/lib/apt/lists/*

# Railway should set the actual start command so the chain-specific endpoint,
# config file, and PORT can be injected via variables.
CMD ["/opt/tycho-indexer/tycho-indexer", "--help"]
