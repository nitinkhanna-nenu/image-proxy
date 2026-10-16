################################################################################
# Chef stage (pre-built cargo-chef image)
################################################################################

FROM lukemathwalker/cargo-chef:latest-rust-1.96.0-alpine AS chef
WORKDIR /app

# Install build dependencies needed to compile Rust crates on Alpine
RUN apk add --no-cache clang lld git nasm dav1d-dev pkgconfig musl-dev libjxl-dev

# Disable static linking to avoid issues with libdav1d
ENV RUSTFLAGS="-C target-feature=-crt-static"

################################################################################
# Planner stage
# Analyze the project and produce a recipe file.
################################################################################

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY image/Cargo.toml image/Cargo.toml
COPY foyer/ foyer/
COPY src/ src/
COPY image/src/ image/src/
RUN cargo chef prepare --recipe-path recipe.json

################################################################################
# Builder stage
# Cache dependencies and compile the application.
################################################################################

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# `otel` compiles in OpenTelemetry export. Pass --build-arg CARGO_FEATURES=
# to leave it out. An empty value builds the image without that feature.
ARG CARGO_FEATURES=otel

# Copy the local path dependencies needed by [patch.crates-io] and foyer
COPY image/ image/
COPY foyer/ foyer/

# Build dependencies - this is the caching Docker layer!
RUN if [ -n "$CARGO_FEATURES" ]; then \
      cargo chef cook --release --features "$CARGO_FEATURES" --recipe-path recipe.json; \
    else \
      cargo chef cook --release --recipe-path recipe.json; \
    fi

# Build application
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY tests/ tests/
RUN if [ -n "$CARGO_FEATURES" ]; then \
      cargo build --locked --release --features "$CARGO_FEATURES"; \
    else \
      cargo build --locked --release; \
    fi && \
    cp ./target/release/image-proxy /bin/image-proxy

################################################################################
# Runtime stage (DOI Alpine image)
# This stage runs the already-compiled binary with minimal dependencies.
################################################################################

FROM docker.io/library/alpine:3.23 AS runtime

# Create a non-privileged user (recommended best practice)
ARG UID=1000

# jemalloc: full-process allocator via LD_PRELOAD (Rust + native codecs).
# Returns free pages after concurrent image encode/decode free storms more
# aggressively than the system multi-arena malloc.
# ca-certificates: S3 and other outbound HTTPS load the system trust store.
# On Alpine that file is /etc/ssl/cert.pem.
RUN apk add --no-cache libdav1d libgcc libjxl jemalloc ca-certificates

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

# Local originals and an optional disk cache. Bind mounts replace these.
# The cache directory must stay writable by uid 1000 when disk cache is on.
RUN mkdir -p /app/data /var/cache/image-proxy \
    && chown appuser:appuser /app/data /var/cache/image-proxy

# Drop privileges for runtime.
USER appuser

# Set working directory
WORKDIR /app

# Copy only the compiled binary from the build stage.
COPY --from=builder /bin/image-proxy /bin/image-proxy

# Document the port your app listens on.
EXPOSE 8000

# Intercept all malloc/free (including libwebp/libjxl/dav1d).
ENV LD_PRELOAD=/usr/lib/libjemalloc.so.2
# background_thread: purge dirty pages without waiting for the next alloc storm
# dirty/muzzy_decay_ms: return free pages to the OS ~1s after they become idle
ENV MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000

# Start the application.
CMD ["/bin/image-proxy"]
