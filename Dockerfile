# ============================================================
# Apiary — Multi-stage Dockerfile
# Builds the Apiary binary and Python wheel from source,
# producing a slim runtime image suitable for any platform.
#
# Build:
#   docker build -t apiary:latest .
#   docker build --platform linux/arm64 -t apiary:latest .   # Raspberry Pi
#
# Multi-arch (requires buildx):
#   docker buildx build --platform linux/amd64,linux/arm64 -t apiary:latest .
# ============================================================

# ---------- Stage 1: Builder ----------
FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    pkg-config \
    python3 \
    python3-dev \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir --break-system-packages maturin

WORKDIR /build
COPY . .

# Build the CLI binary
RUN cargo build --release -p apiary-cli

# Build the Python wheel
RUN maturin build --release

# ---------- Stage 2: Runtime ----------
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash apiary

# Copy the CLI binary
COPY --from=builder /build/target/release/apiary /usr/local/bin/apiary

# Copy and install the Python wheel
COPY --from=builder /build/target/wheels/*.whl /tmp/
RUN pip3 install --no-cache-dir /tmp/*.whl && rm -f /tmp/*.whl

# Install common Python dependencies for working with Apiary
RUN pip3 install --no-cache-dir pyarrow pandas

# Copy the node runner script
COPY scripts/apiary-node.py /usr/local/bin/apiary-node.py
RUN chmod +x /usr/local/bin/apiary-node.py

USER apiary
WORKDIR /home/apiary

# Create data and cache directories
RUN mkdir -p /home/apiary/data /home/apiary/cache

# Expose port for future HTTP API (v2)
EXPOSE 8080

# Default: run the node script (can be overridden)
CMD ["python3", "/usr/local/bin/apiary-node.py"]
