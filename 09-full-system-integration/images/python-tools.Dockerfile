# P09 M1: one image carrying every Python component of the platform.
#
# Why this exists rather than "just run the scripts". The six Python projects
# declare requires-python >= 3.12; the WSL host where the Docker daemon lives
# ships 3.10, which has no tomllib, so P01's config loader cannot even import.
# And Kafka and ClickHouse are published on the WSL host's loopback, which is
# not reachable from Windows at all. An image fixes both at once: one pinned
# interpreter, and a process that can sit on the same network as the brokers.
#
# One image for all six, not six images. They are a toolchain operated as a
# sequence by `p09 pipeline`, not six long-lived services, and six images of the
# same interpreter would cost build time and drift without buying isolation.
# When a component becomes a daemon -- P05 is the first candidate -- it earns its
# own image then.
#
# Pinned by digest is the goal (P09 FR-02 / G0). This pins the tag; `p09
# preflight` reports a tag-only pin as a provenance gap rather than treating it
# as done.
FROM python:3.12.7-slim-bookworm

# Build tooling for any dependency without a wheel, dropped in the same layer so
# it is not part of the runtime surface (NFR-09: nothing in the image that a run
# does not need).
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        build-essential \
        librdkafka-dev \
        ca-certificates; \
    rm -rf /var/lib/apt/lists/*

# Dependencies first, as their own layer: project sources change far more often
# than the dependency set, and a rebuild should not refetch these.
#
# Versions are pinned, and protobuf is pinned to the exact version P02's
# toolchain/versions.lock records. The descriptor pins in P03/P04/P05 are
# checked against a digest this runtime helps produce (P09-M0-03), so the
# protobuf version is part of the contract, not an implementation detail.
RUN pip install --no-cache-dir \
        protobuf==7.35.1 \
        confluent-kafka==2.16.1 \
        clickhouse-connect==1.11.0 \
        pytest==9.2.0 \
        hypothesis==6.170.14 \
        psutil==5.9.8

WORKDIR /platform

COPY 01-market-data-simulator          /platform/01-market-data-simulator
COPY 02-market-data-protobuf-contracts /platform/02-market-data-protobuf-contracts
COPY 05-python-batch-ingestion         /platform/05-python-batch-ingestion
COPY 06-clickhouse-analytics           /platform/06-clickhouse-analytics
COPY 07-replay-and-recovery            /platform/07-replay-and-recovery
COPY 08-performance-benchmark          /platform/08-performance-benchmark
COPY 09-full-system-integration        /platform/09-full-system-integration

# --no-deps is deliberate. The dependency set above is pinned on purpose; letting
# pip resolve per project would let one of them quietly upgrade protobuf and move
# a descriptor digest out from under the pins that check it.
RUN set -eux; \
    pip install --no-cache-dir --no-deps \
        /platform/01-market-data-simulator \
        /platform/02-market-data-protobuf-contracts/python \
        /platform/05-python-batch-ingestion \
        /platform/06-clickhouse-analytics \
        /platform/07-replay-and-recovery \
        /platform/08-performance-benchmark \
        /platform/09-full-system-integration

# Import every package once at build time. A missing module should fail the
# build, not the first pipeline run three stages in.
RUN set -eux; \
    python -c "import market_sim, md_contract, p05_ingest, md_analytics, p07, p08_bench, p09_integration; print('all packages import')"; \
    python -c "import google.protobuf, confluent_kafka, clickhouse_connect; print('all drivers import')"

# A toolchain, so the default is to report and exit. `p09 pipeline` invokes
# specific entry points; an image that started something by itself would make a
# stage run twice.
CMD ["python", "-c", "import sys; print('p09 python tools', sys.version)"]
