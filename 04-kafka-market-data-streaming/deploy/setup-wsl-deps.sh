#!/bin/bash
# Reproducible dependency setup for the Project 04 Linux lab (run as root on
# Ubuntu 22.04, including WSL2).
#
# NFR-14 and FR-12 require every version that can change behaviour to be pinned
# and recorded, so this script pins exact versions rather than taking whatever
# the distribution ships:
#
#   * protobuf 3.21.12 is built from source because Ubuntu 22.04 ships 3.12.4,
#     where proto3 optional is still experimental. The pinned schema in
#     schemas/market_data.proto depends on stable field presence (case D-03), so
#     3.12 is not usable. 3.21.12 is also the last release before Abseil became
#     a hard dependency, which keeps this build short and self-contained.
#   * librdkafka 2.6.1 is built from source because the distribution package is
#     1.8.0 (2021). The run report records the linked version either way; what
#     matters is that one version is pinned and inspected, not assumed
#     (section 11.2 config notes).
#   * The Python verifier gets protobuf 4.21.12, the runtime from the same
#     release train as protoc 3.21.12, so generated code and runtime agree.
#
# Kafka itself is not installed here: it runs from the pinned apache/kafka image
# via deploy/docker-compose.*.yml, which is also what keeps the broker version
# in the run report honest.
#
# Everything lands under /opt/p04-deps so it can be removed in one step and
# never mixes with system packages.
set -uo pipefail

# WSL2 inherits the Windows PATH, which puts C:\msys64\ucrt64\bin on it. CMake
# then finds MSYS2 package config files (CURLConfig.cmake and friends) and tries
# to link Windows libraries into a Linux build. Sanitising PATH here is not
# cosmetic: without it the librdkafka configure step fails looking for Libidn2
# inside /mnt/c.
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
unset CMAKE_PREFIX_PATH CMAKE_MODULE_PATH PKG_CONFIG_PATH

DEPS=${P04_DEPS:-/opt/p04-deps}
PROTOBUF_VERSION=3.21.12
PROTOBUF_TAG=v21.12
LIBRDKAFKA_VERSION=2.6.1
KAFKA_IMAGE=apache/kafka:4.2.0
TOXIPROXY_IMAGE=ghcr.io/shopify/toxiproxy:2.12.0

mkdir -p "$DEPS/src"
export DEBIAN_FRONTEND=noninteractive
step(){ echo; echo "=== STEP: $* ==="; }

step "apt packages"
# Installed one at a time on purpose. Asking apt for the whole set at once fails
# on this image with "pkgProblemResolver::Resolve generated breaks" even though
# every package is individually installable, and a single unrelated conflict
# must not block the packages that do resolve.
apt-get update -qq
APT_MISSING=()
for pkg in build-essential cmake ninja-build pkg-config git curl ca-certificates \
           unzip zlib1g-dev libssl-dev libsasl2-dev liblz4-dev libzstd-dev \
           python3-venv python3-pip python3-dev jq iproute2 procps \
           libgtest-dev libgmock-dev; do
  if dpkg -s "$pkg" >/dev/null 2>&1; then
    continue
  fi
  if apt-get install -y -qq --no-install-recommends "$pkg" >/dev/null 2>&1; then
    echo "installed $pkg"
  else
    echo "FAILED  $pkg"
    APT_MISSING+=("$pkg")
  fi
done
if [ ${#APT_MISSING[@]} -gt 0 ]; then
  echo "WARNING: could not install: ${APT_MISSING[*]}"
fi

step "protobuf $PROTOBUF_VERSION"
if [ -x "$DEPS/protobuf/bin/protoc" ]; then
  echo "already present: $("$DEPS/protobuf/bin/protoc" --version)"
else
  cd "$DEPS/src" || exit 1
  TARBALL=protobuf-cpp-$PROTOBUF_VERSION.tar.gz
  [ -f "$TARBALL" ] || curl -sSL -o "$TARBALL" \
    "https://github.com/protocolbuffers/protobuf/releases/download/$PROTOBUF_TAG/$TARBALL"
  echo "tarball sha256: $(sha256sum "$TARBALL" | cut -d' ' -f1)"
  tar xzf "$TARBALL" --overwrite
  cd "protobuf-$PROTOBUF_VERSION" || exit 1
  cmake -S . -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_BUILD_SHARED_LIBS=OFF \
    -DCMAKE_INSTALL_PREFIX="$DEPS/protobuf" >/dev/null || exit 1
  cmake --build build -j"$(nproc)" >/dev/null || exit 1
  cmake --install build >/dev/null || exit 1
  echo "built: $("$DEPS/protobuf/bin/protoc" --version)"
fi

step "librdkafka $LIBRDKAFKA_VERSION"
if [ -f "$DEPS/librdkafka/include/librdkafka/rdkafka.h" ]; then
  echo "already present"
else
  cd "$DEPS/src" || exit 1
  TARBALL=librdkafka-$LIBRDKAFKA_VERSION.tar.gz
  [ -f "$TARBALL" ] || curl -sSL -o "$TARBALL" \
    "https://github.com/confluentinc/librdkafka/archive/refs/tags/v$LIBRDKAFKA_VERSION.tar.gz"
  echo "tarball sha256: $(sha256sum "$TARBALL" | cut -d' ' -f1)"
  tar xzf "$TARBALL" --overwrite
  cd "librdkafka-$LIBRDKAFKA_VERSION" || exit 1
  # WITH_CURL=OFF: curl is only needed for the OAUTHBEARER OIDC token refresh,
  # which this lab does not use (PLAINTEXT on loopback, section 11.2). Leaving
  # it on makes the build depend on a system curl package config.
  cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DRDKAFKA_BUILD_STATIC=OFF -DRDKAFKA_BUILD_TESTS=OFF -DRDKAFKA_BUILD_EXAMPLES=OFF \
    -DWITH_SSL=ON -DWITH_SASL=ON -DWITH_ZLIB=ON -DWITH_ZSTD=ON -DWITH_CURL=OFF \
    -DCMAKE_INSTALL_PREFIX="$DEPS/librdkafka" >/dev/null || exit 1
  cmake --build build -j"$(nproc)" >/dev/null || exit 1
  cmake --install build >/dev/null || exit 1
  echo "built: $(ls "$DEPS/librdkafka/lib")"
fi

step "python venv for the independent verifier"
[ -x "$DEPS/venv/bin/python" ] || python3 -m venv "$DEPS/venv"
"$DEPS/venv/bin/pip" install -q --upgrade pip setuptools wheel
# lz4 and zstd: the producer pins compression.type=lz4, so a pure-Python
# consumer cannot read a single record without the matching codec. Installing
# both means a benchmark can switch codecs without the verifier going blind.
"$DEPS/venv/bin/pip" install -q "protobuf==4.21.12" "pytest==8.3.4" "tomli==2.2.1" \n  "pyyaml==6.0.2" "lz4==4.3.3" "zstandard==0.23.0"
# The snapshot verifier must not share an implementation with the C++ producer,
# so a pure-Python Kafka client is preferred over confluent-kafka (which is
# librdkafka again). Section 10.1, oracle rule 4.
"$DEPS/venv/bin/pip" install -q "kafka-python>=2.2.0" && echo "kafka-python installed" || \
  { "$DEPS/venv/bin/pip" install -q "kafka-python-ng>=2.2.3" && echo "kafka-python-ng installed"; } || \
  echo "WARNING: no pure-python kafka client installed"
"$DEPS/venv/bin/python" -c "
import google.protobuf, pytest, yaml
print('python protobuf', google.protobuf.__version__)
print('pytest', pytest.__version__)
try:
    import kafka; print('kafka client', kafka.__version__)
except Exception as exc:
    print('kafka client MISSING:', exc)
"

step "docker images"
for img in "$KAFKA_IMAGE" "$TOXIPROXY_IMAGE"; do
  docker pull -q "$img" >/dev/null 2>&1 && echo "pulled $img" || echo "PULL FAILED $img"
done

step "summary"
"$DEPS/protobuf/bin/protoc" --version
grep -m1 "define RD_KAFKA_VERSION  " "$DEPS/librdkafka/include/librdkafka/rdkafka.h" || true
echo "g++: $(g++ --version | head -1)"
echo "cmake: $(cmake --version | head -1)"
echo "SETUP COMPLETE"
