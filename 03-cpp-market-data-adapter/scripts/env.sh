# Shared environment for every build/test script. Sourced, not executed.
#
# The protobuf toolchain is pinned to a local prefix so the build does not
# depend on whatever happens to be installed system-wide (ADR-13). Point
# P03_PROTOBUF_ROOT elsewhere to build against a different pinned install.
: "${P03_PROTOBUF_ROOT:=$HOME/p03-tc/prefix/usr}"
export P03_PROTOBUF_ROOT

# The version that prefix is expected to hold, recorded so it is readable
# without running the build. P02 keeps the same fact in
# toolchain/versions.lock; this project had it nowhere, which made the
# cross-project toolchain matrix unknowable from the source tree alone
# (P09 finding P09-M0-07).
#
# 3.12 matters beyond provenance: it predates proto3 `optional`, so
# cmake/Protobuf.cmake adds --experimental_allow_proto3_optional below 3.15.
# A build that silently used a newer protoc would drop that flag and the
# schema would stop compiling for a reason nothing here explains.
: "${P03_PROTOBUF_VERSION:=3.12.4}"
export P03_PROTOBUF_VERSION
export LD_LIBRARY_PATH="$P03_PROTOBUF_ROOT/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"
export PATH="$P03_PROTOBUF_ROOT/bin:$PATH"

P03_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export P03_ROOT

# Builds live outside the source tree on the Linux filesystem: compiling on a
# /mnt/<drive> 9p mount is several times slower.
: "${P03_BUILD_ROOT:=$HOME/.cache/p03-build}"
export P03_BUILD_ROOT
mkdir -p "$P03_BUILD_ROOT"
