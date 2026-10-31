# Convenience targets. The scripts under tests/ are the real entry points; this
# file only saves typing and documents the usual order.
#
# Everything assumes the pinned dependencies from deploy/setup-wsl-deps.sh are
# installed under /opt/p04-deps. Override P04_DEPS to move them.

P04_DEPS ?= /opt/p04-deps
BUILD_DIR ?= build
VENV := $(P04_DEPS)/venv/bin/python

.PHONY: all deps configure build test e2e faults ladder matrix golden clean help

help:
	@echo "deps       install the pinned dependencies (needs root)"
	@echo "build      configure and build"
	@echo "test       the unit and property suites, no broker needed"
	@echo "e2e        E2E-01 against a real broker, with independent verification"
	@echo "faults     the real-broker negative and fault suite"
	@echo "ladder     the rate ladder (E2E-14)"
	@echo "matrix     regenerate the 280-case verdict matrix"
	@echo "golden     regenerate the key-to-partition golden mapping"

all: build test

deps:
	bash deploy/setup-wsl-deps.sh

configure:
	cmake -S . -B $(BUILD_DIR) -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo

build: configure
	cmake --build $(BUILD_DIR) -j$$(nproc)

test: build
	./$(BUILD_DIR)/p04_unit_tests

# ASan over the asynchronous delivery-report path, which milestone M2 asks for:
# a callback context outliving its attempt would show up here and nowhere else.
asan:
	cmake -S . -B $(BUILD_DIR)-asan -G Ninja -DCMAKE_BUILD_TYPE=Debug -DP04_ENABLE_ASAN=ON
	cmake --build $(BUILD_DIR)-asan -j$$(nproc)
	./$(BUILD_DIR)-asan/p04_unit_tests

e2e: build
	P04_BUILD_DIR=$(PWD)/$(BUILD_DIR) bash tests/integration/run_e2e_01.sh

faults: build
	P04_BUILD_DIR=$(PWD)/$(BUILD_DIR) bash tests/integration/run_fault_suite.sh

ladder: build
	P04_BUILD_DIR=$(PWD)/$(BUILD_DIR) bash tests/stress/run_rate_ladder.sh

matrix:
	$(VENV) tools/build_verdict_matrix.py

golden:
	$(VENV) tools/gen_partition_golden.py --partitions 8 	  --out config/topic-partition-golden.json

clean:
	rm -rf $(BUILD_DIR) $(BUILD_DIR)-asan
