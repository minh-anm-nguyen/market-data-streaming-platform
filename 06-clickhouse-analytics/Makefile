# Project 06 -- make targets of section 19.3.
#
# Every target that produces a verdict exits non-zero when that verdict is not
# PASS, and the CLI distinguishes the kinds: 1 FAIL, 3 NO_GO, 4 INCONCLUSIVE. A
# NO_GO is often the *correct* behaviour, so a CI job that treats all non-zero
# codes as a crash will eventually be "fixed" by deleting the refusal.

PY ?= python
PYTEST ?= $(PY) -m pytest

# PYTHONPATH separator is ';' on Windows and ':' elsewhere. Getting this wrong
# makes every target fail with ModuleNotFoundError, which looks like a broken
# package rather than a broken Makefile.
ifeq ($(OS),Windows_NT)
PATHSEP := ;
else
PATHSEP := :
endif
export PYTHONPATH := src$(PATHSEP)tests

CATALOG ?= .artifacts/catalog
EVIDENCE ?= .artifacts/evidence
FIXTURES ?= .artifacts/fixtures
CLICKHOUSE_URL ?= http://127.0.0.1:8123

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "Correctness:"
	@echo "  make verify             unit + e2e + property + chaos + registry (no server)"
	@echo "  make test               the whole suite, quietly"
	@echo "  make test-fast          unit and e2e only"
	@echo "  make property           metamorphic and property-based tests"
	@echo "  make chaos              fault-injection profiles"
	@echo "  make coverage-report    regenerate tests/registry/coverage_report.json"
	@echo ""
	@echo "ClickHouse (needs a server):"
	@echo "  make dev-up             start the pinned ClickHouse container"
	@echo "  make dev-down           stop it"
	@echo "  make e2e-clickhouse     run the integration tests against it"
	@echo "  make schema-check       print or verify the pinned schema digests"
	@echo ""
	@echo "Pipeline:"
	@echo "  make fixtures-generate  write a corpus to $(FIXTURES)"
	@echo "  make build              audit, stage, verify and publish a build"
	@echo "  make build-golden       the same, on the section 16.1 corpus"
	@echo "  make benchmark          measure the 1m bar query"
	@echo "  make retention-gate     may TTL be enabled? (NFR-11)"
	@echo "  make export-evidence    write the evidence ledger to $(EVIDENCE)"

# ------------------------------------------------------------- correctness ---

.PHONY: verify
verify: test coverage-report
	@echo "verify: correctness suite complete. ClickHouse-semantics cases are"
	@echo "NOT_RUN unless 'make e2e-clickhouse' also ran -- see the coverage report."

.PHONY: test
test:
	$(PYTEST) -q

.PHONY: test-fast
test-fast:
	$(PYTEST) -q tests/unit tests/e2e

.PHONY: property
property:
	$(PYTEST) -q tests/property

.PHONY: chaos
chaos:
	$(PYTEST) -q tests/chaos

.PHONY: coverage-report
coverage-report:
	$(PYTEST) -q tests/registry
	@echo "wrote tests/registry/coverage_report.json"

.PHONY: lint-sql
lint-sql:
	$(PYTEST) -q tests/unit/test_config_and_sql.py

# -------------------------------------------------------------- clickhouse ---

.PHONY: dev-up
dev-up:
	docker compose up -d --wait
	@echo "ClickHouse on $(CLICKHOUSE_URL)"

.PHONY: dev-down
dev-down:
	docker compose down

# Removes the lab volume as well. Separate from dev-down so a stray 'make
# dev-down' cannot delete data someone was still looking at.
.PHONY: dev-destroy
dev-destroy:
	docker compose down -v

.PHONY: e2e-clickhouse
e2e-clickhouse:
	P06_CLICKHOUSE_URL=$(CLICKHOUSE_URL) $(PYTEST) -v tests/clickhouse

.PHONY: schema-check
schema-check:
	$(PY) -m md_analytics.cli schema-check

# ---------------------------------------------------------------- pipeline ---

.PHONY: fixtures-generate
fixtures-generate:
	$(PY) -m md_analytics.cli fixtures-generate --out $(FIXTURES) \
		--dataset-id DEV-01 --symbols EURUSD GBPUSD --ticks-per-symbol 1000 \
		--quote-every 5 --duplicate-every 50

.PHONY: build
build:
	$(PY) -m md_analytics.cli build --catalog $(CATALOG) \
		--dataset-id DEV-01 --symbols EURUSD GBPUSD --ticks-per-symbol 1000

.PHONY: build-golden
build-golden:
	$(PY) -m md_analytics.cli build --catalog $(CATALOG) --golden \
		--dataset-id GOLDEN-01 --block-size 2

.PHONY: benchmark
benchmark:
	$(PY) -m md_analytics.cli benchmark --catalog $(CATALOG) \
		--dataset-id BENCH-01 --symbols EURUSD --ticks-per-symbol 20000 \
		--repetitions 200

.PHONY: retention-gate
retention-gate:
	$(PY) -m md_analytics.cli retention-gate --config config/dev.toml

.PHONY: export-evidence
export-evidence:
	$(PY) scripts/export_evidence.py --out $(EVIDENCE)

.PHONY: clean
clean:
	$(PY) scripts/clean_artifacts.py
