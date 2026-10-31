# P08 benchmark harness.
#
# The target names follow section 21.2 of the design document. Everything runs
# through the venv at the repository root, and nothing here needs Kafka or
# ClickHouse -- the lanes that do are blocked by design until P02-P07 exist.
#
# Start with `make p08-selftest`. Section 8 requires the measuring tools to be
# validated against known inputs before they are pointed at anything.

PY ?= ../.venv/Scripts/python.exe
PROFILE ?= local_small
CONFIG ?= configs/$(PROFILE).toml
SUT ?= mock
SOURCE ?=
RUN_ID ?= P08-LOCAL-SMALL
OUT ?= reports/runs/$(RUN_ID)
RATE ?= 120000
SECONDS ?= 1.0

SOURCE_ARG := $(if $(SOURCE),--source $(SOURCE),)

export PYTHONPATH := src

.PHONY: help
help:
	@echo "p08-selftest     validate the metrology kernel against the golden fixtures"
	@echo "p08-test         run the whole test suite"
	@echo "p08-test-fast    unit tests only"
	@echo "p08-coverage     test suite with a coverage report"
	@echo "p08-preflight    validate a config, a source seal and stage availability"
	@echo "p08-calibrate    judge the driver against a null sink (RATE=...)"
	@echo "p08-bench        run a lane end to end (PROFILE=... SUT=... SOURCE=...)"
	@echo "p08-verify       re-read an evidence bundle (OUT=...)"
	@echo "p08-catalog      inventory of the 512 what-if cases and 48 E2E tests"
	@echo "p08-gate         release coverage gate (PLAN=plan.json)"
	@echo "p08-specs        regenerate specs/ from the code and the design document"
	@echo "p08-specs-check  fail if specs/ is stale"

# -- validation -------------------------------------------------------------

.PHONY: p08-selftest
p08-selftest:
	$(PY) -m p08_bench selftest

.PHONY: p08-test
p08-test:
	$(PY) -m pytest -q

.PHONY: p08-test-fast
p08-test-fast:
	$(PY) -m pytest tests/unit -q

.PHONY: p08-coverage
p08-coverage:
	$(PY) -m coverage run --source=src -m pytest -q
	$(PY) -m coverage report -m

# -- lanes ------------------------------------------------------------------

.PHONY: p08-preflight
p08-preflight:
	$(PY) -m p08_bench preflight --config $(CONFIG) --sut $(SUT) $(SOURCE_ARG)

.PHONY: p08-calibrate
p08-calibrate:
	$(PY) -m p08_bench calibrate --rate $(RATE) --seconds $(SECONDS)

.PHONY: p08-bench
p08-bench:
	$(PY) -m p08_bench bench --config $(CONFIG) --sut $(SUT) $(SOURCE_ARG) --out $(OUT)

.PHONY: p08-verify
p08-verify:
	$(PY) -m p08_bench verify --run-dir $(OUT)

# -- catalog and gate -------------------------------------------------------

.PHONY: p08-catalog
p08-catalog:
	$(PY) -m p08_bench catalog

.PHONY: p08-gate
p08-gate:
	$(PY) -m p08_bench gate $(if $(PLAN),--plan $(PLAN),)

# -- generated artifacts ----------------------------------------------------

.PHONY: p08-specs
p08-specs:
	$(PY) scripts/build_catalog.py
	$(PY) scripts/build_schemas.py

.PHONY: p08-specs-check
p08-specs-check:
	$(PY) scripts/build_schemas.py --check
	$(PY) scripts/build_catalog.py
