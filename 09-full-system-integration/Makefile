# P09 integration targets.
#
# The Python side runs wherever `p09` is installed; the Docker side runs where
# the daemon is, which on this machine is inside WSL. The two are kept as
# separate targets rather than one `make up` that silently works on one host and
# not the other.

PY ?= python
COMPOSE ?= docker compose -f compose/compose.dev.yaml
REPORTS := reports

.PHONY: help install test audit lock lock-check verify render-init render-pipeline \n        images up down ps infra pipeline clean-reports

help:
	@echo "Contract (no infrastructure needed):"
	@echo "  make test         run the suite"
	@echo "  make audit        M0 cross-project contract audit"
	@echo "  make lock         (re)generate contracts/contracts.lock.yaml + reports/audit.json"
	@echo "  make lock-check   CI: fail if the stored lock is stale"
	@echo "  make verify       check the tree against the stored lock"
	@echo "  make render-init  generate compose/init/* from the lock"
	@echo "  make render-pipeline  generate the end-to-end runner from the lock"
	@echo ""
	@echo "Infrastructure (run where the Docker daemon is):"
	@echo "  make up           start the DEV stack and wait for both healthchecks"
	@echo "  make ps / down    status / stop"
	@echo "  make infra        run the init jobs, then judge them against the lock"
	@echo "  make images       build the pinned Python service image"
	@echo "  make pipeline     run the whole platform once, P01 through P07"

install:
	$(PY) -m pip install -e ".[test]"

test:
	$(PY) -m pytest -q

audit:
	$(PY) -m p09_integration audit --verbose

$(REPORTS):
	@mkdir -p $(REPORTS)

lock: $(REPORTS)
	$(PY) -m p09_integration contracts-lock --report $(REPORTS)/audit.json

lock-check:
	$(PY) -m p09_integration contracts-lock --check

verify:
	$(PY) -m p09_integration contracts-verify

render-init:
	$(PY) -m p09_integration render-init

render-pipeline:
	$(PY) -m p09_integration render-pipeline

# Built from the repository root, so every project is in the build context.
images:
	cd .. && docker build -f 09-full-system-integration/images/python-tools.Dockerfile 	  -t p09/python-tools:dev .

up:
	$(COMPOSE) up -d
	@echo "waiting for healthchecks..."
	@for i in $$(seq 1 60); do \
	  k=$$(docker inspect -f '{{.State.Health.Status}}' p09-dev-kafka 2>/dev/null); \
	  c=$$(docker inspect -f '{{.State.Health.Status}}' p09-dev-clickhouse 2>/dev/null); \
	  if [ "$$k" = healthy ] && [ "$$c" = healthy ]; then echo "both healthy"; exit 0; fi; \
	  sleep 2; \
	done; \
	echo "TIMEOUT: kafka=$$k clickhouse=$$c" >&2; exit 1

ps:
	$(COMPOSE) ps

# No `down -v` target, on purpose. P09 FR-18 keeps volume destruction out of the
# ordinary command set: `down -v` discards the Kafka log and every ClickHouse
# row, and a target that convenient gets typed by accident. Remove the volumes
# by name, deliberately, when that is what you mean.
down:
	$(COMPOSE) down

infra: $(REPORTS)
	sh compose/init/01-topics.sh
	sh compose/init/02-apply-ddl.sh
	sh compose/init/03-verify.sh > $(REPORTS)/infra-observed.json
	$(PY) -m p09_integration verify-infra \
	  --observed $(REPORTS)/infra-observed.json --report $(REPORTS)/infra.json

# The whole platform, once. Needs `make up` and `make images` first, and the
# C++ binaries built (03-*/scripts/build.sh, 04-*/Makefile). Runs where the
# Docker daemon is.
pipeline: $(REPORTS)
	sh compose/pipeline/run-pipeline.sh 2>&1 | tee $(REPORTS)/pipeline.log

clean-reports:
	rm -f $(REPORTS)/audit.json $(REPORTS)/infra.json $(REPORTS)/infra-observed.json
