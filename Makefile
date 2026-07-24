# Cairn developer task runner. A thin layer over `cargo` and the conformance harnesses —
# it does NOT replace them (the 31 harnesses in conformance/ are e2e test programs, and
# install.sh is a standalone `curl | sh` installer). Run `make` or `make help` for the list.
#
# `make check` is the gate: it mirrors .github/workflows/ci.yml and aborts on the first
# failure with a non-zero exit, so a passing run is load-bearing (no piped exit code to mask it).

BIN      ?= target/debug/cairn
PY       ?= python3
CARGO    ?= cargo

.DEFAULT_GOAL := help
# The gate targets must run in order even under `make -j`; never parallelize a check chain.
.NOTPARALLEL:

.PHONY: help check check-all fmt fmt-fix lint lint-all test doc build build-release \
        web conformance conformance-suite bench run clean

help: ## List the available targets
	@grep -hE '^[a-z][a-z-]*:.*##' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN{FS=":.*## "}{printf "  \033[1m%-18s\033[0m %s\n", $$1, $$2}'

## --- gate ---------------------------------------------------------------------

check: fmt lint test ## Fast gate: fmt + clippy + nextest + doctests (the inner loop)

check-all: fmt lint lint-all test web ## Full gate mirroring CI: adds --all-features clippy + the web console build

fmt: ## Check formatting (does not modify files)
	$(CARGO) fmt --all --check

fmt-fix: ## Apply formatting
	$(CARGO) fmt --all

lint: ## Clippy with warnings denied
	$(CARGO) clippy --workspace --all-targets -- -D warnings

lint-all: ## Clippy with --all-features (the only leg that compiles the fast-io path)
	$(CARGO) clippy --workspace --all-targets --all-features -- -D warnings

test: ## Run the workspace tests (nextest) + doctests
	$(CARGO) nextest run --workspace
	$(CARGO) test --workspace --doc

doc: ## Doctests only
	$(CARGO) test --workspace --doc

## --- build --------------------------------------------------------------------

build: ## Debug build of the cairn binary
	$(CARGO) build --bin cairn

build-release: ## Optimized release build of the cairn binary
	$(CARGO) build --release --bin cairn

web: ## Build the embedded React console (web/dist, embedded into cairn-web)
	cd web && npm install && npm run build

## --- conformance (drives a REAL cairn binary) --------------------------------

conformance: build ## boto3 smoke: run.sh drives the full object lifecycle
	BIN=$(BIN) PY=$(PY) bash conformance/run.sh

conformance-suite: build ## Run the gated boto3 e2e harnesses (skips failpoints/sudo/multi-node ones)
	@for h in run routing listing multipart objects buckets authz lifecycle \
	          checksums object_lock share sts console_session backup_restore scrub; do \
		echo ">> conformance/$$h.sh"; \
		BIN=$(BIN) PY=$(PY) bash conformance/$$h.sh || exit 1; \
	done

## --- misc ---------------------------------------------------------------------

bench: ## Run the workspace benchmarks
	$(CARGO) bench --workspace

run: ## Run `cairn serve` locally (set CAIRN_* env first; see CLAUDE.md)
	$(CARGO) run --bin cairn -- serve

clean: ## Remove the cargo target directory
	$(CARGO) clean
