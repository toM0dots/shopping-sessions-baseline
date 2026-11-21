IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac, for fast local loops.
PLATFORM ?= linux/amd64
DEV   ?= dev
# In the monorepo the fixtures live two levels up; the published starter has its own copy.
FIXTURES ?= $(if $(wildcard fixtures),fixtures,../../fixtures)
export IMAGE CPUS MEM PLATFORM

PYTHON ?= python3
BUILD_JOBS ?= 4
.PHONY: build fixtures dev run-dev check-dev native native-wheel native-fixtures benchmark

## Build the minimal Arrow/Parquet libraries and native executable
native:
	cmake -DJOBS=$(BUILD_JOBS) -P scripts/build_minimal.cmake

## Faster build-time alternative using the larger PyArrow shared libraries
native-wheel:
	$(PYTHON) scripts/build_native.py

native-fixtures: native
	$(PYTHON) scripts/run_fixtures.py $(FIXTURES) -- build/shopping-sessions

benchmark: native
	$(PYTHON) scripts/benchmark.py --data $(DEV) --threads $(CPUS) --runs 5 -- build/shopping-sessions

## Build the image for the scorer's platform, linux/amd64 (emulated on Apple silicon)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Make the 1M-event dev set with the published generator (needs `generate` on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Compare your dev output with the published dev answer (dev-answer.zip, unzipped): make check-dev ANSWER=dev-answer
ANSWER ?= dev-answer
check-dev:
	python3 scripts/compare.py out $(ANSWER)
