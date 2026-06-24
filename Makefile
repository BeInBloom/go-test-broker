GO      ?= go
PORT    ?= 8080
COUNT   ?= 3
BINARY  ?= bin/broker
PACKAGE := ./...
APP     := ./cmd

.PHONY: help run build test test-e2e test-race test-repeat fmt fmt-check vet check clean

help:
	@echo "make run                Run broker on PORT=$(PORT)"
	@echo "make build              Build $(BINARY)"
	@echo "make test               Run all tests, including e2e"
	@echo "make test-e2e           Run only e2e tests"
	@echo "make test-race          Run all tests with race detector"
	@echo "make test-repeat        Run tests COUNT=$(COUNT) times in random order"
	@echo "make fmt                Format Go sources"
	@echo "make fmt-check          Check Go formatting"
	@echo "make vet                Run go vet"
	@echo "make check              Run formatting, vet, tests, and race detector"
	@echo "make clean              Remove build artifacts"

run:
	$(GO) run $(APP) $(PORT)

build:
	@mkdir -p $(dir $(BINARY))
	$(GO) build -o $(BINARY) $(APP)

test:
	$(GO) test $(PACKAGE)

test-e2e:
	$(GO) test $(APP) -run '^TestE2E' -count=1

test-race:
	$(GO) test -race $(PACKAGE)

test-repeat:
	$(GO) test -count=$(COUNT) -shuffle=on $(PACKAGE)

fmt:
	$(GO) fmt $(PACKAGE)

fmt-check:
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './.git/*'))" || \
		{ echo "Go files are not formatted; run 'make fmt'"; exit 1; }

vet:
	$(GO) vet $(PACKAGE)

check: fmt-check vet test test-race

clean:
	rm -rf bin
