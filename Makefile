LOCAL_BIN=$(CURDIR)/bin

GOENV=PATH=$(LOCAL_BIN):$(PATH)

GOLANGCI_BIN=$(LOCAL_BIN)/golangci-lint
$(GOLANGCI_BIN):
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(LOCAL_BIN) v2.2.2

BENCHSTAT_BIN=$(LOCAL_BIN)/benchstat
$(BENCHSTAT_BIN):
	GOBIN=$(LOCAL_BIN) go install golang.org/x/perf/cmd/benchstat@latest

.PHONY: lint
lint: $(GOLANGCI_BIN)
	$(GOENV) $(GOLANGCI_BIN) run --fix -v ./...

.PHONY: test
test:
	$(GOENV) go test -race ./...

.PHONY: test-cover
test-cover:
	$(GOENV) go test ./... -cover

.PHONY: test-cover-html
test-cover-html:
	$(GOENV) go test ./... -coverprofile=prof.out
	$(GOENV) go tool cover -html=prof.out
