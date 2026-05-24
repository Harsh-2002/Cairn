# Cairn developer convenience targets. Every target wraps a one-line Go or
# shell command; nothing here is load-bearing. See CONTRIBUTING.md for the
# full dev loop.

GO        ?= go
BINARY    ?= cairn
SOURCE    ?= ./cmd/cairn

.PHONY: all build test fmt vet check cross admin clean

all: check build

build:
	$(GO) build -o $(BINARY) $(SOURCE)

test:
	$(GO) test ./... -count=1

fmt:
	gofmt -w .

vet:
	$(GO) vet ./...

check: vet test
	@unformatted=$$(gofmt -l . | grep -v '^admin/'); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt: unformatted files:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

# Cross-compile to every release target. No CGO; static binaries.
cross:
	@mkdir -p dist
	CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 $(GO) build -ldflags='-s -w' -o dist/cairn-linux-amd64        $(SOURCE)
	CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 $(GO) build -ldflags='-s -w' -o dist/cairn-linux-arm64        $(SOURCE)
	CGO_ENABLED=0 GOOS=darwin  GOARCH=amd64 $(GO) build -ldflags='-s -w' -o dist/cairn-darwin-amd64       $(SOURCE)
	CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 $(GO) build -ldflags='-s -w' -o dist/cairn-darwin-arm64       $(SOURCE)
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GO) build -ldflags='-s -w' -o dist/cairn-windows-amd64.exe  $(SOURCE)
	@ls -lh dist/

# Build the Svelte admin SPA and stage it where //go:embed reads from.
admin:
	cd admin && npm ci && npm run build
	mkdir -p internal/server/admin/admin-dist
	rm -rf internal/server/admin/admin-dist/*
	cp -r admin/dist/. internal/server/admin/admin-dist/

clean:
	rm -f $(BINARY)
	rm -rf dist _site
	rm -rf internal/server/admin/admin-dist/*
