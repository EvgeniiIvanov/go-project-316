.PHONY: help
help:
	@echo "Available targets:"
	@echo "  lint      - run linter"
	@echo "  lint-fix  - run linter and fix"
	@echo "  test      - run tests"
	@echo "  build     - build project"

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: lint
lint: fmt vet
	golangci-lint run

.PHONY: lint-fix 
lint-fix: lint
	golangci-lint run --fix

.PHONY: test
test:
	go test ./... -v

.PHONY: build
build:
	go build -o bin/crawler ./cmd/hexlet-go-crawler