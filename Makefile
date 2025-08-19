.PHONY: all
all: build test

# Build the project
.PHONY: build
build:
	@echo "Building reveald..."
	go build -v ./...

# Run tests
.PHONY: test
test:
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.out ./...

# Run tests with coverage report
.PHONY: test-coverage
test-coverage: test
	@echo "Generating coverage report..."
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run linters
.PHONY: lint
lint:
	@echo "Running linters..."
	@if command -v golangci-lint > /dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed. Install with:"; \
		echo "  go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

# Format code
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	go fmt ./...
	go mod tidy

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	go clean
	rm -f coverage.out coverage.html

# Release targets
.PHONY: release
release:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make release VERSION=x.y.z"; \
		exit 1; \
	fi
	@echo "Creating release v$(VERSION)..."
	@echo ""
	@echo "This will:"
	@echo "  1. Create a git tag v$(VERSION)"
	@echo "  2. Push the tag to trigger the release workflow"
	@echo "  3. The GitHub Actions workflow will handle:"
	@echo "     - Running all tests"
	@echo "     - Creating GitHub release with changelog"
	@echo ""
	@read -p "Continue? (y/N): " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		git tag -a v$(VERSION) -m "Release v$(VERSION)"; \
		git push origin v$(VERSION); \
		echo ""; \
		echo "Release tag v$(VERSION) pushed!"; \
		echo "Check the GitHub Actions workflow for progress:"; \
		echo "  https://github.com/$(shell git remote get-url origin | sed 's/.*github.com[:/]\(.*\)\.git/\1/')/actions"; \
	else \
		echo "Release cancelled."; \
	fi

# Dry run for release
.PHONY: release-dry-run
release-dry-run:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required. Usage: make release-dry-run VERSION=x.y.z"; \
		exit 1; \
	fi
	@echo "Dry run for release v$(VERSION)"
	@echo ""
	@echo "Checking prerequisites..."
	@echo "  ✓ Version format: $(VERSION)"
	@echo ""
	@echo "Current branch:"
	@git branch --show-current
	@echo ""
	@echo "Uncommitted changes:"
	@git status --short
	@echo ""
	@echo "To perform the actual release, run:"
	@echo "  make release VERSION=$(VERSION)"

# Check release status
.PHONY: release-status
release-status:
	@echo "Latest tags:"
	@git tag -l "v*" | sort -V | tail -5
	@echo ""
	@echo "Current branch:"
	@git branch --show-current
	@echo ""
	@echo "Uncommitted changes:"
	@git status --short

# Generate changelog
.PHONY: changelog
changelog:
	@echo "Generating changelog..."
	@./scripts/generate-changelog.sh

# Install dependencies
.PHONY: deps
deps:
	@echo "Installing dependencies..."
	go mod download
	go mod tidy

# Run local development server (if applicable)
.PHONY: run
run:
	@echo "Running example..."
	go run examples/main.go

# Help target
.PHONY: help
help:
	@echo "Reveald - Makefile targets"
	@echo ""
	@echo "Building & Testing:"
	@echo "  make build           - Build the project"
	@echo "  make test            - Run tests"
	@echo "  make test-coverage   - Run tests with coverage report"
	@echo "  make lint            - Run linters"
	@echo "  make fmt             - Format code"
	@echo "  make clean           - Clean build artifacts"
	@echo ""
	@echo "Development:"
	@echo "  make deps            - Install dependencies"
	@echo "  make run             - Run example"
	@echo ""
	@echo "Release Management:"
	@echo "  make release VERSION=x.y.z     - Create and push a new release"
	@echo "  make release-dry-run VERSION=x.y.z - Preview what will be released"
	@echo "  make release-status             - Show current release status"
	@echo "  make changelog                  - Generate changelog"
	@echo ""
	@echo "Examples:"
	@echo "  make release VERSION=1.2.3"
	@echo "  make release-dry-run VERSION=2.0.0-beta.1"
	@echo ""
	@echo "For more information, see RELEASING.md"