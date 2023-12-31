PROJ_NAME = gnidump 
VERSION = $(shell git describe --tags)
VER = $(shell git describe --tags --abbrev=0)
DATE = $(shell date -u '+%Y-%m-%d_%H:%M:%S%Z')

NO_C = CGO_ENABLED=0

FLAGS_SHARED = $(NO_C)
FLAGS_LD = -ldflags "-X github.com/gnames/$(PROJ_NAME)/pkg.Build=$(DATE) \
                     -X github.com/gnames/$(PROJ_NAME)/pkg.Version=$(VERSION)"

GOCMD=go
GOINSTALL=$(GOCMD) install
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean

all: install

test: deps install
	$(FLAG_MODULE) go test ./...

## Dependencies
deps: ## Download dependencies
	$(GOCMD) mod download;

## Tools
tools: deps ## Install tools
	@cat tools.go | grep _ | awk -F'"' '{print $$2}' | xargs -tI % go install %



## Build:
build: ## Build binary
	$(FLAGS_SHARED) $(GOBUILD)  $(PROJ_NAME) $(FLAGS_LD)

## Install:
install: ## Build and install binary
	$(NO_C) $(GOINSTALL)

