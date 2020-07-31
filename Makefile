NAME	:= gcssync
VERSION	:= 0.9.6

SRCS	:= $(shell find . -type f -name '*.go')
LDFLAGS := -ldflags="-s -w -X \"main.Version=$(VERSION)\" -extldflags \"-static\""
.DEFAULT_GOAL := bin/$(NAME)

.PHONY: deps
deps:
	go get -u cloud.google.com/go/storage

bin/$(NAME): $(SRCS)
	go build -a -tags netgo -installsuffix netgo $(LDFLAGS) -o bin/$(NAME)

.PHONY: install
install:
	go install $(LDFLAGS)

.PHONY: clean
clean:
	rm -rf bin/*
	rm -rf dist/*

.PHONY: cross-build
cross-build:
	for os in darwin linux windows; do \
		for arch in amd64 386; do \
			GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build -a -tags netgo -installsuffix netgo $(LDFLAGS) -o dist/$$os-$$arch/$(NAME); \
		done; \
	done

