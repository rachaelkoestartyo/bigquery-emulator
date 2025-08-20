VERSION ?= latest
REVISION := $(shell git rev-parse --short HEAD)

emulator/build:
	CGO_ENABLED=1 CXX=clang++ go build -o bigquery-emulator \
		./cmd/bigquery-emulator

docker/build:
	docker build -t bigquery-emulator . --build-arg VERSION=${VERSION}
