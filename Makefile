VERSION ?= latest
REVISION := $(shell git rev-parse --short HEAD)

emulator/build:
	CGO_ENABLED=1 CXX=clang++ CGO_CFLAGS="-fPIC" CGO_CXXFLAGS="-fPIC" go build -work -a -x -o bigquery-emulator \
		-ldflags='${STATIC_LINK_FLAG}' \
		./cmd/bigquery-emulator

docker/build:
	docker build -t bigquery-emulator . --build-arg VERSION=${VERSION}
