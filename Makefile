VERSION ?= latest
REVISION := $(shell git rev-parse --short HEAD)

UNAME_OS := $(shell uname -s)
UNAME_ARCH := $(shell uname -m)
ifeq ($(UNAME_OS),Linux)
  ifeq ($(UNAME_ARCH),aarch64)
    STATIC_LINK_FLAG := -v -linkmode external -extldflags "-static -fPIC -v"
  else
    STATIC_LINK_FLAG := -linkmode external -extldflags "-static -v"
  endif
endif

emulator/build:
	CGO_ENABLED=1 CXX=clang++ CGO_LDFLAGS="-fuse-ld=lld" CGO_CFLAGS="-fPIC" CGO_CXXFLAGS="-fPIC" go build -work -a -x -o bigquery-emulator \
		-ldflags='${STATIC_LINK_FLAG}' \
		./cmd/bigquery-emulator

docker/build:
	docker build -t bigquery-emulator . --build-arg VERSION=${VERSION}
