# Go parameters
GOCMD = go
GOBUILD = $(GOCMD) build -ldflags ${LDFLAGS}
GOCLEAN = $(GOCMD) clean
GOTEST = $(GOCMD) test
BINARY_NAME = binlog2sql_go
BINARY_UNIX = $(BINARY_NAME)
VERSION = $(shell git describe --tags --always)
GIT_COMMIT = $(shell git rev-parse --short HEAD)
BUILD_TIME = $(shell date -R)

define LDFLAGS
"-X 'binlog2sql_go/conf.version=${VERSION}'\
-X 'binlog2sql_go/conf.gitCommit=${GIT_COMMIT}'\
-X 'binlog2sql_go/conf.buildTime=$(BUILD_TIME)'"
endef

all: test build

build:
	$(GOBUILD) -o $(BINARY_NAME) -v

test:
	$(GOTEST) -v ./...

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_UNIX)

run:
	$(GOBUILD) -o $(BINARY_NAME) -v ./...
	./$(BINARY_NAME)



# Cross compilation
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(BINARY_UNIX) -v
