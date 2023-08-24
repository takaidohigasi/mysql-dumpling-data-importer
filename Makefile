VERSION=$(shell cat VERSION)
BINARY_NAME=mysql-dumpling-data-importer
LDFLAGS=-ldflags "-X $$(head -n1 go.mod | awk '{print $$2}')/cmd.Version=$(VERSION) -w -s"

.PHONY: test clean build fmt release
fmt:
	go fmt ./...
clean:
	rm -rf ./dist/*
	touch ./dist/.gitkeep
test:
	# MallocNanoZone=0 is workaround for SIGABRT with race on macOS Monterey: https://github.com/golang/go/issues/49138
	MallocNanoZone=0 go test -v -race -cover ./...

linux:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -trimpath -o ./dist/$(BINARY_NAME) main.go

build: clean
	CGO_ENABLED=1 GOARCH=amd64 go build $(LDFLAGS) -trimpath -o ./dist/$(BINARY_NAME) main.go

release: build
	cd dist && zip -r $(VERSION).zip $(BINARY_NAME) && rm -rf $(BINARY_NAME)
