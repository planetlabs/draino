all: build

# Build manager binary
build: fmt vet
        CGO_ENABLED=0 go build -o bin/draino ./cmd/draino/*.go

# Run unittest
test: fmt vet
	go test ./...

# Run go fmt against code
fmt:
	go fmt ./...

# Run go vet against code
vet:
	go vet ./...
