default: build

build:
	go build ./...

test:
	go test ./... -coverprofile=c1.tmp
	cat c1.tmp | grep -v "mstore_gc.go" > coverage.out
	go tool cover -html=coverage.out


	