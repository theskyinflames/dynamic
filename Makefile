test:
	go test -v --race ./...

lint:
	golangci-lint run
	revive -config revive.toml ./...
	find . -type f -name "*.go" -not -name "*generated.go" -not -name "*zmock*.go" | xargs gofumpt -s -w -l
