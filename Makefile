init:
	go get golang.org/x/tools/cmd/goimports
	# go get -u github.com/golang/protobuf/protoc-gen-go

lint:
	gofmt -s -w .
	go mod tidy
	golangci-lint run
