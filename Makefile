BASEREV = $(shell git merge-base HEAD origin/main)

all: clean build

build :
	go install go.k6.io/xk6/cmd/xk6@latest && CGO_ENABLED=1 xk6 build --with $(shell go list -m)=.

format :
	go fmt ./...

lint :
	golangci-lint run --timeout=3m --out-format=tab --new-from-rev "$(BASEREV)" ./...

tests :
	go test -cover -race ./...

clean:
	rm -f ./k6

.PHONY: build clean format lint tests

