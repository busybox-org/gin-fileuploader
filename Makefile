.PHONY: build docker
build:
	@echo "Building file-uploader"
	@go build -trimpath -ldflags "-w -s" -o bin/file-uploader cmd/main.go

docker:
	@echo "Building docker image"
	@docker build -t file-uploader:latest .