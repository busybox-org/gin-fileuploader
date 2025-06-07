FROM cr.yfdou.com/golang:latest AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY fileuploader .

# Install upx
RUN sed -i "s/deb.debian.org/mirrors.aliyun.com/g" /etc/apt/sources.list.d/* \
    && sed -i "s/security.debian.org/mirrors.aliyun.com/g" /etc/apt/sources.list.d/* \
    && apt-get update \
    && apt-get install git tar xz-utils -y \
	&& wget https://github.com/upx/upx/releases/download/v5.0.0/upx-5.0.0-amd64_linux.tar.xz \
	&& tar -xf upx-5.0.0-amd64_linux.tar.xz \
	&& mv upx-5.0.0-amd64_linux/upx /usr/local/bin/upx \
	&& rm -rf upx-5.0.0-amd64_linux*

# Build
# the GOARCH has not a default value to allow the binary be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -trimpath -ldflags '-w -s' -o uploader cmd/main.go \
    && strip --strip-unneeded uploader \
    && upx --lzma uploader

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM cr.yfdou.com/gcr.io/distroless/static:latest
WORKDIR /
COPY --from=builder /workspace/uploader .
USER root:root

ENTRYPOINT ["/uploader"]