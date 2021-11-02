FROM golang:1.17-alpine3.13

RUN apk add --no-cache \
    bash \
    ca-certificates \
    gcc \
    curl \
    docker \
    git \
    make \
    musl-dev \
    openssl \
    patch \
    python3 \
    py3-pip

RUN set -x && update-ca-certificates

ENV GO111MODULE="on"
