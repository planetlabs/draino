FROM golang:1.19-alpine3.15

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

RUN pip3 install awscli
