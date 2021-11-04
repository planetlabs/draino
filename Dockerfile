# This dockerfile is used for testing, at release time, compute delivery uses its own Dockerfile
FROM golang:1.13.15-alpine3.11 AS build

RUN apk update && apk add git && apk add curl

WORKDIR /go/src/github.com/planetlabs/draino
COPY . .

RUN go build -o /draino ./cmd/draino

FROM alpine:3.11

RUN apk update && apk add ca-certificates
RUN addgroup -S user && adduser -S user -G user
USER user
COPY --from=build /draino /draino
ENV PATH="/:${PATH}"
