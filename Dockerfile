FROM golang:1.13.1-alpine3.10 AS build

RUN apk update && apk add git && apk add curl

WORKDIR /go/src/github.com/planetlabs/draino
COPY . .

RUN go mod tidy && \
    go mod download && \
    go build -o /draino ./cmd/draino

FROM alpine:3.10

RUN apk update && apk add ca-certificates
RUN addgroup -S user && adduser -S user -G user
USER user
COPY --from=build /draino /draino
