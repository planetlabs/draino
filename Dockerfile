FROM golang:1.10-alpine3.8 AS build

RUN apk update && apk add git && apk add curl

WORKDIR /go/src/github.com/planetlabs/draino
COPY . .

RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
RUN dep ensure
RUN go build -o /draino ./cmd/draino

FROM alpine:3.8

RUN apk update && apk add ca-certificates
COPY --from=build /draino /draino
