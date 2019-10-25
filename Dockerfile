FROM golang:1.13.1-alpine3.10 AS build

RUN apk update && apk add git && apk add curl

WORKDIR /go/src/github.com/planetlabs/draino
COPY . .

RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
RUN dep ensure
RUN go build -o /draino ./cmd/draino

FROM alpine:3.10

RUN apk update && apk add ca-certificates
COPY --from=build /draino /draino
