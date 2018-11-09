FROM golang:1.10-alpine3.8 AS build

RUN apk update && apk add git

WORKDIR /go/src/github.com/planetlabs/draino
COPY . .

RUN go get -u github.com/Masterminds/glide
RUN glide install
RUN go build -o /draino ./cmd/draino

FROM alpine:3.8

RUN apk update && apk add ca-certificates
COPY --from=build /draino /draino