#!/usr/bin/env bash

VERSION=$(git rev-parse --short HEAD)
docker build --tag "planetlabs/draino:latest" .
docker build --tag "planetlabs/draino:${VERSION}" .
