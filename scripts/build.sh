#!/usr/bin/env bash

VERSION=$(git rev-parse --short HEAD)
docker build --tag "negz/draino:latest" .
docker build --tag "negz/draino:${VERSION}" .
