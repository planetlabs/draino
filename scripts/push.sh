#!/usr/bin/env bash

set -e

VERSION=$(git rev-parse --short HEAD)
docker push "negz/draino:latest"
docker push "negz/draino:${VERSION}"
