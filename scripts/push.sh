#!/usr/bin/env bash

set -e

VERSION=$(git rev-parse --short HEAD)
docker push "planetlabs/draino:latest"
docker push "planetlabs/draino:${VERSION}"
