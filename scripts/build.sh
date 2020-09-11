#!/usr/bin/env bash

set -e

VERSION=$(git rev-parse --short HEAD)
docker build --tag "planetlabs/draino:${VERSION}" .
