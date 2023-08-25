#!/bin/bash
set -euo pipefail

VERSION=$(git rev-parse --short HEAD)
docker build --tag "planetlabs/draino:${VERSION}" .
