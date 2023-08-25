#!/bin/bash
set -euo pipefail

VERSION=$(git rev-parse --short HEAD)
docker push "planetlabs/draino:${VERSION}"
