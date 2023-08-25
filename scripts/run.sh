#!/bin/bash
set -euo pipefail

./draino --kubeconfig ~/.kube/config --allow-force-delete --debug --evict-emptydir-pods --evict-statefulset-pods --evict-unreplicated-pods --eviction-headroom=10m --ignore-safe-to-evict-annotation --max-grace-period=1m ContainerRuntimeUnhealthy KernelDeadlock FrequentKubeletRestart