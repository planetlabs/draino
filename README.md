# draino [![Docker Pulls](https://img.shields.io/docker/pulls/negz/draino.svg)](https://hub.docker.com/r/negz/draino/) [![Godoc](https://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/negz/draino) [![Travis](https://img.shields.io/travis/negz/draino.svg?maxAge=300)](https://travis-ci.org/negz/draino/) [![Codecov](https://img.shields.io/codecov/c/github/negz/draino.svg?maxAge=3600)](https://codecov.io/gh/negz/draino/)
Automatically cordon and drain Kubernetes nodes based on node conditions.

Usage:
```
usage: draino [<flags>] [<node-conditions>...]

Automatically cordons and drains nodes that match the supplied conditions.

Flags:
      --help                   Show context-sensitive help (also try --help-long and --help-man).
  -d, --debug                  Run with debug logging.
      --listen=":10002"        Address at which to expose /metrics and /healthz.
      --kubeconfig=KUBECONFIG  Path to kubeconfig file. Leave unset to use in-cluster config.
      --master=MASTER          Address of Kubernetes API server. Leave unset to use in-cluster config.
      --dry-run                Emit an event without cordoning or draining matching nodes.
      --max-grace-period=8m0s  The maximum time evicted pods will be given to terminate gracefully.
      --eviction-headroom=30s  The additional time to wait after a pod's termination grace period for it to have been deleted.
      --node-label=KEY=VALUE ...
                               Only nodes with this label will be eligible for cordoning and draining. May be specified multiple times.

Args:
  [<node-conditions>]  Nodes for which any of these conditions are true will be cordoned and drained.
```