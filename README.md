# draino [![Docker Pulls](https://img.shields.io/docker/pulls/negz/draino.svg)](https://hub.docker.com/r/negz/draino/) [![Godoc](https://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/negz/draino) [![Travis](https://img.shields.io/travis/negz/draino.svg?maxAge=300)](https://travis-ci.org/negz/draino/) [![Codecov](https://img.shields.io/codecov/c/github/negz/draino.svg?maxAge=3600)](https://codecov.io/gh/negz/draino/)
Draino automatically drains Kubernetes nodes based on labels and node
conditions. Nodes that match _all_ of the supplied labels and _any_ of the
supplied node conditions will be cordoned immediately and drained after a
configurable `drain-buffer` time.

Draino is intended for use alongside the Kubernetes [Node Problem Detector](https://github.com/kubernetes/node-problem-detector)
and [Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler).
The Node Problem Detector can set a node condition when it detects something
wrong with a node - for instance by watching node logs or running a script. The
Cluster Autoscaler can be configured to delete nodes that are underutilised.
Adding Draino to the mix enables autoremediation:

1. The Node Problem Detector detects a permanent node issue and sets the
   corresponding node condition.
2. Draino notices the node condition. It immediately cordons the node to prevent
   new pods being scheduled there, and schedules a drain of the node.
3. Once the node has been drained the Cluster Autoscaler will consider it
   underutilised. It will be eligible for scale down (i.e. termination) by the
   Autoscaler after a configurable period of time.

## Usage
```
$ docker run negz/draino /draino --help
usage: draino [<flags>] [<node-conditions>...]

Automatically cordons and drains nodes that match the supplied conditions.

Flags:
      --help                   Show context-sensitive help (also try --help-long
                               and --help-man).
  -d, --debug                  Run with debug logging.
      --listen=":10002"        Address at which to expose /metrics and /healthz.
      --kubeconfig=KUBECONFIG  Path to kubeconfig file. Leave unset to use
                               in-cluster config.
      --master=MASTER          Address of Kubernetes API server. Leave unset to
                               use in-cluster config.
      --dry-run                Emit an event without cordoning or draining
                               matching nodes.
      --max-grace-period=8m0s  Maximum time evicted pods will be given to
                               terminate gracefully.
      --eviction-headroom=30s  Additional time to wait after a pod's termination
                               grace period for it to have been deleted.
      --drain-buffer=10m0s     Minimum time between starting each drain. Nodes
                               are always cordoned immediately.
      --node-label=KEY=VALUE ...  
                               Only nodes with this label will be eligible for
                               cordoning and draining. May be specified multiple
                               times.

Args:
  [<node-conditions>]  Nodes for which any of these conditions are true will be
                       cordoned and drained.
```

## Deployment
Draino is automatically built from master and pushed to the [Docker Hub](https://hub.docker.com/r/negz/draino/).
Builds are tagged `negz/draino:latest` and `negz/drain:$(git rev-parse --short HEAD)`.
An [example Kubernetes deployment manifest](manifest.yml) is provided.