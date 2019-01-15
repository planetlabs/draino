# draino [![Docker Pulls](https://img.shields.io/docker/pulls/planetlabs/draino.svg)](https://hub.docker.com/r/planetlabs/draino/) [![Godoc](https://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/planetlabs/draino) [![Travis](https://img.shields.io/travis/org/planetlabs/draino.svg?maxAge=300)](https://travis-ci.org/planetlabs/draino/) [![Codecov](https://img.shields.io/codecov/c/github/planetlabs/draino.svg?maxAge=3600)](https://codecov.io/gh/planetlabs/draino/)
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

1. The Node Problem Detector detects a permanent node problem and sets the
   corresponding node condition.
2. Draino notices the node condition. It immediately cordons the node to prevent
   new pods being scheduled there, and schedules a drain of the node.
3. Once the node has been drained the Cluster Autoscaler will consider it
   underutilised. It will be eligible for scale down (i.e. termination) by the
   Autoscaler after a configurable period of time.

## Usage
```
$ docker run planetlabs/draino /draino --help
usage: draino [<flags>] [<node-conditions>...]

Automatically cordons and drains nodes that match the supplied conditions.

Flags:
      --help                     Show context-sensitive help (also try --help-long and --help-man).
  -d, --debug                    Run with debug logging.
      --listen=":10002"          Address at which to expose /metrics and /healthz.
      --kubeconfig=KUBECONFIG    Path to kubeconfig file. Leave unset to use in-cluster config.
      --master=MASTER            Address of Kubernetes API server. Leave unset to use in-cluster config.
      --dry-run                  Emit an event without cordoning or draining matching nodes.
      --max-grace-period=8m0s    Maximum time evicted pods will be given to terminate gracefully.
      --eviction-headroom=30s    Additional time to wait after a pod's termination grace period for it to have been deleted.
      --drain-buffer=10m0s       Minimum time between starting each drain. Nodes are always cordoned immediately.
      --node-label=KEY=VALUE ...
                                 Only nodes with this label will be eligible for cordoning and draining. May be specified multiple times.
      --evict-daemonset-pods     Evict pods that were created by an extant DaemonSet.
      --evict-emptydir-pods      Evict pods with local storage, i.e. with emptyDir volumes.
      --evict-unreplicated-pods  Evict pods that were not created by a replication controller.

Args:
  [<node-conditions>]  Nodes for which any of these conditions are true will be cordoned and drained.

```

## Considerations
Keep the following in mind before deploying Draino:

* Always run Draino in `--dry-run` mode first to ensure it would drain the nodes
  you expect it to. In dry run mode Draino will emit logs, metrics, and events
  but will not actually cordon or drain nodes.
* Draino immediately cordons nodes that match its configured labels and node
  conditions, but will wait a configurable amount of time (10 minutes by default)
  between draining nodes. i.e. If two nodes begin exhibiting a node condition
  simultaneously one node will be drained immediately and the other in 10 minutes.
* Draino considers a drain to have failed if at least one pod eviction triggered
  by that drain fails. If Draino fails to evict two of five pods it will consider
  the Drain to have failed, but the remaining three pods will always be evicted.

## Deployment
Draino is automatically built from master and pushed to the [Docker Hub](https://hub.docker.com/r/planetlabs/draino/).
Builds are tagged `planetlabs/draino:latest` and `planetlabs/draino:$(git rev-parse --short HEAD)`.
An [example Kubernetes deployment manifest](manifest.yml) is provided.

## Monitoring
Draino provides a simple healthcheck endpoint at `/healthz` and Prometheus
metrics at `/metrics`. The following metrics exist:

```bash
$ kubectl -n kube-system exec -it ${DRAINO_POD} -- apk add curl
$ kubectl -n kube-system exec -it ${DRAINO_POD} -- curl http://localhost:10002/metrics
# HELP draino_cordoned_nodes_total Number of nodes cordoned.
# TYPE draino_cordoned_nodes_total counter
draino_cordoned_nodes_total{result="succeeded"} 2
draino_cordoned_nodes_total{result="failed"} 1
# HELP draino_drained_nodes_total Number of nodes drained.
# TYPE draino_drained_nodes_total counter
draino_drained_nodes_total{result="succeeded"} 1
draino_drained_nodes_total{result="failed"} 1
```
