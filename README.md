# draino [![Docker Pulls](https://img.shields.io/docker/pulls/planetlabs/draino.svg)](https://hub.docker.com/r/planetlabs/draino/) [![Godoc](https://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/planetlabs/draino) [![Travis](https://img.shields.io/travis/com/planetlabs/draino.svg?maxAge=300)](https://travis-ci.com/planetlabs/draino/) [![Codecov](https://img.shields.io/codecov/c/github/planetlabs/draino.svg?maxAge=3600)](https://codecov.io/gh/planetlabs/draino/)
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
usage: draino [<flags>] <node-conditions>...

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
      --node-label="foo=bar"     (DEPRECATED) Only nodes with this label will be eligible for cordoning and draining. May be specified multiple times.
      --node-label-expr="metadata.labels.foo == 'bar'"
                                 This is an expr string https://github.com/antonmedv/expr that must return true or false. See `nodefilters_test.go` for examples
      --namespace="kube-system"  Namespace used to create leader election lock object.	
      --leader-election-lease-duration=15s
                                 Lease duration for leader election.
      --leader-election-renew-deadline=10s
                                 Leader election renew deadline.
      --leader-election-retry-period=2s
                                 Leader election retry period.
      --skip-drain               Whether to skip draining nodes after cordoning.
      --evict-daemonset-pods     Evict pods that were created by an extant DaemonSet.
      --evict-emptydir-pods      Evict pods with local storage, i.e. with emptyDir volumes.
      --evict-unreplicated-pods  Evict pods that were not created by a replication controller.
      --protected-pod-annotation=KEY[=VALUE] ...
                                 Protect pods with this annotation from eviction. May be specified multiple times.

Args:
  <node-conditions>  Nodes for which any of these conditions are true will be cordoned and drained.
```

### Labels and Label Expressions

Draino allows filtering the elligible set of nodes using `--node-label` and `--node-label-expr`.
The original flag `--node-label` is limited to the boolean AND of the specified labels. To express more complex predicates, the new `--node-label-expr`
flag allows for mixed OR/AND/NOT logic via https://github.com/antonmedv/expr.

An example of `--node-label-expr`:

```
(metadata.labels.region == 'us-west-1' && metadata.labels.app == 'nginx') || (metadata.labels.region == 'us-west-2' && metadata.labels.app == 'nginx')
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
* Pods that can't be evicted by the cluster-autoscaler won't be evicted by draino.
  See annotation `"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"` in
  [cluster-autoscaler documentation](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md#what-types-of-pods-can-prevent-ca-from-removing-a-node)

## Deployment

Draino is automatically built from master and pushed to the [Docker Hub](https://hub.docker.com/r/planetlabs/draino/).
Builds are tagged `planetlabs/draino:$(git rev-parse --short HEAD)`.

**Note:** As of September, 2020 we no longer publish `planetlabs/draino:latest`
in order to encourage explicit and pinned releases.

An [example Kubernetes deployment manifest](manifest.yml) is provided.

## Monitoring

### Metrics
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

### Events
Draino is generating event for every relevant step of the eviction process. Here is an example that ends with a reason `DrainFailed`. When everything is fine the last event for a given node will have a reason `DrainSucceeded`.
```
> kubectl get events -n default | grep -E '(^LAST|draino)'

LAST SEEN   FIRST SEEN   COUNT   NAME                                               KIND TYPE      REASON             SOURCE MESSAGE
5m          5m           1       node-demo.15fe0c35f0b4bd10    Node Warning   CordonStarting     draino Cordoning node
5m          5m           1       node-demo.15fe0c35fe3386d8    Node Warning   CordonSucceeded    draino Cordoned node
5m          5m           1       node-demo.15fe0c360bd516f8    Node Warning   DrainScheduled     draino Will drain node after 2020-03-20T16:19:14.91905+01:00
5m          5m           1       node-demo.15fe0c3852986fe8    Node Warning   DrainStarting      draino Draining node
4m          4m           1       node-demo.15fe0c48d010ecb0    Node Warning   DrainFailed        draino Draining failed: timed out waiting for evictions to complete: timed out
```

### Conditions
When a drain is scheduled, on top of the event, a condition is added to the status of the node. This condition will hold information about the beginning and the end of the drain procedure. This is something that you can see by describing the node resource:

```
> kubectl describe node {node-name}
......
Unschedulable:      true
Conditions:
  Type                  Status  LastHeartbeatTime                 LastTransitionTime                Reason                       Message
  ----                  ------  -----------------                 ------------------                ------                       -------
  OutOfDisk             False   Fri, 20 Mar 2020 15:52:41 +0100   Fri, 20 Mar 2020 14:01:59 +0100   KubeletHasSufficientDisk     kubelet has sufficient disk space available
  MemoryPressure        False   Fri, 20 Mar 2020 15:52:41 +0100   Fri, 20 Mar 2020 14:01:59 +0100   KubeletHasSufficientMemory   kubelet has sufficient memory available
  DiskPressure          False   Fri, 20 Mar 2020 15:52:41 +0100   Fri, 20 Mar 2020 14:01:59 +0100   KubeletHasNoDiskPressure     kubelet has no disk pressure
  PIDPressure           False   Fri, 20 Mar 2020 15:52:41 +0100   Fri, 20 Mar 2020 14:01:59 +0100   KubeletHasSufficientPID      kubelet has sufficient PID available
  Ready                 True    Fri, 20 Mar 2020 15:52:41 +0100   Fri, 20 Mar 2020 14:02:09 +0100   KubeletReady                 kubelet is posting ready status. AppArmor enabled
  ec2-host-retirement   True    Fri, 20 Mar 2020 15:23:26 +0100   Fri, 20 Mar 2020 15:23:26 +0100   NodeProblemDetector          Condition added with tooling
  DrainScheduled        True    Fri, 20 Mar 2020 15:50:50 +0100   Fri, 20 Mar 2020 15:23:26 +0100   Draino                       Drain activity scheduled 2020-03-20T15:50:34+01:00
```

  Later when the drain activity will be completed the condition will be amended letting you know if it succeeded of failed:

```
> kubectl describe node {node-name}
......
Unschedulable:      true
Conditions:
  Type                  Status  LastHeartbeatTime                 LastTransitionTime                Reason                       Message
  ----                  ------  -----------------                 ------------------                ------                       -------
  OutOfDisk             False   Fri, 20 Mar 2020 15:52:41 +0100   Fri, 20 Mar 2020 14:01:59 +0100   KubeletHasSufficientDisk     kubelet has sufficient disk space available
  MemoryPressure        False   Fri, 20 Mar 2020 15:52:41 +0100   Fri, 20 Mar 2020 14:01:59 +0100   KubeletHasSufficientMemory   kubelet has sufficient memory available
  DiskPressure          False   Fri, 20 Mar 2020 15:52:41 +0100   Fri, 20 Mar 2020 14:01:59 +0100   KubeletHasNoDiskPressure     kubelet has no disk pressure
  PIDPressure           False   Fri, 20 Mar 2020 15:52:41 +0100   Fri, 20 Mar 2020 14:01:59 +0100   KubeletHasSufficientPID      kubelet has sufficient PID available
  Ready                 True    Fri, 20 Mar 2020 15:52:41 +0100   Fri, 20 Mar 2020 14:02:09 +0100   KubeletReady                 kubelet is posting ready status. AppArmor enabled
  ec2-host-retirement   True    Fri, 20 Mar 2020 15:23:26 +0100   Fri, 20 Mar 2020 15:23:26 +0100   NodeProblemDetector          Condition added with tooling
  DrainScheduled        True    Fri, 20 Mar 2020 15:50:50 +0100   Fri, 20 Mar 2020 15:23:26 +0100   Draino                       Drain activity scheduled 2020-03-20T15:50:34+01:00 | Completed: 2020-03-20T15:50:50+01:00
  ```

If the drain had failed the condition line would look like:
```
  DrainScheduled        True    Fri, 20 Mar 2020 15:50:50 +0100   Fri, 20 Mar 2020 15:23:26 +0100   Draino                       Drain activity scheduled 2020-03-20T15:50:34+01:00| Failed:2020-03-20T15:55:50+01:00
```

## Retrying drain

In some cases the drain activity may failed because of restrictive Pod Disruption Budget or any other reason external to Draino. The node remains `cordon` and the drain condition 
is marked as `Failed`. If you want to reschedule a drain tentative on that node, add the annotation: `draino/drain-retry: true`. A new drain schedule will be created. Note that the annotation is not modified and will trigger retries in loop in case the drain fails again.

```
kubectl annotate node {node-name} draino/drain-retry=true
```
## Modes

### Dry Run
Draino can be run in dry run mode using the `--dry-run` flag.

### Cordon Only
Draino can also optionally be run in a mode where the nodes are only cordoned, and not drained. This can be achieved by using the `--skip-drain` flag.
