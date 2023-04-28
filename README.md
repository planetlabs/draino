# Project maintenance

This is a fork of `planetlabs/draino`. This fork is public because our primary intention was to contribute to the original project. With time the original project was not maintained anymore (difficulties to have reviews, and changes not integrated). This code as diverged a lot compare to the original project. Since the initial design is blocking us for some evolutions, we will soon stop maintaining that public fork soon (before end of 2022). We will maybe come back with a new equivalent project that we be owned and maintained by datadog, with more feature in te domain of workload eviction and node replacement

# draino [![Docker Pulls](https://img.shields.io/docker/pulls/planetlabs/draino.svg)](https://hub.docker.com/r/planetlabs/draino/) [![Godoc](https://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/planetlabs/draino) [![Travis](https://img.shields.io/travis/com/planetlabs/draino.svg?maxAge=300)](https://travis-ci.com/planetlabs/draino/) [![Codecov](https://img.shields.io/codecov/c/github/planetlabs/draino.svg?maxAge=3600)](https://codecov.io/gh/planetlabs/draino/)
Draino automatically drains Kubernetes nodes based on labels and node
conditions. Nodes that match _all_ of the supplied labels and _any_ of the
supplied node conditions will be tainted immediately and drained after a
configurable `drain-buffer` time.

Draino is intended for use alongside the Kubernetes [Node Problem Detector](https://github.com/kubernetes/node-problem-detector)
and [Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler).
The Node Problem Detector can set a node condition when it detects something
wrong with a node - for instance by watching node logs or running a script. The
Cluster Autoscaler can be configured to delete nodes that are underutilised.
Adding Draino to the mix enables autoremediation:

1. The Node Problem Detector detects a permanent node problem and sets the
   corresponding node condition.
2. Draino notices the node condition. When the node is eligible (priorities/filters) it taints the node to prevent
   new pods being scheduled there, and schedules a drain of the node.
3. Once the node has been drained the Cluster Autoscaler will consider it
   underutilised. It will be eligible for scale down (i.e. termination) by the
   Autoscaler after a configurable period of time.

## Usage
```
Usage:
   [flags]
   [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  group       
  help        Help about any command
  log         
  node        
  version     

Flags:
      --candidate-emptydir-pods                    Evict pods with local storage, i.e. with emptyDir volumes. (default true)
      --cloud-provider string                      cloud provider where the application/controller is running
      --cloud-provider-project string              cloud provider project where the application/controller is running. Only make sense for gcp
      --config-name string                         Name of the draino configuration
      --context string                             kubernetes context
      --cordon-protected-pod-annotation strings    Protect nodes hosting pods with this annotation from being candidate. May be specified multiple times. KEY[=VALUE]
      --datacenter string                          datacenter where the application/controller is running
      --debug                                      Run with debug logging.
      --do-not-cordon-pod-controlled-by strings    Do not make candidate nodes hosting pods that are controlled by the designated kind, empty VALUE for uncontrolled pods, May be specified multiple times. kind[[.version].group]] examples: StatefulSets StatefulSets.apps StatefulSets.apps.v1 (default [,StatefulSet])
      --do-not-evict-pod-controlled-by strings     Do not evict pods that are controlled by the designated kind, empty VALUE for uncontrolled pods, May be specified multiple times: kind[[.version].group]] examples: StatefulSets StatefulSets.apps StatefulSets.apps.v1 (default [,StatefulSet,DaemonSet])
      --drain-buffer duration                      Delay to respect between end of previous drain (success or error) and a new attempt within a drain-group. (default 10m0s)
      --drain-buffer-configmap-name string         The name of the configmap used to persist the drain-buffer values. Default will be draino-<config-name>-drain-buffer.
      --drain-group-labels string                  Comma separated list of label keys to be used to form draining groups. KEY1,KEY2,...
      --drain-rate-limit-burst int                 Maximum number of parallel drains within a timeframe (default 1)
      --drain-rate-limit-qps float32               Maximum number of node drains per seconds per condition (default 0.016666668)
      --drain-sim-rate-limit-ratio float32         Which ratio of the overall kube client rate limiting should be used by the drain simulation. 1.0 means that it will use the same. (default 0.7)
      --dry-run                                    Emit an event without tainting or draining matching nodes.
      --duration-before-replacement duration       Max duration we are waiting for a node with Completed drain status to be removed before asking for replacement. (default 1h0m0s)
      --encoding string                            output logs; one of json, json-kube, console (default "json-kube")
      --event-aggregation-period duration          Period for event generation on kubernetes object. (default 15m0s)
      --evict-emptydir-pods                        Evict pods with local storage, i.e. with emptyDir volumes.
      --eviction-headroom duration                 Additional time to wait after a pod's termination grace period for it to have been deleted. (default 30s)
      --exclude-sts-on-node-without-storage        To ensure backward compatibility with draino v1, we have to exclude pod of STS running on node without local-storage (default true)
      --excluded-pod-per-node-estimation int       Estimation of the number of pods that should be excluded from nodes. Used to compute some event cache size. (default 5)
      --group-runner-period duration               Period for running the group runner (default 10s)
  -h, --help                                       help for this command
      --informer-namespace string                  restricts the manager's cache to watch objects in the desired namespace Defaults to all namespaces
      --informer-sync-period duration              minimum frequency at which watched resources are reconciled (default 1h0m0s)
      --klog-verbosity int32                       Verbosity to run klog at (default 4)
      --kube-address string                        kube apiserver address (optional)
      --kube-client-burst int                      Burst to use in kube client (default 10)
      --kube-client-qps float32                    QPS to use in kube client (default 5)
      --kube-cluster-name string                   name of the kubernetes cluster where the application/controller is running
      --kube-config string                         kubeconfig file; empty defaults to in-cluster config
      --kubeconfig string                          Path to kubeconfig file. Leave unset to use in-cluster config.
      --leader-elect                               whether or not to use leader election when starting the manager (default true)
      --leader-elect-id string                     name of the configmap that leader election will use for holding the leader lock
      --leader-elect-lease duration                non-leader candidates will wait to force acquire leadership. This is measured against time of last observed ack (default 15s)
      --leader-elect-namespace string              namespace in which the leader election configmap will be created
      --leader-elect-renew duration                acting master will retry refreshing leadership before giving up (default 10s)
      --leader-elect-retry duration                clients should wait between tries of actions (default 2s)
      --leader-resource-lock string                type of resource that leader election will use for holding the leader lock (default "configmaps")
      --listen string                              Address at which to expose /metrics and /healthz. (default ":10002")
      --log-development                            development mode for logs. This disables the sampling and allows for negative level (beyond Debug that is (-1))
      --log-events                                 Indicate if events sent to kubernetes should also be logged (default true)
      --log-level string                           log level; one of debug, info, warn, error, dpanic, panic, fatal (default "info")
      --log-stacktrace string                      log stacktrace; one of debug, info, warn, error, dpanic, panic, fatal (default "dpanic")
      --master string                              Address of Kubernetes API server. Leave unset to use in-cluster config.
      --max-drain-attempts-before-fail int         Maximum number of failed drain attempts before giving-up on draining the node. (default 8)
      --max-node-replacement-per-hour int          Maximum number of nodes per hour for which draino can ask replacement. (default 2)
      --max-notready-nodes strings                 Maximum number of NotReady nodes in the cluster. When exceeding this value draino stop taking actions. (Value|Value%)
      --max-notready-nodes-period duration         Polling period to check all nodes readiness (default 1m0s)
      --max-pending-pods strings                   Maximum number of Pending Pods in the cluster. When exceeding this value draino stop taking actions. (Value|Value%)
      --max-pending-pods-period duration           Polling period to check volume of pending pods (default 1m0s)
      --min-eviction-timeout duration              Minimum time we wait to evict a pod. The pod terminationGracePeriod will be used if it is bigger. (default 8m0s)
      --namespace string                           namespace where the application/controller is running
      --no-legacy-node-handler                     Deactivate draino legacy node handler
      --node-conditions stringArray                Nodes for which any of these conditions are true will be tainted and drained.
      --node-label strings                         (Deprecated) Nodes with this label will be eligible for tainting and draining. May be specified multiple times
      --node-label-expr string                     Nodes that match this expression will be eligible for tainting and draining.
      --opt-in-pod-annotation strings              Pod filtering out is ignored if the pod holds one of these annotations. In a way, this makes the pod directly eligible for draino eviction. May be specified multiple times. KEY[=VALUE]
      --pod-warmup-delay-extension duration        Extra delay given to the pod to complete is warmup phase (all containers have passed their startProbes) (default 30s)
      --pre-activity-default-timeout duration      Default duration to wait, for a pre activity to finish, before aborting the drain. This can be overridden by an annotation. (default 10m0s)
      --preprovisioning-by-default                 Set this flag to activate pre-provisioning by default for all nodes
      --preprovisioning-check-period duration      Period to check if a node has been preprovisioned (default 30s)
      --preprovisioning-timeout duration           Timeout for a node to be preprovisioned before draining (default 1h20m0s)
      --protected-pod-annotation strings           Protect pods with this annotation from eviction. May be specified multiple times. KEY[=VALUE]
      --pvc-management-by-default                  PVC management is automatically activated for a workload that do not use eviction++
      --reset-config-labels                        Reset the scope label on the nodes
      --retry-backoff-delay duration               Additional delay to add between retry schedules. (default 23m0s)
      --scope-analysis-period duration             Period to run the scope analysis and generate metric (default 5m0s)
      --service-addr string                        http endpoint for the services (default "0.0.0.0:8484")
      --service-shutdown-timeout duration          shutdown timeout for service (default 15s)
      --service-with-healthcheck                   Activate the healthcheck handlers (default true)
      --service-with-metrics                       Activate the metrics handler (default true)
      --service-with-profiling                     Activate the profiling handler (default true)
      --short-lived-pod-annotation strings         Pod that have a short live, just like job; we prefer let them run till the end instead of evicting them; node is cordon. May be specified multiple times. KEY[=VALUE]
      --skip-drain                                 Whether to skip draining nodes after tainting.
      --storage-class-allows-pv-deletion strings   Storage class for which persistent volume (and associated claim) deletion is allowed. May be specified multiple times.
      --tracer-addr string                         tracer server address; empty to disable
      --tracer-service-name string                 set a tracer default service name; optional
      --wait-before-draining duration              Time to wait between moving a node in candidate status and starting the actual drain. (default 30s)
```

### Labels and Label Expressions

Draino allows filtering the elligible set of nodes using `--node-label` and `--node-label-expr`.
The original flag `--node-label` is limited to the boolean AND of the specified labels. To express more complex predicates, the new `--node-label-expr`
flag allows for mixed OR/AND/NOT logic via https://github.com/antonmedv/expr.

An example of `--node-label-expr`:

```
(metadata.labels.region == 'us-west-1' && metadata.labels.app == 'nginx') || (metadata.labels.region == 'us-west-2' && metadata.labels.app == 'nginx')
```

### Ignore pod controlled by ...
It is possible to prevent eviction of pods that are under control of:
- daemonset
- statefulset
- Custom Resource
- ...

or not even on control of anything. For this, use the flag `do-not-evict-pod-controlled-by`; it can be repeated. An empty value means that we block eviction on pods that are uncontrolled.
The value can be a `kind` or a `kind.group` or a `kind.version.group` to designate the owner resource type. If the `version` or/and the `group` are omitted it acts as a wildcard (any version, any group). It is case-sensitive and must match the API Resource definition.

Example:
```shell script
        - --do-not-evict-controlled-by=StatefulSet
        - --do-not-evict-controlled-by=DaemonSet
        - --do-not-evict-controlled-by=ExtendedDaemonSet.v1alpha1.datadoghq.com
        - --do-not-evict-controlled-by=
```

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
# HELP draino_drained_nodes_total Number of nodes drained.
# TYPE draino_drained_nodes_total counter
draino_drained_nodes_total{result="succeeded"} 1
draino_drained_nodes_total{result="failed"} 1
```

### Events
Draino is generating event for every relevant step of the eviction process. 

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

## Node replacement

A node replacement is automatically requested by `draino` if a node is marked with drain completed for a duration longer than `--duration-before-replacement=1h0m0s`. This behavior allows us to unlock situation where the CA cannot collect the drained node due to minSize=1 on the Nodegroup. This node replacement feature is throttle thanks to parameter `--max-node-replacement-per-hour=2`

The user can proactively ask for pre-provisioning a replacement node before drain is actually started by putting the following annotation on the node: `node-lifecycle.datadoghq.com/provision-new-node-before-drain=true`

## Modes

### Dry Run
Draino can be run in dry run mode using the `--dry-run` flag.

### Deleting PV/PVC associated with the evicted pods
Draino can take care of deleting the PVs/PVCs associated with the evicted pod. This is interesting especially for pods using the `local-storage` storage class. Since the old nodes are not eligible for scheduling, the PVCs/PVs of the pod must be recycled to ensure that the pods can be scheduled on another node.

The list of eligible storage classes must be given to draino at start-up: `--storage-class-allows-pv-deletion=local-data` . This flag can be repeated if multiple classes are eligible.

Then each pods has to explicitly opt-in for that data deletion using an annotation: `draino/delete-pvc-and-pv=true`
