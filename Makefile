all: build

# Build manager binary
build: fmt vet
        CGO_ENABLED=0 go build -o bin/draino ./cmd/draino/*.go

# Run against the configured Kubernetes cluster in ~/.kube/config
run-in-rage: fmt vet
	go run ./cmd/draino/*.go  --kubeconfig=$(KUBECONFIG) --namespace=cluster-controllers \
	  --debug \
      --leader-election-token-name=draino-standard \
      --drain-buffer=3m \
      --drain-group-labels=nodegroups.datadoghq.com/namespace \
      --evict-emptydir-pods=true \
      --eviction-headroom=10s \
      --max-grace-period=10m0s \
      --node-label=nodegroups.datadoghq.com/cluster-autoscaler=true \
      --node-label=nodegroups.datadoghq.com/local-storage=false \
      --protected-pod-annotation=cluster-autoscaler.kubernetes.io/daemonset-pod=true \
      --protected-pod-annotation=node-lifecycle.datadoghq.com/do-not-evict-pod=true \
      --max-simultaneous-cordon=15% \
      --max-simultaneous-cordon=25 \
      --max-simultaneous-cordon-for-taints=15%,node \
      --max-simultaneous-cordon-for-taints=15,node \
      --max-simultaneous-cordon-for-labels=15%,app \
      --max-simultaneous-cordon-for-labels=15,app \
      --max-simultaneous-cordon-for-labels=15%,nodegroups.datadoghq.com/name,nodegroups.datadoghq.com/namespace \
      --max-simultaneous-cordon-for-labels=15,nodegroups.datadoghq.com/name,nodegroups.datadoghq.com/namespace \
      --max-notready-nodes=10% \
      --max-notready-nodes=50 \
      --max-pending-pods=10% \
      --max-drain-attempts-before-fail=7 \
      --do-not-evict-pod-controlled-by=StatefulSet \
      --do-not-evict-pod-controlled-by=DaemonSet \
      --do-not-evict-pod-controlled-by=ExtendedDaemonSetReplicaSet \
      --do-not-evict-pod-controlled-by= \
      --cordon-protected-pod-annotation=node-lifecycle.datadoghq.com/enabled=false \
      --do-not-cordon-pod-controlled-by=StatefulSet \
      --do-not-cordon-pod-controlled-by= \
      --opt-in-pod-annotation=node-lifecycle.datadoghq.com/enabled=true \
      --config-name=standard \
      LifetimeExceeded \
      ec2-host-retirement \
      containerd-task-blocked \
      shim-blocked \
      containerd-issue \
      containerd-update \
      disk-issue \
      kernel-issue \
      kernel-update \
      kubelet-issue \
      kubelet-update \
      compliance-update \
      performance \
      application \
      other
# Run unittest
test: fmt vet
	go test ./...

# Run go fmt against code
fmt:
	go fmt ./...

# Run go vet against code
vet:
	go vet ./...
