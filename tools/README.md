# Tools

CLI that should be run from user laptop. User RBAC applies.
Checkout the repo.

`go run tools *.go {options}`

Context: this was initially create to investigate and purge taint following incident. When `draino` is stopped, it may left some unfinished work and associated taints.
`drained` node should be automatically purge by Cluster-Autoscaler because they are empty. 
`draining` taint should be discovered and purged by `draino` itself on restart.
The `drain_candidate` remains. While this should not be a problem when we restart `draino`, because work will resume, under some specific case we may prefer to purge these taints.

## Taints

Command `taints`, helpers to handle taints. Can be applied on either one node, a nodegroup, all the nodes (default)

### Sub-Command 
#### list
List the nodes with NLA taint (all NLA taints)

#### delete
Remove the taint `drain_candidate` from the nodes

Requires `patch/node` permission

#### add
Add a taint (default `drain_candidate`)

Requires `patch/node` permission

