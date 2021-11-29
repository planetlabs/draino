/*
Copyright 2018 Planet Labs Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package kubernetes

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/antonmedv/expr"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// NewNodeLabelFilter returns a filter that returns true if the supplied node satisfies the boolean expression
func NewNodeLabelFilter(expressionStr *string, log *zap.Logger) (func(o interface{}) bool, error) {
	//This feels wrong but this is how the previous behavior worked so I'm only keeping it to maintain compatibility.
	expression, err := expr.Compile(*expressionStr)
	if err != nil && *expressionStr != "" {
		return nil, err
	}

	return func(o interface{}) bool {
		//This feels wrong but this is how the previous behavior worked so I'm only keeping it to maintain compatibility.
		if *expressionStr == "" {
			return true
		}

		n, ok := o.(*core.Node)
		if !ok {
			return false
		}

		nodeLabels := n.GetLabels()

		parameters := map[string]interface{}{
			"metadata": map[string]map[string]string{
				"labels": nodeLabels,
			},
		}

		result, err := expr.Run(expression, parameters)
		if err != nil {
			log.Error(fmt.Sprintf("Could not parse expression: %v", err))
		}
		return result.(bool)
	}, nil
}

// ParseConditions can parse the string array of conditions to a list of
// SuppliedContion to support particular status value and duration.
func ParseConditions(conditions []string) []SuppliedCondition {
	parsed := make([]SuppliedCondition, len(conditions))
	for i, c := range conditions {
		ts := strings.SplitN(c, "=", 2)
		if len(ts) != 2 {
			// Keep backward compatibility
			ts = []string{c, "True,0s"}
		}
		sm := strings.SplitN(ts[1], ",", 2)
		duration, err := time.ParseDuration(sm[1])
		if err == nil {
			parsed[i] = SuppliedCondition{core.NodeConditionType(ts[0]), core.ConditionStatus(sm[0]), duration}
		}
	}
	return parsed
}

// NodeProcessed tracks whether nodes have been processed before using a map.
type NodeProcessed map[types.UID]bool

// NewNodeProcessed returns a new node processed filter.
func NewNodeProcessed() NodeProcessed {
	return make(NodeProcessed)
}

// Filter returns true if the supplied object is a node that this filter has
// not seen before. It is not threadsafe and should always be the last filter
// applied.
func (processed NodeProcessed) Filter(o interface{}) bool {
	n, ok := o.(*core.Node)
	if !ok {
		return false
	}
	if processed[n.GetUID()] {
		return false
	}
	processed[n.GetUID()] = true
	return true
}

// ConvertLabelsToFilterExpr Convert old list labels into new expression syntax
func ConvertLabelsToFilterExpr(labelsSlice []string) (*string, error) {
	labels := map[string]string{}
	for _, label := range labelsSlice {
		tokens := strings.SplitN(label, "=", 2)
		key := tokens[0]
		value := tokens[1]
		if v, found := labels[key]; found && v != value {
			return nil, fmt.Errorf("node-label parameter is used twice with the same key and different values: '%s' , '%s", v, value)
		}
		labels[key] = value
	}
	res := []string{}
	//sort the maps so that the unit tests actually work
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if k != "" && labels[k] == "" {
			res = append(res, fmt.Sprintf(`'%s' in metadata.labels`, k))
		} else {
			res = append(res, fmt.Sprintf(`metadata.labels['%s'] == '%s'`, k, labels[k]))
		}
	}
	temp := strings.Join(res, " && ")
	return &temp, nil
}

// GetUnscheduledPodsBoundToNodeByPV Check if there is any pod that would be bound to that node due to PV/PVC and that is not yet scheduled
func GetUnscheduledPodsBoundToNodeByPV(node *core.Node, store RuntimeObjectStore, logger *zap.Logger) ([]*core.Pod, error) {
	var result []*core.Pod
	// Is there a local PV on the node
	pvs := store.PersistentVolumes().GetPVForNode(node)
	LogForVerboseNode(logger, node, fmt.Sprintf("PVs found for node, count=%d", len(pvs)))
	for _, pv := range pvs {
		LogForVerboseNode(logger, node, fmt.Sprintf("PV found for node: "+pv.Name))
		if pv.Spec.ClaimRef != nil {
			// Check that the PVC is not being terminated (already deleted)
			pvc, err := store.PersistentVolumeClaims().Get(pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
			if apierrors.IsNotFound(err) {
				LogForVerboseNode(logger, node, fmt.Sprintf("PVC does not exist anymore %s/%s", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name))
				continue
			}
			if err != nil {
				logger.Error("Failed to get PVC", zap.String("PVC", pv.Spec.ClaimRef.Namespace+"/"+pv.Spec.ClaimRef.Name), zap.Error(err))
				return nil, err
			}
			if pvc.DeletionTimestamp != nil {
				LogForVerboseNode(logger, node, fmt.Sprintf("Ignoring PVC that is terminating %s/%s", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name))
				continue
			}
			// Get the pods for the PVCs
			pods, err := store.Pods().ListPodsForClaim(pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
			LogForVerboseNode(logger, node, fmt.Sprintf("Pod for claim "+pv.Spec.ClaimRef.Name+", count=%d", len(pods)))
			if err != nil {
				return nil, err
			}
			for _, pod := range pods {
				LogForVerboseNode(logger, node, fmt.Sprintf("Pod for claim "+pv.Spec.ClaimRef.Name+", adding pod "+pod.Name))

				var pendingPodDelay time.Duration
				if PVCStorageClassCleanupEnabled(pod, store) {
					// The pod must be long (enough) pending to be sure that we are not looking at the fresh STS while we are performing the PVC cleanup
					// Adding a 10s delay on top of PVC deletion timeout to be sure that we have time to perform the PVC cleanup
					pendingPodDelay = 10*time.Second + awaitPVCDeletionTimeout
				}
				if pod.Spec.NodeName == "" && time.Now().Sub(pod.CreationTimestamp.Time) > pendingPodDelay {
					result = append(result, pod)
				}
			}
		} else {
			LogForVerboseNode(logger, node, fmt.Sprintf("PV found for node: "+pv.Name+", no claim reference"))
		}
	}
	return result, nil
}

const (
	allowedConditionAnnotationKey = "node-lifecycle.datadoghq.com/allowed-conditions"
)

// atLeastOneConditionAcceptedByTheNode check if at least one of the condition in the list is allowed. If the list of conditions is empty, but at least one allowed-condition is specified it returns false!
func atLeastOneConditionAcceptedByTheNode(conditions []string, n *core.Node) bool {
	allowedConditions, ok := n.Annotations[allowedConditionAnnotationKey]
	if !ok {
		// no condition specify means "all accepted"
		return true
	}
	for _, allowedCondition := range strings.Split(allowedConditions, ",") {
		for _, c := range conditions {
			if c == allowedCondition {
				return true
			}
		}
	}
	return false
}

func hasAllowConditionList(n *core.Node) bool {
	_, ok := n.Annotations[allowedConditionAnnotationKey]
	return ok
}
