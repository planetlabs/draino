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
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/flowcontrol"
)

const (
	DefaultMaxNotReadyNodesPeriod = 60 * time.Second
	DefaultMaxPendingPodsPeriod = 60 * time.Second
)

type NodeReplacementLimiter interface {
	// Can ask for a new node replacement
	CanAskForNodeReplacement() bool
}

type limiterForNodeReplacement struct {
	noReplacementBefore time.Time // We want to skip the first period to avoid abusive NoReplacement in case of controller frequent restarts (OOM, Deployment...)
	rateLimiter         flowcontrol.RateLimiter
}

func (l *limiterForNodeReplacement) CanAskForNodeReplacement() bool {
	if !time.Now().After(l.noReplacementBefore) {
		return false
	}
	if l.rateLimiter != nil && !l.rateLimiter.TryAccept() {
		return false
	}
	return true
}

func NewNodeReplacementLimiter(numberOfNodesPerHours int, currentTime time.Time) NodeReplacementLimiter {
	qps := float32(numberOfNodesPerHours) / 3600
	minutePeriod := 60 / numberOfNodesPerHours
	return &limiterForNodeReplacement{
		noReplacementBefore: currentTime.Add(time.Duration(minutePeriod) * time.Minute),
		rateLimiter:         flowcontrol.NewTokenBucketRateLimiter(qps, 1),
	}
}

type CordonLimiter interface {
	// Check if the node can be cordoned. If not, the name of the limiter is returned
	CanCordon(node *core.Node) (bool, string)

	SetNodeLister(lister NodeLister)

	// Add a named limiter function to the limiter.
	AddLimiter(string, LimiterFunc)
}
type NodeLister interface {
	ListNodes() []*core.Node
}

func NewCordonLimiter(logger *zap.Logger) CordonLimiter {
	return &Limiter{
		logger:      logger,
		rateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1), // limiters are computing % on top of the cache. Here we ensure that the cache has time to be updated.
	}
}

// A PodFilterFunc returns true if the supplied pod passes the filter.
type LimiterFunc func(n *core.Node, cordonNodes, allNodes []*core.Node) (bool, error)

type LimiterError struct {
	Reason string
}

func (l *LimiterError) Error() string {
	return l.Reason
}

func IsLimiterError(e error) bool {
	if e == nil {
		return false
	}
	_, ok := e.(*LimiterError)
	return ok
}

func NewLimiterError(msg string) error {
	return &LimiterError{
		msg,
	}
}

type Limiter struct {
	nodeLister   NodeLister
	rateLimiter  flowcontrol.RateLimiter // since we are relying on the store cache that is asynchronously populated to check cordon status, let's be sure that we don't have cordon burst
	limiterfuncs map[string]LimiterFunc
	logger       *zap.Logger
}

func (l *Limiter) SetNodeLister(lister NodeLister) {
	l.nodeLister = lister
}

func (l *Limiter) CanCordon(node *core.Node) (can bool, reason string) {
	if node.Spec.Unschedulable {
		return true, "" // it is already cordon anyway
	}

	allNodes := l.nodeLister.ListNodes()
	cordonNodes := []*core.Node{}
	for _, n := range allNodes {
		if n.Spec.Unschedulable {
			cordonNodes = append(cordonNodes, n)
		}
	}
	for limiterName, limiterFunc := range l.limiterfuncs {
		canCordon, err := limiterFunc(node, cordonNodes, allNodes)
		if err != nil {
			l.logger.Error("cordon limiter failure", zap.Error(err))
			return false, "error"
		}
		if !canCordon {
			return false, limiterName
		}
	}

	// if all functional limiters are ok, let's ensure that we are not cordoning too fast
	if l.rateLimiter != nil && !l.rateLimiter.TryAccept() {
		return false, "rateLimit"
	}

	return true, ""
}

func (l *Limiter) AddLimiter(name string, f LimiterFunc) {
	if l.limiterfuncs == nil {
		l.limiterfuncs = map[string]LimiterFunc{}
	}
	l.limiterfuncs[name] = f
}

func ParseCordonMax(param string) (max int, isPercent bool, err error) {
	percent := strings.HasSuffix(param, "%")
	max, err = strconv.Atoi(strings.TrimSuffix(param, "%"))
	if err != nil {
		return -1, percent, errors.New("can't Parse argument for cordon limiter value")
	}
	return max, percent, nil
}

func ParseCordonMaxForKeys(param string) (max int, isPercent bool, splittedKeys []string, err error) {
	tokens := strings.SplitN(param, ",", 2)
	if len(tokens) < 2 {
		return -1, false, nil, errors.New("can't Parse argument for cordon limiter, at least 2 tokens are expected in field max-simultaneous-cordon-for-labels")
	}
	max, percent, err := ParseCordonMax(tokens[0])
	if err != nil {
		return max, percent, nil, err
	}
	return max, percent, strings.Split(tokens[1], ","), nil
}

func MaxSimultaneousCordonLimiterFunc(max int, percent bool) LimiterFunc {
	return func(n *core.Node, cordonNodes, allNodes []*core.Node) (bool, error) {
		if len(allNodes) == 0 {
			return false, errors.New("no node discovered")
		}

		if len(cordonNodes) == 0 { // always allow at least one node to be cordon
			return true, nil
		}

		if percent {
			return math.Ceil(100*float64(len(cordonNodes)+1)/float64(len(allNodes))) <= float64(max), nil
		}
		return len(cordonNodes) < max, nil
	}
}

func MaxSimultaneousCordonLimiterForLabelsFunc(max int, percent bool, labelKeys []string) LimiterFunc {
	return func(n *core.Node, cordonNodes, allNodes []*core.Node) (bool, error) {
		if n.Labels == nil {
			return true, nil
		}

		selectorSet := map[string]string{}
		// check if the node has the labels and build the selector
		for _, key := range labelKeys {
			value, found := n.Labels[key]
			if !found {
				return true, nil
			}
			selectorSet[key] = value
		}

		cordonCount, totalMatchCount := getMatchingNodesCount(labels.SelectorFromSet(selectorSet), allNodes)
		if cordonCount == 0 { // always allow at least one node of the group to be cordon
			return true, nil
		}

		if percent {
			if totalMatchCount == 0 {
				return false, errors.New("the proposed node is not yet known by the store")
			}
			percentCordon := int(math.Ceil(100 * float64(cordonCount+1) / float64(totalMatchCount)))
			return percentCordon <= max, nil
		}
		return cordonCount < max, nil
	}
}

func getMatchingNodesCount(selector labels.Selector, nodes []*core.Node) (cordonMatchCount, totalMatchCount int) {
	for _, node := range nodes {
		if selector.Matches(labels.Set(node.Labels)) {
			totalMatchCount++
			if node.Spec.Unschedulable {
				cordonMatchCount++
			}
		}
	}
	return
}

func MaxSimultaneousCordonLimiterForTaintsFunc(max int, percent bool, taintKeys []string) LimiterFunc {
	return func(n *core.Node, cordonNodes, allNodes []*core.Node) (bool, error) {
		if len(n.Spec.Taints) < len(taintKeys) {
			return true, nil // This limiter is not relevant for that node
		}

		selectorSet := map[string]string{}
		// check if the node has the labels and build the selector
		for _, key := range taintKeys {
			for _, t := range n.Spec.Taints {
				if t.Key == key {
					selectorSet[key] = t.Value
					break
				}
			}
		}

		if len(selectorSet) != len(taintKeys) {
			return true, nil // this node does not have the relevant taint keys for that filter
		}

		cordonCount, totalMatchCount := getMatchingNodesForTaintCount(selectorSet, allNodes)
		if cordonCount == 0 { // always allow at least one node of the group to be cordon
			return true, nil
		}

		if percent {
			if totalMatchCount == 0 {
				return false, errors.New("the proposed node is not yet known by the store")
			}
			percentCordon := int(math.Ceil(100 * float64(cordonCount+1) / float64(totalMatchCount)))
			return percentCordon <= max, nil
		}
		return cordonCount < max, nil
	}
}

// getMatchingNodesForTaintCount retrieve the node that have a given set of taint key:value, and count the number of cordon nodes in there.
func getMatchingNodesForTaintCount(selector map[string]string, nodes []*core.Node) (cordonMatchCount, totalMatchCount int) {
NodeLoop:
	for _, node := range nodes {
		for k, v := range selector {
			var taintWithValueFound bool
			for _, t := range node.Spec.Taints {
				if t.Key == k && t.Value == v {
					taintWithValueFound = true
					break
				}
			}
			if !taintWithValueFound {
				continue NodeLoop
			}
		}
		totalMatchCount++
		if node.Spec.Unschedulable {
			cordonMatchCount++
		}
	}
	return
}
