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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/run"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/record"
)

// Component is the name of this application.
const Component = "draino"
const LabelKeyNodeGroupName = "nodegroups.datadoghq.com/name"
const LabelKeyNodeGroupNamespace = "nodegroups.datadoghq.com/namespace"

const (
	// TaintNodeNotReady will be added when node is not ready
	// and removed when node becomes ready.
	TaintNodeNotReady = "node.kubernetes.io/not-ready"

	// TaintNodeDiskPressure will be added when node has disk pressure
	// and removed when node has enough disk.
	TaintNodeDiskPressure = "node.kubernetes.io/disk-pressure"

	// TaintNodeNetworkUnavailable will be added when node's network is unavailable
	// and removed when network becomes ready.
	TaintNodeNetworkUnavailable = "node.kubernetes.io/network-unavailable"
)

// BuildConfigFromFlags is clientcmd.BuildConfigFromFlags with no annoying
// dependencies on glog.
// https://godoc.org/k8s.io/client-go/tools/clientcmd#BuildConfigFromFlags
func BuildConfigFromFlags(apiserver, kubecfg string) (*rest.Config, error) {
	if kubecfg != "" || apiserver != "" {
		return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubecfg},
			&clientcmd.ConfigOverrides{ClusterInfo: api.Cluster{Server: apiserver}}).ClientConfig()
	}
	return rest.InClusterConfig()
}

// NewEventRecorder returns a new record.EventRecorder for the given client.
func NewEventRecorder(c kubernetes.Interface) record.EventRecorder {
	b := record.NewBroadcaster()
	b.StartRecordingToSink(&typedcore.EventSinkImpl{Interface: typedcore.New(c.CoreV1().RESTClient()).Events("")})
	return b.NewRecorder(scheme.Scheme, core.EventSource{Component: Component})
}

// RetryWithTimeout this function retries till the function f return a nil error or timeout expire
// Note the intermediate error trigger the retry and are dropped.
func RetryWithTimeout(f func() error, retryPeriod, timeout time.Duration) error {
	return wait.PollImmediate(retryPeriod, timeout,
		func() (bool, error) {
			if err := f(); err != nil {
				return false, nil
			}
			return true, nil
		})
}

var DummyErrorForRetry = errors.New("retry on error")

func GetAPIResources(discoveryClient discovery.DiscoveryInterface) ([]metav1.APIResource, error) {
	groupList, err := discoveryClient.ServerGroups()
	if groupList == nil || err != nil || groupList.Groups == nil {
		return nil, fmt.Errorf("Fail to discover groups. Error: %v\n", err)
	}

	var allServerResources []metav1.APIResource

	for _, g := range groupList.Groups {
		for _, gvd := range g.Versions {
			gv, err := schema.ParseGroupVersion(gvd.GroupVersion)
			if err != nil {
				return nil, fmt.Errorf("error parsing gvd %s, %s", gvd.GroupVersion, err)
			}
			resourceLists, err := discoveryClient.ServerResourcesForGroupVersion(gvd.GroupVersion)
			if err != nil {
				return nil, fmt.Errorf("cannot list server resources, %s", err)
			}

			if resourceLists == nil {
				continue
			}
			for i := range resourceLists.APIResources {
				resourceLists.APIResources[i].Group = gv.Group
				resourceLists.APIResources[i].Version = gv.Version
				allServerResources = append(allServerResources, resourceLists.APIResources[i])
			}
		}
	}
	return allServerResources, nil
}

// GetAPIResourcesForGroupsKindVersion return the list of APIResources that match the group kind version
func GetAPIResourcesForGroupsKindVersion(apiResources []metav1.APIResource, gvks []string) ([]metav1.APIResource, error) {
	var outputAPIResources []metav1.APIResource
	for _, gvkInput := range gvks {
		if gvkInput == "" {
			return nil, errors.New("empty GroupVersionKind value")
		}

		atLeastOneResourceFound := false
		var gvk schema.GroupVersionKind
		if gvkPtr, gk := schema.ParseKindArg(gvkInput); gvkPtr != nil {
			gvk = *gvkPtr
		} else {
			gvk.Kind = gk.Kind
			gvk.Group = gk.Group
		}

		for i, apiresource := range apiResources {
			if gvk.Version != "" && gvk.Version != apiresource.Version {
				continue
			}

			if gvk.Group != "" && gvk.Group != apiresource.Group {
				continue
			}

			if gvk.Kind != apiresource.Kind {
				continue
			}
			outputAPIResources = append(outputAPIResources, apiResources[i])
			atLeastOneResourceFound = true
		}

		if !atLeastOneResourceFound {
			return nil, fmt.Errorf("could not find any APIResource matching kind[.version[.group]]='%s'", gvkInput)
		}
	}
	return outputAPIResources, nil
}

// GetAPIResourcesForGVK retrieves the apiResources that match the given 'Kind.Version.Group'
// taking into account the empty case that associate a nil value in the list (used for uncontrolled pod filtering)
// and filtering out subresources
func GetAPIResourcesForGVK(discoveryInterface discovery.DiscoveryInterface, gvks []string) ([]*metav1.APIResource, error) {
	hasEmptyGVK := false
	var nonEmptyGVKs []string
	for _, v := range gvks {
		if v == "" {
			hasEmptyGVK = true
			continue
		}
		nonEmptyGVKs = append(nonEmptyGVKs, v)
	}

	allAPIResources, err := GetAPIResources(discoveryInterface)
	if err != nil {
		return nil, err
	}

	apiResources, err := GetAPIResourcesForGroupsKindVersion(allAPIResources, nonEmptyGVKs)
	if err != nil {
		return nil, err
	}

	var output []*metav1.APIResource
	if hasEmptyGVK {
		output = append(output, nil)
	}

	for i := range apiResources {
		if !strings.Contains(apiResources[i].Name, "/") { // filtering out subresources
			output = append(output, &apiResources[i])
		}
	}
	return output, nil
}

type NodeTagsValues struct {
	Team, NgName, NgNamespace string
}

func GetNodeTagsValues(node *core.Node) NodeTagsValues {
	team := node.Labels["managed_by_team"]
	if team == "" {
		team = node.Labels["team"]
	}
	return NodeTagsValues{
		Team:        team,
		NgName:      node.Labels[LabelKeyNodeGroupName],
		NgNamespace: node.Labels[LabelKeyNodeGroupNamespace],
	}
}

func GetNodeGroupNamePrefix(ngName string) string {
	return strings.Split(ngName, "-")[0]
}

func nodeTags(ctx context.Context, node *core.Node) (context.Context, error) {
	values := GetNodeTagsValues(node)
	return tag.New(ctx, tag.Upsert(TagNodegroupNamespace, values.NgNamespace), tag.Upsert(TagNodegroupName, values.NgName), tag.Upsert(TagNodegroupNamePrefix, GetNodeGroupNamePrefix(values.NgName)), tag.Upsert(TagTeam, values.Team))
}

func StatRecordForNode(ctx context.Context, node *core.Node, m stats.Measurement) {
	tagsWithNg, _ := nodeTags(ctx, node)
	stats.Record(tagsWithNg, m)
}

func LoggerForNode(n *core.Node, logger *zap.Logger) *zap.Logger {
	team := n.Labels["managed_by_team"]
	if team == "" {
		team = n.Labels["team"]
	}
	return logger.With(zap.String("node", n.Name), zap.String("ng_name", n.Labels[LabelKeyNodeGroupName]), zap.String("ng_namespace", n.Labels[LabelKeyNodeGroupNamespace]), zap.String("node_team", n.Labels[LabelKeyNodeGroupNamespace]))
}

type Runner interface {
	Run(stop <-chan struct{})
}

func Await(rs ...Runner) error {
	stop := make(chan struct{})
	g := &run.Group{}
	for i := range rs {
		r := rs[i] // https://golang.org/doc/faq#closures_and_goroutines
		g.Add(func() error { r.Run(stop); return nil }, func(err error) { close(stop) })
	}
	return g.Run()
}

type AnnotationPatch struct {
	Metadata struct {
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
}

type AnnotationDeletePatch struct {
	Metadata struct {
		Annotations map[string]interface{} `json:"annotations"`
	} `json:"metadata"`
}

func PatchNodeAnnotationKey(kclient kubernetes.Interface, nodeName string, key string, value string) error {
	var annotationPatch AnnotationPatch
	annotationPatch.Metadata.Annotations = map[string]string{key: value}

	payloadBytes, _ := json.Marshal(annotationPatch)
	_, err := kclient.
		CoreV1().
		Nodes().
		Patch(nodeName, types.MergePatchType, payloadBytes)
	return err
}

func PatchDeleteNodeAnnotationKey(kclient kubernetes.Interface, nodeName string, key string) error {
	var annotationDeletePatch AnnotationDeletePatch
	annotationDeletePatch.Metadata.Annotations = map[string]interface{}{key: nil}

	payloadBytes, _ := json.Marshal(annotationDeletePatch)
	_, err := kclient.
		CoreV1().
		Nodes().
		Patch(nodeName, types.MergePatchType, payloadBytes)
	return err
}

// GetAnnotationFromPodOrController check if an annotation is present on the pod or the associated controller object
// Supported controller object:
// - statefulset
//
// Method made generic to be able to extend to deployments and other controllers later
func GetAnnotationFromPodOrController(annotationKey string, pod *core.Pod, store RuntimeObjectStore) (value string, found bool) {
	//Check directly on the pod and return if any value
	if pod.Annotations != nil {
		if value, ok := pod.Annotations[annotationKey]; ok {
			return value, ok
		}
	}

	if ctrl, found := GetControllerForPod(pod, store); found {
		v, ok := ctrl.GetAnnotations()[annotationKey]
		return v, ok
	}
	return "", false
}

// GetControllerForPod for the moment it handles only statefulSets controller
func GetControllerForPod(pod *core.Pod, store RuntimeObjectStore) (ctrl metav1.Object, found bool) {
	for _, r := range pod.OwnerReferences {
		if r.Kind == "StatefulSet" {
			sts, err := store.StatefulSets().Get(pod.Namespace, r.Name)
			if err != nil {
				return nil, false
			}
			return sts, true
		}
	}
	return nil, false
}

// GetReadinessState gets readiness state for the node
func GetReadinessState(node *core.Node) (isNodeReady bool, err error) {
	canNodeBeReady, readyFound := true, false

	for _, cond := range node.Status.Conditions {
		switch cond.Type {
		case core.NodeReady:
			readyFound = true
			if cond.Status == core.ConditionFalse || cond.Status == core.ConditionUnknown {
				canNodeBeReady = false
			}
		case core.NodeDiskPressure:
			if cond.Status == core.ConditionTrue {
				canNodeBeReady = false
			}
		case core.NodeNetworkUnavailable:
			if cond.Status == core.ConditionTrue {
				canNodeBeReady = false
			}
		}
	}

	notReadyTaints := map[string]bool{
		TaintNodeNotReady:           true,
		TaintNodeDiskPressure:       true,
		TaintNodeNetworkUnavailable: true,
	}
	for _, taint := range node.Spec.Taints {
		if notReadyTaints[taint.Key] {
			canNodeBeReady = false
		}
	}

	if !readyFound {
		return false, fmt.Errorf("readiness information not found")
	}
	return canNodeBeReady, nil
}

func LogForVerboseNode(logger *zap.Logger, node *core.Node, msg string, fields ...zap.Field) {
	if node.Annotations["draino/logs"] == "verbose" {
		logger.Info(msg, append(fields, zap.String("node", node.Name))...)
	}
}
