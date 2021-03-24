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
	"fmt"
	"strings"
	"time"

	"errors"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

func RetryWithTimeout(f func() error, retryPeriod, timeout time.Duration) error {
	return wait.PollImmediate(retryPeriod, timeout,
		func() (bool, error) {
			if err := f(); err != nil {
				return false, nil
			}
			return true, nil
		})
}

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

func StatRecordForEachCondition(ctx context.Context, node *core.Node, conditions []string, m stats.Measurement) {
	team := node.Labels["managed_by_team"]
	if team == "" {
		team = node.Labels["team"]
	}
	ngName := node.Labels[LabelKeyNodeGroupName]
	ngNamespace := node.Labels[LabelKeyNodeGroupNamespace]
	tagsWithNg, _ := tag.New(ctx, tag.Upsert(TagNodegroupNamespace, ngNamespace), tag.Upsert(TagNodegroupName, ngName), tag.Upsert(TagTeam, team))

	for _, c := range conditions {
		tags, _ := tag.New(tagsWithNg, tag.Upsert(TagConditions, c))
		stats.Record(tags, m)
	}
}

func LoggerForNode(n *core.Node, logger *zap.Logger) *zap.Logger {
	team := n.Labels["managed_by_team"]
	if team == "" {
		team = n.Labels["team"]
	}
	return logger.With(zap.String("node", n.Name), zap.String("ng_name", n.Labels[LabelKeyNodeGroupName]), zap.String("ng_namespace", n.Labels[LabelKeyNodeGroupNamespace]), zap.String("node_team", n.Labels[LabelKeyNodeGroupNamespace]))
}
