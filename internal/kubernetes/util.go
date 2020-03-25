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
	"time"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
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
