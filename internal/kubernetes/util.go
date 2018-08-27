package kubernetes

import (
	core "k8s.io/api/core/v1"
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
