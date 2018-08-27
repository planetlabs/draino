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

// A NodeEventRecorder produces an event recorder for a supplied node.
type NodeEventRecorder interface {
	For(nodeName string) record.EventRecorder
}

// An APINodeEventRecorder produces an event recorder that records events to
// a Kubernetes API for a supplied node.
type APINodeEventRecorder struct {
	b         record.EventBroadcaster
	component string
}

// NewAPINodeEventRecorder returns a new NewAPINodeEventRecorder that records
// events via the supplied Kubernetes API client.
func NewAPINodeEventRecorder(c kubernetes.Interface, component string) *APINodeEventRecorder {
	b := record.NewBroadcaster()
	b.StartRecordingToSink(&typedcore.EventSinkImpl{Interface: typedcore.New(c.CoreV1().RESTClient()).Events("")})
	return &APINodeEventRecorder{b: b, component: component}
}

// For returns a record.EventRecorder for the supplied node.
func (r *APINodeEventRecorder) For(nodeName string) record.EventRecorder {
	return r.b.NewRecorder(scheme.Scheme, core.EventSource{Component: r.component, Host: nodeName})
}
