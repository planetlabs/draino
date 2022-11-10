package kubernetes

import (
	"reflect"
	"testing"

	openapi_v2 "github.com/googleapis/gnostic/openapiv2"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
)

type fakeDiscoveryInterface struct {
}

func (f fakeDiscoveryInterface) ServerGroups() (*metav1.APIGroupList, error) {
	return &metav1.APIGroupList{
		Groups: []metav1.APIGroup{
			{
				Name: "apps",
				Versions: []metav1.GroupVersionForDiscovery{
					{
						GroupVersion: "apps/v1",
						Version:      "v1",
					},
					{
						GroupVersion: "apps/v1beta2",
						Version:      "v1beta2",
					},
				},
			},
			{
				Name: "",
				Versions: []metav1.GroupVersionForDiscovery{
					{
						GroupVersion: "v1",
						Version:      "v1",
					},
				},
			},
			{
				Name: "datadoghq.com",
				Versions: []metav1.GroupVersionForDiscovery{
					{
						GroupVersion: "datadoghq.com/v1",
						Version:      "v1",
					},
					{
						GroupVersion: "datadoghq.com/v1alpha1",
						Version:      "v1alpha1",
					},
				},
			},
		},
	}, nil
}

var (
	NodeV1Resource = metav1.APIResource{
		Name:         "nodes",
		SingularName: "node",
		Namespaced:   false,
		Group:        "",
		Version:      "v1",
		Kind:         "Node",
	}

	DaemonSetV1Apps = metav1.APIResource{
		Name:         "daemonsets",
		SingularName: "daemonset",
		Namespaced:   true,
		Group:        "apps",
		Version:      "v1",
		Kind:         "DaemonSet",
	}
	StatefulSetSetV1Apps = metav1.APIResource{
		Name:         "statefulsets",
		SingularName: "statefulset",
		Namespaced:   true,
		Group:        "apps",
		Version:      "v1",
		Kind:         "StatefulSet",
	}
	StatefulSetSetV1beta2Apps = metav1.APIResource{
		Name:         "statefulsets",
		SingularName: "statefulset",
		Namespaced:   true,
		Group:        "apps",
		Version:      "v1beta2",
		Kind:         "StatefulSet",
	}
	ExtendedDaemonSetV1alpha1Datadog = metav1.APIResource{
		Name:         "extendeddaemonsets",
		SingularName: "extendeddaemonset",
		Namespaced:   true,
		Group:        "datadoghq.com",
		Version:      "v1alpha1",
		Kind:         "ExtendedDaemonSet",
	}
	ExtendedDaemonSetReplicaV1alpha1Datadog = metav1.APIResource{
		Name:         "extendeddaemonsetreplicas",
		SingularName: "extendeddaemonsetreplica",
		Namespaced:   true,
		Group:        "datadoghq.com",
		Version:      "v1alpha1",
		Kind:         "ExtendedDaemonSetReplica",
	}
)

func (f fakeDiscoveryInterface) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {

	switch groupVersion {
	case "v1":
		return &metav1.APIResourceList{
			GroupVersion: groupVersion,
			APIResources: []metav1.APIResource{NodeV1Resource},
		}, nil
	case "apps/v1":
		return &metav1.APIResourceList{
			GroupVersion: groupVersion,
			APIResources: []metav1.APIResource{DaemonSetV1Apps, StatefulSetSetV1Apps},
		}, nil
	case "apps/v1beta2":
		return &metav1.APIResourceList{
			GroupVersion: groupVersion,
			APIResources: []metav1.APIResource{StatefulSetSetV1beta2Apps},
		}, nil
	case "datadoghq.com/v1":
		return &metav1.APIResourceList{}, nil
	case "datadoghq.com/v1alpha1":
		return &metav1.APIResourceList{
			GroupVersion: groupVersion,
			APIResources: []metav1.APIResource{ExtendedDaemonSetV1alpha1Datadog, ExtendedDaemonSetReplicaV1alpha1Datadog},
		}, nil
	}
	return nil, nil
}

func (f fakeDiscoveryInterface) RESTClient() rest.Interface {
	return nil // not needed
}

func (f fakeDiscoveryInterface) ServerResources() ([]*metav1.APIResourceList, error) {
	return nil, nil // not needed
}

func (f fakeDiscoveryInterface) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return nil, nil, nil // not needed
}

func (f fakeDiscoveryInterface) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return nil, nil // not needed
}

func (f fakeDiscoveryInterface) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return nil, nil // not needed
}

func (f fakeDiscoveryInterface) ServerVersion() (*version.Info, error) {
	return nil, nil // not needed
}

func (f fakeDiscoveryInterface) OpenAPISchema() (*openapi_v2.Document, error) {
	return nil, nil // not needed
}

func newFakeDiscoveryClient() discovery.DiscoveryInterface {
	return &fakeDiscoveryInterface{}
}

func TestGetAPIResourcesForGVK(t *testing.T) {
	tests := []struct {
		name    string
		gvks    []string
		want    []*metav1.APIResource
		wantErr bool
	}{
		{
			name:    "empty case",
			gvks:    []string{""},
			want:    []*metav1.APIResource{nil},
			wantErr: false,
		},
		{
			name:    "nil case",
			want:    nil,
			wantErr: false,
		},
		{
			name:    "daemonset apps",
			gvks:    []string{"DaemonSet.apps"},
			want:    []*metav1.APIResource{&DaemonSetV1Apps},
			wantErr: false,
		},
		{
			name:    "daemonset apps v1",
			gvks:    []string{"DaemonSet.v1.apps"},
			want:    []*metav1.APIResource{&DaemonSetV1Apps},
			wantErr: false,
		},
		{
			name:    "statefulsets",
			gvks:    []string{"StatefulSet"},
			want:    []*metav1.APIResource{&StatefulSetSetV1Apps, &StatefulSetSetV1beta2Apps},
			wantErr: false,
		},
		{
			name:    "all",
			gvks:    []string{"StatefulSet", "DaemonSet", "", "ExtendedDaemonSet"},
			want:    []*metav1.APIResource{nil, &StatefulSetSetV1Apps, &StatefulSetSetV1beta2Apps, &DaemonSetV1Apps, &ExtendedDaemonSetV1alpha1Datadog},
			wantErr: false,
		},
		{
			name:    "error not found",
			gvks:    []string{"StatefulSet", "DaemonSet", "", "ExtendedDaemonSet", "Wrong"},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetAPIResourcesForGVK(newFakeDiscoveryClient(), tt.gvks, zap.NewNop())
			if (err != nil) != tt.wantErr {
				t.Errorf("GetAPIResourcesForGVK() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetAPIResourcesForGVK() got = %v, want %v", got, tt.want)
			}
		})
	}
}
