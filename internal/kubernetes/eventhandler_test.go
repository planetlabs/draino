package kubernetes

import (
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TODO(negz): Have this test actually test something?
func TestDrainingResourceEventHandler(t *testing.T) {
	cases := []struct {
		name string
		obj  interface{}
	}{
		{
			name: "CordonAndDrainNode",
			obj:  &core.Node{ObjectMeta: meta.ObjectMeta{Name: nodeName}},
		},
		{
			name: "NotANode",
			obj:  &core.Pod{ObjectMeta: meta.ObjectMeta{Name: podName}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewDrainingResourceEventHandler(&NoopCordonDrainer{}, &NoopNodeEventRecorder{}, WithDrainBuffer(0*time.Second))
			h.OnUpdate(nil, tc.obj)
		})
	}
}
