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
	"testing"
	"time"

	"k8s.io/client-go/tools/record"

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
			h := NewDrainingResourceEventHandler(&NoopCordonDrainer{}, &record.FakeRecorder{}, WithDrainBuffer(0*time.Second))
			h.OnUpdate(nil, tc.obj)
		})
	}
}
