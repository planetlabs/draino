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

	"github.com/go-test/deep"
	"github.com/pkg/errors"
	core "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

const (
	name = "name"
)

type getByKeyFunc func(key string) (interface{}, bool, error)

type predictableInformer struct {
	cache.SharedInformer
	fn getByKeyFunc
}

func (i *predictableInformer) GetStore() cache.Store {
	return &cache.FakeCustomStore{GetByKeyFunc: i.fn}
}

func TestNodeWatcher(t *testing.T) {
	cases := []struct {
		name    string
		fn      getByKeyFunc
		want    *core.Node
		wantErr bool
	}{
		{
			name: "NodeExists",
			fn: func(k string) (interface{}, bool, error) {
				return &core.Node{}, true, nil
			},
			want: &core.Node{},
		},
		{
			name: "NodeDoesNotExist",
			fn: func(k string) (interface{}, bool, error) {
				return nil, false, nil
			},
			wantErr: true,
		},
		{
			name: "ErrorGettingNode",
			fn: func(k string) (interface{}, bool, error) {
				return nil, false, errors.New("boom")
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			i := &predictableInformer{fn: tc.fn}
			w := &NodeWatch{i}
			got, err := w.Get(name)
			if err != nil {
				if tc.wantErr {
					return
				}
				t.Errorf("w.Get(%v): %v", name, err)
			}

			if diff := deep.Equal(tc.want, got); diff != nil {
				t.Errorf("w.Get(%v): want != got %v", name, diff)
			}
		})
	}
}
