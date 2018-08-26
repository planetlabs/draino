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
