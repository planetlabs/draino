package groups

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	cachek "k8s.io/client-go/tools/cache"
	cachecr "sigs.k8s.io/controller-runtime/pkg/cache"
)

const (
	SchedulingGroupIdx = "node:scheduling:group"
)

// InitSchedulingGroupIndexer prepare an index with all the nodes for a given scheduling group
func InitSchedulingGroupIndexer(cache cachecr.Cache, keyGetter GroupKeyGetter) error {
	informer, err := cache.GetInformer(context.Background(), &v1.Node{})
	if err != nil {
		return err
	}
	return informer.AddIndexers(map[string]cachek.IndexFunc{
		SchedulingGroupIdx: func(obj interface{}) ([]string, error) {
			if n, ok := obj.(*v1.Node); ok {
				return []string{string(keyGetter.GetGroupKey(n))}, nil
			}
			return nil, fmt.Errorf("Expecting node object in SchedulingGroupIdx indexer")
		},
	})
}
