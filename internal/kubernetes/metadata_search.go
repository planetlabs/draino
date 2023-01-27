package kubernetes

import (
	"context"
	"fmt"
	"strings"

	"github.com/planetlabs/draino/internal/kubernetes/index"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type MetadataSearch[T any] struct {
	Key               string
	metadataGetter    MetadataGetterFunc
	podIndexer        index.PodIndexer
	store             RuntimeObjectStore
	converter         func(string) (T, error)
	stopIfFoundOnPod  bool                                     // mean that we do not explore the controller of pods
	stopIfFoundOnNode bool                                     // mean that we do not explore the pods
	Result            map[string][]MetadataSearchResultItem[T] // the key is the string representation of the value
}

type MetadataSearchResultItem[T any] struct {
	Key          string        `json:"key"`
	Value        T             `json:"value"`
	errorConv    error         // this is private because it can and shouldn't be serialized for diagnostics
	ErrorConvStr string        `json:"errorConversion,omitempty"`
	Node         *core.Node    `json:"-"`
	NodeId       string        `json:"node,omitempty"`
	Pod          *core.Pod     `json:"-"`
	PodId        string        `json:"pod,omitempty"`
	OnController bool          `json:"onController,omitempty"`
	Source       metav1.Object `json:"-"`
}

type MetadataGetterResult struct {
	Key   string
	Value string
}
type MetadataGetterFunc func(object metav1.Object, key string) (values []MetadataGetterResult, exist bool)

var GetExactLabel MetadataGetterFunc = func(object metav1.Object, key string) ([]MetadataGetterResult, bool) {
	labels := object.GetLabels()
	if labels == nil {
		return nil, false
	}
	val, exist := labels[key]
	return []MetadataGetterResult{{Key: key, Value: val}}, exist
}

var GetExactAnnotation MetadataGetterFunc = func(object metav1.Object, key string) ([]MetadataGetterResult, bool) {
	annotations := object.GetAnnotations()
	if annotations == nil {
		return nil, false
	}
	val, exist := annotations[key]
	return []MetadataGetterResult{{Key: key, Value: val}}, exist
}

var GetPrefixedAnnotation MetadataGetterFunc = func(object metav1.Object, prefix string) ([]MetadataGetterResult, bool) {
	annotations := object.GetAnnotations()
	if annotations == nil {
		return nil, false
	}

	results := []MetadataGetterResult{}
	for key, val := range annotations {
		if strings.HasPrefix(key, prefix) {
			results = append(results, MetadataGetterResult{
				Key:   key,
				Value: val,
			})
		}
	}

	return results, len(results) > 0
}

func NewSearch[T any](ctx context.Context, podIndexer index.PodIndexer, store RuntimeObjectStore, converter func(string) (T, error), node *core.Node, annotationKey string, stopIfFoundOnNode, stopIfFoundOnPod bool, metadataFunc MetadataGetterFunc) (*MetadataSearch[T], error) {
	search := &MetadataSearch[T]{
		metadataGetter:    metadataFunc,
		podIndexer:        podIndexer,
		store:             store,
		Key:               annotationKey,
		stopIfFoundOnPod:  stopIfFoundOnPod,
		stopIfFoundOnNode: stopIfFoundOnNode,
		converter:         converter,
		Result:            map[string][]MetadataSearchResultItem[T]{},
	}

	search.processNode(node)
	if len(search.Result) > 0 && stopIfFoundOnNode {
		return search, nil
	}
	if podIndexer == nil {
		return nil, fmt.Errorf("missing indexer to continue on pod exploration")
	}

	pods, err := podIndexer.GetPodsByNode(ctx, node.Name)
	if err != nil {
		return nil, err
	}
	for _, p := range pods {
		search.processPod(p)
	}
	return search, nil
}
func (a *MetadataSearch[T]) processNode(node *core.Node) {
	if values, ok := a.metadataGetter(node, a.Key); ok {
		for _, val := range values {
			var item MetadataSearchResultItem[T]
			item.Key = val.Key
			item.setNode(node)
			item.setValueAndError(a.converter(val.Value))
			item.Source = node
			a.Result[val.Value] = append(a.Result[val.Value], item)
		}
	}
}

func (a *MetadataSearch[T]) processPod(pod *core.Pod) {
	if values, ok := a.metadataGetter(pod, a.Key); ok {
		for _, val := range values {
			var item MetadataSearchResultItem[T]
			item.Key = val.Key
			item.setPod(pod)
			item.setValueAndError(a.converter(val.Value))
			item.Source = pod
			a.Result[val.Value] = append(a.Result[val.Value], item)
		}
		if a.stopIfFoundOnPod {
			return
		}
	}

	if ctrl, found := GetControllerForPod(pod, a.store); found {
		if values, ok := a.metadataGetter(ctrl, a.Key); ok {
			for _, val := range values {
				var item MetadataSearchResultItem[T]
				item.Key = val.Key
				item.setPod(pod)
				item.OnController = true
				item.Source = ctrl
				item.setValueAndError(a.converter(val.Value))
				a.Result[val.Value] = append(a.Result[val.Value], item)
			}
		}
	}
}

func (a *MetadataSearch[T]) Results() (out []MetadataSearchResultItem[T]) {
	for _, v := range a.Result {
		for _, item := range v {
			if item.errorConv != nil {
				continue
			}
			out = append(out, item)
		}
	}
	return
}

func (a *MetadataSearch[T]) ValuesWithoutDupe() (out []T) {
	for _, v := range a.Result {
		for _, item := range v {
			if item.errorConv != nil {
				continue
			}
			out = append(out, item.Value)
			break
		}
	}
	return
}

func (a *MetadataSearch[T]) HandlerError(nodeErrFunc func(*core.Node, error), podErrFunc func(*core.Pod, error)) {
	for _, v := range a.Result {
		for _, item := range v {
			if item.errorConv != nil {
				if item.Node != nil {
					nodeErrFunc(item.Node, item.errorConv)
				}
				if item.Pod != nil {
					podErrFunc(item.Pod, item.errorConv)
				}
			}
		}
	}
}

func (i *MetadataSearchResultItem[T]) setPod(pod *core.Pod) {
	i.PodId = pod.Namespace + "/" + pod.Name
	i.Pod = pod
}
func (i *MetadataSearchResultItem[T]) setNode(node *core.Node) {
	i.NodeId = node.Name
	i.Node = node
}
func (i *MetadataSearchResultItem[T]) setError(e error) {
	i.errorConv = e
	i.ErrorConvStr = e.Error()
}
func (i *MetadataSearchResultItem[T]) setValueAndError(value T, e error) {
	i.Value = value
	i.errorConv = e
	if e != nil {
		i.ErrorConvStr = e.Error()
	}
}
func (i *MetadataSearchResultItem[T]) GetItemId() string {
	if i.NodeId != "" {
		return i.NodeId
	}
	if i.PodId != "" {
		prefix := ""
		if i.OnController {
			prefix = "ctrl-"
		}
		return prefix + i.PodId
	}
	return "not_available"
}

func SearchAnnotationFromNodeAndThenPodOrController[T any](ctx context.Context, podIndexer index.PodIndexer, store RuntimeObjectStore, converter func(string) (T, error), annotationKey string, node *core.Node, stopIfFoundOnNode, stopIfFoundOnPod bool) (*MetadataSearch[T], error) {
	return NewSearch(ctx, podIndexer, store, converter, node, annotationKey, stopIfFoundOnPod, stopIfFoundOnNode, GetExactAnnotation)
}
