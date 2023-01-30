package k8sclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/planetlabs/draino/internal/kubernetes/utils"
	corev1 "k8s.io/api/core/v1"
)

// --------------------------------------------
// Condition Patch
// --------------------------------------------
type NodeConditionPatch struct {
	ConditionType corev1.NodeConditionType
}

var _ client.Patch = &NodeConditionPatch{}

func (_ *NodeConditionPatch) Type() types.PatchType {
	return types.StrategicMergePatchType
}

func (self *NodeConditionPatch) Data(obj client.Object) ([]byte, error) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil, fmt.Errorf("cannot parse object into node")
	}

	_, condition, found := utils.FindNodeCondition(self.ConditionType, node)
	if !found {
		return nil, fmt.Errorf("cannot find condition on node")
	}

	// This is inspired by a kubernetes function that is updating the node conditions in the same way
	// https://github.com/kubernetes/kubernetes/blob/fa1b6765d55c3f3c00299a9e279732342cfb00f7/staging/src/k8s.io/component-helpers/node/util/conditions.go#L45
	// It was tested on a local cluster and works for both create & updates.
	return json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"conditions": []corev1.NodeCondition{condition},
		},
	})
}

// --------------------------------------------
// JSON Patches
// --------------------------------------------
type JSONPatchValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

type JSONAnnotationPatch struct {
	Key   string
	Value string
}

var _ client.Patch = &JSONAnnotationPatch{}

func (p *JSONAnnotationPatch) Type() types.PatchType {
	return types.JSONPatchType
}
func (p *JSONAnnotationPatch) Data(obj client.Object) ([]byte, error) {
	jsonPatch := JSONPatchValue{
		Op: "replace",
		// as the patch path might include "/", we have to escape the slashes in the annotation path with '~1'
		Path:  fmt.Sprintf("/metadata/annotations/%s", strings.ReplaceAll(p.Key, "/", "~1")),
		Value: p.Value,
	}

	return json.Marshal([]JSONPatchValue{jsonPatch})
}

// --------------------------------------------
// Merge patches
// --------------------------------------------
type AnnotationPatch struct {
	Metadata struct {
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
}

func (a AnnotationPatch) Type() types.PatchType {
	return types.MergePatchType
}

func (a AnnotationPatch) Data(obj client.Object) ([]byte, error) {
	return json.Marshal(a)
}

type AnnotationDeletePatch struct {
	Metadata struct {
		Annotations map[string]interface{} `json:"annotations"`
	} `json:"metadata"`
}

func (a AnnotationDeletePatch) Type() types.PatchType {
	return types.MergePatchType
}

func (a AnnotationDeletePatch) Data(obj client.Object) ([]byte, error) {
	return json.Marshal(a)
}

type LabelPatch struct {
	Metadata struct {
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
}

func (l LabelPatch) Type() types.PatchType {
	return types.MergePatchType
}

func (l LabelPatch) Data(obj client.Object) ([]byte, error) {
	return json.Marshal(l)
}

type LabelDeletePatch struct {
	Metadata struct {
		Labels map[string]interface{} `json:"labels"`
	} `json:"metadata"`
}

func (l LabelDeletePatch) Type() types.PatchType {
	return types.MergePatchType
}

func (l LabelDeletePatch) Data(obj client.Object) ([]byte, error) {
	return json.Marshal(l)
}

func PatchNode(ctx context.Context, kclient kubernetes.Interface, nodeName string, patch interface{}) error {
	payloadBytes, _ := json.Marshal(patch)
	_, err := kclient.
		CoreV1().
		Nodes().
		Patch(ctx, nodeName, types.MergePatchType, payloadBytes, metav1.PatchOptions{})
	return err
}

func PatchNodeCR(ctx context.Context, client client.Client, node *corev1.Node, patch client.Patch) error {
	// The client.Patch method is automatically updating the given node.
	// As the pointer is used in other places, it will cause concurrent map read / write panics
	// In order to prevent this, we'll create a deep copy of the node and pass it to the client.Patch.
	nodeCopy := node.DeepCopy()
	return client.Patch(ctx, nodeCopy, patch)
}

func PatchNodeAnnotationKey(ctx context.Context, kclient kubernetes.Interface, nodeName string, key string, value string) error {
	var annotationPatch AnnotationPatch
	annotationPatch.Metadata.Annotations = map[string]string{key: value}
	return PatchNode(ctx, kclient, nodeName, annotationPatch)
}

func PatchDeleteNodeAnnotationKey(ctx context.Context, kclient kubernetes.Interface, nodeName string, key string) error {
	var annotationDeletePatch AnnotationDeletePatch
	annotationDeletePatch.Metadata.Annotations = map[string]interface{}{key: nil}
	return PatchNode(ctx, kclient, nodeName, annotationDeletePatch)
}

func PatchNodeLabelKey(ctx context.Context, kclient kubernetes.Interface, nodeName string, key string, value string) error {
	var labelPatch LabelPatch
	labelPatch.Metadata.Labels = map[string]string{key: value}
	return PatchNode(ctx, kclient, nodeName, labelPatch)
}

func PatchNodeLabelKeyCR(ctx context.Context, client client.Client, node *corev1.Node, key string, value string) error {
	var labelPatch LabelPatch
	labelPatch.Metadata.Labels = map[string]string{key: value}
	return PatchNodeCR(ctx, client, node, labelPatch)
}

func PatchDeleteNodeLabelKeyCR(ctx context.Context, client client.Client, node *corev1.Node, key string) error {
	var labelDeletePatch LabelDeletePatch
	labelDeletePatch.Metadata.Labels = map[string]interface{}{key: nil}
	return PatchNodeCR(ctx, client, node, labelDeletePatch)
}

func PatchDeleteNodeLabelKey(ctx context.Context, kclient kubernetes.Interface, nodeName string, key string) error {
	var labelDeletePatch LabelDeletePatch
	labelDeletePatch.Metadata.Labels = map[string]interface{}{key: nil}
	return PatchNode(ctx, kclient, nodeName, labelDeletePatch)
}

func PatchDeleteNodeAnnotationKeyCR(ctx context.Context, client client.Client, node *corev1.Node, key string) error {
	var annotationDeletePatch AnnotationDeletePatch
	annotationDeletePatch.Metadata.Annotations = map[string]interface{}{key: nil}
	return PatchNodeCR(ctx, client, node, annotationDeletePatch)
}

func PatchNodeAnnotationKeyCR(ctx context.Context, client client.Client, node *corev1.Node, key string, value string) error {
	var annotationPatch AnnotationPatch
	annotationPatch.Metadata.Annotations = map[string]string{key: value}
	return PatchNodeCR(ctx, client, node, annotationPatch)
}
