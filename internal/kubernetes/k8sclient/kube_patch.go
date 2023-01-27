package k8sclient

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type JSONPatchValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

type AnnotationPatch struct {
	Key   string
	Value string
}

var _ client.Patch = &AnnotationPatch{}

func (p *AnnotationPatch) Type() types.PatchType {
	return types.JSONPatchType
}
func (p *AnnotationPatch) Data(obj client.Object) ([]byte, error) {
	jsonPatch := JSONPatchValue{
		Op: "replace",
		// as the patch path might include "/", we have to escape the slashes in the annotation path with '~1'
		Path:  fmt.Sprintf("/metadata/annotations/%s", strings.ReplaceAll(p.Key, "/", "~1")),
		Value: p.Value,
	}

	return json.Marshal([]JSONPatchValue{jsonPatch})
}
