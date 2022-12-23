package candidate_runner

import (
	v1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestCompareNoAnnotationDrainASAP(t *testing.T) {
	tests := []struct {
		name string
		n1   *v1.Node
		n2   *v1.Node
		want bool
	}{
		{
			name: "no labels x2", want: false,
			n1: &v1.Node{},
			n2: &v1.Node{},
		},
		{
			name: "no labels on 1", want: false,
			n1: &v1.Node{},
			n2: &v1.Node{
				ObjectMeta: meta.ObjectMeta{
					Labels: map[string]string{NodeAnnotationDrainASAPKey: ""},
				}},
		},
		{
			name: "no labels on 2", want: true,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Labels: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
			n2: &v1.Node{},
		},
		{
			name: "no labels on 2", want: false,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Labels: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Labels: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
		},
		{
			name: "key only on 1", want: true,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Labels: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Labels: map[string]string{"other": ""},
			}},
		},
		{
			name: "key only on 2", want: false,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Labels: map[string]string{"other": ""},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Labels: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompareNoAnnotationDrainASAP(tt.n1, tt.n2); got != tt.want {
				t.Errorf("CompareNoAnnotationDrainASAP() = %v, want %v", got, tt.want)
			}
		})
	}
}
