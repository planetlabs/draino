package sorters

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
					Annotations: map[string]string{NodeAnnotationDrainASAPKey: ""},
				}},
		},
		{
			name: "no labels on 2", want: true,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
			n2: &v1.Node{},
		},
		{
			name: "labels on both with empty value", want: false,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
		},
		{
			name: "labels on both with empty value and numeric on 1", want: true,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: "10"},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
		},
		{
			name: "default value is 1, so no difference with an annotation with value 1", want: false,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: "1"},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
		},
		{
			name: "default value is 1, so no difference with an annotation with value 1 (on second)", want: false,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: "1"},
			}},
		},
		{
			name: "labels on both with empty value and numeric on 2", want: false,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: "1"},
			}},
		},
		{
			name: "labels on both with values 1 bigger", want: true,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: "10"},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: "9"},
			}},
		},
		{
			name: "labels on both with values 1 smaller", want: false,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: "8"},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: "9"},
			}},
		},
		{
			name: "key only on 1", want: true,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{"other": ""},
			}},
		},
		{
			name: "key only on 2", want: false,
			n1: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{"other": ""},
			}},
			n2: &v1.Node{ObjectMeta: meta.ObjectMeta{
				Annotations: map[string]string{NodeAnnotationDrainASAPKey: ""},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompareNodeAnnotationDrainASAP(tt.n1, tt.n2); got != tt.want {
				t.Errorf("CompareNodeAnnotationDrainASAP() = %v, want %v", got, tt.want)
			}
		})
	}
}
