package scheduler

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

//type ByChar []byte
//
//func (a ByChar) Len() int           { return len(a) }
//func (a ByChar) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
//func (a ByChar) Less(i, j int) bool { return a[i] < a[j] }
//func (a ByChar) At(i int) byte      { return a[i] }
//
//func TestBuckets(t *testing.T) {
//
//	tests := []struct {
//		name     string
//		sortable Sortable[byte]
//		want     [][]byte
//	}{
//		{
//			name:     "empty",
//			sortable: ByChar(""),
//			want:     nil,
//		},
//		{
//			name:     "aaaa",
//			sortable: ByChar("aaaa"),
//			want:     [][]byte{[]byte("aaaa")},
//		},
//		{
//			name:     "zaza",
//			sortable: ByChar("zaza"),
//			want:     [][]byte{[]byte("aa"), []byte("zz")},
//		},
//		{
//			name:     "banana",
//			sortable: ByChar("banana"),
//			want:     [][]byte{[]byte("aaa"), []byte("b"), []byte("nn")},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := Buckets(tt.sortable); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Buckets() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

func TestBucketSlice(t *testing.T) {

	lessByte := func(a, b byte) bool { return a < b }

	tests := []struct {
		name  string
		input []byte
		want  [][]byte
	}{
		{
			name:  "nil",
			input: nil,
			want:  nil,
		},
		{
			name:  "empty",
			input: nil,
			want:  nil,
		},
		{
			name:  "aaaa",
			input: []byte("aaaa"),
			want:  [][]byte{[]byte("aaaa")},
		},
		{
			name:  "zaza",
			input: []byte("zaza"),
			want:  [][]byte{[]byte("aa"), []byte("zz")},
		},
		{
			name:  "banana",
			input: []byte("banana"),
			want:  [][]byte{[]byte("aaa"), []byte("b"), []byte("nn")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bucketSlice(tt.input, lessByte); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("bucketSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewSortingTree2(t *testing.T) {
	By10 := func(a, b int) bool { return a/10 < b/10 }
	ReverseOnMod10 := func(a, b int) bool { return a%10 > b%10 }
	tree := NewSortingTree(
		[]int{19, 55, 3, 13, 2, 21, 33, 15, 56, 21},
		[]LessFunc[int]{By10, ReverseOnMod10})

	fmt.Println(tree.AsDotGraph(true))

	v, found := tree.Next()
	assert.True(t, found, "3 should be found")
	assert.Equal(t, 3, v, "Value should be 3")

	fmt.Println(tree.AsDotGraph(true))

	v, found = tree.Next()
	assert.True(t, found, "2 should be found")
	assert.Equal(t, 2, v, "Value should be 2")

	fmt.Println(tree.AsDotGraph(true))

	v, found = tree.Next()
	assert.True(t, found, "19 should be found")
	assert.Equal(t, 19, v, "Value should be 19")

	fmt.Println(tree.AsDotGraph(true))

	v, found = tree.Next()
	assert.True(t, found, "15 should be found")
	assert.Equal(t, 15, v, "Value should be 15")

	fmt.Println(tree.AsDotGraph(true))

	v, found = tree.Next()
	assert.True(t, found, "13 should be found")
	assert.Equal(t, 13, v, "Value should be 13")

	fmt.Println(tree.AsDotGraph(true))

	v, found = tree.Next()
	assert.True(t, found, "21 should be found")
	assert.Equal(t, 21, v, "Value should be 21")

	fmt.Println(tree.AsDotGraph(true))

	v, found = tree.Next()
	assert.True(t, found, "21 should be found")
	assert.Equal(t, 21, v, "Value should be 21")

	fmt.Println(tree.AsDotGraph(true))

	v, found = tree.Next()
	assert.True(t, found, "33 should be found")
	assert.Equal(t, 33, v, "Value should be 33")

	fmt.Println(tree.AsDotGraph(true))

	v, found = tree.Next()
	assert.True(t, found, "56 should be found")
	assert.Equal(t, 56, v, "Value should be 56")

	fmt.Println(tree.AsDotGraph(true))

	v, found = tree.Next()
	assert.True(t, found, "55 should be found")
	assert.Equal(t, 55, v, "Value should be 55")

	fmt.Println(tree.AsDotGraph(true))

	v, found = tree.Next()
	assert.False(t, found, "Nothing should be left")
}
