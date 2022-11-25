package scheduler

import (
	url2 "net/url"
	"sort"
	"strings"
)

// ItemProvider is in charge of delivering items
type ItemProvider[T any] interface {
	// Next return the next item. The boolean is false and the T is the default value if nothing is found.
	Next() (T, bool)
}

// LessFunc is able to sort 2 items
type LessFunc[T any] func(i, j T) bool

// SortingTree is delivered items by sorting them on the fly.
// Sorting function have a priority, the branches of tree are created by apply the sorting function for each level of depth of the tree.
// The branches and leaves are sorted left is less. The branches of the tree are dynamically built when needed while the user is calling Next.
// It is possible to ask for a dot format representation of the tree.
type SortingTree[T any] interface {
	ItemProvider[T]

	// Initialize the SortingTree with items and sorting functions
	// The oder of sorters function is important: the rank determine the function to be used for a given depth of the tree.
	Initialize(items []T, sorters []LessFunc[T])

	// AsDotGraph export the Tree as a digraph in dot format
	// if url is set the output is a clickable URL to dreampuf.github.io/GraphvizOnline/
	AsDotGraph(url bool) string
}

// bucketSlice helps to isolate items in buckets according to the sorting function.
// Items that are equal are placed in the same bucket.
// The slice of slices is sorted so that the first slice contains the "smallest" items.
func bucketSlice[T any](s []T, less LessFunc[T]) [][]T {
	if len(s) == 0 {
		return nil
	}

	// we can probably optimize this algorithm to make the buckets at the same
	// time that we do the first sort. For the moment we privilege code simplicity against performance.
	sort.Slice(s, func(i, j int) bool { return less(s[i], s[j]) })
	current := []T{s[0]}
	var buckets [][]T
	for i := 1; i < len(s); i++ {
		if !less(s[i-1], s[i]) && !less(s[i], s[i-1]) { // items are equal: they go to same bucket
			current = append(current, s[i])
			continue
		}
		buckets = append(buckets, current)
		current = []T{s[i]}
	}
	buckets = append(buckets, current)
	return buckets
}

// Validate that the SortingTreeImpl correctly implement the interface
var _ SortingTree[any] = &sortingTreeImpl[any]{}

// sortingTreeImpl is a possible implementation of the SortingTree interface
type sortingTreeImpl[T any] struct {
	root    *node[T]
	sorters []LessFunc[T]

	// currentNode is pointing to the node in the tree that should provide the Next item.
	currentNode *node[T]
}

// NewSortingTree creates and initializes a SortingTree
func NewSortingTree[T any](items []T, sorters []LessFunc[T]) SortingTree[T] {
	t := &sortingTreeImpl[T]{}
	t.Initialize(items, sorters)
	return t
}

// Initialize the sortingTreeImpl with items (no need to be sorted) and sorting functions (must be ordered).
func (s *sortingTreeImpl[T]) Initialize(items []T, sorters []LessFunc[T]) {
	s.root = &node[T]{
		rawCollection: items,
		sorters:       sorters,
		parent:        nil,
		children:      nil,
		current:       0,
	}
	s.currentNode = s.root
	s.sorters = sorters
}

// Next implements the ItemProvider interface for the sortingTreeImpl
func (s *sortingTreeImpl[T]) Next() (T, bool) {
	// defined the default value that will be used in return if we don't find any item.
	var null T
	// if no current node that means that we are done with exploration of the tree
	if s.currentNode == nil {
		return null, false
	}
	// check if current node is expended
	t, n, found := s.currentNode.next()
	s.currentNode = n
	if found {
		return t, true
	}
	return null, false
}

// AsDotGraph implements the SortingTree interface for sortingTreeImpl
func (s *sortingTreeImpl[T]) AsDotGraph(url bool) string {
	g := newGraph[T]()
	if s.currentNode != nil {
		g.setCurrent(s.currentNode)
	}
	s.root.buildDotGraph(g, "R")
	if !url {
		return g.String()
	}
	t := &url2.URL{Path: g.String()}
	return "https://dreampuf.github.io/GraphvizOnline/#" + strings.TrimLeft(t.String(), "./")
}

// node of the sortingTreeImpl
// each node has the list of items that node, could be the items for that node
// or for the children nodes once the node has been expanded
type node[T any] struct {
	// all the items under that node.
	// we will iterate over them only if
	// there is no more sorter to apply and expand the tree,
	// else we expand the tree an the items of that collection will be distributed to children nodes
	rawCollection []T

	// ordered list of sorters to expand the subtrees
	sorters []LessFunc[T]

	parent   *node[T]
	children []*node[T]

	// if the node is a leaf the current point at the index in the rawCollection
	// else it is the index in the children list
	// the pointed item is the one to be used for the delivery of the "Next" item
	// a value pointing after the slice indicates that we are done with the exploration
	// of the items for that nodes
	current int
}

// expand create the sub nodes by apply the sorters[0]
// if there is no more sorters to apply, the function does not do anything
func (n *node[T]) expand() {
	if n.children != nil {
		return // already expanded
	}

	// no need to apply the sorter and expand if there is only one item in the list
	if len(n.rawCollection) == 1 {
		return
	}

	if n.sorters != nil && len(n.sorters) > 0 {
		buckets := bucketSlice(n.rawCollection, n.sorters[0])

		for _, b := range buckets {
			newNode := &node[T]{rawCollection: b, sorters: n.sorters[1:], parent: n}
			n.children = append(n.children, newNode)
		}

		// clear the rawCollection to avoid space problem for large trees
		n.rawCollection = nil
	}
}

// isLeaf a leaf cannot be expanded
func (n *node[T]) isLeaf() bool {
	if n.sorters == nil || len(n.sorters) == 0 || len(n.rawCollection) == 1 {
		return true
	}
	return false
}

// next explore the node and return the next item
// the node is expanded if it is not a leaf
// the function is using recursion, the level of recursion cannot exceed the size of the sorters
// because the size of sorters determine the maximum depth of the tree
// the item found is return with the node to be explored at next iteration
// the boolean is false only if there is no more items in the tree.
func (n *node[T]) next() (T, *node[T], bool) {
	var null T
	if n.isLeaf() {
		if n.current < len(n.rawCollection) {
			n.current++
			return n.rawCollection[n.current-1], n, true
		}

		if n.parent == nil {
			return null, nil, false
		}
		n.parent.current++
		return n.parent.next()
	}

	n.expand()
	if n.current < len(n.children) {
		return n.children[n.current].next()
	}

	if n.parent == nil {
		return null, nil, false
	}
	n.parent.current++
	return n.parent.next()
}
