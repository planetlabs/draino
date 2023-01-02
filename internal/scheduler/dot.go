package scheduler

import (
	"fmt"
	"strings"
)

type edge struct {
	node string
}
type graph[T any] struct {
	nodesEdges map[string][]edge
	nodes      map[string]*node[T]
	current    *node[T]
}

func newGraph[T any]() *graph[T] {
	return &graph[T]{
		nodesEdges: make(map[string][]edge),
		nodes:      make(map[string]*node[T]),
	}
}

func (g *graph[T]) addEdge(from, to string) {
	g.nodesEdges[from] = append(g.nodesEdges[from], edge{node: to})
}

func (g *graph[T]) addNode(id string, node *node[T]) {
	g.nodes[id] = node
}

func (g *graph[T]) setCurrent(current *node[T]) {
	g.current = current
}

func (g *graph[T]) getEdges(node string) []edge {
	return g.nodesEdges[node]
}

func (e *edge) String() string {
	return fmt.Sprintf("%v", e.node)
}

func (g *graph[T]) String(renderer ItemRenderer[T]) string {
	out := `digraph tree {
		rankdir=TB;
		ordering="out";
		size="8,5"
		node [
			style=filled
			shape=record
			fontname="Helvetica,Arial,sans-serif"
			fillcolor="#d6d5ed" color="#0004b2"
		];
		current [ fillcolor="#f4db68" color="#8e7c2c" ]
`
	for k, n := range g.nodes {
		out += fmt.Sprintf("%s %s;\n", k, propertiesAsLabel(n, renderer))
	}
	for k := range g.nodesEdges {
		for _, v := range g.getEdges(k) {
			out += fmt.Sprintf("\t%s->%s\t;\n", k, v.node)
		}
	}
	out += "}"
	return out
}

type ItemRenderer[T any] func(T) string

func (n *node[T]) buildDotGraph(g *graph[T], prefix string) {
	g.addNode(prefix, n)
	for i, c := range n.children {
		cID := fmt.Sprintf("%s_%d", prefix, i)
		c.buildDotGraph(g, cID)
		g.addEdge(prefix, cID)
	}
	if g.current == n {
		g.addEdge(prefix, "current\t[dir=back]")
	}
}

func valuesAsLabel[T any](n *node[T]) string {
	return `[ ` + labelJoinedValues(n) + color(n) + ` ]`
}

func propertiesAsLabel[T any](n *node[T], renderer ItemRenderer[T]) string {
	return `[ ` + labelProperties(n, renderer) + color(n) + ` ]`
}

func labelJoinedValues[T any](n *node[T]) string {
	var name []string
	for _, e := range n.rawCollection {
		name = append(name, fmt.Sprintf("%v", e))
	}
	return `label="` + strings.Join(name, "_") + `" `
}

func labelProperties[T any](n *node[T], renderer ItemRenderer[T]) string {
	count := fmt.Sprintf("count:%d\\n", len(n.rawCollection))
	current := fmt.Sprintf("current:%d\\n", n.current)
	leafValues := ""
	if n.isLeaf() {
		var values []string
		for _, e := range n.rawCollection {
			if renderer != nil {
				values = append(values, fmt.Sprintf("%v", renderer(e)))
			} else {
				values = append(values, fmt.Sprintf("%v", e))
			}
		}
		leafValues = "|" + strings.Join(values, "\\n")
	}
	return `label="{` + count + current + leafValues + `}" `
}

func color[T any](n *node[T]) string {
	color := ""
	if n.isLeaf() {
		color = `fillcolor="#d6edd5" color="#04b200"` // green
	} else if n.children == nil {
		color = `fillcolor="#edd6d5" color="#b20400"` // red
	}
	return color + " "
}
