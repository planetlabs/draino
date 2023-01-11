package diagnostics

import "context"

const (
	nodeDiagnosticAnnotationKey = "node-lifecyle.datadoghq.com/diagnostics"
)

type Diagnostician interface {
	GetNodeDiagnostic(ctx context.Context, nodeName string) interface{}
	GetName() string
}
