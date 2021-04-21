module github.com/planetlabs/draino

go 1.13

// Kube 1.15.3
replace k8s.io/client-go => k8s.io/client-go v0.0.0-20190819141724-e14f31a72a77

// Kube 1.15.3
replace k8s.io/api => k8s.io/api v0.0.0-20190819141258-3544db3b9e44

// Kube 1.15.3
replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20190817020851-f2f3a405f61d

require (
	contrib.go.opencensus.io/exporter/prometheus v0.1.0
	github.com/alecthomas/template v0.0.0-20160405071501-a0175ee3bccc // indirect
	github.com/alecthomas/units v0.0.0-20151022065526-2efee857e7cf // indirect
	github.com/antonmedv/expr v1.8.8
	github.com/go-test/deep v1.0.1
	github.com/googleapis/gnostic v0.0.0-20170729233727-0c5108395e2d
	github.com/julienschmidt/httprouter v1.1.0
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/oklog/run v1.0.0
	github.com/onsi/ginkgo v1.12.0 // indirect
	github.com/onsi/gomega v1.9.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stretchr/testify v1.5.1
	go.opencensus.io v0.21.0
	go.uber.org/atomic v1.3.2 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.9.1
	golang.org/x/time v0.0.0-20161028155119-f51c12702a4d
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	k8s.io/api v0.0.0-20190819141258-3544db3b9e44
	k8s.io/apimachinery v0.0.0-20190817020851-f2f3a405f61d
	k8s.io/client-go v8.0.0+incompatible
	k8s.io/klog v0.3.1
)
