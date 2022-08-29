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
	github.com/antonmedv/expr v1.8.8
	github.com/go-test/deep v1.0.2
	github.com/googleapis/gnostic v0.0.0-20170729233727-0c5108395e2d
	github.com/julienschmidt/httprouter v1.1.0
	github.com/oklog/run v1.0.0
	github.com/prometheus/client_golang v0.9.2
	github.com/stretchr/testify v1.7.0
	go.opencensus.io v0.22.4
	go.uber.org/zap v1.13.0
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11
	gopkg.in/DataDog/dd-trace-go.v1 v1.37.1
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	k8s.io/api v0.0.0-20190819141258-3544db3b9e44
	k8s.io/apimachinery v0.17.0
	k8s.io/client-go v8.0.0+incompatible
	k8s.io/klog v0.3.1
)
