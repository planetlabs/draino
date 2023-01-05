module github.com/planetlabs/draino

go 1.18

// Kube 1.21.12
replace k8s.io/client-go => k8s.io/client-go v0.21.12

// Kube 1.21.12
replace k8s.io/api => k8s.io/api v0.21.12

// Kube 1.21.12
replace k8s.io/apimachinery => k8s.io/apimachinery v0.21.12

require (
	contrib.go.opencensus.io/exporter/prometheus v0.1.0
	github.com/antonmedv/expr v1.8.8
	github.com/go-test/deep v1.0.1
	github.com/julienschmidt/httprouter v1.1.0
	github.com/oklog/run v1.0.0
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.6.1
	go.opencensus.io v0.22.3
	go.uber.org/zap v1.9.1
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	k8s.io/api v0.21.12
	k8s.io/apimachinery v0.21.12
	k8s.io/client-go v8.0.0+incompatible
	k8s.io/klog v0.3.1
)

require (
	github.com/alecthomas/template v0.0.0-20160405071501-a0175ee3bccc // indirect
	github.com/alecthomas/units v0.0.0-20151022065526-2efee857e7cf // indirect
	github.com/beorn7/perks v0.0.0-20180321164747-3a771d992973 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/evanphx/json-patch v4.9.0+incompatible // indirect
	github.com/go-logr/logr v0.4.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.0 // indirect
	github.com/google/go-cmp v0.5.5 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/gnostic v0.4.1 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/imdario/mergo v0.3.5 // indirect
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/onsi/ginkgo v1.12.0 // indirect
	github.com/onsi/gomega v1.9.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v0.9.2 // indirect
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4 // indirect
	github.com/prometheus/common v0.0.0-20181126121408-4724e9255275 // indirect
	github.com/prometheus/procfs v0.0.0-20181204211112-1dc9a6cbc91a // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.uber.org/atomic v1.3.2 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	golang.org/x/net v0.0.0-20211209124913-491a49abca63 // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/sys v0.0.0-20210615035016-665e8c7367d1 // indirect
	golang.org/x/term v0.0.0-20210220032956-6a3ed077a48d // indirect
	golang.org/x/text v0.3.6 // indirect
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba // indirect
	google.golang.org/appengine v1.6.5 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
	k8s.io/klog/v2 v2.9.0 // indirect
	k8s.io/kube-openapi v0.0.0-20211110012726-3cc51fd1e909 // indirect
	k8s.io/utils v0.0.0-20211116205334-6203023598ed // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.1 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)
