module github.com/planetlabs/draino

go 1.19

// in order to use the kubernetes package, we have to replace all packages that are using a local path with a proper version
// https://github.com/kubernetes/kubernetes/blob/v1.23.3/go.mod
// https://suraj.io/post/2021/05/k8s-import/
replace (
	k8s.io/api => k8s.io/api v0.26.0
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.26.0
	k8s.io/apimachinery => k8s.io/apimachinery v0.26.0
	k8s.io/apiserver => k8s.io/apiserver v0.26.0
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.26.0
	k8s.io/client-go => k8s.io/client-go v0.26.0
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.26.0
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.26.0
	k8s.io/code-generator => k8s.io/code-generator v0.26.0
	k8s.io/component-base => k8s.io/component-base v0.26.0
	k8s.io/component-helpers => k8s.io/component-helpers v0.26.0
	k8s.io/controller-manager => k8s.io/controller-manager v0.26.0
	k8s.io/cri-api => k8s.io/cri-api v0.26.0
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.26.0
	k8s.io/dynamic-resource-allocation => k8s.io/dynamic-resource-allocation v0.26.0
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.26.0
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.26.0
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.26.0
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.26.0
	k8s.io/kubectl => k8s.io/kubectl v0.26.0
	k8s.io/kubelet => k8s.io/kubelet v0.26.0
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.26.0
	k8s.io/metrics => k8s.io/metrics v0.26.0
	k8s.io/mount-utils => k8s.io/mount-utils v0.26.0
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.26.0
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.26.0
	k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.26.0
	k8s.io/sample-controller => k8s.io/sample-controller v0.26.0
)

require (
	contrib.go.opencensus.io/exporter/prometheus v0.1.0
	github.com/DataDog/compute-go v1.3.7
	github.com/DataDog/go-service-authn v1.3.0
	github.com/antonmedv/expr v1.8.8
	github.com/go-logr/logr v1.2.3
	github.com/go-logr/zapr v1.2.3
	github.com/go-test/deep v1.0.2
	github.com/google/gnostic v0.5.7-v3refs
	github.com/gorilla/mux v1.8.0
	github.com/julienschmidt/httprouter v1.3.0
	github.com/oklog/run v1.0.0
	github.com/prometheus/client_golang v1.14.0
	github.com/spf13/cobra v1.6.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.0
	go.opencensus.io v0.23.0
	go.uber.org/zap v1.24.0
	golang.org/x/exp v0.0.0-20221031165847-c99f073a8326
	golang.org/x/net v0.3.1-0.20221206200815-1e63c2f08a10
	golang.org/x/time v0.3.0
	gopkg.in/DataDog/dd-trace-go.v1 v1.38.1
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	k8s.io/api v0.26.0
	k8s.io/apimachinery v0.26.0
	k8s.io/client-go v8.0.0+incompatible
	k8s.io/klog v1.0.0
	k8s.io/kubernetes v1.26.0
	k8s.io/utils v0.0.0-20221128185143-99ec85e7a448
	sigs.k8s.io/controller-runtime v0.14.1
)

require (
	github.com/DataDog/datadog-agent/pkg/obfuscate v0.0.0-20211129110424-6491aa3bf583 // indirect
	github.com/DataDog/datadog-go v4.8.2+incompatible // indirect
	github.com/DataDog/datadog-go/v5 v5.0.2 // indirect
	github.com/DataDog/sketches-go v1.0.0 // indirect
	github.com/Microsoft/go-winio v0.5.1 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20190924025748-f65c72e2690d // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/emicklei/go-restful/v3 v3.9.0 // indirect
	github.com/evanphx/json-patch v4.12.0+incompatible // indirect
	github.com/evanphx/json-patch/v5 v5.6.0 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.20.0 // indirect
	github.com/go-openapi/swag v0.19.14 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/heptiolabs/healthcheck v0.0.0-20211123025425-613501dd5deb // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/inconshreveable/mousetrap v1.0.1 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/tinylib/msgp v1.1.2 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/oauth2 v0.0.0-20220223155221-ee480838109b // indirect
	golang.org/x/sys v0.3.0 // indirect
	golang.org/x/term v0.3.0 // indirect
	golang.org/x/text v0.5.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gomodules.xyz/jsonpatch/v2 v2.2.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220502173005-c8bf987b8c21 // indirect
	google.golang.org/grpc v1.49.0 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apiextensions-apiserver v0.26.0 // indirect
	k8s.io/component-base v0.26.0 // indirect
	k8s.io/klog/v2 v2.80.1 // indirect
	k8s.io/kube-openapi v0.0.0-20221012153701-172d655c2280 // indirect
	sigs.k8s.io/json v0.0.0-20220713155537-f223a00ba0e2 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)
