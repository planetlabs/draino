package kubernetes

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
)

type GlobalConfig struct {
	// Main context
	Context context.Context

	// ConfigName name of the configuration running this controller, we can have different configuration and multiple draino pods (one per config)
	ConfigName string

	// PVCManagementEnableIfNoEvictionUrl PVC management is enabled by default if there is no evictionURL defined
	PVCManagementEnableIfNoEvictionUrl bool

	// SuppliedConditions List of conditions that the controller should react on
	SuppliedConditions []SuppliedCondition
}

type FilterOptions struct {
	DoNotEvictPodControlledBy              []string
	EvictLocalStoragePods                  bool
	ProtectedPodAnnotations                []string
	DoNotCandidatePodControlledBy          []string
	CandidateLocalStoragePods              bool
	ExcludeStatefulSetOnNodeWithoutStorage bool
	CandidateProtectedPodAnnotations       []string
	OptInPodAnnotations                    []string
	ShortLivedPodAnnotations               []string
	NodeLabels                             []string
	NodeLabelsExpr                         string
}

type FiltersDefinitions struct {
	// CandidatePodFilter, Should the pod block the node for being candidate ?
	CandidatePodFilter PodFilterFunc
	// DrainPodFilter, Should the pod be drained ?
	DrainPodFilter PodFilterFunc

	// NodeLabelFilter, Is the node eligible (label checks only) ?
	NodeLabelFilter NodeLabelFilterFunc
}

func GenerateFilters(cs *kubernetes.Clientset, store RuntimeObjectStore, log *zap.Logger, options FilterOptions) (FiltersDefinitions, error) {
	pf := []PodFilterFunc{MirrorPodFilter}
	if !options.EvictLocalStoragePods {
		pf = append(pf, LocalStoragePodFilter)
	}

	apiResources, err := GetAPIResourcesForGVK(cs, options.DoNotEvictPodControlledBy, log)
	if err != nil {
		return FiltersDefinitions{}, fmt.Errorf("failed to get resources for controlby filtering for eviction: %v", err)
	}
	if len(apiResources) > 0 {
		for _, apiResource := range apiResources {
			if apiResource == nil {
				log.Info("Filtering pod that are uncontrolled for eviction")
			} else {
				log.Info("Filtering pods controlled by apiresource for eviction", zap.Any("apiresource", *apiResource))
			}
		}
		pf = append(pf, NewPodControlledByFilter(apiResources))
	}
	systemKnownAnnotations := []string{
		// https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md#what-types-of-pods-can-prevent-ca-from-removing-a-node
		"cluster-autoscaler.kubernetes.io/safe-to-evict=false",
	}
	pf = append(pf, UnprotectedPodFilter(store, false, append(systemKnownAnnotations, options.ProtectedPodAnnotations...)...))

	// Candidate Filtering
	podFilterCandidate := []PodFilterFunc{}
	if !options.CandidateLocalStoragePods {
		podFilterCandidate = append(podFilterCandidate, LocalStoragePodFilter)
	}
	apiResourcesPodControllerBy, err := GetAPIResourcesForGVK(cs, options.DoNotCandidatePodControlledBy, log)
	if err != nil {
		return FiltersDefinitions{}, fmt.Errorf("failed to get resources for 'controlledBy' filtering for being candidate: %v", err)
	}
	if len(apiResourcesPodControllerBy) > 0 {
		for _, apiResource := range apiResourcesPodControllerBy {
			if apiResource == nil {
				log.Info("Filtering pods that are uncontrolled for being candidate")
			} else {
				log.Info("Filtering pods controlled by apiresource for being candidate", zap.Any("apiresource", *apiResource))
			}
		}
		podFilterCandidate = append(podFilterCandidate, NewPodControlledByFilter(apiResourcesPodControllerBy))
	}
	podFilterCandidate = append(podFilterCandidate, UnprotectedPodFilter(store, true, options.CandidateProtectedPodAnnotations...))

	// To maintain compatibility with draino v1 version we have to exclude pods from STS running on node without local-storage
	if options.ExcludeStatefulSetOnNodeWithoutStorage {
		podFilterCandidate = append(podFilterCandidate, NewPodFiltersNoStatefulSetOnNodeWithoutDisk(store))
	}

	consolidatedOptInAnnotations := append(options.OptInPodAnnotations, options.ShortLivedPodAnnotations...)
	drainerSkipPodFilter := NewPodFiltersIgnoreCompletedPods(
		NewPodFiltersIgnoreShortLivedPods(
			NewPodFiltersWithOptInFirst(PodOrControllerHasAnyOfTheAnnotations(store, consolidatedOptInAnnotations...), NewPodFilters(pf...)),
			store, options.ShortLivedPodAnnotations...))

	podFilteringFunc := NewPodFiltersIgnoreCompletedPods(
		NewPodFiltersWithOptInFirst(
			PodOrControllerHasAnyOfTheAnnotations(store, consolidatedOptInAnnotations...), NewPodFilters(podFilterCandidate...)))

	// Node filtering
	if len(options.NodeLabels) > 0 {
		log.Info("node labels", zap.Any("labels", options.NodeLabels))
		if options.NodeLabelsExpr != "" {
			return FiltersDefinitions{}, fmt.Errorf("nodeLabels and NodeLabelsExpr cannot both be set")
		}
		if ptrStr, err := ConvertLabelsToFilterExpr(options.NodeLabels); err != nil {
			return FiltersDefinitions{}, err
		} else {
			options.NodeLabelsExpr = *ptrStr
		}
	}
	log.Debug("label expression", zap.Any("expr", options.NodeLabelsExpr))
	nodeLabelFilterFunc, err := NewNodeLabelFilter(options.NodeLabelsExpr, log)
	if err != nil {
		return FiltersDefinitions{}, fmt.Errorf("Failed to parse node label expression: %v", err)
	}

	return FiltersDefinitions{
		CandidatePodFilter: podFilteringFunc,
		DrainPodFilter:     drainerSkipPodFilter,
		NodeLabelFilter:    nodeLabelFilterFunc,
	}, nil
}
