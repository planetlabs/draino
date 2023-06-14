/*
Copyright 2018 Planet Labs Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions
and limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/planetlabs/draino/internal/diagnostics"
	"github.com/planetlabs/draino/internal/metrics"

	"github.com/DataDog/compute-go/controllerruntime"
	"github.com/DataDog/compute-go/infraparameters"
	"github.com/DataDog/compute-go/kubeclient"
	"github.com/DataDog/compute-go/service"
	"github.com/DataDog/compute-go/version"
	httptrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/planetlabs/draino/internal/candidate_runner"
	"github.com/planetlabs/draino/internal/candidate_runner/filters"
	"github.com/planetlabs/draino/internal/candidate_runner/sorters"
	"github.com/planetlabs/draino/internal/cli"
	drainbuffer "github.com/planetlabs/draino/internal/drain_buffer"
	"github.com/planetlabs/draino/internal/drain_runner"
	preprocessor "github.com/planetlabs/draino/internal/drain_runner/pre_processor"
	"github.com/planetlabs/draino/internal/groups"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/analyser"
	"github.com/planetlabs/draino/internal/kubernetes/drain"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	drainoklog "github.com/planetlabs/draino/internal/kubernetes/klog"
	"github.com/planetlabs/draino/internal/limit"
	"github.com/planetlabs/draino/internal/observability"
	protector "github.com/planetlabs/draino/internal/protector"

	client "k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Read application flags
	cfg, fs := controllerruntime.ConfigFromFlags(true, false)
	cliHandlers := &cli.CLIHandlers{}
	cliCommands := &cli.CLICommands{ServerAddr: &cfg.Service.ServiceAddr}

	cfg.Service.RouteRegisterFunc = cliHandlers.RegisterRoute
	options, optFlags := optionsFromFlags()
	fs.AddFlagSet(optFlags)

	root := &cobra.Command{
		Short:        "draino",
		Long:         "draino",
		SilenceUsage: true,
	}
	root.PersistentFlags().AddFlagSet(fs)
	root.AddCommand(version.NewCommand())
	root.AddCommand(service.NewLogLevelCommand())
	root.AddCommand(cliCommands.Commands()...)

	root.RunE = func(cmd *cobra.Command, args []string) error {
		if errOptions := options.Validate(); errOptions != nil {
			return errOptions
		}

		zapConfig := zap.NewProductionConfig()
		zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		zlog, err := zapConfig.Build()
		if err != nil {
			return err
		}
		if options.debug {
			zlog, err = zap.NewDevelopment()
		}
		drainoklog.InitializeKlog(options.klogVerbosity)
		drainoklog.RedirectToLogger(zlog)

		defer zlog.Sync() // nolint:errcheck // no check required on program exit

		go launchTracerAndProfiler()

		// use a Go context so we can tell the leaderelection and other pieces when we want to step down
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		globalConfig := kubernetes.GlobalConfig{
			Context:                            ctx,
			ConfigName:                         options.configName,
			SuppliedConditions:                 options.suppliedConditions,
			PVCManagementEnableIfNoEvictionUrl: options.pvcManagementByDefault,
		}

		validationOptions := infraparameters.GetValidateAll()
		validationOptions.Datacenter, validationOptions.CloudProvider, validationOptions.CloudProviderProject, validationOptions.KubeClusterName = false, false, false, false
		if err := cfg.InfraParam.Validate(validationOptions); err != nil {
			return fmt.Errorf("infra param validation error: %v\n", err)
		}

		mgr, logger, _, err := controllerruntime.NewManager(cfg)
		if err != nil {
			return fmt.Errorf("error while creating manager: %v\n", err)
		}

		httpRunner := DrainoLegacyMetrics(ctx, options, logger)
		if err := mgr.Add(httpRunner); err != nil {
			return fmt.Errorf("Failed to add metrics http runner to manager: %v", err)
		}

		cs, err2 := GetKubernetesClientSet(&cfg.KubeClientConfig)
		if err2 != nil {
			return err2
		}

		pods := kubernetes.NewPodWatch(ctx, cs)
		statefulSets := kubernetes.NewStatefulsetWatch(ctx, cs)
		deployments := kubernetes.NewDeploymentWatch(ctx, cs)
		persistentVolumes := kubernetes.NewPersistentVolumeWatch(ctx, cs)
		persistentVolumeClaims := kubernetes.NewPersistentVolumeClaimWatch(ctx, cs)
		nodes := kubernetes.NewNodeWatch(ctx, cs)
		store := &kubernetes.RuntimeObjectStoreImpl{
			DeploymentStore:            deployments,
			StatefulSetsStore:          statefulSets,
			PodsStore:                  pods,
			PersistentVolumeStore:      persistentVolumes,
			PersistentVolumeClaimStore: persistentVolumeClaims,
			NodesStore:                 nodes,
		}

		filteringOptions := kubernetes.FilterOptions{
			DoNotEvictPodControlledBy:              options.doNotEvictPodControlledBy,
			EvictLocalStoragePods:                  options.evictLocalStoragePods,
			ProtectedPodAnnotations:                options.protectedPodAnnotations,
			DoNotCandidatePodControlledBy:          options.doNotCandidatePodControlledBy,
			CandidateLocalStoragePods:              options.candidateLocalStoragePods,
			ExcludeStatefulSetOnNodeWithoutStorage: options.excludeStatefulSetOnNodeWithoutStorage,
			CandidateProtectedPodAnnotations:       options.candidateProtectedPodAnnotations,
			OptInPodAnnotations:                    options.optInPodAnnotations,
			ShortLivedPodAnnotations:               options.shortLivedPodAnnotations,
			NodeLabels:                             options.nodeLabels,
			NodeLabelsExpr:                         options.nodeLabelsExpr,
		}

		filtersDef, err := kubernetes.GenerateFilters(cs, store, zlog, filteringOptions)
		if err != nil {
			return err
		}

		eventRecorderForDrainerActivities, _ := kubernetes.BuildEventRecorderWithAggregationOnEventTypeAndMessage(zapr.NewLogger(zlog), cs, options.eventAggregationPeriod, options.logEvents)
		drainerAPI := kubernetes.NewAPIDrainer(cs,
			eventRecorderForDrainerActivities,
			kubernetes.MaxGracePeriod(options.minEvictionTimeout),
			kubernetes.EvictionHeadroom(options.evictionHeadroom),
			kubernetes.WithSkipDrain(options.skipDrain),
			kubernetes.WithPodFilter(filtersDef.DrainPodFilter),
			kubernetes.WithStorageClassesAllowingDeletion(options.storageClassesAllowingVolumeDeletion),
			kubernetes.WithMaxDrainAttemptsBeforeFail(options.maxDrainAttemptsBeforeFail),
			kubernetes.WithGlobalConfig(globalConfig),
			kubernetes.WithAPIDrainerLogger(zlog),
			kubernetes.WithRuntimeObjectStore(store),
			kubernetes.WithContainerRuntimeClient(mgr.GetClient()),
		)

		indexer, err := index.New(ctx, mgr.GetClient(), mgr.GetCache(), logger)
		if err != nil {
			return fmt.Errorf("error while initializing informer: %v\n", err)
		}

		globalBlocker := kubernetes.NewGlobalBlocker(logger)
		for p, f := range options.maxNotReadyNodesFunctions {
			globalBlocker.AddBlocker("MaxNotReadyNodes:"+p, f(indexer, logger), options.maxNotReadyNodesPeriod)
		}
		for p, f := range options.maxPendingPodsFunctions {
			globalBlocker.AddBlocker("MaxPendingPods:"+p, f(indexer, logger), options.maxPendingPodsPeriod)
		}

		eventRecorder, _ := kubernetes.BuildEventRecorderWithAggregationOnEventType(logger, cs, options.eventAggregationPeriod, options.excludedPodsPerNodeEstimation, options.logEvents)
		eventRecorderForDrainRunnerActivities, _ := kubernetes.BuildEventRecorderWithAggregationOnEventTypeAndMessage(logger, cs, options.eventAggregationPeriod, options.logEvents)

		persistor := drainbuffer.NewConfigMapPersistor(cs.CoreV1().ConfigMaps(cfg.InfraParam.Namespace), options.drainBufferConfigMapName, cfg.InfraParam.Namespace)
		drainBuffer := drainbuffer.NewDrainBuffer(ctx, persistor, clock.RealClock{}, mgr.GetLogger(), eventRecorder, indexer, store, options.drainBuffer)
		// The drain buffer can only be initialized when the manager client cache was started.
		// Adding a custom runnable to the controller manager will make sure, that the initialization will be started as soon as possible.
		if err := mgr.Add(getInitDrainBufferRunner(drainBuffer, &logger)); err != nil {
			logger.Error(err, "cannot setup drain buffer initialization runnable")
			return err
		}

		keyGetter := groups.NewGroupKeyFromNodeMetadata(mgr.GetClient(), mgr.GetLogger(), eventRecorder, indexer, store, strings.Split(options.drainGroupLabelKey, ","), []string{groups.DrainGroupAnnotation}, groups.DrainGroupOverrideAnnotation)

		staticRetryStrategy := &drain.StaticRetryStrategy{AlertThreashold: 7, Delay: options.schedulingRetryBackoffDelay}
		exponentialRetryStrategy := &drain.ExponentialRetryStrategy{AlertThreashold: 7, Delay: options.schedulingRetryBackoffDelay}
		retryWall, errRW := drain.NewRetryWall(mgr.GetClient(), mgr.GetLogger(), staticRetryStrategy, exponentialRetryStrategy)
		if errRW != nil {
			return errRW
		}

		pvcProtector := protector.NewPVCProtector(store, zlog, globalConfig.PVCManagementEnableIfNoEvictionUrl)
		stabilityPeriodChecker := analyser.NewStabilityPeriodChecker(ctx, logger, mgr.GetClient(), nil, store, indexer, analyser.StabilityPeriodCheckerConfiguration{}, filtersDef.DrainPodFilter)
		filterFactory, err := filters.NewFactory(
			filters.WithLogger(mgr.GetLogger()),
			filters.WithRetryWall(retryWall),
			filters.WithRuntimeObjectStore(store),
			filters.WithPodFilterFunc(filtersDef.CandidatePodFilter),
			filters.WithNodeLabelsFilterFunction(filtersDef.NodeLabelFilter),
			filters.WithGlobalConfig(globalConfig),
			filters.WithStabilityPeriodChecker(stabilityPeriodChecker),
			filters.WithDrainBuffer(drainBuffer),
			filters.WithGroupKeyGetter(keyGetter),
			filters.WithGlobalBlocker(globalBlocker),
			filters.WithEventRecorder(eventRecorder),
			filters.WithPVCProtector(pvcProtector),
		)
		if err != nil {
			logger.Error(err, "failed to configure the filters")
			return err
		}

		nodeReplacer := preprocessor.NewNodeReplacer(mgr.GetClient(), mgr.GetLogger(), &clock.RealClock{})
		drainRunnerFactory, err := drain_runner.NewFactory(
			drain_runner.WithKubeClient(mgr.GetClient()),
			drain_runner.WithClock(&clock.RealClock{}),
			drain_runner.WithDrainer(drainerAPI),
			drain_runner.WithPreprocessors(
				preprocessor.NewWaitTimePreprocessor(options.waitBeforeDraining),
				preprocessor.NewNodeReplacementPreProcessor(mgr.GetClient(), options.preprovisioningActivatedByDefault, mgr.GetLogger(), &clock.RealClock{}),
				preprocessor.NewPreActivitiesPreProcessor(mgr.GetClient(), indexer, store, mgr.GetLogger(), eventRecorderForDrainRunnerActivities, clock.RealClock{}, options.preActivityDefaultTimeout),
			),
			drain_runner.WithRerun(options.groupRunnerPeriod),
			drain_runner.WithRetryWall(retryWall),
			drain_runner.WithLogger(mgr.GetLogger()),
			drain_runner.WithSharedIndexInformer(indexer),
			drain_runner.WithEventRecorder(eventRecorderForDrainRunnerActivities),
			drain_runner.WithFilter(filterFactory.BuildCandidateFilter()),
			drain_runner.WithDrainBuffer(drainBuffer),
			drain_runner.WithGlobalConfig(globalConfig),
			drain_runner.WithBeforeReplacementDuration(options.durationBeforeReplacement),
			drain_runner.WithNodeReplacer(nodeReplacer),
			drain_runner.WithPVCProtector(pvcProtector),
		)
		if err != nil {
			logger.Error(err, "failed to configure the drain_runner")
			return err
		}

		simulationPodFilter := kubernetes.NewPodFilters(filtersDef.DrainPodFilter)
		simulationRateLimiter := limit.NewRateLimiter(clock.RealClock{}, cfg.KubeClientConfig.QPS*options.simulationRateLimitingRatio, int(float32(cfg.KubeClientConfig.Burst)*options.simulationRateLimitingRatio))
		simulator := drain.NewDrainSimulator(context.Background(), mgr.GetClient(), indexer, simulationPodFilter, eventRecorder, simulationRateLimiter, logger, store, globalConfig)
		pdbAnalyser := analyser.NewPDBAnalyser(ctx, mgr.GetLogger(), indexer, clock.RealClock{}, options.podWarmupDelayExtension)
		sorters := candidate_runner.NodeSorters{
			sorters.NewAnnotationPrioritizer(store, indexer, eventRecorder, logger),
			sorters.NewConditionComparator(globalConfig.SuppliedConditions),
			pdbAnalyser.CompareNode,
		}

		drainCandidateRunnerFactory, err := candidate_runner.NewFactory(
			candidate_runner.WithKubeClient(mgr.GetClient()),
			candidate_runner.WithClock(&clock.RealClock{}),
			candidate_runner.WithRerun(options.groupRunnerPeriod),
			candidate_runner.WithLogger(mgr.GetLogger()),
			candidate_runner.WithSharedIndexInformer(indexer),
			candidate_runner.WithEventRecorder(eventRecorder),
			candidate_runner.WithMaxSimultaneousCandidates(1), // TODO should we move that to something that can be customized per user
			candidate_runner.WithMaxSimultaneousDrained(5),    // TODO should we move that to something that can be customized per user
			candidate_runner.WithFilter(filterFactory.BuildCandidateFilter()),
			candidate_runner.WithDrainSimulator(simulator),
			candidate_runner.WithNodeSorters(sorters),
			candidate_runner.WithDryRun(options.dryRun),
			candidate_runner.WithRetryWall(retryWall),
			candidate_runner.WithRateLimiter(limit.NewTypedRateLimiter(&clock.RealClock{}, kubernetes.GetRateLimitConfiguration(globalConfig.SuppliedConditions), options.drainRateLimitQPS, options.drainRateLimitBurst)),
			candidate_runner.WithGlobalConfig(globalConfig),
		)
		if err != nil {
			logger.Error(err, "failed to configure the candidate_runner")
			return err
		}

		groupRegistry := groups.NewGroupRegistry(ctx, mgr.GetClient(), mgr.GetLogger(), eventRecorder, keyGetter, drainRunnerFactory, drainCandidateRunnerFactory, filtersDef.NodeLabelFilter, store.HasSynced, options.groupRunnerPeriod)
		if err = groupRegistry.SetupWithManager(mgr); err != nil {
			logger.Error(err, "failed to setup groupRegistry")
			return err
		}
		groupFromPod := groups.NewGroupFromPod(mgr.GetClient(), mgr.GetLogger(), keyGetter, filtersDef.DrainPodFilter, store.HasSynced)
		if err = groupFromPod.SetupWithManager(mgr); err != nil {
			logger.Error(err, "failed to setup groupFromPod")
			return err
		}

		diagnosticFactory, err := diagnostics.NewFactory(
			diagnostics.WithKubeClient(mgr.GetClient()),
			diagnostics.WithClock(&clock.RealClock{}),
			diagnostics.WithLogger(mgr.GetLogger()),
			diagnostics.WithFilter(filterFactory.BuildCandidateFilter()),
			diagnostics.WithDrainSimulator(simulator),
			diagnostics.WithNodeSorters(sorters),
			diagnostics.WithRetryWall(retryWall),
			diagnostics.WithDrainBuffer(drainBuffer),
			diagnostics.WithGlobalConfig(globalConfig),
			diagnostics.WithKeyGetter(keyGetter),
			diagnostics.WithStabilityPeriodChecker(stabilityPeriodChecker),
		)
		if err != nil {
			logger.Error(err, "failed to configure the diagnostics")
			return err
		}

		nodeDiagnostician := diagnosticFactory.BuildDiagnostician()
		diagnostics := diagnostics.NewDiagnosticsController(ctx, mgr.GetClient(), mgr.GetLogger(), eventRecorder, []diagnostics.Diagnostician{nodeDiagnostician}, store.HasSynced)
		if err = diagnostics.SetupWithManager(mgr); err != nil {
			logger.Error(err, "failed to setup diagnostics")
			return err
		}

		if errCli := cliHandlers.Initialize(logger, groupRegistry, drainCandidateRunnerFactory.BuildCandidateInfo(), drainRunnerFactory.BuildRunner(), nodeDiagnostician); errCli != nil {
			logger.Error(errCli, "Failed to initialize CLIHandlers")
			return errCli
		}

		scopeObserver := observability.NewScopeObserver(cs, globalConfig, indexer, store, options.scopeAnalysisPeriod, filtersDef,
			kubernetes.PodOrControllerHasAnyOfTheAnnotations(store, options.optInPodAnnotations...),
			kubernetes.PodOrControllerHasAnyOfTheAnnotations(store, options.candidateProtectedPodAnnotations...),
			zlog, retryWall, keyGetter, groupRegistry, filterFactory.BuildCandidateFilter())

		if options.resetScopeLabel == true {
			err = mgr.Add(&RunOnce{fn: func(context.Context) error { scopeObserver.Reset(); return nil }})
			if err != nil {
				logger.Error(err, "failed to attach scope observer cleanup")
				return err
			}
		}

		mgr.Add(&RunOnce{fn: func(ctx context.Context) error {
			return kubernetes.Await(ctx, nodes, pods, statefulSets, deployments, persistentVolumes, persistentVolumeClaims)
		}})

		if err := mgr.Add(globalBlocker); err != nil {
			logger.Error(err, "failed to setup global blocker with controller runtime")
			return err
		}

		if err := mgr.Add(scopeObserver); err != nil {
			logger.Error(err, "failed to setup scope observer with controller runtime")
			return err
		}

		metrics.DrainoRunning(kubernetes.Component, options.dryRun)

		logger.Info("Starting manager")
		if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
			logger.Error(err, "Controller Manager did exit with error")
			return err
		}

		logger.Info("Manager finished without error")
		return nil
	}

	err := root.Execute()
	_ = zap.L().Sync()
	if err != nil {
		zap.L().Fatal("Program exit on error", zap.Error(err))
	}
}

// launchTracerAndProfiler will initialize and run the tracer and the endpoint for the profiler
// the function is blocking launch it in a dedicated go-routine
func launchTracerAndProfiler() {
	tracer.Start(
		tracer.WithService("draino"),
	)
	defer tracer.Stop()
	mux := httptrace.NewServeMux()
	http.ListenAndServe("localhost:8085", mux) // for go profiler
}

func GetKubernetesClientSet(config *kubeclient.Config) (*client.Clientset, error) {
	if err := k8sclient.DecorateWithRateLimiter(config, "default"); err != nil {
		return nil, err
	}
	c, err := kubeclient.NewKubeConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed create Kubernetes client configuration: %v", err)
	}

	cs, err := client.NewForConfig(c)
	if err != nil {
		return nil, fmt.Errorf("failed create Kubernetes client: %v", err)
	}
	return cs, nil
}

// getInitDrainBufferRunner returns a Runnable that is responsible for initializing the drain buffer
func getInitDrainBufferRunner(drainBuffer drainbuffer.DrainBuffer, logger *logr.Logger) manager.Runnable {
	return &RunTillSuccess{
		logger: logger,
		period: time.Second,
		fn:     func(ctx context.Context) error { return drainBuffer.Initialize(ctx) },
	}
}
