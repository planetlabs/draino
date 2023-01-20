package drainbuffer

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/DataDog/compute-go/logs"
	"github.com/planetlabs/draino/internal/kubernetes"
	"github.com/planetlabs/draino/internal/kubernetes/index"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/duration"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/groups"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/clock"
)

// DrainBuffer will store information about the last successful drains
type DrainBuffer interface {
	// StoreSuccessfulDrain is adding the given group to the internal store
	// It will return an error in case the initialization was not executed yet
	StoreSuccessfulDrain(groups.GroupKey, time.Duration) error
	// NextDrain returns the next possible drain time for the given group
	// It will return a zero time if the next drain can be done immediately
	// It will return an error in case the initialization was not executed yet
	NextDrain(groups.GroupKey) (time.Time, error)
	// Initialize loads the existing data from the persistent backend
	Initialize(context.Context) error
	// IsReady returns true if the initialization was finished successfully
	IsReady() bool
	// GetDrainBufferConfiguration retrieve the drain buffer configuration with a node (and associated pods)
	GetDrainBufferConfiguration(ctx context.Context, node *v1.Node) (time.Duration, error)
	// GetDrainBufferConfigurationDetails retrieve the drain buffer configuration with a node (and associated pods)
	GetDrainBufferConfigurationDetails(ctx context.Context, node *v1.Node) (*kubernetes.MetadataSearch[time.Duration], error)
}

var _ DrainBuffer = &drainBufferImpl{}

// drainCache is cache used internally by the drain buffer
type drainCache map[groups.GroupKey]drainCacheEntry

// drainCacheEntry stores information about specific cache entries
type drainCacheEntry struct {
	LastDrain   time.Time     `json:"last_drain"`
	DrainBuffer time.Duration `json:"drain_buffer"`
}

type drainBufferImpl struct {
	sync.RWMutex

	isInitialized bool
	isDirty       bool

	podIndexer    index.PodIndexer
	store         kubernetes.RuntimeObjectStore
	eventRecorder kubernetes.EventRecorder
	clock         clock.Clock
	persistor     Persistor
	cache         drainCache
	logger        logr.Logger

	defaultDrainBuffer time.Duration
}

const (
	eventDrainBufferBadConfiguration = "DrainBufferBadConfiguration"
)

// GetDrainBufferConfigurationDetails retrieve all the drain configuration details
func (buffer *drainBufferImpl) GetDrainBufferConfigurationDetails(ctx context.Context, node *v1.Node) (*kubernetes.MetadataSearch[time.Duration], error) {
	return kubernetes.SearchAnnotationFromNodeAndThenPodOrController(ctx, buffer.podIndexer, buffer.store, time.ParseDuration, kubernetes.CustomDrainBufferAnnotation, node, false, false)
}

// GetDrainBufferConfiguration does a best effort to find a valid configuration and always return a value. The error can be non nil if something went wrong during the processing, still the value can be used. Worst case you get the default value.
func (buffer *drainBufferImpl) GetDrainBufferConfiguration(ctx context.Context, node *v1.Node) (time.Duration, error) {
	searchResult, err := buffer.GetDrainBufferConfigurationDetails(ctx, node)
	if err != nil {
		return buffer.defaultDrainBuffer, err
	}

	searchResult.HandlerError(
		func(node *v1.Node, err error) {
			buffer.eventRecorder.NodeEventf(ctx, node, v1.EventTypeWarning, eventDrainBufferBadConfiguration, "failed to parse drainBuffer: "+err.Error()) // The parsing error is given to the user
		},
		func(pod *v1.Pod, err error) {
			buffer.eventRecorder.PodEventf(ctx, pod, v1.EventTypeWarning, eventDrainBufferBadConfiguration, "failed to parse drainBuffer: "+err.Error()) // The parsing error is given to the user
		},
	)

	durations := searchResult.ValuesWithoutDupe()
	if len(durations) == 0 {
		return buffer.defaultDrainBuffer, nil
	}

	var max time.Duration
	for _, d := range durations {
		if d > max {
			max = d
		}
	}
	return max, nil
}

// NewDrainBuffer returns a new instance of a drain buffer
func NewDrainBuffer(ctx context.Context, persistor Persistor, clock clock.Clock, logger logr.Logger, eventRecorder kubernetes.EventRecorder, podIndexer index.PodIndexer, store kubernetes.RuntimeObjectStore, defaultDrainBuffer time.Duration) DrainBuffer {
	drainBuffer := &drainBufferImpl{
		defaultDrainBuffer: defaultDrainBuffer,
		isInitialized:      false,
		isDirty:            false,
		clock:              clock,
		persistor:          persistor,
		cache:              drainCache{},
		logger:             logger.WithName("drainBuffer"),
		eventRecorder:      eventRecorder,
		podIndexer:         podIndexer,
		store:              store,
	}

	go drainBuffer.persistenceLoop(ctx)

	return drainBuffer
}

func (buffer *drainBufferImpl) IsReady() bool {
	return buffer.isInitialized
}

func (buffer *drainBufferImpl) StoreSuccessfulDrain(key groups.GroupKey, drainBuffer time.Duration) error {
	if !buffer.IsReady() {
		return errors.New("drain buffer is not initialized")
	}

	buffer.logger.V(logs.ZapDebug).Info("storing drainBuffer", "group", key, "buffer", duration.HumanDuration(drainBuffer), "leadingTo", buffer.clock.Now().Add(drainBuffer))

	buffer.Lock()
	defer buffer.Unlock()

	buffer.cache[key] = drainCacheEntry{
		LastDrain:   buffer.clock.Now(),
		DrainBuffer: drainBuffer,
	}
	buffer.isDirty = true

	return nil
}

func (buffer *drainBufferImpl) NextDrain(key groups.GroupKey) (time.Time, error) {
	if !buffer.IsReady() {
		return time.Time{}, errors.New("drain buffer is not initialized")
	}

	buffer.RLock()
	defer buffer.RUnlock()

	entry, ok := buffer.cache[key]
	if !ok {
		return time.Time{}, nil
	}
	return entry.LastDrain.Add(entry.DrainBuffer), nil
}

func (buffer *drainBufferImpl) Initialize(ctx context.Context) error {
	if buffer.isInitialized {
		return nil
	}

	buffer.Lock()
	defer buffer.Unlock()

	data, exist, err := buffer.persistor.Load(ctx)
	if err != nil {
		return err
	}
	if exist {
		if err := json.Unmarshal(data, &buffer.cache); err != nil {
			return err
		}
	}

	buffer.isInitialized = true
	return nil
}

func (buffer *drainBufferImpl) cleanupCache() {
	if !buffer.isInitialized {
		return
	}

	buffer.Lock()
	defer buffer.Unlock()

	for key, entry := range buffer.cache {
		until := entry.LastDrain.Add(entry.DrainBuffer)
		if until.Before(buffer.clock.Now()) {
			delete(buffer.cache, key)
			buffer.isDirty = true
		}
	}
}

func (buffer *drainBufferImpl) persistenceLoop(ctx context.Context) {
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		if !buffer.isInitialized {
			return
		}
		if err := buffer.cleanupAndPersist(ctx); err != nil {
			buffer.logger.Error(err, "failed to persist drain buffer cache")
		}
	}, time.Second*20)
}

func (buffer *drainBufferImpl) cleanupAndPersist(ctx context.Context) error {
	if !buffer.isInitialized {
		return nil
	}

	buffer.cleanupCache()

	// We don't have to persist the data if nothing changed
	if !buffer.isDirty {
		return nil
	}

	// The lock has to be acquired after the cleanup, because otherwise we'll create a deadlock
	buffer.RLock()
	defer buffer.RUnlock()

	data, err := json.Marshal(buffer.cache)
	if err != nil {
		return err
	}

	err = buffer.persistor.Persist(ctx, data)
	if err != nil {
		return err
	}

	buffer.isDirty = false
	return nil
}
