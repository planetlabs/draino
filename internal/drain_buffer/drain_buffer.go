package drainbuffer

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/groups"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/clock"
)

// DrainBuffer will store information about the last sucessful drains
type DrainBuffer interface {
	// StoreSuccessfulDrain is adding the given group to the internal store
	StoreSuccessfulDrain(groups.GroupKey, time.Duration)
	// NextDrain returns the next possible drain time for the given group
	// It will return a zero time if the next drain can be done immediately
	NextDrain(groups.GroupKey) time.Time
}

// drainCache is cache used internally by the drain buffer
type drainCache map[groups.GroupKey]drainCacheEntry

// drainCacheEntry stores information about specific cache entries
type drainCacheEntry struct {
	LastDrain   time.Time     `json:"last_drain"`
	DrainBuffer time.Duration `json:"drain_buffer"`
}

type drainBufferImpl struct {
	sync.RWMutex
	clock     clock.Clock
	persistor Persistor
	cache     drainCache
	logger    *logr.Logger
}

// NewDrainBuffer returns a new instance of a drain buffer
func NewDrainBuffer(ctx context.Context, persistor Persistor, clock clock.Clock, logger *logr.Logger) (DrainBuffer, error) {
	// load existing cache from the persistence layer
	cacheBytes, exist, err := persistor.Load()
	if err != nil {
		return nil, err
	}

	cache := &drainCache{}
	if exist {
		err = json.Unmarshal(cacheBytes, cache)
		if err != nil {
			return nil, err
		}
	}

	drainBuffer := &drainBufferImpl{
		clock:     clock,
		persistor: persistor,
		cache:     *cache,
		logger:    logger,
	}

	go drainBuffer.persistenceLoop(ctx)

	return drainBuffer, nil
}

func (buffer *drainBufferImpl) StoreSuccessfulDrain(key groups.GroupKey, drainBuffer time.Duration) {
	buffer.Lock()
	defer buffer.Unlock()

	buffer.cache[key] = drainCacheEntry{
		LastDrain:   buffer.clock.Now(),
		DrainBuffer: drainBuffer,
	}
}

func (buffer *drainBufferImpl) NextDrain(key groups.GroupKey) time.Time {
	buffer.RLock()
	defer buffer.RUnlock()

	entry, ok := buffer.cache[key]
	if !ok {
		return time.Time{}
	}
	return entry.LastDrain.Add(entry.DrainBuffer)
}

func (buffer *drainBufferImpl) cleanupCache() {
	buffer.Lock()
	defer buffer.Unlock()

	for key, entry := range buffer.cache {
		until := entry.LastDrain.Add(entry.DrainBuffer)
		if until.Before(buffer.clock.Now()) {
			delete(buffer.cache, key)
		}
	}
}

func (buffer *drainBufferImpl) persistenceLoop(ctx context.Context) {
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := buffer.cleanupAndPersist(); err != nil {
			buffer.logger.Error(err, "failed to persist drain buffer cache")
		}
	}, time.Second*20)
}

func (buffer *drainBufferImpl) cleanupAndPersist() error {
	buffer.cleanupCache()

	// The lock has to be acquired after the cleanup, because otherwise we'll create a deadlock
	buffer.RLock()
	defer buffer.RUnlock()

	data, err := json.Marshal(buffer.cache)
	if err != nil {
		return err
	}

	return buffer.persistor.Persist(data)
}
