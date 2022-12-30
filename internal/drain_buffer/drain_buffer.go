package drainbuffer

import (
	"context"
	"encoding/json"
	"errors"
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

	isInitialized bool
	isDirty       bool

	clock     clock.Clock
	persistor Persistor
	cache     drainCache
	logger    *logr.Logger
}

// NewDrainBuffer returns a new instance of a drain buffer
func NewDrainBuffer(ctx context.Context, persistor Persistor, clock clock.Clock, logger logr.Logger) DrainBuffer {
	drainBuffer := &drainBufferImpl{
		isInitialized: false,
		isDirty:       false,
		clock:         clock,
		persistor:     persistor,
		cache:         drainCache{},
		logger:        &logger,
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
