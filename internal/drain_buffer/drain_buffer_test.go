package drainbuffer

import (
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"k8s.io/apimachinery/pkg/runtime"
	fake "k8s.io/utils/clock/testing"
)

type StaticClock struct {
	time time.Time
}

func TestDrainBuffer(t *testing.T) {
	tests := []struct {
		Name        string
		Clock       *fake.FakeClock
		DrainBuffer time.Duration

		Step       time.Duration
		ShouldFind bool
	}{
		{
			Name:        "shlould find entry in configmap as it's not expired yet",
			Clock:       fake.NewFakeClock(time.Now()),
			DrainBuffer: time.Minute,

			Step:       time.Second * 10,
			ShouldFind: true,
		},
		{
			Name:        "should not find entry anymore, because it's expired",
			Clock:       fake.NewFakeClock(time.Now()),
			DrainBuffer: time.Minute,

			Step:       time.Minute * 2,
			ShouldFind: false,
		},
	}

	logger := logr.Discard()
	cmName := "foobar"
	cmNS := "default"

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: []runtime.Object{}})
			assert.NoError(t, err, "cannot create kube client wrapper")

			// initial setup
			ctx, cancel := context.WithCancel(context.Background())
			persistor := NewConfigMapPersistor(wrapper.GetManagerClient(), cmName, cmNS)
			interf, err := NewDrainBuffer(ctx, persistor, tt.Clock, &logger)
			assert.NoError(t, err, "cannot create drain buffer")
			drainBuffer := interf.(*drainBufferImpl)

			// create new entry
			drainBuffer.StoreSuccessfulDrain("foobar", tt.DrainBuffer)
			err = drainBuffer.cleanupAndPersist()
			assert.NoError(t, err, "cannot persist drain buffer")
			// Close old buffer cleanup
			cancel()

			// Create new buffer to test load mechanism
			ctx, cancel = context.WithCancel(context.Background())
			defer cancel()
			persistor = NewConfigMapPersistor(wrapper.GetManagerClient(), cmName, cmNS)
			interf, err = NewDrainBuffer(ctx, persistor, tt.Clock, &logger)
			assert.NoError(t, err, "cannot create drain buffer")
			drainBuffer = interf.(*drainBufferImpl)

			// Move clock forward & trigger cleanup
			tt.Clock.Step(tt.Step)
			drainBuffer.cleanupCache()

			// Check if clock is properly set
			next := drainBuffer.NextDrain("foobar")
			if tt.ShouldFind {
				until := tt.Clock.Now().Add(-tt.Step).Add(tt.DrainBuffer)
				assert.True(t, next.Before(until.Add(time.Second)))
				assert.True(t, next.After(until.Add(-time.Second)))
			} else {
				assert.True(t, next.IsZero(), "should not find entry")
			}
		})
	}
}
