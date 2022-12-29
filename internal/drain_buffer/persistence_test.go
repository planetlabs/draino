package drainbuffer

import (
	"context"
	"testing"

	"github.com/planetlabs/draino/internal/kubernetes/k8sclient"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestConfigMapPersistor(t *testing.T) {
	cmName := "foobar"
	cmNS := "default"

	tests := []struct {
		Name      string
		ConfigMap corev1.ConfigMap
		Entry     string
	}{
		{
			Name:      "Should store and retrieve entry",
			ConfigMap: corev1.ConfigMap{},
			Entry:     "foobar",
		},
		{
			Name:      "Should override existing entry",
			ConfigMap: createTestCM(cmName, cmNS, "test-entry"),
			Entry:     "foobar",
		},
		{
			Name: "Should not fail with missing data in existing configmap",
			// for some reason the map has to include something, because otherwise it's just going to be replaced by nil
			ConfigMap: corev1.ConfigMap{ObjectMeta: v1.ObjectMeta{Name: cmName, Namespace: cmNS}, Data: map[string]string{"placeholder": "placeholder"}},
			Entry:     "foobar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: []runtime.Object{&tt.ConfigMap}})
			assert.NoError(t, err, "cannot create client wrapper")

			persistor := NewConfigMapPersistor(wrapper.GetManagerClient(), cmName, cmNS)
			err = persistor.Persist(context.Background(), []byte(tt.Entry))
			assert.NoError(t, err)

			persistor = NewConfigMapPersistor(wrapper.GetManagerClient(), cmName, cmNS)
			data, exist, err := persistor.Load(context.Background())
			assert.NoError(t, err)
			assert.True(t, exist)
			assert.Equal(t, tt.Entry, string(data))
		})
	}
}

func TestConfigMapPersistor_Load(t *testing.T) {
	cmName := "foocm"
	cmNS := "default"

	tests := []struct {
		Name      string
		ConfigMap corev1.ConfigMap
		Exist     bool
		Entry     string
	}{
		{
			Name:      "Should find configmap with proper entry",
			ConfigMap: createTestCM(cmName, cmNS, "foobar"),
			Exist:     true,
			Entry:     "foobar",
		},
		{
			Name:      "should not find configmap, but should not fail",
			ConfigMap: corev1.ConfigMap{},
			Exist:     false,
		},
		{
			Name:      "should find configmap without data, but should not fail",
			ConfigMap: corev1.ConfigMap{ObjectMeta: v1.ObjectMeta{Name: cmName, Namespace: cmNS}, Data: make(map[string]string)},
			Exist:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			wrapper, err := k8sclient.NewFakeClient(k8sclient.FakeConf{Objects: []runtime.Object{&tt.ConfigMap}})
			assert.NoError(t, err, "cannot create client wrapper")

			persistor := NewConfigMapPersistor(wrapper.GetManagerClient(), cmName, cmNS)
			data, exist, err := persistor.Load(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, tt.Exist, exist)

			if tt.Exist {
				assert.Equal(t, tt.Entry, string(data))
			}
		})
	}
}

func createTestCM(name, ns, data string) corev1.ConfigMap {
	return corev1.ConfigMap{
		ObjectMeta: v1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string]string{CMDataKey: data},
	}
}
