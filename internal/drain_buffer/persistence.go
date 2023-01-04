package drainbuffer

import (
	"context"
	"errors"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const CMDataKey = "data"

// Persistor is responsible for persist a given data structure of any kind
type Persistor interface {
	// Persist writes the given information to the persitent backend
	Persist(context.Context, []byte) error
	// Load tries to retrieve the information from the backend and returns it
	Load(context.Context) ([]byte, bool, error)
}

// ConfigMapPersistor is an implementation of the Persistor interface that uses configmap as backend
type ConfigMapPersistor struct {
	name      string
	namespace string
	clientCM  v1.ConfigMapInterface
}

// NewConfigMapPersistor creates a new config map persistor instance
func NewConfigMapPersistor(clientCM v1.ConfigMapInterface, name, namespace string) Persistor {
	return &ConfigMapPersistor{
		name:      name,
		namespace: namespace,
		clientCM:  clientCM,
	}
}

func (p *ConfigMapPersistor) Persist(ctx context.Context, data []byte) error {
	if data == nil {
		return errors.New("data is empty")
	}

	cm, exist, err := p.getConfigMap(ctx)
	if err != nil {
		return err
	}

	// If there is no configmap yet, we'll have to create a new one
	if !exist {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      p.name,
				Namespace: p.namespace,
			},
			Data: map[string]string{
				CMDataKey: string(data),
			},
		}
		_, err = p.clientCM.Create(ctx, cm, metav1.CreateOptions{})
		return err
	}

	// if there is a configmap already, we'll just override the existing entry
	cm.Data[CMDataKey] = string(data)
	_, err = p.clientCM.Update(ctx, cm, metav1.UpdateOptions{})
	return err
}

func (p *ConfigMapPersistor) Load(ctx context.Context) ([]byte, bool, error) {
	cm, exist, err := p.getConfigMap(ctx)
	if err != nil {
		return nil, false, err
	}
	if !exist {
		return nil, false, nil
	}

	entry, ok := cm.Data[CMDataKey]
	if !ok {
		return nil, false, nil
	}

	return []byte(entry), true, nil
}

func (p *ConfigMapPersistor) getConfigMap(ctx context.Context) (*corev1.ConfigMap, bool, error) {

	cm, err := p.clientCM.Get(ctx, p.name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	return cm, true, nil
}
