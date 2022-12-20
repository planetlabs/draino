package protector

import (
	"github.com/planetlabs/draino/internal/kubernetes"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
)

// PVProtector is used to check if persistent volumes can be used as expected by the corosponding pods
type PVProtector interface {
	// GetUnscheduledPodsBoundToNodeByPV Check if there is any pod that would be bound to that node due to PV/PVC and that is not yet scheduled
	GetUnscheduledPodsBoundToNodeByPV(*corev1.Node) ([]*corev1.Pod, error)
}

// legacyPVCProtectorImpl is an implementation of the PVProtector interface that uses the legacy system with the runtime object store
type legacyPVCProtectorImpl struct {
	store                                   kubernetes.RuntimeObjectStore
	logger                                  *zap.Logger
	pvcManagementDefaultTrueIfNoEvictionURL bool
}

func NewPVCProtector(store kubernetes.RuntimeObjectStore, logger *zap.Logger, pvcManagementDefaultTrueIfNoEvictionURL bool) PVProtector {
	return &legacyPVCProtectorImpl{
		store:                                   store,
		logger:                                  logger,
		pvcManagementDefaultTrueIfNoEvictionURL: pvcManagementDefaultTrueIfNoEvictionURL,
	}
}

func (protector *legacyPVCProtectorImpl) GetUnscheduledPodsBoundToNodeByPV(node *corev1.Node) ([]*corev1.Pod, error) {
	return kubernetes.GetUnscheduledPodsBoundToNodeByPV(node, protector.store, protector.pvcManagementDefaultTrueIfNoEvictionURL, protector.logger)
}
