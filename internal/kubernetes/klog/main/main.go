package main

import (
	drainoklog "github.com/planetlabs/draino/internal/kubernetes/klog"
	"go.uber.org/zap"
	"k8s.io/klog"
)

// main is a demo of how to use the drainklog utils without using the default os.Args and flag.CommandLine
func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	logger = logger.With(zap.String("foo", "bar"))

	drainoklog.InitializeKlog(4)
	drainoklog.RedirectToLogger(logger)

	for i := 0; i < 10; i++ {
		klog.Info("nov ", i)
		klog.V(klog.Level(i)).Info("withv ", i)
	}
}
