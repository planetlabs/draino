package klog

import (
	"flag"
	"fmt"

	"go.uber.org/zap"
	"k8s.io/klog"
)

// InitializeKlog initializes klog with a verbosity v
func InitializeKlog(v int32) {
	fs := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(fs)
	fs.Set("v", fmt.Sprintf("%d", v))
	fs.Set("logtostderr", "false")
}

// RedirectToLogger redirects all klog logs to a zapper logger.
// This is useful to have a single format for all logs while also getting log messages from libraries that use klog (kubernetes).
func RedirectToLogger(logger *zap.Logger) {
	// constants taken from https://github.com/kubernetes/klog/blob/fafe98e1ea27f704efbd8ce38bfb9fcb9058a6ab/klog.go#L114
	severityToLogFunc := map[string]func(msg string, fields ...zap.Field){
		"INFO":    logger.Info,
		"WARNING": logger.Warn,
		"ERROR":   logger.Error,
		"FATAL":   logger.Fatal,
	}
	for name, logFunc := range severityToLogFunc {
		writer := &zapperWriter{
			logFunc: logFunc,
		}
		klog.SetOutputBySeverity(name, writer)
	}
}

type zapperWriter struct {
	logFunc func(msg string, fields ...zap.Field)
}

func (z *zapperWriter) Write(p []byte) (n int, err error) {
	z.logFunc(string(p), zap.String("source", "klog"))
	return len(p), nil
}
