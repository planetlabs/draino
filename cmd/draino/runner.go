package main

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/wait"
)

// RunTillSuccess implement's the manager.Runnable interface and should be used to execute a specific task until it's done.
// The main intention was to run initialization processes.
type RunTillSuccess struct {
	logger *logr.Logger
	period time.Duration
	fn     func(context.Context) error
}

func (r *RunTillSuccess) Start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		err := r.fn(ctx)
		if err != nil {
			r.logger.Error(err, "failed to run component")
			return
		}
		cancel()
	}, r.period)
	return nil
}

// RunOnce implement's the manager.Runnable interface and should be used to execute an action once
type RunOnce struct {
	fn func(context.Context) error
}

func (r *RunOnce) Start(ctx context.Context) error {
	return r.fn(ctx)
}
