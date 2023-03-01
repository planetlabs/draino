package main

import (
	"context"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	"github.com/julienschmidt/httprouter"
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

type HttpRunner struct {
	address string
	logger  logr.Logger
	h       map[string]http.Handler
}

func (r *HttpRunner) Start(ctx context.Context) error {
	r.Run(ctx.Done())
	return nil
}

func (r *HttpRunner) Run(stop <-chan struct{}) {
	rt := httprouter.New()
	for path, handler := range r.h {
		rt.Handler("GET", path, handler)
	}

	s := &http.Server{Addr: r.address, Handler: rt}
	ctx, cancel := context.WithTimeout(context.Background(), 0*time.Second)
	go func() {
		<-stop
		if err := s.Shutdown(ctx); err != nil {
			r.logger.Error(err, "Failed to shutdown httpRunner")
			return
		}
	}()
	if err := s.ListenAndServe(); err != nil {
		r.logger.Error(err, "Failed to ListenAndServe httpRunner")
	}
	cancel()
}
