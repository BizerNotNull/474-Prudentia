package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Config struct {
	ShutdownGrace time.Duration
}

func (c Config) validate() error {
	if c.ShutdownGrace <= 0 {
		return errors.New("runtime: positive shutdown grace is required")
	}
	return nil
}

type Validator interface {
	Validate(context.Context) error
}

type Component interface {
	Run(context.Context) error
}

type AdmissionGate interface {
	Close(context.Context) error
}

type StreamDrainer interface {
	Drain(context.Context) error
}

type GatewayDeps struct {
	Validator Validator
	Admission AdmissionGate
	Streams   StreamDrainer
	Services  []Component
}

type SchedulerDeps struct {
	Validator Validator
	Request   Component
	Admin     Component
	Workers   []Component
}

type ControllerBootstrap interface {
	Warm(context.Context) error
	AcquireGeneration(context.Context) error
	FenceTakeovers(context.Context) error
}

type ControllerDeps struct {
	Validator Validator
	Bootstrap ControllerBootstrap
	Workers   []Component
}

func RunGateway(ctx context.Context, cfg Config, deps GatewayDeps) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if deps.Validator == nil || deps.Admission == nil || deps.Streams == nil || len(deps.Services) == 0 {
		return errors.New("runtime: incomplete gateway dependencies")
	}
	if err := deps.Validator.Validate(ctx); err != nil {
		return fmt.Errorf("runtime: validate gateway: %w", err)
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	result := run(runCtx, deps.Services)
	var runErr error
	finished := false
	select {
	case <-ctx.Done():
	case runErr = <-result:
		finished = true
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownGrace)
	defer shutdownCancel()
	var shutdownErr error
	if err := deps.Admission.Close(shutdownCtx); err != nil {
		shutdownErr = fmt.Errorf("close gateway admission: %w", err)
	}
	if err := deps.Streams.Drain(shutdownCtx); err != nil && shutdownErr == nil {
		shutdownErr = fmt.Errorf("drain gateway streams: %w", err)
	}
	cancel()
	remaining := len(deps.Services)
	if finished {
		remaining--
	}
	runErr = errors.Join(runErr, awaitAll(result, remaining, cfg.ShutdownGrace))
	return joinLifecycle(runErr, shutdownErr, ctx.Err())
}

func RunScheduler(ctx context.Context, cfg Config, deps SchedulerDeps) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if deps.Validator == nil || deps.Request == nil || deps.Admin == nil {
		return errors.New("runtime: incomplete scheduler dependencies")
	}
	if err := deps.Validator.Validate(ctx); err != nil {
		return fmt.Errorf("runtime: validate scheduler: %w", err)
	}
	components := make([]Component, 0, 2+len(deps.Workers))
	components = append(components, deps.Request, deps.Admin)
	components = append(components, deps.Workers...)
	return runUntilStopped(ctx, cfg.ShutdownGrace, components)
}

func RunController(ctx context.Context, cfg Config, deps ControllerDeps) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if deps.Validator == nil || deps.Bootstrap == nil || len(deps.Workers) == 0 {
		return errors.New("runtime: incomplete controller dependencies")
	}
	if err := deps.Validator.Validate(ctx); err != nil {
		return fmt.Errorf("runtime: validate controller: %w", err)
	}
	if err := deps.Bootstrap.Warm(ctx); err != nil {
		return fmt.Errorf("runtime: warm controller caches: %w", err)
	}
	if err := deps.Bootstrap.AcquireGeneration(ctx); err != nil {
		return fmt.Errorf("runtime: acquire controller generation: %w", err)
	}
	if err := deps.Bootstrap.FenceTakeovers(ctx); err != nil {
		return fmt.Errorf("runtime: fence controller takeovers: %w", err)
	}
	return runUntilStopped(ctx, cfg.ShutdownGrace, deps.Workers)
}

func runUntilStopped(ctx context.Context, grace time.Duration, components []Component) error {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	result := run(runCtx, components)
	var first error
	finished := false
	select {
	case <-ctx.Done():
		first = ctx.Err()
	case first = <-result:
		finished = true
	}
	cancel()
	remaining := len(components)
	if finished {
		remaining--
	}
	return joinLifecycle(first, awaitAll(result, remaining, grace))
}

func run(ctx context.Context, components []Component) <-chan error {
	results := make(chan error, len(components))
	for _, component := range components {
		go func(component Component) {
			results <- component.Run(ctx)
		}(component)
	}
	return results
}

func awaitAll(results <-chan error, count int, grace time.Duration) error {
	if count == 0 {
		return nil
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	var failures []error
	for range count {
		select {
		case err := <-results:
			if err != nil && !errors.Is(err, context.Canceled) {
				failures = append(failures, err)
			}
		case <-timer.C:
			return errors.Join(append(failures, errors.New("runtime: component shutdown exceeded grace"))...)
		}
	}
	return errors.Join(failures...)
}

func joinLifecycle(values ...error) error {
	filtered := make([]error, 0, len(values))
	for _, value := range values {
		if value != nil && !errors.Is(value, context.Canceled) {
			filtered = append(filtered, value)
		}
	}
	return errors.Join(filtered...)
}
