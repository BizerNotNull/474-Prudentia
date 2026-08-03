package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type dependencyFunc func(context.Context) error

func (f dependencyFunc) Check(ctx context.Context) error { return f(ctx) }

func TestCheckerIsRoleAwareAndFenceFailsRequestPlaneClosed(t *testing.T) {
	fenced := &atomic.Bool{}
	checker, err := NewChecker(map[Role]map[string]Dependency{
		RoleGateway:    {"auth": dependencyFunc(func(context.Context) error { return errors.New("down: secret detail") })},
		RoleController: {"cache": dependencyFunc(func(context.Context) error { return nil })},
	}, fenced)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := checker.Check(context.Background(), RoleGateway)
	if err != nil || gateway.Ready || len(gateway.Failed) != 1 || gateway.Failed[0] != "auth" {
		t.Fatalf("unexpected gateway report: %#v, %v", gateway, err)
	}
	checker.SetFenced(true)
	controller, err := checker.Check(context.Background(), RoleController)
	if err != nil || !controller.Ready {
		t.Fatalf("controller must remain ready to rebuild: %#v, %v", controller, err)
	}
	gateway, _ = checker.Check(context.Background(), RoleGateway)
	if gateway.Ready || gateway.Failed[len(gateway.Failed)-1] != "recovery_fence" {
		t.Fatalf("fence did not close gateway readiness: %#v", gateway)
	}
}

func TestHandlerRoutesAndRedactsDependencyErrors(t *testing.T) {
	state := &State{}
	state.SetStarted(true)
	state.SetProgress(true)
	checker, err := NewChecker(map[Role]map[string]Dependency{
		RoleScheduler: {"database": dependencyFunc(func(context.Context) error { return errors.New("postgres://user:password@10.0.0.1") })},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewRoleHandler(state, checker, RoleScheduler)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("got status %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "password") || response.Body.String() != "{\"status\":\"unavailable\"}\n" {
		t.Fatalf("health response leaked detail: %q", response.Body.String())
	}
	state.SetDraining(true)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("draining process should remain live: %d", response.Code)
	}
}
