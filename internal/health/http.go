package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"sync/atomic"
)

// Role controls which dependency set is required for readiness.
type Role string

const (
	RoleGateway    Role = "gateway"
	RoleScheduler  Role = "scheduler"
	RoleController Role = "controller"
)

type Dependency interface {
	Check(context.Context) error
}

type Report struct {
	Role   Role     `json:"role"`
	Ready  bool     `json:"ready"`
	Failed []string `json:"failed,omitempty"`
	Fenced bool     `json:"fenced,omitempty"`
}

type Checker struct {
	checks map[Role]map[string]Dependency
	fenced *atomic.Bool
}

func NewChecker(checks map[Role]map[string]Dependency, fenced *atomic.Bool) (*Checker, error) {
	if fenced == nil {
		fenced = &atomic.Bool{}
	}
	copyChecks := make(map[Role]map[string]Dependency, len(checks))
	for role, dependencies := range checks {
		if role != RoleGateway && role != RoleScheduler && role != RoleController {
			return nil, errors.New("health: unknown process role")
		}
		copyChecks[role] = make(map[string]Dependency, len(dependencies))
		for name, dependency := range dependencies {
			if name == "" || dependency == nil {
				return nil, errors.New("health: dependency name and checker are required")
			}
			copyChecks[role][name] = dependency
		}
	}
	return &Checker{checks: copyChecks, fenced: fenced}, nil
}

func (c *Checker) SetFenced(value bool) { c.fenced.Store(value) }

func (c *Checker) Check(ctx context.Context, role Role) (Report, error) {
	dependencies, ok := c.checks[role]
	if !ok {
		return Report{}, errors.New("health: unknown process role")
	}
	report := Report{Role: role, Ready: true, Fenced: c.fenced.Load()}
	for name, dependency := range dependencies {
		if err := dependency.Check(ctx); err != nil {
			report.Failed = append(report.Failed, name)
		}
	}
	sort.Strings(report.Failed)
	// A recovery fence removes request-plane roles from readiness. Controller
	// readiness remains tied to cache/store synchronization so it can rebuild.
	if report.Fenced && (role == RoleGateway || role == RoleScheduler) {
		report.Failed = append(report.Failed, "recovery_fence")
	}
	report.Ready = len(report.Failed) == 0
	return report, nil
}

type State struct {
	started  atomic.Bool
	draining atomic.Bool
	progress atomic.Bool
}

func (s *State) SetStarted(value bool)  { s.started.Store(value) }
func (s *State) SetDraining(value bool) { s.draining.Store(value) }
func (s *State) SetProgress(value bool) { s.progress.Store(value) }

// SetReady is retained as the process-progress signal used by composition roots.
func (s *State) SetReady(value bool) { s.SetProgress(value) }

type Handler struct {
	state   *State
	checker *Checker
	role    Role
}

func NewHandler(state *State) *Handler { return &Handler{state: state} }

func NewRoleHandler(state *State, checker *Checker, role Role) (*Handler, error) {
	if state == nil || checker == nil {
		return nil, errors.New("health: state and checker are required")
	}
	if role != RoleGateway && role != RoleScheduler && role != RoleController {
		return nil, errors.New("health: unknown process role")
	}
	return &Handler{state: state, checker: checker, role: role}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	report := Report{Role: h.role, Ready: true}
	healthy := false
	switch r.URL.Path {
	case "/livez":
		healthy = h.state.progress.Load() || h.state.started.Load()
	case "/startupz":
		healthy = h.state.started.Load()
	case "/readyz":
		healthy = h.state.started.Load() && h.state.progress.Load() && !h.state.draining.Load()
		if healthy && h.checker != nil {
			var err error
			report, err = h.checker.Check(r.Context(), h.role)
			healthy = err == nil && report.Ready
		}
	default:
		http.NotFound(w, r)
		return
	}
	status := http.StatusOK
	if !healthy {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_ = json.NewEncoder(w).Encode(struct {
			Status string `json:"status"`
		}{Status: map[bool]string{true: "ok", false: "unavailable"}[healthy]})
	}
}
