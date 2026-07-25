package health

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

type State struct {
	started  atomic.Bool
	ready    atomic.Bool
	draining atomic.Bool
}

func (s *State) SetStarted(value bool)  { s.started.Store(value) }
func (s *State) SetReady(value bool)    { s.ready.Store(value) }
func (s *State) SetDraining(value bool) { s.draining.Store(value) }

type Handler struct {
	state *State
}

func NewHandler(state *State) *Handler { return &Handler{state: state} }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	healthy := false
	switch r.URL.Path {
	case "/livez":
		healthy = true
	case "/startupz":
		healthy = h.state.started.Load()
	case "/readyz":
		healthy = h.state.started.Load() && h.state.ready.Load() && !h.state.draining.Load()
	default:
		http.NotFound(w, r)
		return
	}
	status := http.StatusOK
	state := "ok"
	if !healthy {
		status = http.StatusServiceUnavailable
		state = "unavailable"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": state})
}
