package publichttp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/BizerNotNull/474-Prudentia/internal/transport/publichttp"
)

type blockingResponseWriter struct {
	header  http.Header
	entered chan struct{}
	release chan struct{}
}

func (w *blockingResponseWriter) Header() http.Header { return w.header }
func (*blockingResponseWriter) WriteHeader(int)       {}
func (w *blockingResponseWriter) Write([]byte) (int, error) {
	close(w.entered)
	<-w.release
	return 1, nil
}
func (*blockingResponseWriter) Flush() {}

type deadlineResponseWriter struct {
	header   http.Header
	deadline time.Time
	writeErr error
}

func (w *deadlineResponseWriter) Header() http.Header { return w.header }
func (*deadlineResponseWriter) WriteHeader(int)       {}
func (w *deadlineResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}
func (w *deadlineResponseWriter) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	timer := time.NewTimer(time.Until(w.deadline))
	defer timer.Stop()
	<-timer.C
	return 0, context.DeadlineExceeded
}
func (*deadlineResponseWriter) Flush() {}

func TestSSESinkAppliesSynchronousBackpressure(t *testing.T) {
	writer := &blockingResponseWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	sink, err := publichttp.NewSSESink(writer, "req_test")
	if err != nil {
		t.Fatal(err)
	}
	delta, _ := domain.NewDeltaEvent("x")
	done := make(chan error, 1)
	go func() { done <- sink.Write(context.Background(), delta) }()
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("sink did not reach client writer")
	}
	select {
	case <-done:
		t.Fatal("sink returned before slow client accepted the write")
	default:
	}
	close(writer.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("sink remained blocked after client accepted write")
	}
}

func TestSSESinkDeadlineBoundsSlowClientAndClassifiesDisconnect(t *testing.T) {
	delta, _ := domain.NewDeltaEvent("x")
	t.Run("slow", func(t *testing.T) {
		writer := &deadlineResponseWriter{header: make(http.Header)}
		sink, err := publichttp.NewSSESink(writer, "req_slow", "model-a")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		started := time.Now()
		if err := sink.Write(ctx, delta); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error=%v", err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("slow write was not bounded: %v", elapsed)
		}
	})
	t.Run("disconnect", func(t *testing.T) {
		writer := &deadlineResponseWriter{header: make(http.Header), writeErr: io.ErrClosedPipe}
		sink, err := publichttp.NewSSESink(writer, "req_disconnect", "model-a")
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.Write(context.Background(), delta); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestSSESinkCanceledContextNeverWrites(t *testing.T) {
	writer := &blockingResponseWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	sink, err := publichttp.NewSSESink(writer, "req_test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	delta, _ := domain.NewDeltaEvent("secret")
	if err := sink.Write(ctx, delta); err != context.Canceled {
		t.Fatalf("error=%v", err)
	}
	select {
	case <-writer.entered:
		t.Fatal("canceled request wrote response bytes")
	default:
	}
}

func TestPublicErrorTaxonomyIsStableAndSanitized(t *testing.T) {
	for _, test := range []struct {
		kind   domain.ErrorKind
		status int
		code   string
	}{
		{domain.ErrorNoCapacity, http.StatusServiceUnavailable, "no_capacity"},
		{domain.ErrorBackendUnavailable, http.StatusBadGateway, "backend_unavailable"},
		{domain.ErrorDeadlineExceeded, http.StatusGatewayTimeout, "deadline_exceeded"},
		{domain.ErrorRateLimited, http.StatusTooManyRequests, "rate_limited"},
		{domain.ErrorRecoveryFenced, http.StatusServiceUnavailable, "recovery_fenced"},
		{domain.ErrorRetryWindowClosed, http.StatusConflict, "retry_window_closed"},
	} {
		recorder := httptest.NewRecorder()
		publichttp.WritePublicError(recorder, "req_safe", fmt.Errorf("hostile provider 10.0.0.8: %w", domain.NewPublicError(test.kind)))
		if recorder.Code != test.status || !strings.Contains(recorder.Body.String(), `"code":"`+test.code+`"`) {
			t.Fatalf("kind=%v status=%d body=%s", test.kind, recorder.Code, recorder.Body.String())
		}
		if strings.Contains(recorder.Body.String(), "10.0.0.8") || strings.Contains(recorder.Body.String(), "hostile") {
			t.Fatalf("leaked diagnostics: %s", recorder.Body.String())
		}
	}
}

func TestNonStreamingCollectorBoundsAndRequiresTerminal(t *testing.T) {
	collector := publichttp.NewNonStreamingCollector(3, 2)
	delta, _ := domain.NewDeltaEvent("four")
	if domain.ErrorKindOf(collector.Write(context.Background(), delta)) != domain.ErrorUnavailable {
		t.Fatal("overflow was not stable unavailable error")
	}
	if _, err := collector.Result(); domain.ErrorKindOf(err) != domain.ErrorUnavailable {
		t.Fatal("incomplete result accepted")
	}
}
