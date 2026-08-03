package publichttp_test

import (
	"context"
	"net/http"
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
