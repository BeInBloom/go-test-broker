package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type e2eBroker struct {
	command *exec.Cmd
	done    chan struct{}
	stderr  bytes.Buffer
	err     error
	url     string
	client  *http.Client
}

type e2eResponse struct {
	status int
	body   string
	err    error
}

func TestE2EBrokerProcess(t *testing.T) {
	binary := buildBroker(t)
	broker := startBroker(t, binary)

	t.Run("CLI requires port", func(t *testing.T) {
		command := exec.Command(binary)
		output, err := command.CombinedOutput()

		var exitError *exec.ExitError
		if !errors.As(err, &exitError) || exitError.ExitCode() != 2 {
			t.Fatalf("exit error = %v, want exit code 2", err)
		}
		if !strings.Contains(string(output), "usage: broker <port>") {
			t.Fatalf("output = %q, want usage message", output)
		}
	})

	t.Run("HTTP contract", func(t *testing.T) {
		broker.assert(t, http.MethodPut, "/contract", http.StatusBadRequest, "")
		broker.assert(t, http.MethodPost, "/contract", http.StatusMethodNotAllowed, "")
		broker.assert(t, http.MethodGet, "/contract", http.StatusNotFound, "")
		broker.assert(t, http.MethodGet, "/contract?timeout=wrong", http.StatusBadRequest, "")
	})

	t.Run("message FIFO and independent queues", func(t *testing.T) {
		broker.put(t, "pet", "cat")
		broker.put(t, "pet", "dog")
		broker.put(t, "role", "manager")

		broker.get(t, "pet", http.StatusOK, "cat")
		broker.get(t, "pet", http.StatusOK, "dog")
		broker.get(t, "pet", http.StatusNotFound, "")
		broker.get(t, "role", http.StatusOK, "manager")
		broker.get(t, "role", http.StatusNotFound, "")
	})

	t.Run("empty message", func(t *testing.T) {
		broker.put(t, "empty-message", "")
		broker.get(t, "empty-message", http.StatusOK, "")
		broker.get(t, "empty-message", http.StatusNotFound, "")
	})

	t.Run("timeout", func(t *testing.T) {
		started := time.Now()
		broker.assert(
			t,
			http.MethodGet,
			"/timeout?timeout=1",
			http.StatusNotFound,
			"",
		)

		elapsed := time.Since(started)
		if elapsed < 900*time.Millisecond || elapsed > 2*time.Second {
			t.Fatalf("request duration = %v, want about one second", elapsed)
		}
	})

	t.Run("delayed message", func(t *testing.T) {
		response := broker.getAsync("/delayed?timeout=2", nil)
		time.Sleep(100 * time.Millisecond)
		broker.put(t, "delayed", "message")

		assertE2EResponse(t, receiveE2EResponse(t, response), http.StatusOK, "message")
	})

	t.Run("waiter FIFO", func(t *testing.T) {
		first := broker.getAsync("/waiters?timeout=2", nil)
		time.Sleep(100 * time.Millisecond)
		second := broker.getAsync("/waiters?timeout=2", nil)
		time.Sleep(100 * time.Millisecond)

		broker.put(t, "waiters", "first")
		broker.put(t, "waiters", "second")

		assertE2EResponse(t, receiveE2EResponse(t, first), http.StatusOK, "first")
		assertE2EResponse(t, receiveE2EResponse(t, second), http.StatusOK, "second")
	})

	t.Run("canceled receiver does not lose message", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		response := broker.getAsync("/canceled?timeout=10", ctx)
		time.Sleep(100 * time.Millisecond)
		cancel()

		result := receiveE2EResponse(t, response)
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("request error = %v, want context cancellation", result.err)
		}

		broker.put(t, "canceled", "message")
		broker.get(t, "canceled", http.StatusOK, "message")
		broker.get(t, "canceled", http.StatusNotFound, "")
	})
}

func buildBroker(t *testing.T) string {
	t.Helper()

	tempDir := t.TempDir()
	binary := filepath.Join(tempDir, "broker")
	command := exec.Command("go", "build", "-o", binary, ".")
	command.Env = append(os.Environ(), "GOCACHE="+filepath.Join(tempDir, "go-cache"))

	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build broker: %v\n%s", err, output)
	}
	return binary
}

func startBroker(t *testing.T, binary string) *e2eBroker {
	t.Helper()

	port := freePort(t)
	broker := &e2eBroker{
		command: exec.Command(binary, strconv.Itoa(port)),
		done:    make(chan struct{}),
		url:     fmt.Sprintf("http://127.0.0.1:%d", port),
		client:  &http.Client{Timeout: 3 * time.Second},
	}
	broker.command.Stderr = &broker.stderr

	if err := broker.command.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}

	go func() {
		broker.err = broker.command.Wait()
		close(broker.done)
	}()

	t.Cleanup(func() {
		select {
		case <-broker.done:
			return
		default:
		}

		_ = broker.command.Process.Kill()
		select {
		case <-broker.done:
		case <-time.After(3 * time.Second):
			t.Error("broker process did not stop")
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-broker.done:
			t.Fatalf("broker exited during startup: %v\n%s", broker.err, broker.stderr.String())
		default:
		}

		response, err := broker.client.Get(broker.url + "/startup-probe")
		if err == nil {
			_ = response.Body.Close()
			return broker
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("broker did not start on port %d", port)
	return nil
}

func freePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

func (b *e2eBroker) put(t *testing.T, queue, message string) {
	t.Helper()

	path := "/" + queue + "?v=" + url.QueryEscape(message)
	b.assert(t, http.MethodPut, path, http.StatusOK, "")
}

func (b *e2eBroker) get(t *testing.T, queue string, status int, body string) {
	t.Helper()
	b.assert(t, http.MethodGet, "/"+queue, status, body)
}

func (b *e2eBroker) assert(
	t *testing.T,
	method string,
	path string,
	status int,
	body string,
) {
	t.Helper()

	response := b.request(t, method, path)
	assertE2EResponse(t, response, status, body)
}

func (b *e2eBroker) request(t *testing.T, method, path string) e2eResponse {
	t.Helper()

	request, err := http.NewRequest(method, b.url+path, nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	response, err := b.client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s %s response: %v", method, path, err)
	}
	return e2eResponse{status: response.StatusCode, body: string(body)}
}

func (b *e2eBroker) getAsync(
	path string,
	ctx context.Context,
) <-chan e2eResponse {
	result := make(chan e2eResponse, 1)

	go func() {
		if ctx == nil {
			ctx = context.Background()
		}

		request, err := http.NewRequestWithContext(ctx, http.MethodGet, b.url+path, nil)
		if err != nil {
			result <- e2eResponse{err: err}
			return
		}

		response, err := b.client.Do(request)
		if err != nil {
			result <- e2eResponse{err: err}
			return
		}
		defer response.Body.Close()

		body, err := io.ReadAll(response.Body)
		result <- e2eResponse{
			status: response.StatusCode,
			body:   string(body),
			err:    err,
		}
	}()

	return result
}

func receiveE2EResponse(t *testing.T, result <-chan e2eResponse) e2eResponse {
	t.Helper()

	select {
	case response := <-result:
		return response
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not finish")
		return e2eResponse{}
	}
}

func assertE2EResponse(
	t *testing.T,
	response e2eResponse,
	status int,
	body string,
) {
	t.Helper()

	if response.err != nil {
		t.Fatalf("request failed: %v", response.err)
	}
	if response.status != status || response.body != body {
		t.Fatalf(
			"response = (%d, %q), want (%d, %q)",
			response.status,
			response.body,
			status,
			body,
		)
	}
}
