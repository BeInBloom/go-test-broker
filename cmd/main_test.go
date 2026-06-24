package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestHTTPContract(t *testing.T) {
	b := newBroker()

	tests := []struct {
		name       string
		method     string
		target     string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "root is not a queue",
			method:     http.MethodGet,
			target:     "/",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "PUT requires v",
			method:     http.MethodPut,
			target:     "/queue",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unsupported method",
			method:     http.MethodPost,
			target:     "/queue",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "empty timeout",
			method:     http.MethodGet,
			target:     "/queue?timeout=",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "negative timeout",
			method:     http.MethodGet,
			target:     "/queue?timeout=-1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-numeric timeout",
			method:     http.MethodGet,
			target:     "/queue?timeout=soon",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "overflowing timeout",
			method:     http.MethodGet,
			target:     "/queue?timeout=9223372037",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "zero timeout does not wait",
			method:     http.MethodGet,
			target:     "/queue?timeout=0",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "empty queue without timeout",
			method:     http.MethodGet,
			target:     "/queue",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := serve(b, tt.method, tt.target)
			assertResponse(t, response, tt.wantStatus, tt.wantBody)
		})
	}
}

func TestHTTPPutAcceptsEmptyAndEscapedMessages(t *testing.T) {
	b := newBroker()

	assertResponse(t, serve(b, http.MethodPut, "/queue?v="), http.StatusOK, "")
	assertResponse(t, serve(b, http.MethodGet, "/queue"), http.StatusOK, "")

	assertResponse(t, serve(b, http.MethodPut, "/queue?v=hello%20world"), http.StatusOK, "")
	assertResponse(t, serve(b, http.MethodGet, "/queue"), http.StatusOK, "hello world")
}

func TestMessageFIFOAndIndependentQueues(t *testing.T) {
	b := newBroker()

	for i := 0; i < 50; i++ {
		assertResponse(
			t,
			serve(b, http.MethodPut, "/numbers?v="+strconv.Itoa(i)),
			http.StatusOK,
			"",
		)
	}
	assertResponse(t, serve(b, http.MethodPut, "/role?v=manager"), http.StatusOK, "")

	for i := 0; i < 50; i++ {
		assertResponse(
			t,
			serve(b, http.MethodGet, "/numbers"),
			http.StatusOK,
			strconv.Itoa(i),
		)
	}

	assertResponse(t, serve(b, http.MethodGet, "/numbers"), http.StatusNotFound, "")
	assertResponse(t, serve(b, http.MethodGet, "/role"), http.StatusOK, "manager")
	assertResponse(t, serve(b, http.MethodGet, "/role"), http.StatusNotFound, "")
}

func TestQueueNameUsesTheWholePath(t *testing.T) {
	b := newBroker()

	assertResponse(t, serve(b, http.MethodPut, "/team/backend?v=job"), http.StatusOK, "")
	assertResponse(t, serve(b, http.MethodGet, "/team"), http.StatusNotFound, "")
	assertResponse(t, serve(b, http.MethodGet, "/team/backend"), http.StatusOK, "job")
}

func TestGETWaitsForDelayedMessage(t *testing.T) {
	b := newBroker()
	response := serveAsync(b, http.MethodGet, "/queue?timeout=2")

	waitForWaiterCount(t, b, "queue", 1)
	assertResponse(t, serve(b, http.MethodPut, "/queue?v=message"), http.StatusOK, "")
	assertResponse(t, receiveResponse(t, response), http.StatusOK, "message")
	assertQueueDeleted(t, b, "queue")
}

func TestGETTimesOut(t *testing.T) {
	b := newBroker()
	started := time.Now()

	response := serve(b, http.MethodGet, "/queue?timeout=1")

	if elapsed := time.Since(started); elapsed < 900*time.Millisecond {
		t.Fatalf("GET returned before its timeout: %v", elapsed)
	}
	assertResponse(t, response, http.StatusNotFound, "")
	assertQueueDeleted(t, b, "queue")
}

func TestWaiterFIFO(t *testing.T) {
	b := newBroker()
	const waiterCount = 20

	responses := make([]<-chan *httptest.ResponseRecorder, waiterCount)
	for i := range responses {
		responses[i] = serveAsync(b, http.MethodGet, "/queue?timeout=2")
		waitForWaiterCount(t, b, "queue", i+1)
	}

	for i := range responses {
		target := "/queue?v=" + strconv.Itoa(i)
		assertResponse(t, serve(b, http.MethodPut, target), http.StatusOK, "")
	}

	for i, response := range responses {
		assertResponse(t, receiveResponse(t, response), http.StatusOK, strconv.Itoa(i))
	}
	assertQueueDeleted(t, b, "queue")
}

func TestCanceledWaiterDoesNotConsumeMessage(t *testing.T) {
	b := newBroker()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan waitResult, 1)

	go func() {
		result <- b.get(ctx, "queue", time.Second)
	}()
	waitForWaiterCount(t, b, "queue", 1)

	cancel()
	got := receiveWaitResult(t, result)
	if got.status != requestCanceled {
		t.Fatalf("canceled GET returned %#v", got)
	}

	b.put("queue", "message")
	assertWaitResult(t, b.get(context.Background(), "queue", 0), messageReceived, "message")
	assertQueueDeleted(t, b, "queue")
}

func TestCanceledFirstWaiterDoesNotBlockSecond(t *testing.T) {
	b := newBroker()
	firstContext, cancelFirst := context.WithCancel(context.Background())
	first := make(chan waitResult, 1)
	second := make(chan waitResult, 1)

	go func() {
		first <- b.get(firstContext, "queue", time.Second)
	}()
	waitForWaiterCount(t, b, "queue", 1)

	go func() {
		second <- b.get(context.Background(), "queue", time.Second)
	}()
	waitForWaiterCount(t, b, "queue", 2)

	b.mu.Lock()
	cancelFirst()
	putDone := make(chan struct{})
	go func() {
		b.put("queue", "message")
		close(putDone)
	}()
	b.mu.Unlock()

	if got := receiveWaitResult(t, first); got.status != requestCanceled {
		t.Fatalf("first waiter returned %#v", got)
	}
	assertWaitResult(t, receiveWaitResult(t, second), messageReceived, "message")
	waitForSignal(t, putDone)
	assertQueueDeleted(t, b, "queue")
}

func TestExpiredFirstWaiterDoesNotBlockSecond(t *testing.T) {
	b := newBroker()
	first := make(chan waitResult, 1)
	second := make(chan waitResult, 1)

	go func() {
		first <- b.get(context.Background(), "queue", 20*time.Millisecond)
	}()
	waitForWaiterCount(t, b, "queue", 1)

	go func() {
		second <- b.get(context.Background(), "queue", time.Second)
	}()
	waitForWaiterCount(t, b, "queue", 2)

	b.mu.Lock()
	time.Sleep(25 * time.Millisecond)
	putDone := make(chan struct{})
	go func() {
		b.put("queue", "message")
		close(putDone)
	}()
	b.mu.Unlock()

	assertWaitResult(t, receiveWaitResult(t, first), messageNotFound, "")
	assertWaitResult(t, receiveWaitResult(t, second), messageReceived, "message")
	waitForSignal(t, putDone)
	assertQueueDeleted(t, b, "queue")
}

func TestAlreadyCanceledGETDoesNotConsumeReadyMessage(t *testing.T) {
	b := newBroker()
	b.put("queue", "message")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assertWaitResult(t, b.get(ctx, "queue", 0), requestCanceled, "")
	assertWaitResult(t, b.get(context.Background(), "queue", 0), messageReceived, "message")
	assertQueueDeleted(t, b, "queue")
}

func TestTimeoutPutRaceNeverLosesOrDuplicatesMessage(t *testing.T) {
	b := newBroker()
	const iterations = 100

	for i := 0; i < iterations; i++ {
		name := fmt.Sprintf("queue-%d", i)
		first := make(chan waitResult, 1)

		go func() {
			first <- b.get(context.Background(), name, 10*time.Millisecond)
		}()
		deadline := waitForFirstWaiter(t, b, name).deadline

		putDone := make(chan struct{})
		time.AfterFunc(time.Until(deadline), func() {
			b.put(name, "message")
			close(putDone)
		})

		firstResult := receiveWaitResult(t, first)
		waitForSignal(t, putDone)
		secondResult := b.get(context.Background(), name, 0)

		received := 0
		for _, result := range []waitResult{firstResult, secondResult} {
			switch result.status {
			case messageReceived:
				if result.message != "message" {
					t.Fatalf("iteration %d received wrong message: %#v", i, result)
				}
				received++
			case messageNotFound:
			default:
				t.Fatalf("iteration %d returned unexpected result: %#v", i, result)
			}
		}

		if received != 1 {
			t.Fatalf(
				"iteration %d delivered message %d times: first=%#v second=%#v",
				i,
				received,
				firstResult,
				secondResult,
			)
		}
		assertQueueDeleted(t, b, name)
	}
}

func TestCancellationPutRaceNeverLosesMessage(t *testing.T) {
	b := newBroker()
	const iterations = 100

	for i := 0; i < iterations; i++ {
		name := fmt.Sprintf("queue-%d", i)
		ctx, cancel := context.WithCancel(context.Background())
		first := make(chan waitResult, 1)

		go func() {
			first <- b.get(ctx, name, time.Second)
		}()
		waitForWaiterCount(t, b, name, 1)

		b.mu.Lock()
		cancel()
		putDone := make(chan struct{})
		go func() {
			b.put(name, "message")
			close(putDone)
		}()
		b.mu.Unlock()

		if got := receiveWaitResult(t, first); got.status != requestCanceled {
			t.Fatalf("iteration %d canceled GET returned %#v", i, got)
		}
		waitForSignal(t, putDone)
		assertWaitResult(t, b.get(context.Background(), name, 0), messageReceived, "message")
		assertQueueDeleted(t, b, name)
	}
}

func TestConcurrentDeliveryDoesNotLoseOrDuplicateMessages(t *testing.T) {
	b := newBroker()
	const messageCount = 200

	results := make(chan waitResult, messageCount)
	for range messageCount {
		go func() {
			results <- b.get(context.Background(), "queue", 2*time.Second)
		}()
	}
	waitForWaiterCount(t, b, "queue", messageCount)

	var puts sync.WaitGroup
	for i := 0; i < messageCount; i++ {
		puts.Add(1)
		go func() {
			defer puts.Done()
			b.put("queue", strconv.Itoa(i))
		}()
	}
	puts.Wait()

	seen := make(map[string]bool, messageCount)
	for range messageCount {
		result := receiveWaitResult(t, results)
		if result.status != messageReceived {
			t.Fatalf("concurrent GET returned %#v", result)
		}
		if seen[result.message] {
			t.Fatalf("message %q was delivered more than once", result.message)
		}
		seen[result.message] = true
	}

	for i := 0; i < messageCount; i++ {
		message := strconv.Itoa(i)
		if !seen[message] {
			t.Fatalf("message %q was lost", message)
		}
	}
	assertQueueDeleted(t, b, "queue")
}

func TestEmptyQueuesAreDeleted(t *testing.T) {
	b := newBroker()

	b.put("ready", "message")
	assertWaitResult(t, b.get(context.Background(), "ready", 0), messageReceived, "message")
	assertQueueDeleted(t, b, "ready")

	assertWaitResult(
		t,
		b.get(context.Background(), "timeout", 10*time.Millisecond),
		messageNotFound,
		"",
	)
	assertQueueDeleted(t, b, "timeout")

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan waitResult, 1)
	go func() {
		result <- b.get(ctx, "cancel", time.Second)
	}()
	waitForWaiterCount(t, b, "cancel", 1)
	cancel()
	assertWaitResult(t, receiveWaitResult(t, result), requestCanceled, "")
	assertQueueDeleted(t, b, "cancel")
}

func TestParseTimeoutBoundaries(t *testing.T) {
	maxSeconds := int64(1<<63-1) / int64(time.Second)

	tests := []struct {
		query string
		want  time.Duration
		ok    bool
	}{
		{query: "", want: 0, ok: true},
		{query: "?timeout=0", want: 0, ok: true},
		{query: "?timeout=1", want: time.Second, ok: true},
		{
			query: "?timeout=" + strconv.FormatInt(maxSeconds, 10),
			want:  time.Duration(maxSeconds) * time.Second,
			ok:    true,
		},
		{query: "?timeout=" + strconv.FormatInt(maxSeconds+1, 10), ok: false},
		{query: "?timeout=-1", ok: false},
		{query: "?timeout=text", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/queue"+tt.query, nil)
			got, ok := parseTimeout(request)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("parseTimeout() = (%v, %v), want (%v, %v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func serve(b *Broker, method, target string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, nil)
	response := httptest.NewRecorder()
	b.ServeHTTP(response, request)
	return response
}

func serveAsync(b *Broker, method, target string) <-chan *httptest.ResponseRecorder {
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- serve(b, method, target)
	}()
	return done
}

func receiveResponse(
	t *testing.T,
	response <-chan *httptest.ResponseRecorder,
) *httptest.ResponseRecorder {
	t.Helper()

	select {
	case got := <-response:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP request did not finish")
		return nil
	}
}

func receiveWaitResult(t *testing.T, result <-chan waitResult) waitResult {
	t.Helper()

	select {
	case got := <-result:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("broker operation did not finish")
		return waitResult{}
	}
}

func waitForSignal(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not finish")
	}
}

func waitForWaiterCount(t *testing.T, b *Broker, name string, want int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		q := b.queues[name]
		count := 0
		if q != nil {
			count = q.waiters.Len()
		}
		b.mu.Unlock()

		if count == want {
			return
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("queue %q did not reach %d waiters", name, want)
}

func waitForFirstWaiter(t *testing.T, b *Broker, name string) *Waiter {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b.mu.Lock()
		q := b.queues[name]
		if q != nil && q.waiters.Len() > 0 {
			waiter := q.waiters.Front().Value.(*Waiter)
			b.mu.Unlock()
			return waiter
		}
		b.mu.Unlock()
		time.Sleep(time.Millisecond)
	}

	t.Fatalf("queue %q did not get a waiter", name)
	return nil
}

func assertResponse(
	t *testing.T,
	response *httptest.ResponseRecorder,
	wantStatus int,
	wantBody string,
) {
	t.Helper()

	if response.Code != wantStatus || response.Body.String() != wantBody {
		t.Fatalf(
			"response = (%d, %q), want (%d, %q)",
			response.Code,
			response.Body.String(),
			wantStatus,
			wantBody,
		)
	}
}

func assertWaitResult(
	t *testing.T,
	got waitResult,
	wantStatus receiveStatus,
	wantMessage string,
) {
	t.Helper()

	if got.status != wantStatus || got.message != wantMessage {
		t.Fatalf(
			"result = (%d, %q), want (%d, %q)",
			got.status,
			got.message,
			wantStatus,
			wantMessage,
		)
	}
}

func assertQueueDeleted(t *testing.T, b *Broker, name string) {
	t.Helper()

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; ok {
		t.Fatalf("empty queue %q remains in broker", name)
	}
}
