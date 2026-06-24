package main

import (
	"container/list"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	mu     sync.Mutex
	queues map[string]*Queue
}

type Queue struct {
	name     string
	messages []string
	waiters  *list.List
}

type Waiter struct {
	ch       chan waitResult
	ctx      context.Context
	deadline time.Time
	q        *Queue
	elem     *list.Element
}

type receiveStatus uint8

const (
	messageNotFound receiveStatus = iota
	messageReceived
	requestCanceled
)

type waitResult struct {
	message string
	status  receiveStatus
}

func newBroker() *Broker {
	return &Broker{queues: make(map[string]*Queue)}
}

func (b *Broker) queue(name string) *Queue {
	q := b.queues[name]
	if q == nil {
		q = &Queue{name: name, waiters: list.New()}
		b.queues[name] = q
	}
	return q
}

func (b *Broker) takeMessage(q *Queue) string {
	msg := q.messages[0]
	q.messages[0] = ""
	q.messages = q.messages[1:]

	if len(q.messages) == 0 {
		q.messages = nil
	}

	b.deleteIfEmpty(q)
	return msg
}

func (b *Broker) put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.queue(name)
	if b.deliverToWaiter(q, msg) {
		return
	}
	q.messages = append(q.messages, msg)
}

func (b *Broker) deliverToWaiter(q *Queue, msg string) bool {
	for q.waiters.Len() > 0 {
		w := q.waiters.Front().Value.(*Waiter)
		result := waitResult{message: msg, status: messageReceived}

		switch {
		case w.ctx.Err() != nil:
			result = waitResult{status: requestCanceled}
		case !time.Now().Before(w.deadline):
			result = waitResult{status: messageNotFound}
		}

		b.removeWaiter(w)
		w.ch <- result

		if result.status == messageReceived {
			b.deleteIfEmpty(q)
			return true
		}
	}
	return false
}

func (b *Broker) get(ctx context.Context, name string, timeout time.Duration) waitResult {
	b.mu.Lock()

	if ctx.Err() != nil {
		b.mu.Unlock()
		return waitResult{status: requestCanceled}
	}

	q := b.queues[name]

	if q != nil && len(q.messages) > 0 {
		msg := b.takeMessage(q)
		b.mu.Unlock()
		return waitResult{message: msg, status: messageReceived}
	}

	if timeout <= 0 {
		b.mu.Unlock()
		return waitResult{status: messageNotFound}
	}

	if q == nil {
		q = b.queue(name)
	}

	w := &Waiter{
		ch:       make(chan waitResult, 1),
		ctx:      ctx,
		deadline: time.Now().Add(timeout),
		q:        q,
	}
	w.elem = q.waiters.PushBack(w)

	b.mu.Unlock()

	return b.wait(ctx, w)
}

func (b *Broker) wait(ctx context.Context, w *Waiter) waitResult {
	timer := time.NewTimer(time.Until(w.deadline))
	defer timer.Stop()

	select {
	case result := <-w.ch:
		return result
	case <-timer.C:
		return b.stopWaiting(w, messageNotFound)
	case <-ctx.Done():
		return b.stopWaiting(w, requestCanceled)
	}
}

func (b *Broker) stopWaiting(w *Waiter, status receiveStatus) waitResult {
	b.mu.Lock()

	if w.elem != nil {
		b.removeWaiter(w)
		b.deleteIfEmpty(w.q)
		b.mu.Unlock()
		return waitResult{status: status}
	}

	b.mu.Unlock()
	return <-w.ch
}

func (b *Broker) removeWaiter(w *Waiter) {
	w.q.waiters.Remove(w.elem)
	w.elem = nil
}

func (b *Broker) deleteIfEmpty(q *Queue) {
	if len(q.messages) == 0 && q.waiters.Len() == 0 && b.queues[q.name] == q {
		delete(b.queues, q.name)
	}
}

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	if name == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		b.handlePut(w, r, name)
	case http.MethodGet:
		b.handleGet(w, r, name)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (b *Broker) handlePut(w http.ResponseWriter, r *http.Request, name string) {
	values, ok := r.URL.Query()["v"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	b.put(name, values[0])
	w.WriteHeader(http.StatusOK)
}

func (b *Broker) handleGet(w http.ResponseWriter, r *http.Request, name string) {
	timeout, ok := parseTimeout(r)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	result := b.get(r.Context(), name, timeout)
	switch result.status {
	case requestCanceled:
		return
	case messageNotFound:
		w.WriteHeader(http.StatusNotFound)
		return
	}

	_, _ = w.Write([]byte(result.message))
}

func parseTimeout(r *http.Request) (time.Duration, bool) {
	values, ok := r.URL.Query()["timeout"]
	if !ok {
		return 0, true
	}

	n, err := strconv.ParseInt(values[0], 10, 64)

	const maxDurationSeconds = int64(1<<63-1) / int64(time.Second)

	if err != nil || n < 0 || n > maxDurationSeconds {
		return 0, false
	}

	return time.Duration(n) * time.Second, true
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: broker <port>")
		os.Exit(2)
	}

	port := os.Args[1]
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	if err := http.ListenAndServe(port, newBroker()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
