package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Worker delivers due messages.
type Worker struct {
	Store       *Store
	Client      *http.Client
	Concurrency int
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Lease       time.Duration
	Poll        time.Duration
	Log         *slog.Logger
}

// newClient builds the HTTP client for deliveries. It doesn't follow redirects, because a redirect could
// send the request somewhere the endpoint's URL check never saw.
func newClient(timeout time.Duration, allowPrivate bool) *http.Client {
	return &http.Client{
		Timeout:   timeout,
		Transport: &http.Transport{DialContext: guardedDialer(allowPrivate), Proxy: nil},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// Run delivers until ctx ends, then returns once the deliveries in progress have finished and been
// recorded.
func (w *Worker) Run(ctx context.Context) error {
	for {
		n, err := w.runOnce(ctx)
		if err != nil && ctx.Err() == nil {
			w.Log.Error("claiming messages failed", "err", err)
		}
		if n > 0 && err == nil {
			continue // there may be more waiting
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.Poll):
		}
	}
}

// runOnce claims one batch and delivers it, returning how many it claimed.
func (w *Worker) runOnce(ctx context.Context) (int, error) {
	claims, err := w.Store.claimDue(ctx, w.Concurrency, w.Lease)
	if err != nil {
		return 0, err
	}
	var wg sync.WaitGroup
	for _, c := range claims {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.deliver(ctx, c)
		}()
	}
	wg.Wait()
	return len(claims), nil
}

func (w *Worker) deliver(ctx context.Context, c claim) {
	// Not cancelled by shutdown: the request may already be on its way, and the client's timeout bounds it.
	status, retryAfter, err := w.send(context.WithoutCancel(ctx), c)
	var (
		httpStatus *int
		attemptErr *string
	)
	if status != 0 {
		httpStatus = &status
	}
	if err != nil {
		text := err.Error()
		attemptErr = &text
	}
	outcome, next, disable := w.decide(c.Attempt, status, retryAfter, err)
	// Record the outcome even if we're shutting down: the delivery has already happened.
	recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if recErr := w.Store.recordAttempt(recordCtx, c, outcome, next, httpStatus, attemptErr, disable); recErr != nil {
		// The lease will run out and the message gets sent again, which at-least-once allows.
		w.Log.Error("recording an attempt failed", "message", c.ID, "err", recErr)
	}
}

func (w *Worker) send(ctx context.Context, c claim) (int, time.Duration, error) {
	body := []byte(c.Body)
	at := time.Now()
	signature, err := sign(c.Secret, c.ID, at, body)
	if err != nil {
		return 0, 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "webhook-relay")
	req.Header.Set("webhook-id", c.ID)
	req.Header.Set("webhook-timestamp", strconv.FormatInt(at.Unix(), 10))
	req.Header.Set("webhook-signature", signature)
	resp, err := w.Client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.CopyN(io.Discard, resp.Body, 64<<10)
	return resp.StatusCode, parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()), nil
}

// decide turns the result of an attempt into what happens next.
func (w *Worker) decide(attempt, status int, retryAfter time.Duration, err error) (outcome string, next time.Duration, disable bool) {
	switch {
	case err == nil && status >= 200 && status < 300:
		return "delivered", 0, false
	case err == nil && status == http.StatusGone:
		// The receiver is saying it never wants these again, so stop rather than retry.
		return "dead", 0, true
	case attempt >= w.MaxAttempts:
		return "dead", 0, false
	}
	next = w.backoff(attempt)
	if (status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable) && retryAfter > next {
		next = min(retryAfter, w.MaxDelay)
	}
	return "pending", next, false
}

// backoff doubles per attempt up to MaxDelay, with jitter so failed deliveries don't return together.
func (w *Worker) backoff(attempt int) time.Duration {
	delay := w.MaxDelay
	if shift := attempt - 1; shift < 32 && w.BaseDelay<<shift < w.MaxDelay && w.BaseDelay<<shift > 0 {
		delay = w.BaseDelay << shift
	}
	return delay/2 + rand.N(delay/2+1)
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}
