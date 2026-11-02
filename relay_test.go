package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testToken = "test-token-0123456789"

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL isn't set")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := &Store{pool: pool}
	if err := s.migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), "TRUNCATE attempts, messages, endpoints"); err != nil {
		t.Fatal(err)
	}
	return s
}

func newWorker(s *Store) *Worker {
	return &Worker{
		Store:       s,
		Client:      newClient(5*time.Second, true),
		Concurrency: 8,
		MaxAttempts: 4,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    50 * time.Millisecond,
		Lease:       10 * time.Second,
		Poll:        5 * time.Millisecond,
		Log:         quiet,
	}
}

// runUntil runs the worker until the condition holds.
func runUntil(t *testing.T, w *Worker, done func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_ = w.Run(ctx)
	}()
	defer func() {
		cancel()
		<-finished
	}()
	deadline := time.Now().Add(15 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func status(t *testing.T, s *Store, id string) *Message {
	t.Helper()
	m, err := s.message(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func enqueue(t *testing.T, s *Store, endpoint string) string {
	t.Helper()
	id, err := s.enqueue(context.Background(), endpoint, "order.paid", json.RawMessage(`{"order":42}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func endpointFor(t *testing.T, s *Store, url string) Endpoint {
	t.Helper()
	e, err := s.createEndpoint(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestSignatureMatchesTheStandardWebhooksExample(t *testing.T) {
	// The example published with the Standard Webhooks and Svix documentation.
	got, err := sign("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw", "msg_p5jXN8AQM9LWM0D4loKWxJek", time.Unix(1614265330, 0), []byte(`{"test": 2432232314}`))
	if err != nil {
		t.Fatal(err)
	}
	if want := "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestDeliversASignedMessage(t *testing.T) {
	s := newStore(t)
	var got atomic.Pointer[http.Request]
	var gotBody atomic.Pointer[[]byte]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody.Store(&body)
		got.Store(r)
	}))
	defer srv.Close()
	e := endpointFor(t, s, srv.URL)
	id := enqueue(t, s, e.ID)
	runUntil(t, newWorker(s), func() bool { return status(t, s, id).Status == "delivered" })

	r := got.Load()
	if r.Header.Get("webhook-id") != id {
		t.Errorf("webhook-id %q", r.Header.Get("webhook-id"))
	}
	ts, err := strconv.ParseInt(r.Header.Get("webhook-timestamp"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	want, err := sign(e.Secret, id, time.Unix(ts, 0), *gotBody.Load())
	if err != nil {
		t.Fatal(err)
	}
	if r.Header.Get("webhook-signature") != want {
		t.Errorf("signature %q, want %q", r.Header.Get("webhook-signature"), want)
	}
	var body struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(*gotBody.Load(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Type != "order.paid" || string(body.Data) != `{"order":42}` {
		t.Errorf("body %s", *gotBody.Load())
	}
}

func TestRetriesUntilItWorksKeepingTheSameID(t *testing.T) {
	s := newStore(t)
	var calls atomic.Int32
	var mu sync.Mutex
	ids := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ids[r.Header.Get("webhook-id")] = true
		mu.Unlock()
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	id := enqueue(t, s, endpointFor(t, s, srv.URL).ID)
	runUntil(t, newWorker(s), func() bool { return status(t, s, id).Status == "delivered" })
	if calls.Load() != 3 {
		t.Errorf("%d calls", calls.Load())
	}
	if len(ids) != 1 {
		t.Errorf("the ID changed between attempts: %v", ids)
	}
	if a := status(t, s, id).Attempts; len(a) != 3 || *a[0].HTTPStatus != 500 || *a[2].HTTPStatus != 200 {
		t.Errorf("attempts: %+v", a)
	}
}

func TestGoesDeadAfterTheLastAttemptAndCanBeRedriven(t *testing.T) {
	s := newStore(t)
	var healthy atomic.Bool
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !healthy.Load() {
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	defer srv.Close()
	id := enqueue(t, s, endpointFor(t, s, srv.URL).ID)
	w := newWorker(s)
	runUntil(t, w, func() bool { return status(t, s, id).Status == "dead" })
	if calls.Load() != int32(w.MaxAttempts) {
		t.Errorf("%d calls, want %d", calls.Load(), w.MaxAttempts)
	}

	// A redrive gets the full number of attempts again, not one last try.
	found, err := s.redrive(context.Background(), id)
	if err != nil || !found {
		t.Fatalf("redrive: %v %v", found, err)
	}
	runUntil(t, w, func() bool { return status(t, s, id).Status == "dead" })
	if calls.Load() != int32(2*w.MaxAttempts) {
		t.Errorf("%d calls after a redrive, want %d", calls.Load(), 2*w.MaxAttempts)
	}

	healthy.Store(true)
	if found, err := s.redrive(context.Background(), id); err != nil || !found {
		t.Fatalf("redrive: %v %v", found, err)
	}
	runUntil(t, w, func() bool { return status(t, s, id).Status == "delivered" })

	if found, _ := s.redrive(context.Background(), id); found {
		t.Error("a delivered message shouldn't be redriven")
	}
}

func TestA410DisablesTheEndpointAndStopsDeliveries(t *testing.T) {
	s := newStore(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusGone)
	}))
	defer srv.Close()
	e := endpointFor(t, s, srv.URL)
	first := enqueue(t, s, e.ID)
	w := newWorker(s)
	runUntil(t, w, func() bool { return status(t, s, first).Status == "dead" })
	second := enqueue(t, s, e.ID)
	time.Sleep(50 * time.Millisecond)
	if n, err := w.runOnce(context.Background()); err != nil || n != 0 {
		t.Fatalf("claimed %d for a disabled endpoint: %v", n, err)
	}
	if calls.Load() != 1 {
		t.Errorf("%d calls", calls.Load())
	}
	if found, err := s.enableEndpoint(context.Background(), e.ID); err != nil || !found {
		t.Fatalf("enable: %v %v", found, err)
	}
	if n, err := w.runOnce(context.Background()); err != nil || n != 1 {
		t.Fatalf("after enabling, claimed %d: %v", n, err)
	}
	if got := status(t, s, second).Status; got != "dead" {
		t.Errorf("status %s", got)
	}
}

func TestRetryAfterIsHonoredUpToTheLimit(t *testing.T) {
	w := &Worker{MaxAttempts: 8, BaseDelay: time.Second, MaxDelay: time.Minute}
	if _, next, _ := w.decide(1, 429, 30*time.Second, nil); next != 30*time.Second {
		t.Errorf("got %v", next)
	}
	if _, next, _ := w.decide(1, 503, time.Hour, nil); next != time.Minute {
		t.Errorf("a long Retry-After should be capped, got %v", next)
	}
	if _, next, _ := w.decide(1, 500, 30*time.Second, nil); next >= 30*time.Second {
		t.Errorf("Retry-After shouldn't apply to a 500, got %v", next)
	}
	if got := parseRetryAfter("120", time.Now()); got != 2*time.Minute {
		t.Errorf("got %v", got)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := parseRetryAfter(now.Add(time.Minute).Format(http.TimeFormat), now); got != time.Minute {
		t.Errorf("got %v", got)
	}
	if got := parseRetryAfter("soon", now); got != 0 {
		t.Errorf("got %v", got)
	}
}

func TestBackoffGrowsAndStaysUnderTheLimit(t *testing.T) {
	w := &Worker{BaseDelay: time.Second, MaxDelay: 30 * time.Second}
	for attempt := 1; attempt <= 80; attempt++ {
		for range 50 {
			d := w.backoff(attempt)
			if d <= 0 || d > w.MaxDelay {
				t.Fatalf("attempt %d: %v", attempt, d)
			}
		}
	}
	if d := w.backoff(1); d > time.Second {
		t.Errorf("the first retry should stay near the base delay, got %v", d)
	}
	if d := w.backoff(10); d < 15*time.Second {
		t.Errorf("late retries should be near the limit, got %v", d)
	}
}

func TestAWorkerThatDiesOnlyDelaysTheMessage(t *testing.T) {
	s := newStore(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer srv.Close()
	id := enqueue(t, s, endpointFor(t, s, srv.URL).ID)
	ctx := context.Background()

	// A worker claims it and is never heard from again.
	claims, err := s.claimDue(ctx, 1, 200*time.Millisecond)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim: %v %v", claims, err)
	}
	// While the lease holds, nobody else can take it.
	if more, err := s.claimDue(ctx, 1, time.Second); err != nil || len(more) != 0 {
		t.Fatalf("claimed during the lease: %v %v", more, err)
	}
	w := newWorker(s)
	runUntil(t, w, func() bool { return status(t, s, id).Status == "delivered" })
	if calls.Load() != 1 {
		t.Errorf("%d calls", calls.Load())
	}
}

func TestShutdownLetsADeliveryFinishAndRecordsIt(t *testing.T) {
	s := newStore(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(started)
		<-release
	}))
	defer srv.Close()
	id := enqueue(t, s, endpointFor(t, s, srv.URL).ID)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = newWorker(s).Run(ctx)
	}()
	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("the worker stopped while a delivery was in progress")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-done
	if got := status(t, s, id); got.Status != "delivered" || calls.Load() != 1 {
		t.Fatalf("status %s after %d calls: a shutdown shouldn't cost a retry", got.Status, calls.Load())
	}
}

func TestAWorkerWhoseLeaseRanOutCantOverwriteANewerAttempt(t *testing.T) {
	s := newStore(t)
	id := enqueue(t, s, endpointFor(t, s, "http://example.invalid").ID)
	ctx := context.Background()
	stale, err := s.claimDue(ctx, 1, time.Millisecond)
	if err != nil || len(stale) != 1 {
		t.Fatal(stale, err)
	}
	time.Sleep(20 * time.Millisecond)
	fresh, err := s.claimDue(ctx, 1, time.Minute)
	if err != nil || len(fresh) != 1 {
		t.Fatal(fresh, err)
	}
	if err := s.recordAttempt(ctx, fresh[0], "delivered", 0, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := s.recordAttempt(ctx, stale[0], "dead", 0, nil, nil, true); err != nil {
		t.Fatal(err)
	}
	if got := status(t, s, id); got.Status != "delivered" || len(got.Attempts) != 1 {
		t.Fatalf("the stale worker overwrote the result: %+v", got)
	}
	var disabled bool
	if err := s.pool.QueryRow(ctx, "SELECT disabled_at IS NOT NULL FROM endpoints").Scan(&disabled); err != nil || disabled {
		t.Fatalf("the stale worker disabled the endpoint: %v %v", disabled, err)
	}
}

func TestWorkersNeverDeliverTheSameMessageTwice(t *testing.T) {
	s := newStore(t)
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Header.Get("webhook-id")]++
		mu.Unlock()
	}))
	defer srv.Close()
	e := endpointFor(t, s, srv.URL)
	const total = 300
	for range total {
		enqueue(t, s, e.ID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = newWorker(s).Run(ctx)
		}()
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		var pending int
		if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM messages WHERE status <> 'delivered'").Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d still not delivered", pending)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != total {
		t.Errorf("%d messages arrived, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("%s arrived %d times", id, n)
		}
	}
}

func TestDoesNotFollowRedirects(t *testing.T) {
	s := newStore(t)
	var hit atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit.Store(true) }))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer srv.Close()
	id := enqueue(t, s, endpointFor(t, s, srv.URL).ID)
	w := newWorker(s)
	w.MaxAttempts = 1
	runUntil(t, w, func() bool { return status(t, s, id).Status == "dead" })
	if hit.Load() {
		t.Error("the redirect was followed")
	}
}

func TestRefusesPrivateAddresses(t *testing.T) {
	for _, addr := range []string{
		"127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", "169.254.169.254", "0.0.0.0", "100.64.0.1",
		"224.0.0.1", "::1", "fe80::1", "fc00::1", "::", "::ffff:127.0.0.1", "::ffff:169.254.169.254", "64:ff9b::7f00:1",
		"::ffff:100.64.0.1", "::ffff:10.0.0.1", "192.0.2.1",
	} {
		if isPublic(netip.MustParseAddr(addr)) {
			t.Errorf("%s counted as public", addr)
		}
	}
	for _, addr := range []string{"8.8.8.8", "93.184.216.34", "2606:4700:4700::1111"} {
		if !isPublic(netip.MustParseAddr(addr)) {
			t.Errorf("%s counted as private", addr)
		}
	}
}

func TestTheDialerBlocksAPrivateAddressEvenAfterTheURLCheck(t *testing.T) {
	var hit atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit.Store(true) }))
	defer srv.Close()
	// A hostname passes the URL check, then resolves to loopback, which is how DNS rebinding looks.
	port := srv.URL[strings.LastIndex(srv.URL, ":"):]
	url := "https://localhost" + port
	if err := checkEndpointURL(url, false); err != nil {
		t.Fatalf("a hostname should pass the early check: %v", err)
	}
	resp, err := newClient(time.Second, false).Get("http://localhost" + port)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("the request should have been blocked")
	}
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("wrong error: %v", err)
	}
	if hit.Load() {
		t.Error("the request reached the server")
	}
}

func TestChecksEndpointURLs(t *testing.T) {
	for _, bad := range []string{
		"", "not a url", "/relative", "http://example.com/hook", "ftp://example.com", "https://user:pw@example.com",
		"https://127.0.0.1/hook", "https://[::1]/hook", "https://169.254.169.254/latest/meta-data",
	} {
		if err := checkEndpointURL(bad, false); err == nil {
			t.Errorf("%q should be refused", bad)
		}
	}
	if err := checkEndpointURL("https://hooks.example.com/in", false); err != nil {
		t.Error(err)
	}
	if err := checkEndpointURL("http://localhost:9000/in", true); err != nil {
		t.Error(err)
	}
}

func apiCall(t *testing.T, h http.Handler, token, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestTheAPINeedsTheToken(t *testing.T) {
	h := (&API{Store: newStore(t), Token: testToken, Log: quiet}).Handler()
	for _, token := range []string{"", "wrong"} {
		if rec := apiCall(t, h, token, "POST", "/v1/endpoints", `{"url":"https://example.com"}`); rec.Code != 401 {
			t.Errorf("token %q: %d", token, rec.Code)
		}
	}
	if rec := apiCall(t, h, "", "GET", "/healthz", ""); rec.Code != 204 {
		t.Errorf("healthz: %d", rec.Code)
	}
}

func TestTheAPIFlow(t *testing.T) {
	s := newStore(t)
	h := (&API{Store: s, Token: testToken, Log: quiet}).Handler()
	if rec := apiCall(t, h, testToken, "POST", "/v1/endpoints", `{"url":"https://127.0.0.1/x"}`); rec.Code != 400 {
		t.Fatalf("a private address: %d", rec.Code)
	}
	rec := apiCall(t, h, testToken, "POST", "/v1/endpoints", `{"url":"https://hooks.example.com/in"}`)
	if rec.Code != 201 {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	var e Endpoint
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(e.Secret, secretPrefix) {
		t.Errorf("secret %q", e.Secret)
	}

	send := func(key string) string {
		rec := apiCall(t, h, testToken, "POST", "/v1/messages", fmt.Sprintf(`{"endpoint_id":%q,"type":"t","data":{"a":1},"idempotency_key":%q}`, e.ID, key))
		if rec.Code != 202 {
			t.Fatalf("send: %d %s", rec.Code, rec.Body)
		}
		var out struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out.ID
	}
	first := send("key-1")
	if again := send("key-1"); again != first {
		t.Errorf("the same idempotency key made a new message: %s, %s", first, again)
	}
	if other := send("key-2"); other == first {
		t.Error("a different key should make a new message")
	}

	if rec := apiCall(t, h, testToken, "GET", "/v1/messages/"+first, ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"pending"`) {
		t.Errorf("get: %d %s", rec.Code, rec.Body)
	}
	if rec := apiCall(t, h, testToken, "GET", "/v1/messages/nope", ""); rec.Code != 404 {
		t.Errorf("get unknown: %d", rec.Code)
	}
	if rec := apiCall(t, h, testToken, "POST", "/v1/messages", `{"endpoint_id":"nope","type":"t","data":{}}`); rec.Code != 404 {
		t.Errorf("unknown endpoint: %d %s", rec.Code, rec.Body)
	}
	if rec := apiCall(t, h, testToken, "POST", "/v1/messages", `{"endpoint_id":"x"}`); rec.Code != 400 {
		t.Errorf("missing fields: %d", rec.Code)
	}
	if rec := apiCall(t, h, testToken, "POST", "/v1/messages/"+first+"/redrive", ""); rec.Code != 409 {
		t.Errorf("redrive of a pending message: %d", rec.Code)
	}
	huge := `{"endpoint_id":"x","type":"t","data":"` + string(bytes.Repeat([]byte("x"), maxRequestBody)) + `"}`
	if rec := apiCall(t, h, testToken, "POST", "/v1/messages", huge); rec.Code != 400 {
		t.Errorf("oversized body: %d", rec.Code)
	}
}
