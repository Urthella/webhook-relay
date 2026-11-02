// Command webhook-relay delivers signed webhooks reliably, with retries and a dead-letter state.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	var (
		database     = flag.String("database", os.Getenv("DATABASE_URL"), "Postgres connection string (or DATABASE_URL)")
		token        = flag.String("token", os.Getenv("API_TOKEN"), "bearer token for the API (or API_TOKEN)")
		addr         = flag.String("addr", ":8080", "address for the API")
		concurrency  = flag.Int("concurrency", 16, "deliveries at once")
		maxAttempts  = flag.Int("max-attempts", 8, "attempts before a message goes dead")
		baseDelay    = flag.Duration("base-delay", 5*time.Second, "delay before the first retry, doubling after that")
		maxDelay     = flag.Duration("max-delay", time.Hour, "longest delay between attempts")
		timeout      = flag.Duration("timeout", 15*time.Second, "time limit for one delivery")
		allowPrivate = flag.Bool("allow-private-addresses", false, "allow http and private addresses, for local development only")
	)
	flag.Parse()
	if err := run(*database, *token, *addr, *concurrency, *maxAttempts, *baseDelay, *maxDelay, *timeout, *allowPrivate); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "webhook-relay:", err)
		os.Exit(1)
	}
}

func run(database, token, addr string, concurrency, maxAttempts int, baseDelay, maxDelay, timeout time.Duration, allowPrivate bool) error {
	if database == "" {
		return errors.New("set -database or DATABASE_URL")
	}
	if len(token) < 16 {
		return errors.New("set -token or API_TOKEN, at least 16 characters")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, database)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := &Store{pool: pool}
	if err := store.migrate(ctx); err != nil {
		return fmt.Errorf("create tables: %w", err)
	}
	log := slog.Default()
	worker := &Worker{
		Store:       store,
		Client:      newClient(timeout, allowPrivate),
		Concurrency: concurrency,
		MaxAttempts: maxAttempts,
		BaseDelay:   baseDelay,
		MaxDelay:    maxDelay,
		// Longer than a delivery can take, so a healthy delivery never gets sent twice for this reason.
		Lease: timeout + 30*time.Second,
		Poll:  time.Second,
		Log:   log,
	}
	api := &API{Store: store, Token: token, AllowPrivate: allowPrivate, Log: log}
	server := &http.Server{Addr: addr, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("worker stopped", "err", err)
		}
	}()
	log.Info("listening", "addr", addr)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		stop()
		<-workerDone
		return err
	}
	// The pool closes when this returns, and deliveries in progress still need it to record their results.
	<-workerDone
	return ctx.Err()
}
