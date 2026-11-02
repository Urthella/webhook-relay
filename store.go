package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

const uniqueViolation = "23505"

// Store holds endpoints and messages in Postgres, which is what makes delivery survive a crash.
type Store struct {
	pool *pgxpool.Pool
}

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schema)
	return err
}

// Endpoint is a URL that receives webhooks.
type Endpoint struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	Secret   string `json:"secret,omitempty"`
	Disabled bool   `json:"disabled"`
}

// Message is what the API reports about a webhook.
type Message struct {
	ID       string    `json:"id"`
	Endpoint string    `json:"endpoint_id"`
	Status   string    `json:"status"`
	Attempts []Attempt `json:"attempts"`
}

// Attempt is one try at delivering a message.
type Attempt struct {
	Attempt    int       `json:"attempt"`
	At         time.Time `json:"at"`
	HTTPStatus *int      `json:"http_status,omitempty"`
	Error      *string   `json:"error,omitempty"`
}

func (s *Store) createEndpoint(ctx context.Context, url string) (Endpoint, error) {
	e := Endpoint{ID: newID("ep_"), URL: url, Secret: newSecret()}
	_, err := s.pool.Exec(ctx, "INSERT INTO endpoints (id, url, secret) VALUES ($1, $2, $3)", e.ID, e.URL, e.Secret)
	return e, err
}

func (s *Store) enableEndpoint(ctx context.Context, id string) (bool, error) {
	tag, err := s.pool.Exec(ctx, "UPDATE endpoints SET disabled_at = NULL WHERE id = $1", id)
	return tag.RowsAffected() == 1, err
}

// ErrUnknownEndpoint means a message named an endpoint that doesn't exist.
var ErrUnknownEndpoint = errors.New("unknown endpoint")

// enqueue stores a message. A repeated idempotency key returns the original message.
func (s *Store) enqueue(ctx context.Context, endpoint, eventType string, data json.RawMessage, key *string) (string, error) {
	body, err := json.Marshal(struct {
		Type      string          `json:"type"`
		Timestamp string          `json:"timestamp"`
		Data      json.RawMessage `json:"data"`
	}{eventType, time.Now().UTC().Format(time.RFC3339), data})
	if err != nil {
		return "", err
	}
	id := newID("msg_")
	_, err = s.pool.Exec(ctx, "INSERT INTO messages (id, endpoint_id, idempotency_key, body) VALUES ($1, $2, $3, $4)", id, endpoint, key, string(body))
	var pgErr *pgconn.PgError
	switch {
	case err == nil:
		return id, nil
	case errors.As(err, &pgErr) && pgErr.Code == "23503":
		return "", ErrUnknownEndpoint
	case errors.As(err, &pgErr) && pgErr.Code == uniqueViolation:
		err = s.pool.QueryRow(ctx, "SELECT id FROM messages WHERE endpoint_id = $1 AND idempotency_key = $2", endpoint, key).Scan(&id)
		return id, err
	default:
		return "", err
	}
}

func (s *Store) message(ctx context.Context, id string) (*Message, error) {
	m := Message{ID: id, Attempts: []Attempt{}}
	err := s.pool.QueryRow(ctx, "SELECT endpoint_id, status FROM messages WHERE id = $1", id).Scan(&m.Endpoint, &m.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, "SELECT attempt, at, http_status, error FROM attempts WHERE message_id = $1 ORDER BY attempt", id)
	if err != nil {
		return nil, err
	}
	m.Attempts, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (Attempt, error) {
		var a Attempt
		return a, r.Scan(&a.Attempt, &a.At, &a.HTTPStatus, &a.Error)
	})
	return &m, err
}

// redrive sends a dead message around again from the start.
func (s *Store) redrive(ctx context.Context, id string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE messages SET status = 'pending', attempts = 0, next_at = now(), locked_until = NULL
		WHERE id = $1 AND status = 'dead'`, id)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 1 {
		_, err = s.pool.Exec(ctx, "DELETE FROM attempts WHERE message_id = $1", id)
	}
	return tag.RowsAffected() == 1, err
}

type claim struct {
	ID       string
	Attempt  int
	Body     string
	URL      string
	Secret   string
	Endpoint string
}

// claimDue locks due messages for this worker. SKIP LOCKED keeps workers from taking the same row, and
// the lease means a worker that dies without reporting back only delays a message.
func (s *Store) claimDue(ctx context.Context, limit int, lease time.Duration) ([]claim, error) {
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT m.id FROM messages m JOIN endpoints e ON e.id = m.endpoint_id
			WHERE m.status = 'pending' AND m.next_at <= now()
				AND (m.locked_until IS NULL OR m.locked_until <= now())
				AND e.disabled_at IS NULL
			ORDER BY m.next_at
			LIMIT $1
			FOR UPDATE OF m SKIP LOCKED
		)
		UPDATE messages m
		SET attempts = m.attempts + 1, locked_until = now() + make_interval(secs => $2)
		FROM due, endpoints e
		WHERE m.id = due.id AND e.id = m.endpoint_id
		RETURNING m.id, m.attempts, m.body, e.url, e.secret, e.id`, limit, lease.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (claim, error) {
		var c claim
		return c, r.Scan(&c.ID, &c.Attempt, &c.Body, &c.URL, &c.Secret, &c.Endpoint)
	})
}

// recordAttempt writes the outcome of one attempt. The attempt number fences it: a worker whose lease
// ran out while another one took over can't overwrite the newer attempt.
func (s *Store) recordAttempt(ctx context.Context, c claim, status string, next time.Duration, httpStatus *int, attemptErr *string, disable bool) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE messages SET status = $1, next_at = now() + make_interval(secs => $2), locked_until = NULL
			WHERE id = $3 AND attempts = $4`, status, next.Seconds(), c.ID, c.Attempt)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		_, err = tx.Exec(ctx, "INSERT INTO attempts (message_id, attempt, http_status, error) VALUES ($1, $2, $3, $4)", c.ID, c.Attempt, httpStatus, attemptErr)
		if err != nil {
			return err
		}
		if disable {
			_, err = tx.Exec(ctx, "UPDATE endpoints SET disabled_at = now() WHERE id = $1", c.Endpoint)
		}
		return err
	})
}
