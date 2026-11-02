CREATE TABLE IF NOT EXISTS endpoints (
    id          text PRIMARY KEY,
    url         text NOT NULL,
    secret      text NOT NULL,
    disabled_at timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS messages (
    id              text PRIMARY KEY,
    endpoint_id     text NOT NULL REFERENCES endpoints (id),
    idempotency_key text,
    body            text NOT NULL,
    status          text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivered', 'dead')),
    attempts        int NOT NULL DEFAULT 0,
    next_at         timestamptz NOT NULL DEFAULT now(),
    locked_until    timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (endpoint_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS messages_due ON messages (next_at) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS attempts (
    message_id  text NOT NULL REFERENCES messages (id),
    attempt     int NOT NULL,
    at          timestamptz NOT NULL DEFAULT now(),
    http_status int,
    error       text,
    PRIMARY KEY (message_id, attempt)
);
