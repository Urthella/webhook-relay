// Package verify checks webhooks signed by webhook-relay, or by anything else that follows Standard
// Webhooks. Use it in the service that receives them.
//
//	func handle(w http.ResponseWriter, r *http.Request) {
//		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
//		if err != nil {
//			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
//			return
//		}
//		if err := verify.Request(secret, r.Header, body); err != nil {
//			http.Error(w, "bad signature", http.StatusUnauthorized)
//			return
//		}
//		// Retries repeat the webhook-id, so deduplicate on it before acting.
//	}
package verify

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Tolerance is how far a timestamp may be from now. It limits how long a captured request can be
// replayed, and leaves room for clock differences.
const Tolerance = 5 * time.Minute

const secretPrefix = "whsec_"

var (
	ErrMissingHeaders = errors.New("verify: missing webhook headers")
	ErrBadTimestamp   = errors.New("verify: timestamp is invalid or outside the tolerance")
	ErrBadSignature   = errors.New("verify: no matching signature")
	ErrBadSecret      = errors.New("verify: the secret isn't valid base64")
)

// Sign computes a signature in the Standard Webhooks format: v1, then a base64 HMAC-SHA256 over
// id.timestamp.body.
func Sign(secret, id string, at time.Time, body []byte) (string, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, secretPrefix))
	if err != nil {
		return "", ErrBadSecret
	}
	return "v1," + base64.StdEncoding.EncodeToString(mac(key, id, strconv.FormatInt(at.Unix(), 10), body)), nil
}

func mac(key []byte, id, timestamp string, body []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(id + "." + timestamp + "."))
	h.Write(body)
	return h.Sum(nil)
}

// Request checks a request's headers and raw body, using the current time.
func Request(secret string, headers http.Header, body []byte) error {
	return RequestAt(secret, headers, body, time.Now())
}

// RequestAt is Request with an explicit time.
func RequestAt(secret string, headers http.Header, body []byte, now time.Time) error {
	id := headers.Get("webhook-id")
	timestamp := headers.Get("webhook-timestamp")
	signatures := headers.Get("webhook-signature")
	if id == "" || timestamp == "" || signatures == "" {
		return ErrMissingHeaders
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, secretPrefix))
	if err != nil {
		return ErrBadSecret
	}
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrBadTimestamp
	}
	if diff := now.Sub(time.Unix(seconds, 0)); diff > Tolerance || diff < -Tolerance {
		return ErrBadTimestamp
	}
	want := mac(key, id, timestamp, body)
	// The header can hold several signatures, separated by spaces, so senders can rotate secrets.
	for _, candidate := range strings.Fields(signatures) {
		version, encoded, found := strings.Cut(candidate, ",")
		if !found || version != "v1" {
			continue
		}
		got, err := base64.StdEncoding.DecodeString(encoded)
		if err == nil && hmac.Equal(got, want) {
			return nil
		}
	}
	return ErrBadSignature
}
