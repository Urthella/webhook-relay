package verify

import (
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"
)

const (
	secret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	id     = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	stamp  = 1614265330
	body   = `{"test": 2432232314}`
	sig    = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

var at = time.Unix(stamp, 0)

func headers(signature string) http.Header {
	h := http.Header{}
	h.Set("webhook-id", id)
	h.Set("webhook-timestamp", strconv.Itoa(stamp))
	h.Set("webhook-signature", signature)
	return h
}

func TestAcceptsTheStandardWebhooksExample(t *testing.T) {
	if err := RequestAt(secret, headers(sig), []byte(body), at); err != nil {
		t.Fatal(err)
	}
	got, err := Sign(secret, id, at, []byte(body))
	if err != nil || got != sig {
		t.Fatalf("Sign gave %q, %v", got, err)
	}
}

func TestAcceptsASecretWithoutThePrefix(t *testing.T) {
	if err := RequestAt(secret[len(secretPrefix):], headers(sig), []byte(body), at); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptsAnyOfSeveralSignatures(t *testing.T) {
	h := headers("v1,AAAA v2,ignored " + sig)
	if err := RequestAt(secret, h, []byte(body), at); err != nil {
		t.Fatal(err)
	}
}

func TestRefuses(t *testing.T) {
	tampered := func(mutate func(http.Header)) http.Header {
		h := headers(sig)
		mutate(h)
		return h
	}
	cases := []struct {
		name    string
		secret  string
		headers http.Header
		body    string
		now     time.Time
		want    error
	}{
		{"a changed body", secret, headers(sig), `{"test": 1}`, at, ErrBadSignature},
		{"a changed id", secret, tampered(func(h http.Header) { h.Set("webhook-id", "msg_other") }), body, at, ErrBadSignature},
		{"a changed timestamp", secret, tampered(func(h http.Header) { h.Set("webhook-timestamp", strconv.Itoa(stamp+1)) }), body, at, ErrBadSignature},
		{"the wrong secret", "whsec_" + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", headers(sig), body, at, ErrBadSignature},
		{"a v2 signature with a valid v1 value", secret, headers("v2,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="), body, at, ErrBadSignature},
		{"a signature that isn't base64", secret, headers("v1,!!!"), body, at, ErrBadSignature},
		{"an empty signature value", secret, headers("v1,"), body, at, ErrBadSignature},
		{"a replay after the tolerance", secret, headers(sig), body, at.Add(Tolerance + time.Second), ErrBadTimestamp},
		{"a timestamp too far ahead", secret, headers(sig), body, at.Add(-Tolerance - time.Second), ErrBadTimestamp},
		{"a timestamp that isn't a number", secret, tampered(func(h http.Header) { h.Set("webhook-timestamp", "soon") }), body, at, ErrBadTimestamp},
		{"no id", secret, tampered(func(h http.Header) { h.Del("webhook-id") }), body, at, ErrMissingHeaders},
		{"no timestamp", secret, tampered(func(h http.Header) { h.Del("webhook-timestamp") }), body, at, ErrMissingHeaders},
		{"no signature", secret, tampered(func(h http.Header) { h.Del("webhook-signature") }), body, at, ErrMissingHeaders},
		{"a secret that isn't base64", "whsec_!!!", headers(sig), body, at, ErrBadSecret},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := RequestAt(c.secret, c.headers, []byte(c.body), c.now); !errors.Is(err, c.want) {
				t.Fatalf("got %v, want %v", err, c.want)
			}
		})
	}
}

func TestToleranceEdgesAreAccepted(t *testing.T) {
	for _, now := range []time.Time{at.Add(Tolerance), at.Add(-Tolerance)} {
		if err := RequestAt(secret, headers(sig), []byte(body), now); err != nil {
			t.Errorf("at %v: %v", now.Sub(at), err)
		}
	}
}

func TestRequestUsesTheCurrentTime(t *testing.T) {
	if err := Request(secret, headers(sig), []byte(body)); !errors.Is(err, ErrBadTimestamp) {
		t.Fatalf("a request from 2021 should be too old, got %v", err)
	}
}
