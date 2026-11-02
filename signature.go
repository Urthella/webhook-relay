package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"
)

const secretPrefix = "whsec_"

// newSecret makes a signing secret in the Standard Webhooks format.
func newSecret() string {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(err) // crypto/rand doesn't fail on supported platforms
	}
	return secretPrefix + base64.StdEncoding.EncodeToString(key)
}

func newID(prefix string) string {
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(raw)
}

// sign computes the signature for one attempt, per https://www.standardwebhooks.com. The timestamp is
// the attempt's, so a retry carries a new signature over the same message ID.
func sign(secret, id string, at time.Time, body []byte) (string, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, secretPrefix))
	if err != nil {
		return "", errors.New("the secret isn't valid base64")
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + strconv.FormatInt(at.Unix(), 10) + "."))
	mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}
