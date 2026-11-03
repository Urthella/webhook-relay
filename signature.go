package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
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
