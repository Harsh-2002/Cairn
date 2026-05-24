package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
)

// handleNotionWebhook is an HMAC-SHA256-authenticated POST endpoint for
// Notion-driven syncs. The receiver decodes the signature header and runs a
// constant-time comparison.
func handleNotionWebhook(s *State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.WebhookSecret == "" {
			writeError(w, http.StatusServiceUnavailable, "webhooks not configured")
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "read body: "+err.Error())
			return
		}
		sig := strings.TrimPrefix(r.Header.Get("X-Cairn-Signature"), "sha256=")
		if !verifyHMAC(body, sig, s.WebhookSecret) {
			writeError(w, http.StatusUnauthorized, "bad signature")
			return
		}
		// TODO: parse payload and dispatch a sync job. For now, accept the
		// payload (matches the Rust stub).
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	}
}

// verifyHMAC checks the SHA-256 HMAC of body against the given hex signature.
// Constant-time compare via hmac.Equal.
func verifyHMAC(body []byte, signature, secret string) bool {
	expected := computeHMAC([]byte(secret), body)
	provided, err := hex.DecodeString(signature)
	if err != nil {
		return false
	}
	want, err := hex.DecodeString(expected)
	if err != nil {
		return false
	}
	return hmac.Equal(want, provided)
}

// computeHMAC returns the hex-encoded SHA-256 HMAC of body keyed by key.
func computeHMAC(key, body []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
