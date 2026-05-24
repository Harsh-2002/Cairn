// Package server is the Cairn admin HTTP server. It hosts:
//   - the embedded Svelte admin SPA (or a Vite dev proxy)
//   - a small /api/* surface (presign, posts CRUD, webhook receivers)
//
// The GitHub token (when configured) is held in process memory and never
// returned to the browser. All write paths go through this server so an
// XSS on the SPA cannot exfiltrate write access to the repository.
package server

import (
	"crypto/hmac"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Harsh-2002/Cairn/internal/repo"
	"github.com/Harsh-2002/Cairn/internal/server/signer"
)

// State is shared by all handlers.
type State struct {
	AdminSecret   string
	Signer        signer.URLSigner
	Repo          repo.Provider
	PresignExpiry time.Duration
	WebhookSecret string // empty disables webhook routes
	ViteProxy     string // empty disables proxy
}

// Router builds the http.Handler with every route mounted.
func Router(s *State) http.Handler {
	r := chi.NewRouter()

	// Admin-secret-gated API.
	r.Route("/api", func(api chi.Router) {
		api.Get("/healthz", handleHealthz)
		api.Group(func(g chi.Router) {
			g.Use(requireAdminSecret(s.AdminSecret))
			g.Post("/assets/presign", handlePresign(s))
			g.Get("/posts", handleListPosts(s))
			g.Post("/posts", handleCreatePost(s))
			g.Route("/posts/{slug}", func(p chi.Router) {
				p.Get("/", handleReadPost(s))
				p.Delete("/", handleDeletePost(s))
				p.Put("/autosave", handleAutosave(s))
				p.Post("/publish", handlePublish(s))
			})
		})
		// Webhook routes use HMAC, not the admin secret.
		api.Post("/webhook/notion", handleNotionWebhook(s))
	})

	// Admin SPA fallback (anything that didn't match /api).
	r.NotFound(adminHandler(s))
	return r
}

// requireAdminSecret returns middleware that gates the next handler on
// constant-time comparison of the X-Admin-Secret header.
func requireAdminSecret(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := r.Header.Get("X-Admin-Secret")
			if !hmac.Equal([]byte(provided), []byte(secret)) {
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// presignRequest is the JSON body of POST /api/assets/presign.
type presignRequest struct {
	SHA256  string `json:"sha256"`
	Ext     string `json:"ext"`
	Variant string `json:"variant,omitempty"`
}

type presignResponse struct {
	URL              string `json:"url"`
	Key              string `json:"key"`
	ExpiresInSeconds int64  `json:"expires_in_seconds"`
}

func handlePresign(s *State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req presignRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
		if !isValidSHA256(req.SHA256) {
			writeError(w, http.StatusBadRequest, "sha256 must be 64 lowercase hex characters")
			return
		}
		if !isValidExt(req.Ext) {
			writeError(w, http.StatusBadRequest, "ext must be 1–8 lowercase alphanumeric characters")
			return
		}
		key := req.SHA256 + "/original." + req.Ext
		if req.Variant != "" {
			key = req.SHA256 + "/" + req.Variant + "." + req.Ext
		}
		url, err := s.Signer.SignPut(r.Context(), key, s.PresignExpiry)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "sign: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, presignResponse{
			URL:              url,
			Key:              key,
			ExpiresInSeconds: int64(s.PresignExpiry.Seconds()),
		})
	}
}

func isValidSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

func isValidExt(s string) bool {
	if len(s) == 0 || len(s) > 8 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'z':
		default:
			return false
		}
	}
	return true
}

// --- error/JSON helpers -----------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(msg))
}
