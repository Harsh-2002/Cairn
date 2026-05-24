package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Harsh-2002/Cairn/internal/core"
	"github.com/Harsh-2002/Cairn/internal/repo"
	"github.com/Harsh-2002/Cairn/internal/server/signer"
)

// noopRepo is a minimal repo.Provider for handler tests.
type noopRepo struct{}

func (noopRepo) Read(context.Context, core.RepoPath, *core.CommitRef) (repo.FileRead, error) {
	return repo.FileRead{}, &repo.NotFoundError{}
}
func (noopRepo) List(context.Context, core.RepoPath, *core.CommitRef) ([]repo.TreeEntry, error) {
	return nil, nil
}
func (noopRepo) Commit(context.Context, core.FileChangeSet, string, core.CommitHint, *core.CommitRef) (core.CommitRef, error) {
	return "deadbeef", nil
}
func (noopRepo) ForceSetRef(context.Context, string, core.TreeRef, string) (core.CommitRef, error) {
	return "deadbeef", nil
}
func (noopRepo) ForceCommitToBranch(context.Context, string, core.FileChangeSet, string) (core.CommitRef, error) {
	return "deadbeef", nil
}
func (noopRepo) DeleteBranch(context.Context, string) error { return nil }
func (noopRepo) ResolveRef(context.Context, string) (*core.CommitRef, error) {
	return nil, nil
}

func newTestState(secret string) *State {
	return &State{
		AdminSecret:   secret,
		Signer:        signer.NewMockSigner("https://bucket.example.com"),
		Repo:          noopRepo{},
		PresignExpiry: 300 * time.Second,
		WebhookSecret: "hooksecret",
	}
}

func TestHealthzNoAdminSecret(t *testing.T) {
	srv := httptest.NewServer(Router(newTestState("topsecret")))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}
}

func TestPresignWithoutSecretIs401(t *testing.T) {
	srv := httptest.NewServer(Router(newTestState("topsecret")))
	defer srv.Close()
	body := strings.NewReader(`{"sha256":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","ext":"png"}`)
	resp, err := http.Post(srv.URL+"/api/assets/presign", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", resp.StatusCode)
	}
}

func TestPresignWithSecretReturnsURL(t *testing.T) {
	srv := httptest.NewServer(Router(newTestState("topsecret")))
	defer srv.Close()
	req, _ := http.NewRequest("POST", srv.URL+"/api/assets/presign",
		strings.NewReader(`{"sha256":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","ext":"png"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Secret", "topsecret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}
	var out presignResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.URL, "/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789/original.png") {
		t.Errorf("url = %q", out.URL)
	}
	if out.ExpiresInSeconds != 300 {
		t.Errorf("expires = %d", out.ExpiresInSeconds)
	}
}

func TestPresignRejectsBadSHA(t *testing.T) {
	srv := httptest.NewServer(Router(newTestState("topsecret")))
	defer srv.Close()
	req, _ := http.NewRequest("POST", srv.URL+"/api/assets/presign", strings.NewReader(`{"sha256":"short","ext":"png"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Secret", "topsecret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d; want 400", resp.StatusCode)
	}
}

func TestPresignWrongSecretIs401(t *testing.T) {
	srv := httptest.NewServer(Router(newTestState("right")))
	defer srv.Close()
	req, _ := http.NewRequest("POST", srv.URL+"/api/assets/presign",
		strings.NewReader(`{"sha256":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","ext":"png"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Secret", "wrong")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", resp.StatusCode)
	}
}

func TestPresignVariantIncluded(t *testing.T) {
	srv := httptest.NewServer(Router(newTestState("topsecret")))
	defer srv.Close()
	req, _ := http.NewRequest("POST", srv.URL+"/api/assets/presign",
		strings.NewReader(`{"sha256":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","ext":"png","variant":"1200w"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Secret", "topsecret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out presignResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if !strings.Contains(out.URL, "/1200w.png") {
		t.Errorf("url missing variant: %q", out.URL)
	}
}

func TestWebhookHMACAccepts(t *testing.T) {
	srv := httptest.NewServer(Router(newTestState("topsecret")))
	defer srv.Close()
	body := []byte(`{"ok":1}`)
	mac := hmacHex("hooksecret", body)
	req, _ := http.NewRequest("POST", srv.URL+"/api/webhook/notion", bytes.NewReader(body))
	req.Header.Set("X-Cairn-Signature", "sha256="+mac)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}
}

func TestWebhookHMACRejectsBadSig(t *testing.T) {
	srv := httptest.NewServer(Router(newTestState("topsecret")))
	defer srv.Close()
	body := []byte(`{"ok":1}`)
	req, _ := http.NewRequest("POST", srv.URL+"/api/webhook/notion", bytes.NewReader(body))
	req.Header.Set("X-Cairn-Signature", "sha256="+hex.EncodeToString([]byte("nope")))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", resp.StatusCode)
	}
}

func hmacHex(secret string, body []byte) string {
	return hexHMAC([]byte(secret), body)
}

// hexHMAC duplicates the verifyHMAC computation for test signatures.
func hexHMAC(key, body []byte) string {
	if verifyHMAC(body, "", string(key)) {
		// unreachable; calling for side-effect-free imports
	}
	return computeHMAC(key, body)
}
