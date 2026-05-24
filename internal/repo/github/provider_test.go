package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Harsh-2002/Cairn/internal/core"
	"github.com/Harsh-2002/Cairn/internal/repo"
)

// mockRoute matches a request by method + path prefix.
type mockRoute struct {
	method, path string
	status       int
	body         any
}

// mockServer dispatches a request to the first matching route.
func mockServer(t *testing.T, routes ...mockRoute) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	// Group routes by path so we can dispatch by method inside.
	byPath := map[string]map[string]mockRoute{}
	for _, r := range routes {
		if byPath[r.path] == nil {
			byPath[r.path] = map[string]mockRoute{}
		}
		byPath[r.path][r.method] = r
	}
	for p, methods := range byPath {
		methods := methods
		mux.HandleFunc(p, func(w http.ResponseWriter, req *http.Request) {
			r, ok := methods[req.Method]
			if !ok {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(r.status)
			if r.body != nil {
				_ = json.NewEncoder(w).Encode(r.body)
			}
		})
	}
	return httptest.NewServer(mux)
}

func newTestProvider(server *httptest.Server) *Provider {
	return New("owner", "repo", "test-token").
		WithBaseURL(server.URL).
		WithBranch("main").
		WithAuthor("Test", "test@local")
}

func rp(t *testing.T, s string) core.RepoPath {
	t.Helper()
	r, err := core.NewRepoPath(s)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestReadViaContentsAPIDecodesBase64(t *testing.T) {
	srv := mockServer(t, mockRoute{
		method: "GET",
		path:   "/repos/owner/repo/contents/posts/hello.md",
		status: 200,
		body:   map[string]any{"content": "aGVsbG8=", "sha": "abc123", "encoding": "base64"},
	})
	defer srv.Close()
	p := newTestProvider(srv)
	r, err := p.Read(context.Background(), rp(t, "posts/hello.md"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.Bytes, []byte("hello")) {
		t.Errorf("bytes = %q; want hello", r.Bytes)
	}
	if string(r.Blob) != "abc123" {
		t.Errorf("blob = %q", r.Blob)
	}
}

func TestRead404MapsToNotFound(t *testing.T) {
	srv := mockServer(t, mockRoute{method: "GET", path: "/repos/owner/repo/contents/nope.md", status: 404})
	defer srv.Close()
	p := newTestProvider(srv)
	_, err := p.Read(context.Background(), rp(t, "nope.md"), nil)
	var nfe *repo.NotFoundError
	if !errors.As(err, &nfe) {
		t.Errorf("err = %v; want *NotFoundError", err)
	}
}

func TestResolveRefMissingReturnsNil(t *testing.T) {
	srv := mockServer(t, mockRoute{method: "GET", path: "/repos/owner/repo/git/refs/heads/main", status: 404})
	defer srv.Close()
	p := newTestProvider(srv)
	r, err := p.ResolveRef(context.Background(), "main")
	if err != nil {
		t.Fatal(err)
	}
	if r != nil {
		t.Errorf("expected nil, got %v", *r)
	}
}

func TestResolveRefReturnsSHA(t *testing.T) {
	srv := mockServer(t, mockRoute{
		method: "GET", path: "/repos/owner/repo/git/refs/heads/main", status: 200,
		body: map[string]any{"object": map[string]any{"sha": "deadbeef"}},
	})
	defer srv.Close()
	p := newTestProvider(srv)
	r, err := p.ResolveRef(context.Background(), "main")
	if err != nil {
		t.Fatal(err)
	}
	if r == nil || string(*r) != "deadbeef" {
		t.Errorf("got %v", r)
	}
}

func TestSingleFileDraftUsesContentsAPI(t *testing.T) {
	srv := mockServer(t,
		mockRoute{method: "GET", path: "/repos/owner/repo/contents/draft.md", status: 404},
		mockRoute{
			method: "PUT", path: "/repos/owner/repo/contents/draft.md", status: 201,
			body: map[string]any{"commit": map[string]any{"sha": "newcommit123"}},
		},
	)
	defer srv.Close()
	p := newTestProvider(srv)
	var cs core.FileChangeSet
	cs.Push(core.Write(rp(t, "draft.md"), []byte("draft body")))
	r, err := p.Commit(context.Background(), cs, "autosave", core.CommitHintDraft, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(r) != "newcommit123" {
		t.Errorf("commit = %q", r)
	}
}

func TestMultiFilePublishUsesGitDataAPI(t *testing.T) {
	srv := mockServer(t,
		mockRoute{method: "GET", path: "/repos/owner/repo/git/refs/heads/main", status: 404},
		mockRoute{
			method: "POST", path: "/repos/owner/repo/git/blobs", status: 201,
			body: map[string]any{"sha": "blobsha"},
		},
		mockRoute{
			method: "POST", path: "/repos/owner/repo/git/trees", status: 201,
			body: map[string]any{"sha": "treesha"},
		},
		mockRoute{
			method: "POST", path: "/repos/owner/repo/git/commits", status: 201,
			body: map[string]any{"sha": "commitsha"},
		},
		mockRoute{method: "PATCH", path: "/repos/owner/repo/git/refs/heads/main", status: 404},
		mockRoute{method: "POST", path: "/repos/owner/repo/git/refs", status: 201, body: map[string]any{}},
	)
	defer srv.Close()
	p := newTestProvider(srv)
	var cs core.FileChangeSet
	cs.Push(core.Write(rp(t, "a.md"), []byte("1")))
	cs.Push(core.Write(rp(t, "b.md"), []byte("2")))
	r, err := p.Commit(context.Background(), cs, "publish two files", core.CommitHintPublish, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(r) != "commitsha" {
		t.Errorf("commit = %q", r)
	}
}

func TestPublishWithStaleExpectedHeadReturnsConflict(t *testing.T) {
	srv := mockServer(t, mockRoute{
		method: "GET", path: "/repos/owner/repo/git/refs/heads/main", status: 200,
		body: map[string]any{"object": map[string]any{"sha": "currentsha"}},
	})
	defer srv.Close()
	p := newTestProvider(srv)
	var cs core.FileChangeSet
	cs.Push(core.Write(rp(t, "a.md"), []byte("1")))
	stale := core.CommitRef("stalesha")
	_, err := p.Commit(context.Background(), cs, "publish", core.CommitHintPublish, &stale)
	if !errors.Is(err, repo.ErrConflict) {
		t.Errorf("err = %v; want ErrConflict", err)
	}
}

func TestListFiltersByPrefixAndBlobKind(t *testing.T) {
	srv := mockServer(t,
		mockRoute{
			method: "GET", path: "/repos/owner/repo/git/refs/heads/main", status: 200,
			body: map[string]any{"object": map[string]any{"sha": "headsha"}},
		},
		mockRoute{
			method: "GET", path: "/repos/owner/repo/git/trees/headsha", status: 200,
			body: map[string]any{
				"tree": []map[string]any{
					{"path": "posts/a.md", "type": "blob", "sha": "s1", "size": 5},
					{"path": "posts/b.md", "type": "blob", "sha": "s2", "size": 5},
					{"path": "posts", "type": "tree", "sha": "s3"},
					{"path": "other/c.md", "type": "blob", "sha": "s4", "size": 3},
				},
			},
		},
	)
	defer srv.Close()
	p := newTestProvider(srv)
	entries, err := p.List(context.Background(), rp(t, "posts"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("len = %d; want 2", len(entries))
	}
	if entries[0].Path.AsStr() != "posts/a.md" || entries[1].Path.AsStr() != "posts/b.md" {
		t.Errorf("got %v %v", entries[0].Path, entries[1].Path)
	}
}

// sanity: User-Agent header is sent on requests
func TestUserAgentHeaderIsSent(t *testing.T) {
	got := ""
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/owner/repo/git/refs/heads/main", func(w http.ResponseWriter, req *http.Request) {
		got = req.Header.Get("User-Agent")
		w.WriteHeader(404)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	p := newTestProvider(srv)
	if _, err := p.ResolveRef(context.Background(), "main"); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "cairn") {
		t.Errorf("user-agent = %q", got)
	}
}
