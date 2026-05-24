// Package github implements repo.Provider over the GitHub REST API. No local
// clone. Dispatches by CommitHint: Draft + single file -> Contents API (one
// request); everything else -> Git Data API for atomic multi-file commits.
package github

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/Harsh-2002/Cairn/internal/core"
	"github.com/Harsh-2002/Cairn/internal/repo"
)

const defaultGitHubAPI = "https://api.github.com"
const defaultUserAgent = "cairn"

// Provider is a repo.Provider over the GitHub REST API.
type Provider struct {
	baseURL     string
	token       string
	owner       string
	repo        string
	branch      string
	authorName  string
	authorEmail string
	client      *http.Client
	userAgent   string
}

// New constructs a Provider for the given owner/repo with a token. The token
// is held in memory only and never logged.
func New(owner, repoName, token string) *Provider {
	return &Provider{
		baseURL:     defaultGitHubAPI,
		token:       token,
		owner:       owner,
		repo:        repoName,
		branch:      "main",
		authorName:  "Cairn",
		authorEmail: "cairn@local",
		client:      &http.Client{Timeout: 30 * time.Second},
		userAgent:   defaultUserAgent,
	}
}

// WithBaseURL overrides the API base URL (for staging, GHES, or test mocks).
func (p *Provider) WithBaseURL(url string) *Provider { p.baseURL = url; return p }

// WithBranch sets the branch this provider commits to. Default "main".
func (p *Provider) WithBranch(b string) *Provider { p.branch = b; return p }

// WithAuthor sets the author/committer identity.
func (p *Provider) WithAuthor(name, email string) *Provider {
	p.authorName = name
	p.authorEmail = email
	return p
}

// WithHTTPClient overrides the http.Client (default has a 30s timeout).
func (p *Provider) WithHTTPClient(c *http.Client) *Provider { p.client = c; return p }

func (p *Provider) endpoint(path string) string {
	return strings.TrimRight(p.baseURL, "/") + "/" + strings.TrimLeft(path, "/")
}

func (p *Provider) repoPath(suffix string) string {
	return fmt.Sprintf("repos/%s/%s/%s", p.owner, p.repo, suffix)
}

func (p *Provider) doJSON(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, &repo.InvalidInputError{Msg: err.Error()}
		}
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.endpoint(path), reader)
	if err != nil {
		return nil, &repo.NetworkError{Err: err}
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", p.userAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, &repo.NetworkError{Err: err}
	}
	return resp, nil
}

func drainBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func decodeJSON(resp *http.Response, out any) error {
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

// --- API types --------------------------------------------------------------

type refResponse struct {
	Object struct {
		SHA string `json:"sha"`
	} `json:"object"`
}

type contentsFile struct {
	Content  string `json:"content"`
	SHA      string `json:"sha"`
	Encoding string `json:"encoding"`
}

type createBlobReq struct {
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
}

type createBlobResp struct {
	SHA string `json:"sha"`
}

type treeEntryInput struct {
	Path string  `json:"path"`
	Mode string  `json:"mode"`
	Type string  `json:"type"`
	SHA  *string `json:"sha,omitempty"`
}

type createTreeReq struct {
	BaseTree *string          `json:"base_tree,omitempty"`
	Tree     []treeEntryInput `json:"tree"`
}

type createTreeResp struct {
	SHA string `json:"sha"`
}

type commitAuthor struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type createCommitReq struct {
	Message   string       `json:"message"`
	Tree      string       `json:"tree"`
	Parents   []string     `json:"parents"`
	Author    commitAuthor `json:"author"`
	Committer commitAuthor `json:"committer"`
}

type createCommitResp struct {
	SHA string `json:"sha"`
}

type updateRefReq struct {
	SHA   string `json:"sha"`
	Force bool   `json:"force"`
}

type createRefReq struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

type putContentsReq struct {
	Message   string       `json:"message"`
	Content   string       `json:"content"`
	SHA       *string      `json:"sha,omitempty"`
	Branch    string       `json:"branch"`
	Committer commitAuthor `json:"committer"`
}

type putContentsResp struct {
	Commit struct {
		SHA string `json:"sha"`
	} `json:"commit"`
}

type treeListing struct {
	Tree []struct {
		Path string `json:"path"`
		Type string `json:"type"`
		SHA  string `json:"sha"`
		Size int64  `json:"size"`
	} `json:"tree"`
	Truncated bool `json:"truncated"`
}

type commitDetail struct {
	Tree struct {
		SHA string `json:"sha"`
	} `json:"tree"`
}

// --- Provider implementation ------------------------------------------------

func (p *Provider) getRefSHA(ctx context.Context, branch string) (string, bool, error) {
	resp, err := p.doJSON(ctx, http.MethodGet, p.repoPath("git/refs/heads/"+branch), nil)
	if err != nil {
		return "", false, err
	}
	defer drainBody(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		var body refResponse
		if err := decodeJSON(resp, &body); err != nil {
			return "", false, &repo.BackendError{Err: err}
		}
		return body.Object.SHA, true, nil
	case http.StatusNotFound, http.StatusConflict:
		// 409: GitHub returns this for empty repos.
		return "", false, nil
	case http.StatusUnauthorized, http.StatusForbidden:
		return "", false, repo.ErrUnauthenticated
	default:
		return "", false, &repo.BackendError{Err: fmt.Errorf("GET ref: %s", resp.Status)}
	}
}

// Read implements repo.Provider.
func (p *Provider) Read(ctx context.Context, rp core.RepoPath, at *core.CommitRef) (repo.FileRead, error) {
	ref := p.branch
	if at != nil {
		ref = string(*at)
	}
	endpoint := p.repoPath(fmt.Sprintf("contents/%s?ref=%s", rp.AsStr(), ref))
	resp, err := p.doJSON(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return repo.FileRead{}, err
	}
	defer drainBody(resp)
	switch resp.StatusCode {
	case http.StatusOK:
		var body contentsFile
		if err := decodeJSON(resp, &body); err != nil {
			return repo.FileRead{}, &repo.BackendError{Err: err}
		}
		if body.Encoding != "base64" {
			return repo.FileRead{}, &repo.BackendError{Err: fmt.Errorf("unsupported encoding %q", body.Encoding)}
		}
		cleaned := strings.Map(stripWhitespace, body.Content)
		decoded, err := base64.StdEncoding.DecodeString(cleaned)
		if err != nil {
			return repo.FileRead{}, &repo.BackendError{Err: fmt.Errorf("base64: %w", err)}
		}
		return repo.FileRead{Path: rp, Bytes: decoded, Blob: core.BlobRef(body.SHA)}, nil
	case http.StatusNotFound:
		return repo.FileRead{}, &repo.NotFoundError{Path: rp}
	case http.StatusUnauthorized, http.StatusForbidden:
		return repo.FileRead{}, repo.ErrUnauthenticated
	default:
		return repo.FileRead{}, &repo.BackendError{Err: fmt.Errorf("GET contents: %s", resp.Status)}
	}
}

func stripWhitespace(r rune) rune {
	if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
		return -1
	}
	return r
}

// List implements repo.Provider.
func (p *Provider) List(ctx context.Context, prefix core.RepoPath, at *core.CommitRef) ([]repo.TreeEntry, error) {
	var headSHA string
	if at != nil {
		headSHA = string(*at)
	} else {
		sha, ok, err := p.getRefSHA(ctx, p.branch)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		headSHA = sha
	}
	endpoint := p.repoPath(fmt.Sprintf("git/trees/%s?recursive=1", headSHA))
	resp, err := p.doJSON(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer drainBody(resp)
	if resp.StatusCode != http.StatusOK {
		return nil, &repo.BackendError{Err: fmt.Errorf("GET tree: %s", resp.Status)}
	}
	var listing treeListing
	if err := decodeJSON(resp, &listing); err != nil {
		return nil, &repo.BackendError{Err: err}
	}
	prefixStr := prefix.AsStr()
	var out []repo.TreeEntry
	for _, e := range listing.Tree {
		if e.Type != "blob" || !strings.HasPrefix(e.Path, prefixStr) {
			continue
		}
		rp, err := core.NewRepoPath(e.Path)
		if err != nil {
			continue
		}
		out = append(out, repo.TreeEntry{Path: rp, Blob: core.BlobRef(e.SHA), Size: e.Size})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path.AsStr() < out[j].Path.AsStr() })
	return out, nil
}

// Commit implements repo.Provider with CommitHint dispatch.
func (p *Provider) Commit(ctx context.Context, changes core.FileChangeSet, message string, hint core.CommitHint, expectedHead *core.CommitRef) (core.CommitRef, error) {
	if hint == core.CommitHintDraft && changes.Len() == 1 {
		return p.commitViaContentsAPI(ctx, changes, message)
	}
	return p.commitViaGitDataAPI(ctx, changes, message, expectedHead)
}

func (p *Provider) commitViaContentsAPI(ctx context.Context, changes core.FileChangeSet, message string) (core.CommitRef, error) {
	change := changes.Changes[0]
	if change.Op != core.FileOpWrite {
		return "", &repo.InvalidInputError{Msg: "Contents API draft path does not support Delete"}
	}
	// Look up existing blob SHA so the PUT updates rather than fails.
	readPath := p.repoPath(fmt.Sprintf("contents/%s?ref=%s", change.Path.AsStr(), p.branch))
	resp, err := p.doJSON(ctx, http.MethodGet, readPath, nil)
	if err != nil {
		return "", err
	}
	var existingSHA *string
	if resp.StatusCode == http.StatusOK {
		var file contentsFile
		if err := decodeJSON(resp, &file); err == nil {
			s := file.SHA
			existingSHA = &s
		}
	} else {
		drainBody(resp)
	}

	body := putContentsReq{
		Message:   message,
		Content:   base64.StdEncoding.EncodeToString(change.Bytes),
		SHA:       existingSHA,
		Branch:    p.branch,
		Committer: commitAuthor{Name: p.authorName, Email: p.authorEmail},
	}
	putPath := p.repoPath("contents/" + change.Path.AsStr())
	resp, err = p.doJSON(ctx, http.MethodPut, putPath, body)
	if err != nil {
		return "", err
	}
	defer drainBody(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &repo.BackendError{Err: fmt.Errorf("PUT contents: %s", resp.Status)}
	}
	var out putContentsResp
	if err := decodeJSON(resp, &out); err != nil {
		return "", &repo.BackendError{Err: err}
	}
	return core.CommitRef(out.Commit.SHA), nil
}

func (p *Provider) commitViaGitDataAPI(ctx context.Context, changes core.FileChangeSet, message string, expectedHead *core.CommitRef) (core.CommitRef, error) {
	parentSHA, hasParent, err := p.getRefSHA(ctx, p.branch)
	if err != nil {
		return "", err
	}
	if expectedHead != nil {
		actual := ""
		if hasParent {
			actual = parentSHA
		}
		if actual != string(*expectedHead) {
			return "", repo.ErrConflict
		}
	}
	treeInput, err := p.buildTreeInput(ctx, changes)
	if err != nil {
		return "", err
	}
	baseTree, err := p.fetchBaseTree(ctx, parentSHA, hasParent)
	if err != nil {
		return "", err
	}
	newTree, err := p.createTree(ctx, treeInput, baseTree)
	if err != nil {
		return "", err
	}
	parents := []string{}
	if hasParent {
		parents = append(parents, parentSHA)
	}
	commitSHA, err := p.createCommit(ctx, newTree, message, parents)
	if err != nil {
		return "", err
	}

	// Fast-forward update (force=false) preserves optimistic concurrency.
	if err := p.updateRefStrict(ctx, p.branch, commitSHA); err != nil {
		switch {
		case errors.Is(err, errRefNotFound):
			if cerr := p.createRef(ctx, p.branch, commitSHA); cerr != nil {
				return "", cerr
			}
		case errors.Is(err, repo.ErrConflict):
			return "", repo.ErrConflict
		default:
			return "", err
		}
	}
	return core.CommitRef(commitSHA), nil
}

// ForceSetRef implements repo.Provider.
func (p *Provider) ForceSetRef(ctx context.Context, branch string, tree core.TreeRef, message string) (core.CommitRef, error) {
	commitSHA, err := p.createCommit(ctx, string(tree), message, nil)
	if err != nil {
		return "", err
	}
	if err := p.updateOrCreateRef(ctx, branch, commitSHA); err != nil {
		return "", err
	}
	return core.CommitRef(commitSHA), nil
}

// ForceCommitToBranch implements repo.Provider.
func (p *Provider) ForceCommitToBranch(ctx context.Context, branch string, changes core.FileChangeSet, message string) (core.CommitRef, error) {
	parentSHA, hasParent, err := p.getRefSHA(ctx, branch)
	if err != nil {
		return "", err
	}
	treeInput, err := p.buildTreeInput(ctx, changes)
	if err != nil {
		return "", err
	}
	baseTree, err := p.fetchBaseTree(ctx, parentSHA, hasParent)
	if err != nil {
		return "", err
	}
	newTree, err := p.createTree(ctx, treeInput, baseTree)
	if err != nil {
		return "", err
	}
	parents := []string{}
	if hasParent {
		parents = append(parents, parentSHA)
	}
	commitSHA, err := p.createCommit(ctx, newTree, message, parents)
	if err != nil {
		return "", err
	}
	if err := p.updateOrCreateRef(ctx, branch, commitSHA); err != nil {
		return "", err
	}
	return core.CommitRef(commitSHA), nil
}

// DeleteBranch implements repo.Provider.
func (p *Provider) DeleteBranch(ctx context.Context, branch string) error {
	resp, err := p.doJSON(ctx, http.MethodDelete, p.repoPath("git/refs/heads/"+branch), nil)
	if err != nil {
		return err
	}
	defer drainBody(resp)
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusNotFound, http.StatusUnprocessableEntity:
		return nil
	default:
		return &repo.BackendError{Err: fmt.Errorf("delete ref: %s", resp.Status)}
	}
}

// ResolveRef implements repo.Provider.
func (p *Provider) ResolveRef(ctx context.Context, name string) (*core.CommitRef, error) {
	bare := strings.TrimPrefix(name, "refs/heads/")
	sha, ok, err := p.getRefSHA(ctx, bare)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	cr := core.CommitRef(sha)
	return &cr, nil
}

// --- internal helpers -------------------------------------------------------

var errRefNotFound = errors.New("ref not found")

func (p *Provider) buildTreeInput(ctx context.Context, changes core.FileChangeSet) ([]treeEntryInput, error) {
	out := make([]treeEntryInput, 0, len(changes.Changes))
	for _, ch := range changes.Changes {
		switch ch.Op {
		case core.FileOpWrite:
			sha, err := p.createBlob(ctx, ch.Bytes)
			if err != nil {
				return nil, err
			}
			s := sha
			out = append(out, treeEntryInput{Path: ch.Path.AsStr(), Mode: "100644", Type: "blob", SHA: &s})
		case core.FileOpDelete:
			out = append(out, treeEntryInput{Path: ch.Path.AsStr(), Mode: "100644", Type: "blob"})
		}
	}
	return out, nil
}

func (p *Provider) fetchBaseTree(ctx context.Context, parentSHA string, hasParent bool) (*string, error) {
	if !hasParent {
		return nil, nil
	}
	resp, err := p.doJSON(ctx, http.MethodGet, p.repoPath("git/commits/"+parentSHA), nil)
	if err != nil {
		return nil, err
	}
	defer drainBody(resp)
	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}
	var detail commitDetail
	if err := decodeJSON(resp, &detail); err != nil {
		return nil, &repo.BackendError{Err: err}
	}
	s := detail.Tree.SHA
	return &s, nil
}

func (p *Provider) createTree(ctx context.Context, entries []treeEntryInput, baseTree *string) (string, error) {
	resp, err := p.doJSON(ctx, http.MethodPost, p.repoPath("git/trees"), createTreeReq{BaseTree: baseTree, Tree: entries})
	if err != nil {
		return "", err
	}
	defer drainBody(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &repo.BackendError{Err: fmt.Errorf("create tree: %s", resp.Status)}
	}
	var out createTreeResp
	if err := decodeJSON(resp, &out); err != nil {
		return "", &repo.BackendError{Err: err}
	}
	return out.SHA, nil
}

func (p *Provider) createCommit(ctx context.Context, treeSHA, message string, parents []string) (string, error) {
	if parents == nil {
		parents = []string{}
	}
	body := createCommitReq{
		Message:   message,
		Tree:      treeSHA,
		Parents:   parents,
		Author:    commitAuthor{Name: p.authorName, Email: p.authorEmail},
		Committer: commitAuthor{Name: p.authorName, Email: p.authorEmail},
	}
	resp, err := p.doJSON(ctx, http.MethodPost, p.repoPath("git/commits"), body)
	if err != nil {
		return "", err
	}
	defer drainBody(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &repo.BackendError{Err: fmt.Errorf("create commit: %s", resp.Status)}
	}
	var out createCommitResp
	if err := decodeJSON(resp, &out); err != nil {
		return "", &repo.BackendError{Err: err}
	}
	return out.SHA, nil
}

func (p *Provider) createBlob(ctx context.Context, content []byte) (string, error) {
	body := createBlobReq{Content: base64.StdEncoding.EncodeToString(content), Encoding: "base64"}
	resp, err := p.doJSON(ctx, http.MethodPost, p.repoPath("git/blobs"), body)
	if err != nil {
		return "", err
	}
	defer drainBody(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &repo.BackendError{Err: fmt.Errorf("create blob: %s", resp.Status)}
	}
	var out createBlobResp
	if err := decodeJSON(resp, &out); err != nil {
		return "", &repo.BackendError{Err: err}
	}
	return out.SHA, nil
}

// updateRefStrict tries a fast-forward (force=false). Returns errRefNotFound
// when the ref doesn't exist, or repo.ErrConflict when the update would not
// be a fast-forward.
func (p *Provider) updateRefStrict(ctx context.Context, branch, sha string) error {
	resp, err := p.doJSON(ctx, http.MethodPatch, p.repoPath("git/refs/heads/"+branch), updateRefReq{SHA: sha, Force: false})
	if err != nil {
		return err
	}
	defer drainBody(resp)
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusNotFound:
		return errRefNotFound
	case resp.StatusCode == http.StatusUnprocessableEntity:
		// 422 = ref missing OR not fast-forward. Probe to distinguish.
		drainBody(resp)
		_, ok, err := p.getRefSHA(ctx, branch)
		if err != nil {
			return err
		}
		if !ok {
			return errRefNotFound
		}
		return repo.ErrConflict
	default:
		return &repo.BackendError{Err: fmt.Errorf("update ref: %s", resp.Status)}
	}
}

func (p *Provider) createRef(ctx context.Context, branch, sha string) error {
	resp, err := p.doJSON(ctx, http.MethodPost, p.repoPath("git/refs"), createRefReq{Ref: "refs/heads/" + branch, SHA: sha})
	if err != nil {
		return err
	}
	defer drainBody(resp)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &repo.BackendError{Err: fmt.Errorf("create ref: %s", resp.Status)}
	}
	return nil
}

func (p *Provider) updateOrCreateRef(ctx context.Context, branch, sha string) error {
	// Force update first (idempotent move).
	resp, err := p.doJSON(ctx, http.MethodPatch, p.repoPath("git/refs/heads/"+branch), updateRefReq{SHA: sha, Force: true})
	if err != nil {
		return err
	}
	defer drainBody(resp)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return p.createRef(ctx, branch, sha)
}
