package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Harsh-2002/Cairn/internal/core"
	"github.com/Harsh-2002/Cairn/internal/frontmatter"
	"github.com/Harsh-2002/Cairn/internal/repo"
)

const postsDir = "content/posts"

// postSummary is the JSON shape returned by the list endpoint.
type postSummary struct {
	Slug  string `json:"slug"`
	Title string `json:"title"`
	Date  string `json:"date"`
	Draft bool   `json:"draft"`
	Path  string `json:"path"`
}

func handleListPosts(s *State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		prefix, _ := core.NewRepoPath(postsDir)
		entries, err := s.Repo.List(ctx, prefix, nil)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		out := make([]postSummary, 0, len(entries))
		for _, e := range entries {
			if !strings.HasSuffix(e.Path.AsStr(), ".md") {
				continue
			}
			read, err := s.Repo.Read(ctx, e.Path, nil)
			if err != nil {
				continue
			}
			summary, err := summarise(read.Path.AsStr(), read.Bytes)
			if err != nil {
				continue
			}
			out = append(out, summary)
		}
		writeJSON(w, http.StatusOK, out)
	}
}

func handleReadPost(s *State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := chi.URLParam(r, "slug")
		if slug == "" {
			writeError(w, http.StatusBadRequest, "slug required")
			return
		}
		path, err := core.NewRepoPath(postsDir + "/" + slug + ".md")
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		read, err := s.Repo.Read(r.Context(), path, nil)
		if err != nil {
			var nfe *repo.NotFoundError
			if errors.As(err, &nfe) {
				writeError(w, http.StatusNotFound, "post not found")
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"slug": slug,
			"path": path.AsStr(),
			"body": string(read.Bytes),
			"blob": string(read.Blob),
		})
	}
}

type createReq struct {
	Slug string `json:"slug"`
	Body string `json:"body"`
}

func handleCreatePost(s *State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if req.Slug == "" {
			writeError(w, http.StatusBadRequest, "slug required")
			return
		}
		path, err := core.NewRepoPath(postsDir + "/" + req.Slug + ".md")
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		var cs core.FileChangeSet
		cs.Push(core.Write(path, []byte(req.Body)))
		sha, err := s.Repo.Commit(r.Context(), cs, "create "+req.Slug, core.CommitHintPublish, nil)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"slug": req.Slug, "commit": string(sha)})
	}
}

type autosaveReq struct {
	Session string `json:"session"`
	Body    string `json:"body"`
}

func handleAutosave(s *State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := chi.URLParam(r, "slug")
		var req autosaveReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if req.Session == "" {
			writeError(w, http.StatusBadRequest, "session required")
			return
		}
		branch := fmt.Sprintf("cairn/drafts/%s/%s", slug, req.Session)
		path, err := core.NewRepoPath(postsDir + "/" + slug + ".md")
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		var cs core.FileChangeSet
		cs.Push(core.Write(path, []byte(req.Body)))
		sha, err := s.Repo.ForceCommitToBranch(r.Context(), branch, cs, "autosave")
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"commit": string(sha), "branch": branch})
	}
}

type publishReq struct {
	Session string `json:"session"`
}

func handlePublish(s *State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := chi.URLParam(r, "slug")
		var req publishReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		branch := fmt.Sprintf("cairn/drafts/%s/%s", slug, req.Session)
		path, err := core.NewRepoPath(postsDir + "/" + slug + ".md")
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		// Read draft branch's version of the file.
		draftCommit, err := s.Repo.ResolveRef(r.Context(), branch)
		if err != nil || draftCommit == nil {
			writeError(w, http.StatusNotFound, "no draft to publish")
			return
		}
		read, err := s.Repo.Read(r.Context(), path, draftCommit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		var cs core.FileChangeSet
		cs.Push(core.Write(path, read.Bytes))
		sha, err := s.Repo.Commit(r.Context(), cs, "publish "+slug, core.CommitHintPublish, nil)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		_ = s.Repo.DeleteBranch(r.Context(), branch)
		writeJSON(w, http.StatusOK, map[string]any{"commit": string(sha)})
	}
}

func handleDeletePost(s *State) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := chi.URLParam(r, "slug")
		path, err := core.NewRepoPath(postsDir + "/" + slug + ".md")
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		var cs core.FileChangeSet
		cs.Push(core.Delete(path))
		sha, err := s.Repo.Commit(r.Context(), cs, "delete "+slug, core.CommitHintPublish, nil)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"commit": string(sha)})
	}
}

// summarise extracts a postSummary from a raw markdown source.
func summarise(path string, body []byte) (postSummary, error) {
	src := string(body)
	if !strings.HasPrefix(src, "---\n") {
		return postSummary{}, fmt.Errorf("no frontmatter")
	}
	rest := src[4:]
	end := strings.Index(rest, "\n---\n")
	if end < 0 {
		return postSummary{}, fmt.Errorf("unterminated frontmatter")
	}
	parsed, err := frontmatter.Parse(rest[:end])
	if err != nil {
		return postSummary{}, err
	}
	slug, _ := parsed.Frontmatter.EffectiveSlug()
	return postSummary{
		Slug:  slug,
		Title: parsed.Frontmatter.Title,
		Date:  parsed.Frontmatter.Date.String(),
		Draft: parsed.Frontmatter.Draft,
		Path:  path,
	}, nil
}
