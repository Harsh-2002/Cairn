// Package plugins implements Cairn's external-process plugin runner. See
// docs/PLUGIN_CONTRACT.md for the authoritative wire contract: a runner
// walks plugins/<hook>/, executes every executable file in filename order,
// pipes JSON to stdin and reads JSON from stdout. 30s timeout per plugin.
package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"
)

// Hook is one of the seven fixed plugin lifecycle points.
type Hook string

const (
	HookPreIngest  Hook = "pre-ingest"
	HookPostIngest Hook = "post-ingest"
	HookPreAsset   Hook = "pre-asset"
	HookPostAsset  Hook = "post-asset"
	HookPreRender  Hook = "pre-render"
	HookPostRender Hook = "post-render"
	HookPostDeploy Hook = "post-deploy"
)

// Response is the JSON document a plugin writes to stdout. Empty/null fields
// mean "no change".
type Response struct {
	Abort              bool            `json:"abort,omitempty"`
	Reason             string          `json:"reason,omitempty"`
	Body               *string         `json:"body,omitempty"`
	Source             *string         `json:"source,omitempty"`
	HTML               *string         `json:"html,omitempty"`
	FrontmatterUpdates json.RawMessage `json:"frontmatter_updates,omitempty"`
	Skip               bool            `json:"skip,omitempty"`
}

// Entry is one discovered plugin (an executable file in a hook directory).
type Entry struct {
	Path string
	Name string
}

// Error categories. Use errors.As to inspect details.
type (
	NotExecutableError struct{ Name string }
	NonZeroExitError   struct {
		Name   string
		Code   int
		Stderr string
	}
	TimeoutError struct{ Name string }
	BadJSONError struct {
		Name string
		Err  error
	}
	AbortedError struct {
		Name   string
		Reason string
	}
)

func (e *NotExecutableError) Error() string {
	return fmt.Sprintf("plugin %q was not executable; skipped", e.Name)
}

func (e *NonZeroExitError) Error() string {
	return fmt.Sprintf("plugin %q failed with exit code %d: %s", e.Name, e.Code, e.Stderr)
}

func (e *TimeoutError) Error() string { return fmt.Sprintf("plugin %q timed out", e.Name) }

func (e *BadJSONError) Error() string {
	return fmt.Sprintf("plugin %q produced invalid JSON: %s", e.Name, e.Err)
}

func (e *BadJSONError) Unwrap() error { return e.Err }

func (e *AbortedError) Error() string {
	return fmt.Sprintf("plugin %q requested abort: %s", e.Name, e.Reason)
}

// Runner discovers and executes plugins for a given hook.
type Runner struct {
	root    string
	timeout time.Duration
}

// New constructs a Runner over the plugins directory at root.
func New(root string) *Runner { return &Runner{root: root, timeout: 30 * time.Second} }

// WithTimeout overrides the per-plugin timeout (default 30 seconds).
func (r *Runner) WithTimeout(d time.Duration) *Runner { r.timeout = d; return r }

// Discover lists the executable files under plugins/<hook>/ in filename order.
func (r *Runner) Discover(hook Hook) []Entry {
	dir := filepath.Join(r.root, string(hook))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []Entry
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.Mode()&0o111 == 0 {
			continue
		}
		out = append(out, Entry{Path: filepath.Join(dir, e.Name()), Name: e.Name()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Run executes every plugin under hook, piping payload to stdin and reading
// JSON responses from stdout. Returns the aggregated responses; aborts on the
// first plugin that returns abort=true or fails.
func (r *Runner) Run(ctx context.Context, hook Hook, payload json.RawMessage) ([]Response, error) {
	entries := r.Discover(hook)
	var responses []Response
	for _, e := range entries {
		resp, err := r.execOne(ctx, e, payload)
		if err != nil {
			return responses, err
		}
		if resp.Abort {
			return responses, &AbortedError{Name: e.Name, Reason: resp.Reason}
		}
		responses = append(responses, resp)
	}
	return responses, nil
}

func (r *Runner) execOne(ctx context.Context, entry Entry, payload json.RawMessage) (Response, error) {
	cctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, entry.Path)
	cmd.Stdin = bytes.NewReader(payload)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if cctx.Err() == context.DeadlineExceeded {
		return Response{}, &TimeoutError{Name: entry.Name}
	}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return Response{}, &NonZeroExitError{Name: entry.Name, Code: exitErr.ExitCode(), Stderr: stderr.String()}
		}
		return Response{}, fmt.Errorf("plugin %q: %w", entry.Name, err)
	}
	// Forward stderr to our own stderr.
	if stderr.Len() > 0 {
		_, _ = io.Copy(os.Stderr, &stderr)
	}
	if stdout.Len() == 0 {
		return Response{}, nil
	}
	var resp Response
	if err := json.Unmarshal(stdout.Bytes(), &resp); err != nil {
		return Response{}, &BadJSONError{Name: entry.Name, Err: err}
	}
	return resp, nil
}
