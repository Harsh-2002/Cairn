//go:build unix

package plugins

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writePlugin(t *testing.T, root string, hook Hook, name, script string) {
	t.Helper()
	dir := filepath.Join(root, string(hook))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoverInFilenameOrder(t *testing.T) {
	root := t.TempDir()
	writePlugin(t, root, HookPreIngest, "20-second.sh", "#!/bin/sh\necho '{}'\n")
	writePlugin(t, root, HookPreIngest, "10-first.sh", "#!/bin/sh\necho '{}'\n")
	r := New(root)
	entries := r.Discover(HookPreIngest)
	if len(entries) != 2 {
		t.Fatalf("len = %d", len(entries))
	}
	if entries[0].Name != "10-first.sh" || entries[1].Name != "20-second.sh" {
		t.Errorf("order: %v %v", entries[0].Name, entries[1].Name)
	}
}

func TestRunSuccessReturnsResponse(t *testing.T) {
	root := t.TempDir()
	writePlugin(t, root, HookPostRender, "00-noop.sh", `#!/bin/sh
cat > /dev/null
echo '{"abort": false}'
`)
	r := New(root)
	resp, err := r.Run(context.Background(), HookPostRender, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 1 {
		t.Fatalf("len = %d", len(resp))
	}
	if resp[0].Abort {
		t.Errorf("expected abort false")
	}
}

func TestRunAbortStopsRunner(t *testing.T) {
	root := t.TempDir()
	writePlugin(t, root, HookPostRender, "00-abort.sh", `#!/bin/sh
echo '{"abort": true, "reason": "no thanks"}'
`)
	r := New(root)
	_, err := r.Run(context.Background(), HookPostRender, json.RawMessage(`{}`))
	var ae *AbortedError
	if !errors.As(err, &ae) {
		t.Fatalf("err = %v; want *AbortedError", err)
	}
	if ae.Reason != "no thanks" {
		t.Errorf("reason = %q", ae.Reason)
	}
}

func TestRunBadJSONFails(t *testing.T) {
	root := t.TempDir()
	writePlugin(t, root, HookPostRender, "00-bad.sh", `#!/bin/sh
echo 'not-json'
`)
	r := New(root)
	_, err := r.Run(context.Background(), HookPostRender, json.RawMessage(`{}`))
	var bj *BadJSONError
	if !errors.As(err, &bj) {
		t.Errorf("err = %v; want *BadJSONError", err)
	}
}

func TestRunNonZeroExitFails(t *testing.T) {
	root := t.TempDir()
	writePlugin(t, root, HookPostRender, "00-fail.sh", `#!/bin/sh
exit 2
`)
	r := New(root)
	_, err := r.Run(context.Background(), HookPostRender, json.RawMessage(`{}`))
	var ne *NonZeroExitError
	if !errors.As(err, &ne) {
		t.Errorf("err = %v; want *NonZeroExitError", err)
	}
	if ne.Code != 2 {
		t.Errorf("code = %d", ne.Code)
	}
}

func TestRunTimeoutKillsLongPlugin(t *testing.T) {
	root := t.TempDir()
	writePlugin(t, root, HookPostRender, "00-slow.sh", `#!/bin/sh
sleep 5
echo '{}'
`)
	r := New(root).WithTimeout(100 * time.Millisecond)
	_, err := r.Run(context.Background(), HookPostRender, json.RawMessage(`{}`))
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Errorf("err = %v; want *TimeoutError", err)
	}
}

func TestEmptyDirReturnsNoResponses(t *testing.T) {
	r := New(t.TempDir())
	resp, err := r.Run(context.Background(), HookPostRender, json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) != 0 {
		t.Errorf("len = %d", len(resp))
	}
}

func TestNonExecutableSkipped(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, string(HookPostRender))
	_ = os.MkdirAll(dir, 0o755)
	_ = os.WriteFile(filepath.Join(dir, "not-exec.sh"), []byte("#!/bin/sh\necho '{}'\n"), 0o644)
	r := New(root)
	entries := r.Discover(HookPostRender)
	if len(entries) != 0 {
		t.Errorf("len = %d; want 0", len(entries))
	}
}
