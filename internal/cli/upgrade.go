package cli

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

func newUpgradeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "upgrade",
		Short: "Check for and install a newer Cairn release.",
		RunE:  func(_ *cobra.Command, _ []string) error { return runUpgrade() },
	}
}

const releasesURL = "https://api.github.com/repos/Harsh-2002/Cairn/releases/latest"

type releaseResp struct {
	TagName string `json:"tag_name"`
	Assets  []struct {
		Name               string `json:"name"`
		BrowserDownloadURL string `json:"browser_download_url"`
	} `json:"assets"`
}

func runUpgrade() error {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(releasesURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("releases endpoint: %s", resp.Status)
	}
	var r releaseResp
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return err
	}
	target := platformAssetName()
	var asset *struct {
		Name               string `json:"name"`
		BrowserDownloadURL string `json:"browser_download_url"`
	}
	for i := range r.Assets {
		if r.Assets[i].Name == target {
			asset = &r.Assets[i]
			break
		}
	}
	if asset == nil {
		return fmt.Errorf("no asset for %s found in release %s", target, r.TagName)
	}
	fmt.Printf("Downloading %s from %s\n", asset.Name, r.TagName)
	binData, err := downloadAndExtract(asset.BrowserDownloadURL)
	if err != nil {
		return err
	}
	if err := verifyChecksum(client, r, asset.Name, binData); err != nil {
		fmt.Fprintf(os.Stderr, "warning: checksum verification failed: %v\n", err)
	}
	return atomicReplace(binData)
}

func platformAssetName() string {
	os := runtime.GOOS
	arch := runtime.GOARCH
	switch os {
	case "linux":
		return fmt.Sprintf("cairn-linux-%s.tar.gz", arch)
	case "darwin":
		return fmt.Sprintf("cairn-macos-%s.tar.gz", arch)
	case "windows":
		return fmt.Sprintf("cairn-windows-%s.zip", arch)
	}
	return ""
}

func downloadAndExtract(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download: %s", resp.Status)
	}
	if strings.HasSuffix(url, ".tar.gz") {
		return extractTarGz(resp.Body)
	}
	return nil, errors.New("only tar.gz archives supported in this implementation")
}

func extractTarGz(r io.Reader) ([]byte, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag == tar.TypeReg && strings.HasSuffix(hdr.Name, "cairn") {
			return io.ReadAll(tr)
		}
	}
	return nil, fmt.Errorf("no `cairn` binary in archive")
}

func verifyChecksum(client *http.Client, r releaseResp, name string, data []byte) error {
	for _, a := range r.Assets {
		if a.Name == "SHA256SUMS" {
			resp, err := client.Get(a.BrowserDownloadURL)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			for _, line := range strings.Split(string(body), "\n") {
				parts := strings.Fields(line)
				if len(parts) == 2 && strings.TrimPrefix(parts[1], "./") == name {
					actual := sha256.Sum256(data)
					if hex.EncodeToString(actual[:]) != parts[0] {
						return fmt.Errorf("checksum mismatch")
					}
					return nil
				}
			}
		}
	}
	return errors.New("SHA256SUMS not found")
}

func atomicReplace(data []byte) error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	dir := filepath.Dir(self)
	tmp := filepath.Join(dir, ".cairn.new")
	if err := os.WriteFile(tmp, data, 0o755); err != nil {
		return err
	}
	if err := os.Rename(tmp, self); err != nil {
		return err
	}
	fmt.Printf("Replaced %s\n", self)
	return nil
}
