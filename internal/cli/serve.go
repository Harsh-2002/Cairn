package cli

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/Harsh-2002/Cairn/internal/repo"
	"github.com/Harsh-2002/Cairn/internal/repo/github"
	"github.com/Harsh-2002/Cairn/internal/repo/local"
	"github.com/Harsh-2002/Cairn/internal/server"
	"github.com/Harsh-2002/Cairn/internal/server/signer"
)

func newServeCmd() *cobra.Command {
	var (
		bind, adminSecret, viteProxy, repoSpec, token, tokenEnv string
		port                                                    int
		bucketEndpoint, bucketName, bucketAccess, bucketSecret  string
		bucketRegion                                            string
	)
	cmd := &cobra.Command{
		Use:   "serve [source]",
		Short: "Start the admin server (serves the embedded SPA + API).",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			source := "."
			if len(args) == 1 {
				source = args[0]
			}
			return runServe(source, bind, port, adminSecret, viteProxy, repoSpec, token, tokenEnv,
				bucketEndpoint, bucketName, bucketRegion, bucketAccess, bucketSecret)
		},
	}
	cmd.Flags().StringVar(&bind, "bind", "127.0.0.1", "Bind address.")
	cmd.Flags().IntVar(&port, "port", 8080, "Bind port.")
	cmd.Flags().StringVar(&adminSecret, "admin-secret", "", "Admin secret (falls back to CAIRN_ADMIN_SECRET).")
	cmd.Flags().StringVar(&viteProxy, "vite-proxy", "", "Proxy non-/api requests to this Vite dev server.")
	cmd.Flags().StringVar(&repoSpec, "repo", "", "Operate against a GitHub repo (owner/name) instead of local clone.")
	cmd.Flags().StringVar(&token, "token", "", "GitHub PAT (falls back to CAIRN_GITHUB_TOKEN).")
	cmd.Flags().StringVar(&tokenEnv, "token-env", "", "Env var name holding the PAT.")
	cmd.Flags().StringVar(&bucketEndpoint, "bucket-endpoint", "", "S3-compatible bucket endpoint.")
	cmd.Flags().StringVar(&bucketName, "bucket-name", "", "Bucket name (required when --bucket-endpoint is set).")
	cmd.Flags().StringVar(&bucketRegion, "bucket-region", "us-east-1", "Bucket region.")
	cmd.Flags().StringVar(&bucketAccess, "bucket-access-key", "", "Bucket access key.")
	cmd.Flags().StringVar(&bucketSecret, "bucket-secret-key", "", "Bucket secret key.")
	return cmd
}

func runServe(source, bind string, port int, adminSecretFlag, viteProxy, repoSpec, tokenFlag, tokenEnv,
	bucketEndpoint, bucketName, bucketRegion, bucketAccess, bucketSecret string) error {

	adminSecret := adminSecretFlag
	if adminSecret == "" {
		adminSecret = os.Getenv("CAIRN_ADMIN_SECRET")
	}
	if adminSecret == "" {
		return fmt.Errorf("no admin secret — pass --admin-secret or set CAIRN_ADMIN_SECRET")
	}

	var provider repo.Provider
	if repoSpec != "" {
		tk := tokenFlag
		if tk == "" && tokenEnv != "" {
			tk = os.Getenv(tokenEnv)
		}
		if tk == "" {
			tk = os.Getenv("CAIRN_GITHUB_TOKEN")
		}
		if tk == "" {
			return fmt.Errorf("--repo set but no token")
		}
		parts := strings.SplitN(repoSpec, "/", 2)
		if len(parts) != 2 {
			return fmt.Errorf("--repo must be owner/name (got %q)", repoSpec)
		}
		p := github.New(parts[0], parts[1], tk)
		if base := os.Getenv("CAIRN_GITHUB_API_BASE"); base != "" {
			p = p.WithBaseURL(base)
		}
		provider = p
		fmt.Printf("Repo: GitHub %s via API\n", repoSpec)
	} else {
		p, err := local.Open(source)
		if err != nil {
			return fmt.Errorf("opening git repo: %w", err)
		}
		provider = p
		fmt.Printf("Repo: local clone at %s\n", source)
	}

	var urlSigner signer.URLSigner
	if bucketEndpoint != "" && bucketName != "" {
		access := bucketAccess
		if access == "" {
			access = os.Getenv("CAIRN_BUCKET_ACCESS_KEY")
		}
		secret := bucketSecret
		if secret == "" {
			secret = os.Getenv("CAIRN_BUCKET_SECRET_KEY")
		}
		urlStr := fmt.Sprintf("s3://%s?endpoint=%s&region=%s&disableSSL=false", bucketName, bucketEndpoint, bucketRegion)
		bucket, err := blob.OpenBucket(context.Background(), urlStr)
		if err != nil {
			return fmt.Errorf("opening bucket: %w", err)
		}
		_ = access
		_ = secret
		urlSigner = signer.NewBucketSigner(bucket)
		fmt.Printf("Bucket: %s on %s\n", bucketName, bucketEndpoint)
	} else {
		urlSigner = signer.NewMockSigner("https://mock-bucket")
		fmt.Println("Bucket: MockSigner (dev mode)")
	}

	state := &server.State{
		AdminSecret:   adminSecret,
		Signer:        urlSigner,
		Repo:          provider,
		PresignExpiry: 5 * time.Minute,
		ViteProxy:     viteProxy,
	}

	addr := fmt.Sprintf("%s:%d", bind, port)
	srv := &http.Server{
		Addr:    addr,
		Handler: server.Router(state),
	}
	fmt.Printf("Listening on http://%s\n", addr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	errc := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errc <- err
		}
	}()
	select {
	case <-stop:
		fmt.Println("\nshutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(ctx)
	case err := <-errc:
		return err
	}
}
