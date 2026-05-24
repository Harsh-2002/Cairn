package server

import (
	"embed"
	"io/fs"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strings"
)

//go:embed admin/admin-dist
var adminDist embed.FS

// adminHandler serves the embedded admin SPA, or proxies to a Vite dev
// server when State.ViteProxy is set.
func adminHandler(s *State) http.HandlerFunc {
	if s.ViteProxy != "" {
		target, err := url.Parse(s.ViteProxy)
		if err == nil {
			proxy := httputil.NewSingleHostReverseProxy(target)
			return proxy.ServeHTTP
		}
	}
	sub, err := fs.Sub(adminDist, "admin/admin-dist")
	if err != nil {
		return func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "admin dist not embedded", http.StatusInternalServerError)
		}
	}
	fileServer := http.FileServer(http.FS(sub))
	return func(w http.ResponseWriter, r *http.Request) {
		// SPA fallback: if the requested path doesn't exist in the embedded
		// FS, serve index.html so client-side routing works.
		clean := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if clean == "" || !exists(sub, clean) {
			indexData, err := fs.ReadFile(sub, "index.html")
			if err != nil {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(indexData)
			return
		}
		fileServer.ServeHTTP(w, r)
	}
}

func exists(fsys fs.FS, name string) bool {
	_, err := fs.Stat(fsys, name)
	return err == nil
}
