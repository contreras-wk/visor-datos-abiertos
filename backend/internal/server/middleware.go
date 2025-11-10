package server

import (
	"net/http"
	"strings"
)

// ContentTypeJSON middleware fuerza Content-Type a JSON
func ContentTypeJSON(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next(w, r)
	}
}

// CacheControl middleware para cache del navegador
func CacheControl(maxAge int) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if maxAge > 0 {
				w.Header().Set("Cache-Control", "public max-age="+string(rune(maxAge)))
			} else {
				w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			}
			next(w, r)
		}
	}
}

func APIKeyAuth(validKey string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			apiKey := r.Header.Get("X-API-Key")

			if apiKey == "" {
				// Intentar desde un query param
				apiKey = r.URL.Query().Get("api_key")
			}

			if validKey != "" && apiKey != validKey {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next(w, r)
		}
	}
}

func Compression(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Verificar si el cliente acepta gzip

		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next(w, r)
			return
		}

		// TODO: Implementar gzip writer !!
		next(w, r)
	}
}
