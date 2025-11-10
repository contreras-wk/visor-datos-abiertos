package handlers

import (
	"encoding/json"
	"net/http"
	"time"
)

type HealthHandler struct{}

func NewHealthHandler() *HealthHandler {
	return &HealthHandler{}
}

func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	response := map[string]interface{}{
		"status":    "ok",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"service":   "visor-datos-abiertos-api",
		"version":   "0.1.0",
	}

	json.NewEncoder(w).Encode(response)
}
