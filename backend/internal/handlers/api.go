package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
	"visor-datos-abiertos-go/internal/cache"
	"visor-datos-abiertos-go/internal/dataset"
)

type APIHandler struct {
	datasetManager *dataset.Manager
	cacheManager   *cache.Manager
}

func NewAPIHandler(dm *dataset.Manager, cm *cache.Manager) *APIHandler {
	return &APIHandler{
		datasetManager: dm,
		cacheManager:   cm,
	}
}

// GetFilters retorna los filtros disponibles para un dataset
func (h *APIHandler) GetFilters(w http.ResponseWriter, r *http.Request) {
	// Extraer el UUID de la URL (/api/filters/{uuid})

	uuid := strings.TrimPrefix(r.URL.Path, "/api/filters/")

	if uuid == "" {
		http.Error(w, "UUID requerido", http.StatusBadRequest)
		return
	}

	// Cache key
	cacheKey := "filters:" + uuid

	// Verificar cache Redis (24 horas)
	if cached, found := h.cacheManager.GetFromRedis(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.Write(cached)
		return
	}

	// Obtener filtros
	filters, err := h.datasetManager.GetAvailableFilters(r.Context(), uuid)
	if err != nil {
		log.Printf("Error obteniendo filtros: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// serializar
	data, err := json.Marshal(map[string]interface{}{
		"filters": filters,
		"cached":  false,
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// cachear por 24 horas
	h.cacheManager.SetToRedis(cacheKey, data, 24*time.Hour)

	// Retornar
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.Write(data)
}

// GetFilteredData retorna datos filtrados
func (h *APIHandler) GetFilteredData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	// Extraer el UUID
	uuid := strings.TrimPrefix(r.URL.Path, "/api/data/")
	if uuid != "" {
		http.Error(w, "UUID requerido", http.StatusBadRequest)
		return
	}

	// Parse request body
	var params dataset.FilterParams
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, "datos inválidos", http.StatusBadRequest)
		return
	}

	// Cache Key
	cacheKey := h.cacheManager.GenerateKey("data", map[string]interface{}{
		"uuid":   uuid,
		"params": params,
	})

	// Verificar cache (30 min)
	if cached, found := h.cacheManager.GetFromRedis(cacheKey); found {
		w.Header().Set("Content_Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.Write(cached)
		return
	}

	// Obtener datos
	data, err := h.datasetManager.GetFilteredData(r.Context(), uuid, params)
	if err != nil {
		log.Printf("Error obteniendo datos: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Serializar
	response := map[string]interface{}{
		"data":   data,
		"total":  len(data),
		"cached": false,
	}

	jsonData, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.cacheManager.SetToRedis(cacheKey, jsonData, 30*time.Minute)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.Header().Set("Cache-Control", "public, max-age=1800")
	w.Write(jsonData)

}

func (h *APIHandler) GetAggregatedData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	// Extraer el UUID
	uuid := strings.TrimPrefix(r.URL.Path, "/api/aggregated/")
	if uuid != "" {
		http.Error(w, "UUID requerido", http.StatusBadRequest)
		return
	}

	// Parse request body
	var params dataset.AggregationParams
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, "datos inválidos", http.StatusBadRequest)
		return
	}

	// Cache Key
	cacheKey := h.cacheManager.GenerateKey("agg", map[string]interface{}{
		"uuid":   uuid,
		"params": params,
	})

	// Verificar cache (1 hora)
	if cached, found := h.cacheManager.GetFromRedis(cacheKey); found {
		w.Header().Set("Content_Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.Write(cached)
		return
	}

	// Obtener datos agregados
	data, err := h.datasetManager.GetAggregatedData(r.Context(), uuid, params)
	if err != nil {
		log.Printf("Error obteniendo datos agregados: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"data":   data,
		"total":  len(data),
		"cached": false,
	}

	jsonData, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Cachear (1 hora)
	h.cacheManager.SetToRedis(cacheKey, jsonData, time.Hour)

	// Retornar
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.Header().Set("Cache-Control", "public, max-age=1800")
	w.Write(jsonData)

}

func (h *APIHandler) GetMetadata(w http.ResponseWriter, r *http.Request) {
	// Extraer el UUID
	uuid := strings.TrimPrefix(r.URL.Path, "/api/metadata/")
	if uuid != "" {
		http.Error(w, "UUID requerido", http.StatusBadRequest)
		return
	}

	cacheKey := "metadata:" + uuid

	// verificar cache (24 horas)
	if cached, found := h.cacheManager.GetFromRedis(cacheKey); found {
		w.Header().Set("Content-Type", "applicaction/json")
		w.Header().Set("X-Cache", "HIT")
		w.Write(cached)
		return
	}

	// Obtener metadata desde CKAN
	resource, err := h.datasetManager.GetCKANCLient().GetResource(r.Context(), uuid)
	if err != nil {
		log.Printf("Error obteniendo el metadata: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := json.Marshal(resource)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Cachear data
	h.cacheManager.SetToRedis(cacheKey, data, 24*time.Hour)

	// Responder
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.Write(data)
}

func (h *APIHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	//  Extraer el UUID
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/stats/"), "/")
	if len(parts) < 2 {
		http.Error(w, "UUID y columna requeridos", http.StatusBadRequest)
		return
	}

	uuid := parts[0]
	column := parts[1]

	// Parse filtros
	var filters map[string]interface{}
	if r.Method == http.MethodPost {
		json.NewDecoder(r.Body).Decode(&filters)
	}

	// Cache Key
	cacheKey := h.cacheManager.GenerateKey("stats", map[string]interface{}{
		"uuid":    uuid,
		"column":  column,
		"filters": filters,
	})

	// Verificar cache
	if cached, found := h.cacheManager.GetFromRedis(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.Write(cached)
		return
	}

	// Obtener stats
	stats, err := h.datasetManager.GetStats(r.Context(), uuid, column, filters)
	if err != nil {
		log.Printf("erro obteniendo stats: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Serializar y cachear
	jsonData, _ := json.Marshal(stats)
	h.cacheManager.SetToRedis(cacheKey, jsonData, time.Hour)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.Write(jsonData)
}

// GetTopValues retorna los valores más frecuentes
func (h *APIHandler) GetTopValues(w http.ResponseWriter, r *http.Request) {
	// Extraer UUID
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/top/"), "/")
	if len(parts) < 2 {
		http.Error(w, "UUID y columna requeridos", http.StatusBadRequest)
		return
	}

	uuid := parts[0]
	column := parts[1]

	// Limit desde query param
	limit := 10
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		fmt.Sscanf(limitStr, "%d", &limit)
	}

	// Filtros
	var filters map[string]interface{}
	if r.Method == http.MethodPost {
		json.NewDecoder(r.Body).Decode(&filters)
	}

	// CacheKey
	cacheKey := h.cacheManager.GenerateKey("top", map[string]interface{}{
		"uuid":    uuid,
		"column":  column,
		"limit":   limit,
		"filters": filters,
	})

	// Verificar cache
	if cached, found := h.cacheManager.GetFromRedis(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "HIT")
		w.Write(cached)
		return
	}

	// Obtener top values
	data, err := h.datasetManager.GetTopValues(r.Context(), uuid, column, limit, filters)
	if err != nil {
		log.Printf("Error obteniendo top values: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Serializar y cachear
	jsonData, _ := json.Marshal(data)
	h.cacheManager.SetToRedis(cacheKey, jsonData, time.Hour)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.Write(jsonData)
}
