package server

import (
	"io/fs"
	"log"
	"net/http"
	"time"
	"visor-datos-abiertos-go/internal/cache"
	"visor-datos-abiertos-go/internal/dataset"
	"visor-datos-abiertos-go/internal/handlers"
)

type Server struct {
	config         *Config
	datasetManager *dataset.Manager
	cacheManager   *cache.Manager
	mux            *http.ServeMux
}

func New(config *Config, dm *dataset.Manager, cm *cache.Manager) *Server {
	s := &Server{
		config:         config,
		datasetManager: dm,
		cacheManager:   cm,
		mux:            http.NewServeMux(),
	}

	// registrar rutas(endpoints)
	s.registerRoutes()

	return s
}

func (s *Server) registerRoutes() {
	// Health check
	s.mux.HandleFunc("/api/health", s.withMiddleware(handlers.NewHealthHandler().Health))

	// API handlers
	apiHandler := handlers.NewAPIHandler(s.datasetManager, s.cacheManager)

	s.mux.HandleFunc("/api/filters/", s.withMiddleware(apiHandler.GetFilters))
	s.mux.HandleFunc("/api/data/", s.withMiddleware(apiHandler.GetFilteredData))
	s.mux.HandleFunc("/api/aggregated/", s.withMiddleware(apiHandler.GetAggregatedData))
	s.mux.HandleFunc("/api/metadata/", s.withMiddleware(apiHandler.GetMetadata))
	s.mux.HandleFunc("/api/stats/", s.withMiddleware(apiHandler.GetStats))
	s.mux.HandleFunc("/api/top/", s.withMiddleware(apiHandler.GetTopValues))
	s.mux.HandleFunc("/api/status/", s.withMiddleware(apiHandler.GetDownloadStatus))
}

func (s *Server) MountFrontend(frontendFS fs.FS) {
	s.mux.Handle("/", s.spaHandler(frontendFS))
}

func (s *Server) Router() http.Handler {
	return s.mux
}

func (s *Server) withMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return s.recoverMiddleware(
		s.loggingMiddleware(
			s.corsMiddleware(next),
		),
	)
}

// Logging Middleware
func (s *Server) loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Crear ResponseWriter que captura el status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next(wrapped, r)

		duration := time.Since(start)
		log.Printf("%s %s %d %v", r.Method, r.URL.Path, wrapped.statusCode, duration)
	}
}

// Cors Middleware
func (s *Server) corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// NOTE: Revisar lista de origenes permitidos
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONs")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

func (s *Server) recoverMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Panic: %v", err)
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()
		next(w, r)
	}
}

func (s *Server) spaHandler(fsys fs.FS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// Si es la raíz servir el index.html
		if path == "/" {
			path = "index.html"
		}

		// Intentar abrir el archivo
		file, err := fsys.Open(path)
		if err != nil {
			// Si no existe, servir el index.html (SPA routing)
			file, err = fsys.Open("index.html")
			if err != nil {
				http.NotFound(w, r)
				return
			}
		}
		defer file.Close()

		// Obtener la info del archivo
		stat, err := file.Stat()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.ServeContent(w, r, path, stat.ModTime(), file.(http.File))
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
