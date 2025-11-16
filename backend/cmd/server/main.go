package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"visor-datos-abiertos-go/internal/cache"
	"visor-datos-abiertos-go/internal/dataset"
	"visor-datos-abiertos-go/internal/server"
)

var frontendFS embed.FS

func main() {
	fmt.Println("Hola mundo !!!")
	fmt.Print("Nuevo visor de datos abiertos")

	config := &server.Config{
		Port:          getEnv("PORT", "8080"),
		CKANBaseURL:   getEnv("CKAN_URL", "https://datos.gob.mx/api/3/action"),
		RedisURL:      getEnv("REDIS_URL", "redis://localhost:6379/0"),
		CacheDir:      getEnv("CACHE_DIR", "/tmp/datasets"),
		MemoryCacheGB: 4,
		DiskCacheGB:   50,
	}

	// Crear directorio de cache
	if err := os.MkdirAll(config.CacheDir, 0755); err != nil {
		log.Fatalf("Error creando el directorio de cache: %v", err)
	}

	// Inicializar cache manager
	log.Println("Inicializando cache manager...")
	cacheManager, err := cache.NewManager(
		config.RedisURL,
		config.MemoryCacheGB*1024*1024*1024,
		config.DiskCacheGB*1024*1024*1024,
		config.CacheDir,
	)
	if err != nil {
		log.Fatalf("Error inicializando cache: %v", err)
	}
	defer cacheManager.Close()

	// Inicializando dataset managerl
	log.Println("Inicializando dataset manager...")
	datasetManager := dataset.NewManager(config.CKANBaseURL, cacheManager)
	defer datasetManager.Close()

	// Crear servidor

	srv := server.New(config, datasetManager, cacheManager)

	// Montar frontend (SPA)
	frontendDist, err := fs.Sub(frontendFS, "frontend/dist")
	if err != nil {
		log.Fatalf("Error montando frontend: %v", err)
	}

	srv.MountFrontend(frontendDist)

	// Servidor HTTP
	httpServer := &http.Server{
		Addr:           ":" + config.Port,
		Handler:        srv.Router(),
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	// Iniciar Servidor
	go func() {
		log.Printf("🚀 Servidor iniciado en http://localhost:%s", config.Port)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Error en servidor: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("🛑 Apagando servidor...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("Error en shutdown: %v", err)
	}

	log.Println("✓ Servidor apagado correctamente")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
