package dataset

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"visor-datos-abiertos-go/internal/cache"
	"visor-datos-abiertos-go/internal/ckan"
)

type Manager struct {
	ckanClient   *ckan.Client
	cacheManager *cache.Manager
	connections  sync.Map // Pool de conexiones DuckDB
	// mu           sync.RWMutex
}

func NewManager(ckanURL string, cacheManager *cache.Manager) *Manager {
	return &Manager{
		ckanClient:   ckan.NewClient(ckanURL),
		cacheManager: cacheManager,
	}
}

// GetConnection obtiene o crea una conexión DuckDB para un dataset
func (m *Manager) GetConnection(ctx context.Context, uuid string) (*sql.DB, error) {
	// 1. Verificar si ya tenemos la conexión en el pool
	if conn, ok := m.connections.Load(uuid); ok {
		return conn.(*sql.DB), nil
	}

	// 2. Verificar cache en memoria (LRU)
	dbPath, found := m.cacheManager.GetFromMemory(uuid)
	if found {
		log.Printf(" Dataset %s encontrado en memoria", uuid)
		return m.openConnection(uuid, dbPath)
	}

	// 3. Verificar cache en disco
	dbPath, found = m.cacheManager.GetFromDisk(uuid)
	if found {
		log.Printf("Dataset %s  encontrado en disco, promoviendo a memoria", uuid)
		m.cacheManager.SetToMemory(uuid, dbPath)
		return m.openConnection(uuid, dbPath)
	}

	// 4. Descargar desde CKAN y convertir a DuckDB
	log.Printf("Descargando dataset %s desde CKAN...", uuid)
	dbPath, err := m.downloadAndConvert(ctx, uuid)
	if err != nil {
		return nil, fmt.Errorf("error descargando dataset: %w", err)
	}

	// Guardar en cache
	if err := m.cacheManager.SetToDisk(uuid, dbPath); err != nil {
		log.Printf("Warning: error guardando en disco cache: %v", err)
	}
	m.cacheManager.SetToMemory(uuid, dbPath)

	return m.openConnection(uuid, dbPath)

}

func (m *Manager) openConnection(uuid, dbPath string) (*sql.DB, error) {
	// Abrir conexión read-only
	conn, err := sql.Open("duckdb", dbPath+"?access_mode=read_only")
	if err != nil {
		return nil, fmt.Errorf("error abriendo DuckDB: %w", err)
	}

	// Configurar pool
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(time.Hour)

	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("error ping DuckDB: %w", err)
	}

	// Guardar en pool
	m.connections.Store(uuid, conn)

	log.Printf("Conexión DuckDB establecida para dataset %s", uuid)
	return conn, nil
}

// Close cierra todas las conexiones
func (m *Manager) Close() error {
	var lastErr error
	m.connections.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*sql.DB); ok {
			if err := conn.Close(); err != nil {
				lastErr = err
				log.Printf("Error cerrando conexión %v: %v", key, err)
			}
		}
		return true
	})
	return lastErr
}

// GetCKANCLient retorna el cliente CKAN (para metadata)
func (m *Manager) GetCKANCLient() *ckan.Client {
	return m.ckanClient
}
