// backend/internal/dataset/download_manager.go
package dataset

import (
	"context"
	"log"
	"sync"
	"time"
)

type DownloadStatus string

const (
	StatusPending     DownloadStatus = "pending"
	StatusDownloading DownloadStatus = "downloading"
	StatusProcessing  DownloadStatus = "processing"
	StatusReady       DownloadStatus = "ready"
	StatusFailed      DownloadStatus = "failed"
)

type DownloadJob struct {
	UUID       string         `json:"uuid"`
	Status     DownloadStatus `json:"status"`
	Progress   float64        `json:"progress"`
	Error      error          `json:"-"`
	ErrorMsg   string         `json:"error,omitempty"`
	StartTime  time.Time      `json:"start_time"`
	EndTime    time.Time      `json:"end_time,omitempty"`
	FileSize   int64          `json:"file_size"`
	Downloaded int64          `json:"downloaded"`
	Message    string         `json:"message"`
}

type DownloadManager struct {
	jobs    map[string]*DownloadJob
	mu      sync.RWMutex
	manager *Manager
}

func NewDownloadManager(m *Manager) *DownloadManager {
	return &DownloadManager{
		jobs:    make(map[string]*DownloadJob),
		manager: m,
	}
}

func (dm *DownloadManager) StartDownload(uuid string) *DownloadJob {
	dm.mu.Lock()

	// Si ya existe un job, retornarlo
	if job, exists := dm.jobs[uuid]; exists {
		dm.mu.Unlock()
		return job
	}

	// Crear nuevo job
	job := &DownloadJob{
		UUID:      uuid,
		Status:    StatusPending,
		StartTime: time.Now(),
		Message:   "Iniciando descarga...",
	}
	dm.jobs[uuid] = job
	dm.mu.Unlock()

	log.Printf("🚀 Iniciando descarga asíncrona de dataset: %s", uuid)

	// Iniciar descarga en goroutine con contexto background
	go dm.downloadInBackground(uuid)

	return job
}

func (dm *DownloadManager) downloadInBackground(uuid string) {
	// Usar contexto background para que no se cancele
	ctx := context.Background()

	dm.updateJob(uuid, func(job *DownloadJob) {
		job.Status = StatusDownloading
		job.Message = "Descargando CSV desde CKAN..."
	})

	// Callback de progreso
	progressCallback := func(downloaded, total int64) {
		dm.updateJob(uuid, func(job *DownloadJob) {
			job.Downloaded = downloaded
			job.FileSize = total
			if total > 0 {
				// 0-80% para descarga
				job.Progress = float64(downloaded) / float64(total) * 80
			}
		})
	}

	// Descargar y convertir (ya crea en la ubicación correcta del cache)
	dbPath, err := dm.manager.downloadAndConvertWithProgress(ctx, uuid, progressCallback)

	if err != nil {
		log.Printf("❌ Error en descarga de %s: %v", uuid, err)
		dm.updateJob(uuid, func(job *DownloadJob) {
			job.Status = StatusFailed
			job.Error = err
			job.ErrorMsg = err.Error()
			job.EndTime = time.Now()
			job.Message = "Error en descarga"
		})
		return
	}

	dm.updateJob(uuid, func(job *DownloadJob) {
		job.Status = StatusProcessing
		job.Progress = 95
		job.Message = "Registrando en cache..."
	})

	// ✅ El archivo YA está en la ubicación correcta
	// Solo registrarlo en memoria LRU
	dm.manager.cacheManager.SetToMemory(uuid, dbPath)

	dm.updateJob(uuid, func(job *DownloadJob) {
		job.Status = StatusReady
		job.Progress = 100
		job.EndTime = time.Now()
		job.Message = "Dataset listo para consultar"
	})

	duration := time.Since(dm.jobs[uuid].StartTime)
	log.Printf("✅ Dataset %s listo en %.2f segundos", uuid, duration.Seconds())
	log.Printf("📁 Ubicación: %s", dbPath)
}

func (dm *DownloadManager) updateJob(uuid string, updateFn func(*DownloadJob)) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if job, exists := dm.jobs[uuid]; exists {
		updateFn(job)
	}
}

func (dm *DownloadManager) GetJob(uuid string) (*DownloadJob, bool) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if job, exists := dm.jobs[uuid]; exists {
		// Crear copia para evitar race conditions
		jobCopy := *job
		if job.Error != nil {
			jobCopy.ErrorMsg = job.Error.Error()
		}
		return &jobCopy, true
	}
	return nil, false
}

func (dm *DownloadManager) CleanupOldJobs() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	now := time.Now()
	for uuid, job := range dm.jobs {
		// Limpiar jobs completados después de 1 hora
		if job.Status == StatusReady || job.Status == StatusFailed {
			if !job.EndTime.IsZero() && now.Sub(job.EndTime) > time.Hour {
				log.Printf("🗑️  Limpiando job antiguo: %s", uuid)
				delete(dm.jobs, uuid)
			}
		}
	}
}
