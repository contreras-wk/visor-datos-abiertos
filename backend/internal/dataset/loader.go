package dataset

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"visor-datos-abiertos-go/internal/ckan"
)

// downloadAndConvert descarga el CSV desde CKAN y lo convierte a DuckDB
func (m *Manager) downloadAndConvert(ctx context.Context, uuid string) (string, error) {

	// 1. Obtener info del recurso desde CKAN
	resource, err := m.ckanClient.GetResource(ctx, uuid)
	if err != nil {
		return "", fmt.Errorf("error obteniendo recurso de CKAN: %w", err)
	}
	log.Printf("Recurso: %s (%s)", resource.Name, resource.Format)
	log.Printf("URL: %s", resource.URL)

	// 2. Crear archivo temporal para el CSV
	tmpCSV := filepath.Join(os.TempDir(), fmt.Sprintf("%s_%d.csv", uuid, time.Now().Unix()))
	defer os.Remove(tmpCSV)

	// 3. Descargar CSV
	if err := m.downloadFile(ctx, resource.URL, tmpCSV); err != nil {
		return "", fmt.Errorf("error descargando CSV: %w", err)
	}

	log.Printf("CSV descargado: %s", tmpCSV)

	// 4. Crear base de datos DuckDB
	dbPath := filepath.Join(os.TempDir(), fmt.Sprintf("%s.duckdb", uuid))

	conn, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return "", fmt.Errorf("error creando DuckDB: %w", err)
	}
	defer conn.Close()

	// 5. Cargar CSV en DuckDB  usando función nativa
	log.Printf("Convirtiendo CSV a DuckDB...")

	query := fmt.Sprintf(`
		CREATE TABLE data AS 
		SELECT * FROM read_csv_auto('%s', 
			header = true,
			ignore_errors = true,
			sample_size = -1,
			null_padding = true,
			dateformat = '%%Y-%%m-%%d'
		)
	`, tmpCSV)

	if _, err := conn.ExecContext(ctx, query); err != nil {
		return "", fmt.Errorf("error cargando CSV en DuckDB: %w", err)
	}

	// 6. Obtener estadísticas
	var rowCount int64

	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM data").Scan(&rowCount)
	if err != nil {
		log.Printf("Warning: no se pudo obtener count: %v", err)
	} else {
		log.Printf("Cargados %d registros", rowCount)
	}

	// 7. Crear indices para mejorar queries
	if err := m.createIndexes(ctx, conn, resource); err != nil {
		log.Printf("Warning: error creando índices: %v", err)
	}

	// 8. Optimizar base de datos
	if _, err := conn.ExecContext(ctx, "CHECKPOINT"); err != nil {
		log.Printf("Warning: error en checkpoint: %v", err)
	}

	log.Printf("DuckDB creado exitosamente: %s", dbPath)
	return dbPath, nil

}

// downloadFile descarga un archivo desde una URL
func (m *Manager) downloadFile(ctx context.Context, url, filepath string) error {
	// Crear request con contexto
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	// Hacer request
	client := &http.Client{
		Timeout: 5 * time.Minute, // Timeout amplio para archivos grandes
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	// Crear archivo
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Copiar contenido con progreso
	written, err := io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	log.Printf("Descargados %.2f MB", float64(written)/(1024*1024))
	return nil
}

// createIndexes crea índices inteligentes basados en las columnas
func (m *Manager) createIndexes(ctx context.Context, conn *sql.DB, resource *ckan.Resource) error {
	// Obtener las columnas de la tabla
	rows, err := conn.QueryContext(ctx, "PRAGMA table_info('data')")
	if err != nil {
		return err
	}
	defer rows.Close()

	type Column struct {
		CID       int
		Name      string
		Type      string
		NotNull   int
		DfltValue interface{}
		PK        int
	}

	var columns []Column
	for rows.Next() {
		var col Column
		if err := rows.Scan(&col.CID, &col.Name, &col.Type, &col.NotNull, &col.DfltValue, &col.PK); err != nil {
			continue
		}
		columns = append(columns, col)
	}

	log.Printf("Creando indices inteligentes...")

	// Creando índices para columnas relevantes
	indexCount := 0
	for _, col := range columns {
		colLower := strings.ToLower(col.Name)

		// Índices para fechas
		if strings.Contains(colLower, "fecha") || strings.Contains(colLower, "date") {
			if err := m.createIndex(ctx, conn, col.Name); err == nil {
				indexCount++
			}
		}
		// TODO: crear los índices solo sobre las variables a visualizar !
		// TODO: crear el tipo de indice de acuerdo al tipo de la columna

		// Índices para categorías comunes
		categories := []string{"entidad", "estado", "municipio", "tipo", "categoria", "clasificacion"}
		for _, cat := range categories {
			if strings.Contains(colLower, cat) {
				if err := m.createIndex(ctx, conn, col.Name); err == nil {
					indexCount++
				}
				break
			}
		}
	}
	log.Printf("Creados %d índices", indexCount)
	return nil
}

func (m *Manager) createIndex(ctx context.Context, conn *sql.DB, columnName string) error {
	// Limpiar nombre de la columna

	safeName := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, columnName)

	query := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s ON data ("%s)`, safeName, columnName)
	_, err := conn.ExecContext(ctx, query)
	if err != nil {
		log.Printf("Warning: no se pudo crear el índice para %s: %v", columnName, err)
		return err
	}
	return nil
}
