package dataset

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// FilterParams representa los parámetros de filtrado
type FilterParams struct {
	Filters map[string]interface{} `json:"filters"`
	Limit   int                    `json:"limit"`
	Offset  int                    `json:"offset"`
}

// GetFilteredData obtiene datos filtrados
func (m *Manager) GetFilteredData(ctx context.Context, uuid string, params FilterParams) ([]map[string]interface{}, error) {
	// Obtener conexión
	conn, err := m.GetConnection(ctx, uuid)
	if err != nil {
		return nil, err
	}

	// Construir query
	query, args := m.buildFilterQuery(params)

	// Ejecutar query
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error ejecutando query: %w", err)
	}
	defer rows.Close()

	// convertir a slice de maps
	return m.rowsToMaps(rows)
}

func (m *Manager) buildFilterQuery(params FilterParams) (string, []interface{}) {
	query := "SELECT * FROM data WHERE 1=1"
	args := []interface{}{}

	// Agregar filtros
	for key, value := range params.Filters {
		if value == nil || value == "" || value == "Todas" {
			continue
		}

		// Escapar nombre de la columna
		safeKey := fmt.Sprintf(`"%s"`, key)

		// Si es array (multiples valores), usar IN
		if arr, ok := value.([]interface{}); ok {
			if len(arr) > 0 {
				placeholders := make([]string, len(arr))
				for i, v := range arr {
					args = append(args, v)
					placeholders[i] = "?"
				}
				query += fmt.Sprintf(" AND %s IN (%s)", safeKey, strings.Join(placeholders, ","))
			}
		} else {
			//  Valor único
			query += fmt.Sprintf(" AND %s = ?", safeKey)
			args = append(args, value)
		}
	}

	// Limit y Offset
	if params.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", params.Limit)
	}
	if params.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", params.Offset)
	}

	return query, args
}

// rowsToMaps convierte un sql.Rows a slice de maps
func (m *Manager) rowsToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}

	for rows.Next() {
		// Crear slice de interfaces para escanear
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))

		for i := range values {
			pointers[i] = &values[i]
		}

		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}

		// Crear map
		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			// Convertir []byte a string
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		result = append(result, row)
	}

	return result, rows.Err()
}

// GetAvailableFilters obtiene valores únicos para los filtros
func (m *Manager) GetAvailableFilters(ctx context.Context, uuid string) (map[string]interface{}, error) {
	conn, err := m.GetConnection(ctx, uuid)
	if err != nil {
		return nil, err
	}

	// Obtener columnas
	columns, err := m.getColumns(ctx, conn)
	if err != nil {
		return nil, err
	}

	filters := make(map[string]interface{})

	// Para cada columna, determinar si es categórica
	for _, col := range columns {
		// Contar valores distintos
		var distinctCount int
		query := fmt.Sprintf(`SELECT COUNT(DISTINCT "%s") FROM data`, col.Name)
		if err := conn.QueryRowContext(ctx, query).Scan(&distinctCount); err != nil {
			continue
		}

		// Si tiene menos de 100 valores únicos, es categórica
		if distinctCount < 100 && distinctCount > 0 {
			values, err := m.getDistinctValues(ctx, conn, col.Name)
			if err != nil {
				continue
			}
			filters[col.Name] = values
		}
	}
	// Obtener rangos de fechas
	dateColumns := m.getDateColumns(columns)
	if len(dateColumns) > 0 {
		for _, dateCol := range dateColumns {
			var minDate, maxDate string
			query := fmt.Sprintf(`SELECT MIN("%s"), MAX("%s") FROM data`, dateCol, dateCol)
			if err := conn.QueryRowContext(ctx, query).Scan(&minDate, &maxDate); err != nil {
				continue
			}
			filters[dateCol+"_range"] = map[string]string{
				"min": minDate,
				"max": maxDate,
			}
		}
	}
	return filters, nil
}

type ColumnInfo struct {
	Name string
	Type string
}

func (m *Manager) getColumns(ctx context.Context, conn *sql.DB) ([]ColumnInfo, error) {
	rows, err := conn.QueryContext(ctx, "PRAGMA table_info('data')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var cid int
		var col ColumnInfo
		var notnull, pk int
		var dfltValue interface{}

		if err := rows.Scan(&cid, &col.Name, &col.Type, &notnull, &dfltValue, &pk); err != nil {
			continue
		}
		columns = append(columns, col)
	}

	return columns, nil
}

func (m *Manager) getDistinctValues(ctx context.Context, conn *sql.DB, column string) ([]string, error) {
	query := fmt.Sprintf(`SELECT DISTINCT "%s" FROM data WHERE "%s" IS NOT NULL ORDER BY  "%s" LIMIT 1000`, column, column, column)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			continue
		}
		values = append(values, value)
	}
	return values, nil
}

func (m *Manager) getDateColumns(columns []ColumnInfo) []string {
	var dateColumns []string
	for _, col := range columns {
		colLower := strings.ToLower(col.Name)
		if strings.Contains(colLower, "fecha") || strings.Contains(colLower, "date") {
			dateColumns = append(dateColumns, col.Name)
		}
	}
	return dateColumns
}
