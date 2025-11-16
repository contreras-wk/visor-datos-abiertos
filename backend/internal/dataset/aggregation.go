package dataset

import (
	"context"
	"fmt"
	"strings"
)

type AggregationParams struct {
	Filters    map[string]interface{}
	Agg        string
	VarAgg     string
	GroupBy    []string
	OrderBy    string
	OrderDir   string
	Limit      int
	DateFormat string
}

func (m *Manager) GetAggregatedData(ctx context.Context, uuid string, params AggregationParams) ([]map[string]interface{}, error) {
	// Obtener conexión db
	conn, err := m.GetConnection(ctx, uuid)
	if err != nil {
		return nil, err
	}

	// Construir query de agregación
	query, args := m.buildAggregationQuery(params)

	// Ejecutar query
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error ejecutando agregación: %w", err)
	}
	defer rows.Close()

	// Convertir a slice de maps
	return m.rowsToMaps(rows)
}

// buildAggregationQuery construye query SQL de agregación
func (m *Manager) buildAggregationQuery(params AggregationParams) (string, []interface{}) {
	var query strings.Builder
	args := []interface{}{}

	// SELECT clause
	query.WriteString("SELECT ")

	// Columnas de agrupación con formato de fecha si aplica
	selectCols := []string{}
	for _, col := range params.GroupBy {
		formattedCol := m.formatDateColumn(col, params.DateFormat)
		selectCols = append(selectCols, formattedCol)
	}

	if len(selectCols) > 0 {
		query.WriteString(strings.Join(selectCols, ", "))
		query.WriteString(", ")
	}

	// Funciones de agregación
	aggFunc := m.buildAggregationFunction(params.Agg, params.VarAgg)
	query.WriteString(aggFunc)
	query.WriteString(" as total")

	// FROM clause (filtros)
	query.WriteString(" FROM data")

	// WHERE clause (filtros)
	if len(params.Filters) > 0 {
		query.WriteString(" WHERE ")
		whereClauses := []string{}

		for key, value := range params.Filters {
			if value == nil || value == "" || value == "Todas" {
				continue
			}

			safekey := fmt.Sprintf(`"%s"`, key)

			//  Si es un array, usar IN
			if arr, ok := value.([]interface{}); ok {
				if len(arr) > 0 {
					placeholders := make([]string, len(arr))
					for i, v := range arr {
						args = append(args, v)
						placeholders[i] = "?"
					}
					whereClauses = append(whereClauses, fmt.Sprintf("%s IN (%s)", safekey, strings.Join(placeholders, ", ")))
				}
			} else {
				whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", safekey))
				args = append(args, value)
			}
		}

		if len(whereClauses) > 0 {
			query.WriteString(strings.Join(whereClauses, " AND "))
		} else {
			query.WriteString("1=1")
		}
	}

	// GROUP BY  clause
	if len(params.GroupBy) > 0 {
		query.WriteString(" GROUP BY ")

		//  Usar los números de columna para agrupar (más simple con formateo de fechas)
		groupCols := []string{}
		for i := range params.GroupBy {
			groupCols = append(groupCols, fmt.Sprintf("%d", i+1))
		}
		query.WriteString(strings.Join(groupCols, ", "))
	}

	// ORDER BY clause
	if params.OrderBy != "" {
		query.WriteString(fmt.Sprintf(" ORDER BY \"%s\"", params.OrderBy))
		if params.OrderDir != "" && strings.ToLower(params.OrderDir) == "asc" {
			query.WriteString(" ASC")
		} else {
			query.WriteString(" DESC")
		}
	} else if len(params.GroupBy) > 0 {
		// Por defecto ordenar por la primera columna de agrupación
		query.WriteString(" ORDER BY 1")
	} else {
		// Si no hay GROUP BY, ordenar por total descendente
		query.WriteString(" ORDER BY total DESC")
	}

	// LIMIT clauses
	if params.Limit > 0 {
		query.WriteString(fmt.Sprintf(" LIMIT %d", params.Limit))
	}

	return query.String(), args
}

// buildAggregationFunction construye la función de agregación SQL
func (m *Manager) buildAggregationFunction(agg, varAgg string) string {
	agg = strings.ToLower(agg)

	switch agg {
	case "count":
		return "COUNT(*)"
	case "sum":
		if varAgg == "" {
			return "COUNT(*)" // Fallback
		}
		return fmt.Sprintf(`SUM("%s")`, varAgg)
	case "avg", "mean":
		if varAgg == "" {
			return "COUNT(*)" // Fallback
		}
		return fmt.Sprintf(`AVG("%s")`, varAgg)
	case "min":
		if varAgg == "" {
			return "COUNT(*)"
		}
		return fmt.Sprintf(`MIN("%s")`, varAgg)
	case "max":
		if varAgg == "" {
			return "COUNT(*)"
		}
		return fmt.Sprintf(`MAX("%s")`, varAgg)
	case "median":
		if varAgg == "" {
			return "COUNT(*)"
		}
		return fmt.Sprintf(`MEDIAN("%s")`, varAgg)
	case "stddev":
		if varAgg == "" {
			return "COUNT(*)"
		}
		return fmt.Sprintf(`STDDEV("%s")`, varAgg)
	default:
		return "COUNT(*)"
	}
}

// formatDateColumn formatea columna de fecha según el formato solicitado
func (m *Manager) formatDateColumn(col, format string) string {
	colLower := strings.ToLower(col)

	// Verifica si es columna de fecha
	if !strings.Contains(colLower, "fecha") && !strings.Contains(colLower, "date") {
		//  No es fecha, retorna como esta
		return fmt.Sprintf(`"%s"`, col)
	}

	format = strings.ToLower(format)
	safeCol := fmt.Sprintf(`"%s"`, col)

	switch format {
	case "year", "año":
		return fmt.Sprintf("YEAR(%s) as %s", safeCol, col)
	case "month", "mes":
		return fmt.Sprintf("DATE_TRUNC('month', %s) as %s", safeCol, col)
	case "week", "semana":
		return fmt.Sprintf("DATE_TRUNC('week', %s) as %s", safeCol, col)
	case "day", "dia":
		return fmt.Sprintf("DATE_TRUNC('day', %s) as %s", safeCol, col)
	case "quarter", "trimestre":
		return fmt.Sprintf("DATE_TRUNC('quarter', %s) as %s", safeCol, col)
	case "yearmonth", "año-mes":
		return fmt.Sprintf("STRFTIME(%s, '%%Y-%%m') as %s", safeCol, col)
	default:
		// Por defecto se retorna la fecha completa
		return safeCol
	}
}

// GetStats obtiene estadísticas descriptivas de una columna
func (m *Manager) GetStats(ctx context.Context, uuid, column string, filters map[string]interface{}) (map[string]interface{}, error) {
	conn, err := m.GetConnection(ctx, uuid)
	if err != nil {
		return nil, err
	}

	// Construir WHERE clause
	whereClause := "WHERE 1=1"
	args := []interface{}{}

	for key, value := range filters {
		if value == nil || value == "" || value == "Todas" {
			continue
		}
		whereClause += fmt.Sprintf(` AND "%s" = ? `, key)
		args = append(args, value)
	}

	// Query para estadísticas
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) as count,
			COUNT(DISTINCT "%s") as distinct_count,
			MIN("%s") as min,
			MAX("%s") as max,
			AVG("%s") as mean,
			MEDIAN("%s") as median,
			STDDEV("%s") as stddev,
			PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "%s") as q25,
			PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "%s") as q75
		FROM  data
		%s
	`, column, column, column, column, column, column, column, column, whereClause)

	row := conn.QueryRowContext(ctx, query, args...)

	var stats struct {
		Count         int64
		DistinctCount int64
		Min           float64
		Max           float64
		Mean          float64
		Median        float64
		Stddev        float64
		Q25           float64
		Q75           float64
	}

	err = row.Scan(
		&stats.Count,
		&stats.DistinctCount,
		&stats.Min,
		&stats.Max,
		&stats.Mean,
		&stats.Median,
		&stats.Stddev,
		&stats.Q25,
		&stats.Q75,
	)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"count":          stats.Count,
		"distinct_count": stats.DistinctCount,
		"min":            stats.Min,
		"max":            stats.Max,
		"mean":           stats.Mean,
		"median":         stats.Median,
		"stddev":         stats.Stddev,
		"q25":            stats.Q25,
		"q75":            stats.Q75,
		"iqr":            stats.Q75 - stats.Q25,
	}, nil
}

// GetTopValues obtienen los N valores más  frecuentes de una columna
func (m *Manager) GetTopValues(ctx context.Context, uuid, column string, limit int, filters map[string]interface{}) ([]map[string]interface{}, error) {
	conn, err := m.GetConnection(ctx, uuid)
	if err != nil {
		return nil, err
	}

	// Construir WHERE clause
	whereClause := "WHERE 1=1"
	args := []interface{}{}

	for key, value := range filters {
		if value == nil || value == "" || value == "Todas" {
			continue
		}
		whereClause += fmt.Sprintf(` AND "%s" = ?`, key)
		args = append(args, value)
	}

	//  Query
	query := fmt.Sprintf(`
		SELECT
			%s as value,
			COUNT(*) as count,
			COUNT(*) * 100.0 / (SELECT COUNT(*) FROM data %s) as percentage
		FROM data
		%s
		GROUP BY "%s"
		ORDER BY count DESC
		LIMIT %d 
	`, column, whereClause, whereClause, column, limit)

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return m.rowsToMaps(rows)
}

// GetTimeSeries obtiene serie temporal agregada
func (m *Manager) GetTimeSeries(ctx context.Context, uuid, dateColumn, valueColumn, aggFunc string, filters map[string]interface{}) ([]map[string]interface{}, error) {
	params := AggregationParams{
		Filters:    filters,
		Agg:        aggFunc,
		VarAgg:     valueColumn,
		GroupBy:    []string{dateColumn},
		DateFormat: "day",
		OrderBy:    dateColumn,
		OrderDir:   "asc",
	}
	return m.GetAggregatedData(ctx, uuid, params)
}

// GetCrossTab obtiene tabla cruzada (pivot)
func (m *Manager) GetCrossTab(ctx context.Context, uuid, rowVar, colVar, valueVar, aggFunc string, filters map[string]interface{}) ([]map[string]interface{}, error) {
	conn, err := m.GetConnection(ctx, uuid)
	if err != nil {
		return nil, err
	}

	// Construir WHERE clause
	whereClause := "WHERE 1=1"
	args := []interface{}{}

	for key, value := range filters {
		if value == nil || value == "" || value == "Todas" {
			continue
		}
		whereClause += fmt.Sprintf(` AND "%s" = ?`, key)
		args = append(args, value)
	}

	// Determinar función de agregación
	aggFunction := "COUNT(*)"
	if aggFunc != "" && aggFunc != "count" && valueVar != "" {
		switch strings.ToLower(aggFunc) {
		case "sum":
			aggFunction = fmt.Sprintf(`SUM("%s")`, valueVar)
		case "avg", "mean":
			aggFunction = fmt.Sprintf(`AVG("%s")`, valueVar)
		case "min":
			aggFunction = fmt.Sprintf(`MIN("%s")`, valueVar)
		case "max":
			aggFunction = fmt.Sprintf(`MAX("%s")`, valueVar)
		}
	}

	// Query para crosstab usando PIVOT
	query := fmt.Sprintf(`
		SELECT 
			"%s" as row_value,
			"%s" as col_value,
			%s as value
		FROM data
		%s
		GROUP BY "%s", "%s"
		ORDER BY "%s", "%s"
	`, rowVar, colVar, aggFunction, whereClause, rowVar, colVar, rowVar, colVar)

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return m.rowsToMaps(rows)
}

// GetPercentiles obtiene percentiles de una distribución
func (m *Manager) GetPercentiles(ctx context.Context, uuid, column string, percentiles []float64, filters map[string]interface{}) (map[string]float64, error) {
	conn, err := m.GetConnection(ctx, uuid)
	if err != nil {
		return nil, err
	}

	// Construir WHERE clause
	whereClause := "WHERE 1=1"
	args := []interface{}{}

	for key, value := range filters {
		if value == nil || value == "" || value == "Todas" {
			continue
		}
		whereClause += fmt.Sprintf(` AND "%s" = ?`, key)
		args = append(args, value)
	}

	results := make(map[string]float64)

	for _, p := range percentiles {
		query := fmt.Sprintf(`
			SELECT PERCENTILE_CONT(%f) WITHIN GROUP (ORDER BY "%s")
			FROM data
			%s
		`, p, column, whereClause)

		var value float64
		err := conn.QueryRowContext(ctx, query, args...).Scan(&value)
		if err != nil {
			return nil, err
		}

		key := fmt.Sprintf("p%.0f", p*100)
		results[key] = value
	}

	return results, nil
}

// GetCorrelation calcula correlación entre dos variables
func (m *Manager) GetCorrelation(ctx context.Context, uuid, col1, col2 string, filters map[string]interface{}) (float64, error) {
	conn, err := m.GetConnection(ctx, uuid)
	if err != nil {
		return 0.0, err
	}

	// Construir WHERE clause
	whereClause := "WHERE 1=1"
	args := []interface{}{}

	for key, value := range filters {
		if value == nil || value == "" || value == "Todas" {
			continue
		}
		whereClause += fmt.Sprintf(` AND "%s" = ?`, key)
		args = append(args, value)
	}

	query := fmt.Sprintf(`
		SELECT CORR(%s, %s)
		FROM data
		%s
	`, col1, col2, whereClause)

	var correlation float64
	err = conn.QueryRowContext(ctx, query, args...).Scan(&correlation)
	if err != nil {
		return 0, err
	}

	return correlation, nil
}
