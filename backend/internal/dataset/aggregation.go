package dataset

import "context"

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
	return nil, nil
}

func (m *Manager) buildAggregationQuery(params AggregationParams) (string, []interface{}) {
	return "", nil
}

func (m *Manager) buildAggregationFunction(agg, varAgg string) string {
	return ""
}

func (m *Manager) formatDateColumn(col, format string) string {
	return ""
}

func (m *Manager) GetStats(ctx context.Context, uuid, column string, filters map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
}

func (m *Manager) GetTopValues(ctx context.Context, uuid, column string, limit int, filters map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
}

func (m *Manager) GetTimeSeries(ctx context.Context, uuid, dateColumn, valueColumn, aggFunc string, filters map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
}

func (m *Manager) GetCrossTab(ctx context.Context, uuid, rowVar, colVar, valueVar, aggFunc string, filters map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
}

func (m *Manager) GetPercentiles(ctx context.Context, uuid, column string, percentiles []float64, filters map[string]interface{}) (map[string]float64, error) {
	return nil, nil
}

func (m *Manager) GetCorrelation(ctx context.Context, uuid, col1, col2 string, filters map[string]interface{}) (float64, error) {
	return 0.0, nil
}
