package storage

// AggregationFunc defines aggregation functions
type AggregationFunc int

const (
	Count AggregationFunc = iota
	Sum
	Avg
	Min
	Max
	Median
	Mode
	StdDev
	Variance
	CountDistinct
)
