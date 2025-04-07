package storage

// QueryPlan represents a query execution plan
type QueryPlan interface {
	Explain() string
	Execute() (Cursor, error)
	Optimize() QueryPlan
}

// QueryPlanner creates query execution plans
type QueryPlanner interface {
	CreatePlan(filter Filter, fields []string) QueryPlan
}
