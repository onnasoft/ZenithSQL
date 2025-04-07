package storage

// Filter provides data filtering capabilities
type Filter interface {
	Equals(field string, value interface{}) Filter
	NotEquals(field string, value interface{}) Filter
	Range(field string, min, max interface{}) Filter
	Contains(field string, value interface{}) Filter
	StartsWith(field string, value interface{}) Filter
	EndsWith(field string, value interface{}) Filter
	IsNull(field string) Filter
	IsNotNull(field string) Filter
	And(filters ...Filter) Filter
	Or(filters ...Filter) Filter
	Not(filter Filter) Filter

	Apply(data map[string]interface{}) bool
}
