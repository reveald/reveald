package reveald

import "github.com/olivere/elastic/v7"

// QueryBuilder is a construct to build a
// dynamic Elasticsearch query
type QueryBuilder struct {
	request         *Request
	aggs            map[string]elastic.Aggregation
	root            *elastic.BoolQuery
	postFilter      *elastic.BoolQuery
	indices         []string
	selection       *DocumentSelector
	scriptedFields  []*elastic.ScriptField
	runtimeMappings elastic.RuntimeMappings
	docValueFields  []string
}

// NewQueryBuilder returns a new base query for
// a set of indices
func NewQueryBuilder(r *Request, indices ...string) *QueryBuilder {
	return &QueryBuilder{
		request:         r,
		aggs:            make(map[string]elastic.Aggregation),
		root:            elastic.NewBoolQuery(),
		indices:         indices,
		selection:       nil,
		scriptedFields:  nil,
		runtimeMappings: elastic.RuntimeMappings{},
		docValueFields:  []string{},
	}
}

// Request returns the current Request instance
func (qb *QueryBuilder) Request() *Request {
	return qb.request
}

// Indices returns the targets for the Elasticsearch
// query
func (qb *QueryBuilder) Indices() []string {
	return qb.indices
}

// SetIndices changes sets the query builder indices to be used
// while searching in Elasticsearch.
func (qb *QueryBuilder) SetIndices(indices ...string) {
	qb.indices = indices
}

// With filters documents based on the specified query
func (qb *QueryBuilder) With(query elastic.Query) {
	qb.root.Must(query)
}

// Without filters document based on an inverted
// query
func (qb *QueryBuilder) Without(query elastic.Query) {
	qb.root.MustNot(query)
}

// Boost document based on specified query
func (qb *QueryBuilder) Boost(query elastic.Query) {
	qb.root.Should(query)
}

// PostFilterWith post filters documents based on the specified query
func (qb *QueryBuilder) PostFilterWith(query elastic.Query) {
	if qb.postFilter == nil {
		qb.postFilter = elastic.NewBoolQuery()
	}
	qb.postFilter.Must(query)
}

// PostFilterWithout post filters document based on an inverted
// query
func (qb *QueryBuilder) PostFilterWithout(query elastic.Query) {
	if qb.postFilter == nil {
		qb.postFilter = elastic.NewBoolQuery()
	}
	qb.postFilter.MustNot(query)
}

// PostFilterBoost postfilter document based on specified query
func (qb *QueryBuilder) PostFilterBoost(query elastic.Query) {
	if qb.postFilter == nil {
		qb.postFilter = elastic.NewBoolQuery()
	}
	qb.postFilter.Should(query)
}

// Selection returns a DocumentSelector specifying
// pagination and sort
func (qb *QueryBuilder) Selection() *DocumentSelector {
	if qb.selection == nil {
		qb.selection = NewDocumentSelector()
	}

	return qb.selection
}

// Aggregation adds a new aggregation result to the
// Elasticsearch query
func (qb *QueryBuilder) Aggregation(name string, agg elastic.Aggregation) {
	qb.aggs[name] = agg
}

// RawQuery returns the current Elasticsearch query
func (qb *QueryBuilder) RawQuery() elastic.Query {
	return qb.root
}

// WithScriptedFields specifies scripted fields to add to query
func (qb *QueryBuilder) WithScriptedField(scriptedField *elastic.ScriptField) {
	qb.scriptedFields = append(qb.scriptedFields, scriptedField)
}

// WithRuntimeMappings specifies optional runtime mappings.
func (qb *QueryBuilder) WithRuntimeMappings(runtimeMappings elastic.RuntimeMappings) {
	for k, v := range runtimeMappings {
		qb.runtimeMappings[k] = v
	}
}

// DocvalueFields adds one or more fields to load from the field data cache
// and return as part of the search request.
func (qb *QueryBuilder) DocvalueFields(docvalueFields ...string) {
	qb.docValueFields = append(qb.docValueFields, docvalueFields...)
}

// Build creates the final Elasticsearch query, containing
// queries, aggregations, sort options, and pagination settings
func (qb *QueryBuilder) Build() *elastic.SearchSource {
	src := elastic.NewSearchSource()

	src = src.RuntimeMappings(qb.runtimeMappings)
	src = src.DocvalueFields(qb.docValueFields...)

	query := src.Query(qb.root).ScriptFields(qb.scriptedFields...)

	if qb.postFilter != nil {
		query.PostFilter(qb.postFilter)
	}

	for name, agg := range qb.aggs {
		query.Aggregation(name, agg)
	}

	if qb.selection == nil {
		return src
	}

	ctx := &elastic.FetchSourceContext{}
	if len(qb.selection.inclusions) > 0 {
		ctx.Include(qb.selection.inclusions...)
	}
	if len(qb.selection.exclusions) > 0 {
		ctx.Exclude(qb.selection.exclusions...)
	}

	ctx.SetFetchSource(true)
	src = src.
		FetchSourceContext(ctx).
		Size(qb.selection.pageSize).
		From(qb.selection.offset)

	if qb.selection.sort != nil {
		src = src.SortBy(qb.selection.sort)
	}

	return src
}
