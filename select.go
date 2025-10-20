package squirrel

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lann/builder"
)

type selectData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           BaseRunner
	Prefixes          []Sqlizer
	CTEs              []Sqlizer
	Options           []string
	Columns           []Sqlizer
	From              Sqlizer
	Joins             []Sqlizer
	Compounds         []Sqlizer
	WhereParts        []Sqlizer
	GroupBys          []string
	HavingParts       []Sqlizer
	OrderByParts      []Sqlizer
	Limit             string
	Offset            string
	Suffixes          []Sqlizer
}

func (d *selectData) Exec() (sql.Result, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return ExecWith(d.RunWith, d)
}

func (d *selectData) Query() (*sql.Rows, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}
	return QueryWith(d.RunWith, d)
}

func (d *selectData) QueryRow() RowScanner {
	if d.RunWith == nil {
		return &Row{err: RunnerNotSet}
	}
	queryRower, ok := d.RunWith.(QueryRower)
	if !ok {
		return &Row{err: RunnerNotQueryRunner}
	}
	return QueryRowWith(queryRower, d)
}

func (d *selectData) ToSql() (sqlStr string, args []interface{}, err error) {
	sqlStr, args, err = d.toSqlRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *selectData) toSqlRaw() (sqlStr string, args []interface{}, err error) {
	if len(d.Columns) == 0 {
		err = fmt.Errorf("select statements must have at least one result column")
		return
	}

	sql := &bytes.Buffer{}

	if len(d.Prefixes) > 0 {
		args, err = appendToSql(d.Prefixes, sql, " ", args)
		if err != nil {
			return
		}

		sql.WriteString(" ")
	}

	if len(d.CTEs) > 0 {
		sql.WriteString("WITH ")
		args, err = appendToSql(d.CTEs, sql, ", ", args)
		if err != nil {
			return
		}
		sql.WriteString(" ")
	}

	sql.WriteString("SELECT ")

	if len(d.Options) > 0 {
		sql.WriteString(strings.Join(d.Options, " "))
		sql.WriteString(" ")
	}

	if len(d.Columns) > 0 {
		args, err = appendToSql(d.Columns, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if d.From != nil {
		sql.WriteString(" FROM ")
		args, err = appendToSql([]Sqlizer{d.From}, sql, "", args)
		if err != nil {
			return
		}
	}

	if len(d.Joins) > 0 {
		sql.WriteString(" ")
		args, err = appendToSql(d.Joins, sql, " ", args)
		if err != nil {
			return
		}
	}

	if len(d.WhereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendToSql(d.WhereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.GroupBys) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(d.GroupBys, ", "))
	}

	if len(d.HavingParts) > 0 {
		sql.WriteString(" HAVING ")
		args, err = appendToSql(d.HavingParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.Compounds) > 0 {
		sql.WriteString(" ")
		args, err = appendToSql(d.Compounds, sql, " ", args)
		if err != nil {
			return
		}
	}

	if len(d.OrderByParts) > 0 {
		sql.WriteString(" ORDER BY ")
		args, err = appendToSql(d.OrderByParts, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if len(d.Limit) > 0 {
		sql.WriteString(" LIMIT ")
		sql.WriteString(d.Limit)
	}

	if len(d.Offset) > 0 {
		sql.WriteString(" OFFSET ")
		sql.WriteString(d.Offset)
	}

	if len(d.Suffixes) > 0 {
		sql.WriteString(" ")

		args, err = appendToSql(d.Suffixes, sql, " ", args)
		if err != nil {
			return
		}
	}

	sqlStr = sql.String()
	return
}

// Builder

// SelectBuilder builds SQL SELECT statements.
type SelectBuilder builder.Builder

func init() {
	builder.Register(SelectBuilder{}, selectData{})
}

type selectJoinPart struct {
	joinType string
	target   Sqlizer
	alias    string
	onClause interface{}
	onArgs   []interface{}
}

func newSelectJoinPart(joinType string, sel SelectBuilder, alias string, onClause interface{}, onArgs []interface{}) selectJoinPart {
	sel = sel.PlaceholderFormat(Question)
	return selectJoinPart{
		joinType: joinType,
		target:   Alias(sel, alias),
		alias:    alias,
		onClause: onClause,
		onArgs:   onArgs,
	}
}

func (p selectJoinPart) ToSql() (string, []interface{}, error) {
	if strings.TrimSpace(p.joinType) == "" {
		return "", nil, fmt.Errorf("join type must not be empty")
	}
	if strings.TrimSpace(p.alias) == "" {
		return "", nil, fmt.Errorf("join alias must not be empty")
	}

	targetSql, targetArgs, err := nestedToSql(p.target)
	if err != nil {
		return "", nil, err
	}

	sql := p.joinType + " " + targetSql
	args := targetArgs

	if p.onClause == nil {
		if len(p.onArgs) > 0 {
			return "", nil, fmt.Errorf("join ON clause arguments provided without an ON clause")
		}
		return sql, args, nil
	}

	var onSql string
	var onArgs []interface{}

	switch on := p.onClause.(type) {
	case string:
		if strings.TrimSpace(on) == "" {
			if len(p.onArgs) > 0 {
				return "", nil, fmt.Errorf("join ON clause arguments provided without an ON clause")
			}
			return sql, args, nil
		}
		onSqlizer := Expr(on, p.onArgs...)
		onSql, onArgs, err = onSqlizer.ToSql()
		if err != nil {
			return "", nil, err
		}
	case Sqlizer:
		if len(p.onArgs) > 0 {
			return "", nil, fmt.Errorf("join ON clause arguments must be empty when ON clause is a Sqlizer")
		}
		onSql, onArgs, err = nestedToSql(on)
		if err != nil {
			return "", nil, err
		}
	default:
		return "", nil, fmt.Errorf("unsupported join ON clause type %T", p.onClause)
	}

	if onSql != "" {
		sql += " ON " + onSql
		args = append(args, onArgs...)
	}

	return sql, args, nil
}

type compoundSelectPart struct {
	operator string
	query    SelectBuilder
}

func newCompoundSelectPart(operator string, sel SelectBuilder) compoundSelectPart {
	sel = sel.PlaceholderFormat(Question)
	return compoundSelectPart{
		operator: operator,
		query:    sel,
	}
}

func (p compoundSelectPart) ToSql() (string, []interface{}, error) {
	if strings.TrimSpace(p.operator) == "" {
		return "", nil, fmt.Errorf("compound operator must not be empty")
	}
	sql, args, err := nestedToSql(p.query)
	if err != nil {
		return "", nil, err
	}
	if strings.TrimSpace(sql) == "" {
		return "", nil, fmt.Errorf("compound SELECT must not be empty")
	}
	return p.operator + " " + sql, args, nil
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b SelectBuilder) PlaceholderFormat(f PlaceholderFormat) SelectBuilder {
	return builder.Set(b, "PlaceholderFormat", f).(SelectBuilder)
}

// Runner methods

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
// For most cases runner will be a database connection.
//
// Internally we use this to mock out the database connection for testing.
func (b SelectBuilder) RunWith(runner BaseRunner) SelectBuilder {
	return setRunWith(b, runner).(SelectBuilder)
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b SelectBuilder) Exec() (sql.Result, error) {
	data := builder.GetStruct(b).(selectData)
	return data.Exec()
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b SelectBuilder) Query() (*sql.Rows, error) {
	data := builder.GetStruct(b).(selectData)
	return data.Query()
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b SelectBuilder) QueryRow() RowScanner {
	data := builder.GetStruct(b).(selectData)
	return data.QueryRow()
}

// Scan is a shortcut for QueryRow().Scan.
func (b SelectBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// SQL methods

// ToSql builds the query into a SQL string and bound args.
func (b SelectBuilder) ToSql() (string, []interface{}, error) {
	data := builder.GetStruct(b).(selectData)
	return data.ToSql()
}

func (b SelectBuilder) toSqlRaw() (string, []interface{}, error) {
	data := builder.GetStruct(b).(selectData)
	return data.toSqlRaw()
}

// MustSql builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b SelectBuilder) MustSql() (string, []interface{}) {
	sql, args, err := b.ToSql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b SelectBuilder) Prefix(sql string, args ...interface{}) SelectBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b SelectBuilder) PrefixExpr(expr Sqlizer) SelectBuilder {
	return builder.Append(b, "Prefixes", expr).(SelectBuilder)
}

// Distinct adds a DISTINCT clause to the query.
func (b SelectBuilder) Distinct() SelectBuilder {
	return b.Options("DISTINCT")
}

// Options adds select option to the query
func (b SelectBuilder) Options(options ...string) SelectBuilder {
	return builder.Extend(b, "Options", options).(SelectBuilder)
}

// With adds a non-recursive CTE to the query.
func (b SelectBuilder) With(alias string, expr Sqlizer) SelectBuilder {
	return b.WithCTE(CTE{Alias: alias, ColumnList: []string{}, Recursive: false, Expression: expr})
}

// WithRecursive adds a recursive CTE to the query.
func (b SelectBuilder) WithRecursive(alias string, expr Sqlizer) SelectBuilder {
	return b.WithCTE(CTE{Alias: alias, ColumnList: []string{}, Recursive: true, Expression: expr})
}

// WithCTE adds an arbitrary Sqlizer to the query.
// The sqlizer will be sandwiched between the keyword WITH and, if there's more than one CTE, a comma.
func (b SelectBuilder) WithCTE(cte Sqlizer) SelectBuilder {
	return builder.Append(b, "CTEs", cte).(SelectBuilder)
}

// Columns adds result columns to the query.
func (b SelectBuilder) Columns(columns ...string) SelectBuilder {
	parts := make([]interface{}, 0, len(columns))
	for _, str := range columns {
		parts = append(parts, newPart(str))
	}
	return builder.Extend(b, "Columns", parts).(SelectBuilder)
}

// RemoveColumns remove all columns from query.
// Must add a new column with Column or Columns methods, otherwise
// return a error.
func (b SelectBuilder) RemoveColumns() SelectBuilder {
	return builder.Delete(b, "Columns").(SelectBuilder)
}

// Column adds a result column to the query.
// Unlike Columns, Column accepts args which will be bound to placeholders in
// the columns string, for example:
//
//	Column("IF(col IN ("+squirrel.Placeholders(3)+"), 1, 0) as col", 1, 2, 3)
func (b SelectBuilder) Column(column interface{}, args ...interface{}) SelectBuilder {
	return builder.Append(b, "Columns", newPart(column, args...)).(SelectBuilder)
}

// From sets the FROM clause of the query.
func (b SelectBuilder) From(from string) SelectBuilder {
	return builder.Set(b, "From", newPart(from)).(SelectBuilder)
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b SelectBuilder) FromSelect(from SelectBuilder, alias string) SelectBuilder {
	// Prevent misnumbered parameters in nested selects (#183).
	from = from.PlaceholderFormat(Question)
	return builder.Set(b, "From", Alias(from, alias)).(SelectBuilder)
}

// UnionSelect sets one or more union SelectBuilders which remove duplicate rows.
// --> UNION combines the result from multiple SELECT statements into a single result set.
func (b SelectBuilder) UnionSelect(unions ...SelectBuilder) SelectBuilder {
	return b.unionSelectWithType("UNION", unions...)
}

// UnionAllSelect sets one or more union SelectBuilders which include duplicate rows.
// --> UNION ALL combines the result from multiple SELECT statements into a single result set.
func (b SelectBuilder) UnionAllSelect(unions ...SelectBuilder) SelectBuilder {
	return b.unionSelectWithType("UNION ALL", unions...)
}

// JoinClause adds a join clause to the query.
func (b SelectBuilder) JoinClause(pred interface{}, args ...interface{}) SelectBuilder {
	return builder.Append(b, "Joins", newPart(pred, args...)).(SelectBuilder)
}

// Join adds a JOIN clause to the query.
func (b SelectBuilder) Join(join string, rest ...interface{}) SelectBuilder {
	return b.JoinClause("JOIN "+join, rest...)
}

// JoinSelect adds a JOIN clause that wraps the provided SelectBuilder as a subquery with an alias and ON clause.
func (b SelectBuilder) JoinSelect(sel SelectBuilder, alias string, onClause interface{}, args ...interface{}) SelectBuilder {
	return b.JoinClause(newSelectJoinPart("JOIN", sel, alias, onClause, args))
}

// LeftJoin adds a LEFT JOIN clause to the query.
func (b SelectBuilder) LeftJoin(join string, rest ...interface{}) SelectBuilder {
	return b.JoinClause("LEFT JOIN "+join, rest...)
}

// LeftJoinSelect adds a LEFT JOIN clause that wraps the provided SelectBuilder as a subquery with an alias and ON clause.
func (b SelectBuilder) LeftJoinSelect(sel SelectBuilder, alias string, onClause interface{}, args ...interface{}) SelectBuilder {
	return b.JoinClause(newSelectJoinPart("LEFT JOIN", sel, alias, onClause, args))
}

// RightJoin adds a RIGHT JOIN clause to the query.
func (b SelectBuilder) RightJoin(join string, rest ...interface{}) SelectBuilder {
	return b.JoinClause("RIGHT JOIN "+join, rest...)
}

// RightJoinSelect adds a RIGHT JOIN clause that wraps the provided SelectBuilder as a subquery with an alias and ON clause.
func (b SelectBuilder) RightJoinSelect(sel SelectBuilder, alias string, onClause interface{}, args ...interface{}) SelectBuilder {
	return b.JoinClause(newSelectJoinPart("RIGHT JOIN", sel, alias, onClause, args))
}

// InnerJoin adds a INNER JOIN clause to the query.
func (b SelectBuilder) InnerJoin(join string, rest ...interface{}) SelectBuilder {
	return b.JoinClause("INNER JOIN "+join, rest...)
}

// InnerJoinSelect adds an INNER JOIN clause that wraps the provided SelectBuilder as a subquery with an alias and ON clause.
func (b SelectBuilder) InnerJoinSelect(sel SelectBuilder, alias string, onClause interface{}, args ...interface{}) SelectBuilder {
	return b.JoinClause(newSelectJoinPart("INNER JOIN", sel, alias, onClause, args))
}

// CrossJoin adds a CROSS JOIN clause to the query.
func (b SelectBuilder) CrossJoin(join string, rest ...interface{}) SelectBuilder {
	return b.JoinClause("CROSS JOIN "+join, rest...)
}

// Union adds UNION to the query. (duplicate rows are removed)
func (b SelectBuilder) Union(join string, rest ...interface{}) SelectBuilder {
	return builder.Append(b, "Compounds", newPart("UNION "+join, rest...)).(SelectBuilder)
}

// UnionAll adds UNION ALL to the query. (includes all matching rows)
func (b SelectBuilder) UnionAll(join string, rest ...interface{}) SelectBuilder {
	return builder.Append(b, "Compounds", newPart("UNION ALL "+join, rest...)).(SelectBuilder)
}

func (b SelectBuilder) unionSelectWithType(operator string, unions ...SelectBuilder) SelectBuilder {
	if len(unions) == 0 {
		return b
	}
	for _, union := range unions {
		union = union.PlaceholderFormat(Question)
		b = builder.Append(b, "Compounds", newCompoundSelectPart(operator, union)).(SelectBuilder)
	}
	return b
}

// Where adds an expression to the WHERE clause of the query.
//
// Expressions are ANDed together in the generated SQL.
//
// Where accepts several types for its pred argument:
//
// nil OR "" - ignored.
//
// string - SQL expression.
// If the expression has SQL placeholders then a set of arguments must be passed
// as well, one for each placeholder.
//
// map[string]interface{} OR Eq - map of SQL expressions to values. Each key is
// transformed into an expression like "<key> = ?", with the corresponding value
// bound to the placeholder. If the value is nil, the expression will be "<key>
// IS NULL". If the value is an array or slice, the expression will be "<key> IN
// (?,?,...)", with one placeholder for each item in the value. These expressions
// are ANDed together.
//
// Where will panic if pred isn't any of the above types.
func (b SelectBuilder) Where(pred interface{}, args ...interface{}) SelectBuilder {
	if pred == nil || pred == "" {
		return b
	}
	return builder.Append(b, "WhereParts", newWherePart(pred, args...)).(SelectBuilder)
}

// GroupBy adds GROUP BY expressions to the query.
func (b SelectBuilder) GroupBy(groupBys ...string) SelectBuilder {
	return builder.Extend(b, "GroupBys", groupBys).(SelectBuilder)
}

// Having adds an expression to the HAVING clause of the query.
//
// See Where.
func (b SelectBuilder) Having(pred interface{}, rest ...interface{}) SelectBuilder {
	return builder.Append(b, "HavingParts", newWherePart(pred, rest...)).(SelectBuilder)
}

// OrderByClause adds ORDER BY clause to the query.
func (b SelectBuilder) OrderByClause(pred interface{}, args ...interface{}) SelectBuilder {
	return builder.Append(b, "OrderByParts", newPart(pred, args...)).(SelectBuilder)
}

// OrderBy adds ORDER BY expressions to the query.
func (b SelectBuilder) OrderBy(orderBys ...string) SelectBuilder {
	for _, orderBy := range orderBys {
		b = b.OrderByClause(orderBy)
	}

	return b
}

// Limit sets a LIMIT clause on the query.
func (b SelectBuilder) Limit(limit uint64) SelectBuilder {
	return builder.Set(b, "Limit", fmt.Sprintf("%d", limit)).(SelectBuilder)
}

// Limit ALL allows to access all records with limit
func (b SelectBuilder) RemoveLimit() SelectBuilder {
	return builder.Delete(b, "Limit").(SelectBuilder)
}

// Offset sets a OFFSET clause on the query.
func (b SelectBuilder) Offset(offset uint64) SelectBuilder {
	return builder.Set(b, "Offset", fmt.Sprintf("%d", offset)).(SelectBuilder)
}

// RemoveOffset removes OFFSET clause.
func (b SelectBuilder) RemoveOffset() SelectBuilder {
	return builder.Delete(b, "Offset").(SelectBuilder)
}

// Suffix adds an expression to the end of the query
func (b SelectBuilder) Suffix(sql string, args ...interface{}) SelectBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b SelectBuilder) SuffixExpr(expr Sqlizer) SelectBuilder {
	return builder.Append(b, "Suffixes", expr).(SelectBuilder)
}
