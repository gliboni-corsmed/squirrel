package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCaseWithVal(t *testing.T) {
	caseStmt := Case("number").
		When("1", "one").
		When("2", "two").
		Else(Expr("?", "big number"))

	qb := Select().
		Column(caseStmt).
		From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE number " +
		"WHEN 1 THEN one " +
		"WHEN 2 THEN two " +
		"ELSE ? " +
		"END " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{"big number"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithComplexVal(t *testing.T) {
	caseStmt := Case("? > ?", 10, 5).
		When("true", "'T'")

	qb := Select().
		Column(Alias(caseStmt, "complexCase")).
		From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT (CASE ? > ? " +
		"WHEN true THEN 'T' " +
		"END) AS complexCase " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{10, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoVal(t *testing.T) {
	caseStmt := Case().
		When(Eq{"x": 0}, "x is zero").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE " +
		"WHEN x = ? THEN x is zero " +
		"WHEN x > ? THEN CONCAT('x is greater than ', ?) " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithExpr(t *testing.T) {
	caseStmt := Case(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE x = ? " +
		"WHEN true THEN ? " +
		"ELSE 42 " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{true, "it's true!"}
	assert.Equal(t, expectedArgs, args)
}

func TestMultipleCase(t *testing.T) {
	caseStmtNoval := Case(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")
	caseStmtExpr := Case().
		When(Eq{"x": 0}, "'x is zero'").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().
		Column(Alias(caseStmtNoval, "case_noval")).
		Column(Alias(caseStmtExpr, "case_expr")).
		From("table")

	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT " +
		"(CASE x = ? WHEN true THEN ? ELSE 42 END) AS case_noval, " +
		"(CASE WHEN x = ? THEN 'x is zero' WHEN x > ? THEN CONCAT('x is greater than ', ?) END) AS case_expr " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{
		true, "it's true!",
		0, 1, 2,
	}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoWhenClause(t *testing.T) {
	caseStmt := Case("something").
		Else("42")

	qb := Select().Column(caseStmt).From("table")

	_, _, err := qb.ToSql()

	assert.Error(t, err)

	assert.Equal(t, "case expression must contain at lease one WHEN clause", err.Error())
}

func TestCaseBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestCaseBuilderMustSql should have panicked!")
		}
	}()
	Case("").MustSql()
}

func TestCaseWithIntegerValues(t *testing.T) {
	// Test for issue #388 - CASE WHEN should accept integer values
	caseStmt := Case("order_no").
		When("'ORD001'", 500).
		When("'ORD002'", 600).
		Else(0)

	sql, args, err := Update("orders").
		Set("amount", caseStmt).
		Where(Eq{"status": "pending"}).
		ToSql()

	assert.NoError(t, err)

	expectedSql := "UPDATE orders SET amount = CASE order_no " +
		"WHEN 'ORD001' THEN ? " +
		"WHEN 'ORD002' THEN ? " +
		"ELSE ? " +
		"END " +
		"WHERE status = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{500, 600, 0, "pending"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithMixedTypes(t *testing.T) {
	// Test CASE with mixed types (strings, integers, floats, booleans)
	caseStmt := Case().
		When(Eq{"type": "A"}, 100).
		When(Eq{"type": "B"}, 200.5).
		When(Eq{"type": "C"}, true).
		Else("default")

	qb := Select().Column(Alias(caseStmt, "value")).From("table")
	sql, args, err := qb.ToSql()

	assert.NoError(t, err)

	expectedSql := "SELECT (CASE " +
		"WHEN type = ? THEN ? " +
		"WHEN type = ? THEN ? " +
		"WHEN type = ? THEN ? " +
		"ELSE default " +
		"END) AS value " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{"A", 100, "B", 200.5, "C", true}
	assert.Equal(t, expectedArgs, args)
}
