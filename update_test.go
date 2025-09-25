package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateBuilderToSql(t *testing.T) {
	b := Update("").
		Prefix("WITH prefix AS ?", 0).
		Table("a").
		Set("b", Expr("? + 1", 1)).
		SetMap(Eq{"c": 2}).
		Set("c1", Case("status").When("1", "2").When("2", "1")).
		Set("c2", Case().When("a = 2", Expr("?", "foo")).When("a = 3", Expr("?", "bar"))).
		Set("c3", Select("a").From("b")).
		Where("d = ?", 3).
		OrderBy("e").
		Limit(4).
		Offset(5).
		Suffix("RETURNING ?", 6)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"UPDATE a SET b = ? + 1, c = ?, " +
			"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
			"c2 = CASE WHEN a = 2 THEN ? WHEN a = 3 THEN ? END, " +
			"c3 = (SELECT a FROM b) " +
			"WHERE d = ? " +
			"ORDER BY e LIMIT 4 OFFSET 5 " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 2, "foo", "bar", 3, 6}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderToSqlErr(t *testing.T) {
	_, _, err := Update("").Set("x", 1).ToSql()
	assert.Error(t, err)

	_, _, err = Update("x").ToSql()
	assert.Error(t, err)
}

func TestUpdateBuilderWithCTE(t *testing.T) {
	sql, args, err := Update("users").
		With("cte", Select("id").From("active_users")).
		Set("status", "updated").
		Where("id IN (SELECT id FROM cte)").
		ToSql()

	assert.NoError(t, err)
	expectedSql := "WITH cte AS (SELECT id FROM active_users) UPDATE users SET status = ? WHERE id IN (SELECT id FROM cte)"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"updated"}, args)
}

func TestUpdateBuilderWithMultipleCTEs(t *testing.T) {
	sql, args, err := Update("users").
		With("active_cte", Select("id").From("active_users")).
		With("inactive_cte", Select("id").From("inactive_users")).
		Set("status", "processed").
		Where("id IN (SELECT id FROM active_cte) OR id IN (SELECT id FROM inactive_cte)").
		ToSql()

	assert.NoError(t, err)
	expectedSql := "WITH active_cte AS (SELECT id FROM active_users), inactive_cte AS (SELECT id FROM inactive_users) UPDATE users SET status = ? WHERE id IN (SELECT id FROM active_cte) OR id IN (SELECT id FROM inactive_cte)"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"processed"}, args)
}

func TestUpdateBuilderWithRecursiveCTE(t *testing.T) {
	sql, args, err := Update("categories").
		WithRecursive("category_tree", Select("id", "parent_id").From("categories").Where("parent_id IS NULL")).
		Set("level", 1).
		Where("id IN (SELECT id FROM category_tree)").
		ToSql()

	assert.NoError(t, err)
	expectedSql := "WITH RECURSIVE category_tree AS (SELECT id, parent_id FROM categories WHERE parent_id IS NULL) UPDATE categories SET level = ? WHERE id IN (SELECT id FROM category_tree)"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{1}, args)
}

func TestUpdateBuilderWithCTEAndComplexQuery(t *testing.T) {
	cte := CTE{
		Alias:      "user_stats",
		ColumnList: []string{"user_id", "total_orders"},
		Recursive:  false,
		Expression: Select("user_id", "COUNT(*)").From("orders").GroupBy("user_id"),
	}

	sql, args, err := Update("users").
		WithCTE(cte).
		Set("order_count", Expr("(SELECT total_orders FROM user_stats WHERE user_stats.user_id = users.id)")).
		Set("last_updated", "NOW()").
		Where("EXISTS (SELECT 1 FROM user_stats WHERE user_stats.user_id = users.id)").
		ToSql()

	assert.NoError(t, err)
	expectedSql := "WITH user_stats(user_id, total_orders) AS (SELECT user_id, COUNT(*) FROM orders GROUP BY user_id) UPDATE users SET order_count = (SELECT total_orders FROM user_stats WHERE user_stats.user_id = users.id), last_updated = ? WHERE EXISTS (SELECT 1 FROM user_stats WHERE user_stats.user_id = users.id)"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"NOW()"}, args)
}

func TestUpdateBuilderCTEErrorBubblesUp(t *testing.T) {
	// a SELECT with no columns raises an error
	_, _, err := Update("users").
		With("cte", SelectBuilder{}.From("test")).
		Set("x", 1).
		ToSql()

	assert.Error(t, err)
}

func TestUpdateBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestUpdateBuilderMustSql should have panicked!")
		}
	}()
	Update("").MustSql()
}

func TestUpdateBuilderPlaceholders(t *testing.T) {
	b := Update("test").SetMap(Eq{"x": 1, "y": 2})

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "UPDATE test SET x = ?, y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "UPDATE test SET x = $1, y = $2", sql)
}

func TestUpdateBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Update("test").Set("x", 1).RunWith(db)

	expectedSql := "UPDATE test SET x = ?"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestUpdateBuilderNoRunner(t *testing.T) {
	b := Update("test").Set("x", 1)

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestUpdateBuilderFrom(t *testing.T) {
	sql, _, err := Update("employees").Set("sales_count", 100).From("accounts").Where("accounts.name = ?", "ACME").ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE employees SET sales_count = ? FROM accounts WHERE accounts.name = ?", sql)
}

func TestUpdateBuilderFromSelect(t *testing.T) {
	sql, _, err := Update("employees").
		Set("sales_count", 100).
		FromSelect(Select("id").
			From("accounts").
			Where("accounts.name = ?", "ACME"), "subquery").
		Where("employees.account_id = subquery.id").ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"UPDATE employees " +
			"SET sales_count = ? " +
			"FROM (SELECT id FROM accounts WHERE accounts.name = ?) AS subquery " +
			"WHERE employees.account_id = subquery.id"
	assert.Equal(t, expectedSql, sql)
}
