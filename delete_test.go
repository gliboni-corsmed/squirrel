package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteBuilderToSql(t *testing.T) {
	b := Delete("").
		Prefix("WITH prefix AS ?", 0).
		From("a").
		Where("b = ?", 1).
		OrderBy("c").
		Limit(2).
		Offset(3).
		Suffix("RETURNING ?", 4)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"DELETE FROM a WHERE b = ? ORDER BY c LIMIT 2 OFFSET 3 " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []interface{}{0, 1, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderToSqlErr(t *testing.T) {
	_, _, err := Delete("").ToSql()
	assert.Error(t, err)
}

func TestDeleteBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestDeleteBuilderMustSql should have panicked!")
		}
	}()
	Delete("").MustSql()
}

func TestDeleteBuilderPlaceholders(t *testing.T) {
	b := Delete("test").Where("x = ? AND y = ?", 1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = $1 AND y = $2", sql)
}

func TestDeleteBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Delete("test").Where("x = ?", 1).RunWith(db)

	expectedSql := "DELETE FROM test WHERE x = ?"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestDeleteBuilderNoRunner(t *testing.T) {
	b := Delete("test")

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestDeleteWithQuery(t *testing.T) {
	db := &DBStub{}
	b := Delete("test").Where("id=55").Suffix("RETURNING path").RunWith(db)

	expectedSql := "DELETE FROM test WHERE id=55 RETURNING path"
	b.Query()

	assert.Equal(t, expectedSql, db.LastQuerySql)
}

func TestDeleteBuilderJoin(t *testing.T) {
	sql, args, err := Delete("orders").
		Join("customers ON orders.customer_id = customers.id").
		Where("customers.status = ?", "inactive").
		ToSql()

	assert.NoError(t, err)
	expectedSql := "DELETE FROM orders JOIN customers ON orders.customer_id = customers.id WHERE customers.status = ?"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"inactive"}, args)
}

func TestDeleteBuilderInnerJoin(t *testing.T) {
	sql, args, err := Delete("order_items").
		InnerJoin("orders ON order_items.order_id = orders.id").
		Where("orders.status = ?", "cancelled").
		ToSql()

	assert.NoError(t, err)
	expectedSql := "DELETE FROM order_items INNER JOIN orders ON order_items.order_id = orders.id WHERE orders.status = ?"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"cancelled"}, args)
}

func TestDeleteBuilderLeftJoin(t *testing.T) {
	sql, args, err := Delete("users").
		LeftJoin("user_sessions ON users.id = user_sessions.user_id").
		Where("user_sessions.last_activity < ?", "2023-01-01").
		ToSql()

	assert.NoError(t, err)
	expectedSql := "DELETE FROM users LEFT JOIN user_sessions ON users.id = user_sessions.user_id WHERE user_sessions.last_activity < ?"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"2023-01-01"}, args)
}

func TestDeleteBuilderMultipleJoins(t *testing.T) {
	sql, args, err := Delete("products").
		Join("categories ON products.category_id = categories.id").
		Join("suppliers ON products.supplier_id = suppliers.id").
		Where("categories.deprecated = ?", true).
		Where("suppliers.active = ?", false).
		ToSql()

	assert.NoError(t, err)
	expectedSql := "DELETE FROM products JOIN categories ON products.category_id = categories.id JOIN suppliers ON products.supplier_id = suppliers.id WHERE categories.deprecated = ? AND suppliers.active = ?"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{true, false}, args)
}

func TestDeleteBuilderJoinWithParams(t *testing.T) {
	sql, args, err := Delete("logs").
		Join("log_levels ON logs.level_id = log_levels.id AND log_levels.name = ?", "DEBUG").
		Where("logs.created_at < ?", "2024-01-01").
		ToSql()

	assert.NoError(t, err)
	expectedSql := "DELETE FROM logs JOIN log_levels ON logs.level_id = log_levels.id AND log_levels.name = ? WHERE logs.created_at < ?"
	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, []interface{}{"DEBUG", "2024-01-01"}, args)
}

func TestDeleteBuilderNilOrClause(t *testing.T) {
	// Test for issue #382 - nil Or should not add WHERE clause in DELETE
	var filter Or
	sql, args, err := Delete("users").
		Where(filter).
		ToSql()

	assert.NoError(t, err)
	expectedSql := "DELETE FROM users"
	assert.Equal(t, expectedSql, sql)
	assert.Empty(t, args)
}

func TestDeleteBuilderEmptyAndClause(t *testing.T) {
	// Test for issue #382 - empty And should not add WHERE clause in DELETE
	sql, args, err := Delete("users").
		Where(And{}).
		ToSql()

	assert.NoError(t, err)
	expectedSql := "DELETE FROM users"
	assert.Equal(t, expectedSql, sql)
	assert.Empty(t, args)
}
