package squirrel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertBuilderToSql(t *testing.T) {
	b := Insert("").
		Prefix("WITH prefix AS ?", 0).
		Into("a").
		Options("DELAYED", "IGNORE").
		Columns("b", "c").
		Values(1, 2).
		Values(3, Expr("? + 1", 4)).
		Suffix("RETURNING ?", 5)

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSQL :=
		"WITH prefix AS ? " +
			"INSERT DELAYED IGNORE INTO a (b,c) VALUES (?,?),(?,? + 1) " +
			"RETURNING ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{0, 1, 2, 3, 4, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderToSqlErr(t *testing.T) {
	_, _, err := Insert("").Values(1).ToSql()
	assert.Error(t, err)

	_, _, err = Insert("x").ToSql()
	assert.Error(t, err)
}

func TestInsertBuilderMustSql(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestInsertBuilderMustSql should have panicked!")
		}
	}()
	Insert("").MustSql()
}

func TestInsertBuilderPlaceholders(t *testing.T) {
	b := Insert("test").Values(1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES (?,?)", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES ($1,$2)", sql)
}

func TestInsertBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Insert("test").Values(1).RunWith(db)

	expectedSQL := "INSERT INTO test VALUES (?)"

	b.Exec()
	assert.Equal(t, expectedSQL, db.LastExecSql)
}

func TestInsertBuilderNoRunner(t *testing.T) {
	b := Insert("test").Values(1)

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestInsertBuilderSetMap(t *testing.T) {
	b := Insert("table").SetMap(Eq{"field1": 1, "field2": 2, "field3": 3})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table (field1,field2,field3) VALUES (?,?,?)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderSelect(t *testing.T) {
	sb := Select("field1").From("table1").Where(Eq{"field1": 1})
	ib := Insert("table2").Columns("field1").Select(sb)

	sql, args, err := ib.ToSql()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table2 (field1) SELECT field1 FROM table1 WHERE field1 = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderReplace(t *testing.T) {
	b := Replace("table").Values(1)

	expectedSQL := "REPLACE INTO table VALUES (?)"

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
}

func TestInsertOnDuplicateKeyUpdate(t *testing.T) {
	// Test for issue #372 - ON DUPLICATE KEY UPDATE for MySQL upserts
	b := Insert("users").
		Columns("id", "name", "email").
		Values(1, "John", "john@example.com").
		OnDuplicateKeyUpdate(map[string]interface{}{
			"name":  "John",
			"email": "john@example.com",
		})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name,email) VALUES (?,?,?) ON DUPLICATE KEY UPDATE email = ?, name = ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, "John", "john@example.com", "john@example.com", "John"}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertOnDuplicateKeyUpdateWithExpr(t *testing.T) {
	// Test ON DUPLICATE KEY UPDATE with Expr for column references
	b := Insert("counters").
		Columns("id", "count").
		Values(1, 1).
		OnDuplicateKeyUpdate(map[string]interface{}{
			"count": Expr("count + 1"),
		})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO counters (id,count) VALUES (?,?) ON DUPLICATE KEY UPDATE count = count + 1"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, 1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertOnDuplicateKeyUpdateMultipleRows(t *testing.T) {
	// Test ON DUPLICATE KEY UPDATE with multiple rows
	b := Insert("users").
		Columns("id", "name").
		Values(1, "John").
		Values(2, "Jane").
		OnDuplicateKeyUpdate(map[string]interface{}{
			"name": Expr("VALUES(name)"),
		})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO users (id,name) VALUES (?,?),(?,?) ON DUPLICATE KEY UPDATE name = VALUES(name)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []interface{}{1, "John", 2, "Jane"}
	assert.Equal(t, expectedArgs, args)
}
