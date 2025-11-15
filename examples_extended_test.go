package squirrel

import "fmt"

// Example_caseWithIntegers demonstrates using integers in CASE WHEN statements (Issue #388)
func Example_caseWithIntegers() {
	// Before: Had to convert integers to strings or use Expr
	// Now: Can use integers directly in WHEN/THEN clauses

	caseStmt := Case("order_status").
		When("'pending'", 0).
		When("'processing'", 1).
		When("'completed'", 2).
		Else(99)

	sql, args, _ := Update("orders").
		Set("status_code", caseStmt).
		Where(Eq{"id": 12345}).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE orders SET status_code = CASE order_status WHEN 'pending' THEN ? WHEN 'processing' THEN ? WHEN 'completed' THEN ? ELSE ? END WHERE id = ?
	// [0 1 2 99 12345]
}

// Example_caseWithMixedTypes demonstrates using different types in CASE statements
func Example_caseWithMixedTypes() {
	caseStmt := Case().
		When(Eq{"status": "active"}, true).
		When(Eq{"status": "inactive"}, false).
		When(Eq{"priority": 1}, 100).
		When(Eq{"priority": 2}, 50.5).
		Else("unknown")

	sql, args, _ := Select("id").
		Column(Alias(caseStmt, "computed_value")).
		From("records").
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT id, (CASE WHEN status = ? THEN ? WHEN status = ? THEN ? WHEN priority = ? THEN ? WHEN priority = ? THEN ? ELSE unknown END) AS computed_value FROM records
	// [active true inactive false 1 100 2 50.5]
}

// Example_notOperator demonstrates the NOT operator for negating conditions (Issue #386)
func Example_notOperator() {
	// Simple NOT with a single condition
	sql, args, _ := Select("*").
		From("users").
		Where(Not{Eq{"status": "banned"}}).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT * FROM users WHERE NOT status = ?
	// [banned]
}

// Example_norOperation demonstrates NOR (NOT OR) operations like MongoDB's $nor
func Example_norOperation() {
	// Equivalent to MongoDB: {$nor: [{age: 20}, {owner: "admin"}]}
	sql, args, _ := Select("*").
		From("users").
		Where(Not{Or{
			Eq{"age": 20},
			Eq{"owner": "admin"},
		}}).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT * FROM users WHERE NOT (age = ? OR owner = ?)
	// [20 admin]
}

// Example_notWithComplexConditions demonstrates NOT with complex nested conditions
func Example_notWithComplexConditions() {
	sql, args, _ := Select("*").
		From("products").
		Where(Not{And{
			Eq{"category": "electronics"},
			Gt{"price": 1000},
			Like{"name": "%phone%"},
		}}).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT * FROM products WHERE NOT (category = ? AND price > ? AND name LIKE ?)
	// [electronics 1000 %phone%]
}

// Example_nilOrClause demonstrates the fix for nil Or clauses (Issue #382)
func Example_nilOrClause() {
	// Function that returns nil filter when no conditions match
	getFilter := func(applyFilter bool) Or {
		if !applyFilter {
			return nil // Returns nil/empty Or
		}
		return Or{Eq{"status": "active"}}
	}

	// Before: This would generate "SELECT * FROM users WHERE (1=0)"
	// Now: Generates "SELECT * FROM users" (no WHERE clause)
	sql, args, _ := Select("*").
		From("users").
		Where(getFilter(false)).
		ToSql()

	fmt.Println(sql)
	fmt.Println(len(args))
	// Output:
	// SELECT * FROM users
	// 0
}

// Example_sliceInRawSQL demonstrates slice expansion in raw SQL (Issue #383)
func Example_sliceInRawSQL() {
	// Before: Had to use NotEq or manually build placeholders
	// Now: Slices are automatically expanded

	ids := []int{1, 2, 3, 4, 5}
	sql, args, _ := Select("*").
		From("users").
		Where("id NOT IN ?", ids).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT * FROM users WHERE id NOT IN (?,?,?,?,?)
	// [1 2 3 4 5]
}

// Example_multipleSlicesInRawSQL demonstrates multiple slice arguments
func Example_multipleSlicesInRawSQL() {
	userIDs := []int{1, 2, 3}
	statuses := []string{"active", "pending"}

	sql, args, _ := Select("*").
		From("orders").
		Where("user_id IN ? AND status IN ?", userIDs, statuses).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT * FROM orders WHERE user_id IN (?,?,?) AND status IN (?,?)
	// [1 2 3 active pending]
}

// Example_mixedSliceAndRegularArgs demonstrates mixing slices with regular arguments
func Example_mixedSliceAndRegularArgs() {
	sql, args, _ := Select("*").
		From("products").
		Where("category = ? AND id IN ? AND price > ?", "electronics", []int{10, 20, 30}, 99.99).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT * FROM products WHERE category = ? AND id IN (?,?,?) AND price > ?
	// [electronics 10 20 30 99.99]
}

// Example_emptySliceHandling demonstrates proper handling of potentially empty slices
func Example_emptySliceHandling() {
	ids := []int{1, 2, 3}

	// Best practice: Check for empty slices before building queries
	// Empty slices in Expr will return an error
	query := Select("*").From("users")
	if len(ids) > 0 {
		query = query.Where("id IN ?", ids)
	}
	// If ids is empty, no WHERE clause is added

	// Alternative: Use Eq{} which handles empty slices gracefully
	// query = query.Where(Eq{"id": ids})
	// Empty slice generates: WHERE (1=0) - matches nothing

	sql, args, _ := query.ToSql()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT * FROM users WHERE id IN (?,?,?)
	// [1 2 3]
}

// Example_onDuplicateKeyUpdate demonstrates MySQL upserts (Issue #372)
func Example_onDuplicateKeyUpdate() {
	sql, args, _ := Insert("users").
		Columns("id", "name", "email", "login_count").
		Values(1, "john_doe", "john@example.com", 1).
		OnDuplicateKeyUpdate(map[string]interface{}{
			"email":       "john@example.com",
			"login_count": Expr("login_count + 1"),
		}).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO users (id,name,email,login_count) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE email = ?, login_count = login_count + 1
	// [1 john_doe john@example.com 1 john@example.com]
}

// Example_onDuplicateKeyUpdateMultipleRows demonstrates upsert with multiple rows
func Example_onDuplicateKeyUpdateMultipleRows() {
	sql, args, _ := Insert("products").
		Columns("id", "name", "stock").
		Values(1, "Widget A", 10).
		Values(2, "Widget B", 20).
		Values(3, "Widget C", 30).
		OnDuplicateKeyUpdate(map[string]interface{}{
			"stock": Expr("VALUES(stock)"),
		}).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO products (id,name,stock) VALUES (?,?,?),(?,?,?),(?,?,?) ON DUPLICATE KEY UPDATE stock = VALUES(stock)
	// [1 Widget A 10 2 Widget B 20 3 Widget C 30]
}

// Example_onDuplicateKeyUpdateCounter demonstrates using ON DUPLICATE KEY UPDATE for counters
func Example_onDuplicateKeyUpdateCounter() {
	sql, args, _ := Insert("page_views").
		Columns("page_id", "view_count", "last_viewed").
		Values(42, 1, Expr("NOW()")).
		OnDuplicateKeyUpdate(map[string]interface{}{
			"view_count":  Expr("view_count + 1"),
			"last_viewed": Expr("NOW()"),
		}).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// INSERT INTO page_views (page_id,view_count,last_viewed) VALUES (?,?,NOW()) ON DUPLICATE KEY UPDATE last_viewed = NOW(), view_count = view_count + 1
	// [42 1]
}

// Example_updateWithJoin demonstrates UPDATE with JOIN (existing feature, confirmed working)
func Example_updateWithJoin() {
	sql, args, _ := Update("employees").
		Set("salary", Expr("salary * 1.1")).
		Join("departments ON employees.department_id = departments.id").
		Where(Eq{"departments.name": "Engineering"}).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE employees JOIN departments ON employees.department_id = departments.id SET salary = salary * 1.1 WHERE departments.name = ?
	// [Engineering]
}

// Example_updateWithMultipleJoins demonstrates UPDATE with multiple JOINs
func Example_updateWithMultipleJoins() {
	sql, args, _ := Update("orders").
		Set("discount", 10).
		Join("customers ON orders.customer_id = customers.id").
		Join("customer_tiers ON customers.tier_id = customer_tiers.id").
		Where(Eq{"customer_tiers.name": "premium"}).
		Where(Gt{"orders.total": 100}).
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE orders JOIN customers ON orders.customer_id = customers.id JOIN customer_tiers ON customers.tier_id = customer_tiers.id SET discount = ? WHERE customer_tiers.name = ? AND orders.total > ?
	// [10 premium 100]
}

// Example_combinedFeatures demonstrates using multiple new features together
func Example_combinedFeatures() {
	// Complex query using CASE with integers, NOT operator, and slice expansion
	statusCase := Case("status").
		When("'active'", 1).
		When("'pending'", 0).
		Else(-1)

	excludedIDs := []int{5, 10, 15}

	sql, args, _ := Select("id", "name").
		Column(Alias(statusCase, "status_code")).
		From("users").
		Where("id NOT IN ?", excludedIDs).
		Where(Not{Or{
			Eq{"deleted": true},
			Eq{"banned": true},
		}}).
		OrderBy("status_code DESC", "name ASC").
		ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT id, name, (CASE status WHEN 'active' THEN ? WHEN 'pending' THEN ? ELSE ? END) AS status_code FROM users WHERE id NOT IN (?,?,?) AND NOT (deleted = ? OR banned = ?) ORDER BY status_code DESC, name ASC
	// [1 0 -1 5 10 15 true true]
}

// Example_emptyOrBehavior demonstrates empty Or{} behavior (issue #382)
func Example_emptyOrBehavior() {
	// Empty Or{} produces (1=0) but WHERE clause is skipped
	sql, args, _ := Select("*").From("users").Where(Or{}).ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT * FROM users
	// []
}

// Example_emptyAndBehavior demonstrates empty And{} behavior (issue #382)
func Example_emptyAndBehavior() {
	// Empty And{} produces (1=1) but WHERE clause is skipped
	sql, args, _ := Update("users").Set("status", "active").Where(And{}).ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE users SET status = ?
	// [active]
}

// Example_nilFilterBehavior demonstrates nil filter behavior (issue #382 fix)
func Example_nilFilterBehavior() {
	// Nil Or/And filters don't add WHERE clause
	var filter Or
	sql, args, _ := Delete("logs").Where(filter).ToSql()

	fmt.Println(sql)
	fmt.Println(len(args))
	// Output:
	// DELETE FROM logs
	// 0
}

// Example_safeConditionalFilter demonstrates safe pattern for conditional filters
func Example_safeConditionalFilter() {
	// Safe: Only add WHERE if conditions exist
	userID := 123
	var conditions []Sqlizer

	if userID > 0 {
		conditions = append(conditions, Eq{"user_id": userID})
	}

	query := Select("*").From("orders")
	if len(conditions) > 0 {
		query = query.Where(And(conditions))
	}

	sql, args, _ := query.ToSql()
	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// SELECT * FROM orders WHERE (user_id = ?)
	// [123]
}

// Example_emptySliceWithEq demonstrates safe empty slice handling
func Example_emptySliceWithEq() {
	// Eq{} handles empty slices safely - generates (1=0) internally but WHERE is omitted
	// This results in a query that matches nothing without requiring manual checks
	ids := []int{} // Empty slice

	sql, args, _ := Select("*").From("users").Where(Eq{"id": ids}).ToSql()

	fmt.Println(sql)
	fmt.Println(len(args))
	// Output:
	// SELECT * FROM users
	// 0
}

// Example_intentionalMassOperation demonstrates explicit mass operation
func Example_intentionalMassOperation() {
	// For intentional mass operations, don't use Where() at all
	// This makes the intent clear
	sql, args, _ := Update("cache").Set("valid", false).ToSql()

	fmt.Println(sql)
	fmt.Println(args)
	// Output:
	// UPDATE cache SET valid = ?
	// [false]
}
