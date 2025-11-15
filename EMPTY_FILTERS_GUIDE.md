# Empty Filters and Mass Operations - Usage Guide

## Overview

This guide explains how Squirrel handles empty `And{}` and `Or{}` filters, potential risks, and best practices for safe usage.

## Behavior Summary

### Empty Conjunctions

When you pass empty `And{}` or `Or{}` to `Where()`:

```go
Update("users").Set("status", "inactive").Where(And{})
// Generates: UPDATE users SET status = ?
// Result: Updates ALL rows in the table

Delete("orders").Where(Or{})
// Generates: DELETE FROM orders
// Result: Deletes ALL rows in the table
```

**⚠️ Warning**: No WHERE clause is generated, so **ALL rows** are affected!

### How It Works Internally

1. Empty `And{}` generates `(1=1)`
2. Empty `Or{}` generates `(1=0)`
3. WHERE clause generation **skips** `(1=1)` and `(1=0)` to avoid misleading SQL
4. Result: No WHERE clause in final SQL

### Why This Design?

This was implemented to fix [Issue #382](https://github.com/Masterminds/squirrel/issues/382) where nil filters incorrectly added WHERE clauses:

```go
var filter Or  // nil
Select("*").From("users").Where(filter)
// Old: SELECT * FROM users WHERE (1=0)  ❌ Returns zero rows unexpectedly
// New: SELECT * FROM users               ✓ No WHERE clause - clearer intent
```

---

## Safety Guidelines

### ⚠️ Risk: Unintended Mass Operations

**DANGEROUS**:
```go
// DON'T: Accidentally update/delete all rows
func UpdateUsers(conditions []Sqlizer) error {
    query := Update("users").Set("status", "inactive")

    // If conditions is empty, this updates ALL users!
    query = query.Where(And(conditions))

    return query.Exec()  // 💥 DANGER
}
```

**SAFE**:
```go
// DO: Check for empty conditions
func UpdateUsers(conditions []Sqlizer) error {
    if len(conditions) == 0 {
        return errors.New("refusing to update all users without conditions")
    }

    query := Update("users").Set("status", "inactive").Where(And(conditions))
    return query.Exec()  // ✓ Safe
}
```

---

## Best Practices

### 1. Always Validate Conditions

```go
// ✓ GOOD: Validate before building query
func DeleteOrders(orderIDs []int) error {
    if len(orderIDs) == 0 {
        return errors.New("no order IDs provided")
    }

    return Delete("orders").
        Where(Eq{"id": orderIDs}).
        Exec()
}
```

### 2. Use Conditional WHERE

```go
// ✓ GOOD: Only add WHERE if conditions exist
func GetUsers(activeOnly bool, minAge *int) (*sql.Rows, error) {
    query := Select("*").From("users")

    if activeOnly {
        query = query.Where(Eq{"active": true})
    }

    if minAge != nil {
        query = query.Where(Gt{"age": *minAge})
    }

    return query.Query()
}
```

### 3. Use Eq{} for Empty Slice Safety

When dealing with potentially empty slices, use `Eq{}` instead of raw SQL:

```go
ids := []int{}  // Empty slice

// ❌ UNSAFE: Returns error (good) but requires error handling
query := Select("*").From("users").Where("id IN ?", ids)
_, _, err := query.ToSql()
// err: "empty slice passed to Expr placeholder"

// ✓ SAFE: Automatically handles empty slices
query = Select("*").From("users").Where(Eq{"id": ids})
sql, _, _ := query.ToSql()
// sql: "SELECT * FROM users WHERE (1=0)"
// Result: Returns zero rows (safe default)
```

### 4. Intentional Mass Operations

If you **intentionally** want to update/delete all rows, **don't use `Where()` at all**:

```go
// ✓ GOOD: Explicit intent to clear all cache entries
Delete("cache_entries").Exec()

// ✓ GOOD: Explicit intent to reset all counters
Update("counters").Set("count", 0).Exec()

// ❌ CONFUSING: Using Where(And{}) is misleading
Update("counters").Set("count", 0).Where(And{}).Exec()
```

---

## Common Patterns

### Pattern 1: Dynamic Filters

```go
func SearchProducts(filters map[string]interface{}) ([]Product, error) {
    query := Select("*").From("products")

    // Build conditions dynamically
    var conditions []Sqlizer

    if category, ok := filters["category"].(string); ok && category != "" {
        conditions = append(conditions, Eq{"category": category})
    }

    if minPrice, ok := filters["min_price"].(float64); ok && minPrice > 0 {
        conditions = append(conditions, Gt{"price": minPrice})
    }

    // Only add WHERE if we have conditions
    if len(conditions) > 0 {
        query = query.Where(And(conditions))
    }

    return query.Query()
}
```

### Pattern 2: Optional Filters with Validation

```go
func DeleteOldRecords(olderThan *time.Time) error {
    query := Delete("logs")

    if olderThan != nil {
        query = query.Where(Lt{"created_at": *olderThan})
    } else {
        // Require explicit confirmation for mass delete
        return errors.New("refusing to delete all records without time filter")
    }

    return query.Exec()
}
```

### Pattern 3: Helper Function

```go
// Helper to safely build UPDATE queries
func SafeUpdate(table string, updates map[string]interface{}, conditions []Sqlizer) error {
    if len(conditions) == 0 {
        return fmt.Errorf("refusing to update all rows in %s without conditions", table)
    }

    query := Update(table).SetMap(updates).Where(And(conditions))
    _, err := query.Exec()
    return err
}

// Usage
err := SafeUpdate("users",
    map[string]interface{}{"status": "inactive"},
    []Sqlizer{Eq{"last_login": nil}},
)
```

---

## Comparison: Empty Slices vs Empty Conjunctions

| Scenario | Behavior | Safety |
|----------|----------|--------|
| `Where("id IN ?", []int{})` | **Error** at build time | ✓ Safe - prevents invalid SQL |
| `Where(Eq{"id": []int{}})` | `WHERE (1=0)` | ✓ Safe - matches zero rows |
| `Where(Or{})` | No WHERE clause | ⚠️ Matches ALL rows |
| `Where(And{})` | No WHERE clause | ⚠️ Matches ALL rows |

**Key Difference**:
- Empty slices in `Eq{}` are **safe** (generate `WHERE (1=0)`)
- Empty slices in `Expr()` return **error** (fail fast)
- Empty conjunctions allow **mass operations** (require validation)

---

## Migration from Issue #382 Fix

If you were relying on the old behavior of `WHERE (1=0)` or `WHERE (1=1)`:

### Before (Pre-Fix)
```go
var filter Or
query := Select("*").From("users").Where(filter)
// Generated: SELECT * FROM users WHERE (1=0)
// Returned: Zero rows (unexpected)
```

### After (Post-Fix)
```go
var filter Or
query := Select("*").From("users").Where(filter)
// Generates: SELECT * FROM users
// Returns: All rows (clearer - no WHERE clause)
```

**Action Required**: If you were using empty `Or{}` to intentionally filter out all rows, use explicit conditions instead:

```go
// Instead of: Where(Or{})
Where(Expr("1=0"))  // Explicit "match nothing" filter

// Or better: Don't execute the query at all
if hasNoConditions {
    return emptyResult, nil
}
```

---

## Summary

✅ **DO**:
- Check `len(conditions)` before calling `Where()`
- Use `Eq{}` for slices (handles empty slices safely)
- Validate inputs in application logic
- Be explicit when mass operations are intentional

❌ **DON'T**:
- Pass potentially empty `And{}` or `Or{}` without validation
- Assume empty filters will prevent operations
- Use `Where(And{})` for intentional mass operations (omit `Where()` instead)

---

## Related Issues

- [#382](https://github.com/Masterminds/squirrel/issues/382) - Nil Or Clause Bug (fixed)
- [#383](https://github.com/Masterminds/squirrel/issues/383) - Raw SQL with Slice Arguments (fixed)

---

## Need Help?

If you need stricter validation or want to prevent mass operations entirely, consider:

1. **Wrapper functions** that validate conditions before building queries
2. **Database-level safeguards** (e.g., always require WHERE in UPDATE/DELETE)
3. **Application-level checks** before calling `.Exec()`

Example wrapper:
```go
type SafeDB struct {
    *sql.DB
}

func (db *SafeDB) Exec(query string, args ...interface{}) (sql.Result, error) {
    // Reject UPDATE/DELETE without WHERE
    upperQuery := strings.ToUpper(query)
    if (strings.HasPrefix(upperQuery, "UPDATE") ||
        strings.HasPrefix(upperQuery, "DELETE")) &&
       !strings.Contains(upperQuery, "WHERE") {
        return nil, errors.New("UPDATE/DELETE without WHERE clause not allowed")
    }

    return db.DB.Exec(query, args...)
}
```

This provides an extra safety layer at the execution level.
