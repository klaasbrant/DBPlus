# Executing Statements

## The `execute()` Method

Use `db.execute()` for SQL statements that modify data: INSERT, UPDATE, DELETE, and DDL statements. It returns the number of affected rows.

```python
# INSERT
db.execute('INSERT INTO users (name, age) VALUES (?, ?)', 'Alice', 30)

# UPDATE - returns number of rows updated
updated = db.execute('UPDATE users SET active = ? WHERE age < ?', 0, 18)
print(f'{updated} users deactivated')

# DELETE - returns number of rows deleted
deleted = db.execute('DELETE FROM users WHERE active = ?', 0)
print(f'{deleted} users deleted')
```

### Named Parameters

```python
db.execute(
    'INSERT INTO users (name, email) VALUES (:name, :email)',
    name='Alice',
    email='alice@example.com'
)
```

### Positional Parameters

```python
db.execute(
    'INSERT INTO users (name, email) VALUES (?, ?)',
    'Alice',
    'alice@example.com'
)
```

## Transactions

### Context Manager (Recommended)

The `transaction()` context manager automatically commits on success and rolls back on any exception:

```python
with db.transaction():
    db.execute('DELETE FROM order_items WHERE order_id = ?', order_id)
    db.execute('DELETE FROM orders WHERE id = ?', order_id)
    db.execute('UPDATE inventory SET qty = qty + ? WHERE item_id = ?', qty, item_id)
# Committed automatically when the block exits normally
```

If an exception occurs inside the block, the transaction is rolled back and the exception is re-raised:

```python
try:
    with db.transaction():
        db.execute('INSERT INTO accounts VALUES (?, ?)', 1, 1000)
        raise ValueError('Something went wrong')
        # This line is never reached
except ValueError:
    # Transaction was rolled back
    pass
```

### Manual Transaction Control

You can also manage transactions manually:

```python
db.begin_transaction()
try:
    db.execute('INSERT INTO orders VALUES (?, ?)', 1, 'Widget')
    db.execute('UPDATE inventory SET qty = qty - 1 WHERE item = ?', 'Widget')
    db.commit()
except Exception:
    db.rollback()
    raise
```

### Transaction Rules

- **No nesting**: Starting a transaction while one is already active raises `DBError`
- **No manual commit/rollback inside context manager**: Calling `db.commit()` or `db.rollback()` inside a `with db.transaction()` block raises `DBError`
- **Check state**: Use `db.is_transaction_active()` to check if a transaction is in progress

```python
# This raises DBError:
with db.transaction():
    db.commit()  # Error! Cannot commit inside transaction block

# This also raises DBError:
with db.transaction():
    with db.transaction():  # Error! Nested transactions not supported
        pass
```

## Stored Procedures

Call stored procedures with `db.callproc()`. It returns a tuple of `(RecordCollection, *output_params)`:

```python
# Call a stored procedure with parameters
result, *output = db.callproc('find_employee', ['000010', ''])
print(result)           # RecordCollection with result set
print(output)           # Output parameter values

# Procedure with no output
result = db.callproc('update_stats')
```

### Multiple Result Sets

Some stored procedures return multiple result sets. Use `next_result()` on the `RecordCollection` to access subsequent result sets:

```python
result, *params = db.callproc('read_emp_dept')
print(result)                       # First result set

next_rs = result.next_result()      # Advance to second result set
print(next_rs)
```

## Last Insert ID

After an INSERT into a table with an auto-increment or identity column, retrieve the generated ID:

```python
db.execute('INSERT INTO users (name) VALUES (?)', 'Alice')
new_id = db.last_insert_id()
print(f'New user ID: {new_id}')
```

For DB2, this uses `IDENTITY_VAL_LOCAL()`. For SQLite, it uses the cursor's `lastrowid` attribute.

## Error Handling

DBPlus raises specific exceptions for different error conditions:

```python
from dbplus import Database, DBError

try:
    db = Database('sqlite:///mydata.db')
    db.execute('INSERT INTO nonexistent VALUES (?)', 1)
except RuntimeError as e:
    print(f'SQL error: {e}')
except DBError as e:
    print(f'DBPlus error: {e}')
except ValueError as e:
    print(f'Connection error: {e}')
```

- `ValueError` - Invalid database URL or driver initialization failure
- `RuntimeError` - SQL execution errors (bad SQL, constraint violations, etc.)
- `DBError` - Transaction logic errors (nested transactions, improper commit/rollback)
- `LookupError` - Missing parameter in SQL binding (named or positional parameter not found)

You can also inspect the last error from the driver:

```python
print(db.error_code())  # Driver-specific error code
print(db.error_info())  # Driver-specific error message
```
