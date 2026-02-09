# QueryStore

The `QueryStore` class lets you organize SQL queries in external `.sql` files and load them by name. This keeps SQL out of your Python code and makes queries easier to manage.

## SQL File Format

Queries are defined in `.sql` files using `-- name:` comments to mark each query:

```sql
-- name: get-all-employees
-- Retrieve all employees from the database
SELECT *
FROM employees

-- name: get-employee-by-id
-- Find a specific employee by their ID
SELECT *
FROM employees
WHERE empno = :empno

-- name: active-employees-in-dept
-- Get active employees in a department
SELECT firstname, lastname, salary
FROM employees
WHERE workdept = :dept
  AND status = 'ACTIVE'
```

Each query definition consists of:

1. `-- name: query_name` - The query name (required). Hyphens are converted to underscores.
2. `-- comment text` - Optional description comments (any SQL comments after the name line)
3. The SQL statement itself (everything until the next `-- name:` or end of file)

## Loading Queries

### From a Single File

```python
from dbplus import QueryStore

Q = QueryStore('path/to/queries.sql')
```

### From a Directory

When loading from a directory, all `.sql` files are loaded recursively:

```python
Q = QueryStore('path/to/sql/')
```

Files in subdirectories get their query names prefixed with the file stem to avoid name conflicts. For example, a query named `get_all` in `reports/sales.sql` becomes `sales_get_all`.

### File Extension Filter

By default, only `.sql` files are loaded. Change this with the `ext` parameter:

```python
Q = QueryStore('path/to/sql/', ext=('.sql', '.query'))
```

### Name Prefixing

Control whether file names are used as prefixes for query names:

```python
# No prefix (default for single files)
Q = QueryStore('queries.sql', prefix=False)

# Force prefix with file stem
Q = QueryStore('queries.sql', prefix=True)
# Query "get_all" in "reports.sql" becomes "reports_get_all"
```

## Using Queries

Access loaded queries as attributes on the `QueryStore` object:

```python
Q = QueryStore('queries.sql')

# Access query by attribute name (hyphens become underscores)
rows = db.query(Q.get_all_employees)
rows = db.query(Q.get_employee_by_id, empno='000010')
```

The attribute returns a `Query` named tuple that can be passed directly to `db.query()` or `db.execute()`.

## The Query Object

Each loaded query is a `Query` named tuple with these fields:

| Field      | Type                     | Description                           |
|------------|--------------------------|---------------------------------------|
| `name`     | `str`                    | The query name                        |
| `comments` | `str`                    | Description from SQL comments         |
| `sql`      | `str`                    | The SQL statement                     |
| `floc`     | `(Path, int)` or `None`  | Source file path and line number       |

```python
Q = QueryStore('queries.sql')

print(Q.get_all_employees.name)       # 'get_all_employees'
print(Q.get_all_employees.comments)   # 'Retrieve all employees from the database'
print(Q.get_all_employees.sql)        # 'SELECT * FROM employees'
print(Q.get_all_employees.floc)       # (PosixPath('queries.sql'), 2)
```

## Listing All Queries

Iterate over the `query_store` dictionary to see all loaded queries:

```python
Q = QueryStore('path/to/sql/')

for name, query in Q.query_store.items():
    print(f'{name}: {query.sql} (from {query.floc})')
```

## Duplicate Detection

If two queries have the same name (after prefix and hyphen-to-underscore conversion), a `SQLLoadException` is raised:

```python
# This raises SQLLoadException if any query names collide
Q = QueryStore('path/to/sql/')
```

## Exceptions

| Exception           | Description                                      |
|---------------------|--------------------------------------------------|
| `SQLLoadException`  | File/directory not found, or duplicate query name |
| `SQLParseException` | Invalid query name format                         |

Query names must start with a letter or underscore (not a digit) and contain only word characters.

## Complete Example

**File: `queries/employees.sql`**

```sql
-- name: get-emp
-- Get employees by education level and department
SELECT * FROM emp
WHERE edlevel = :edlevel AND workdept = :wd

-- name: count-by-dept
-- Count employees per department
SELECT workdept, COUNT(*) as cnt
FROM emp
GROUP BY workdept
```

**Python code:**

```python
from dbplus import Database, QueryStore

db = Database('db2://user:pass@host:50000/sample')
Q = QueryStore('queries/employees.sql')

# Use loaded queries
rows = db.query(Q.get_emp, edlevel=18, wd='A00')
print(rows)

count = db.query(Q.count_by_dept)
print(count.as_DataFrame())
```
