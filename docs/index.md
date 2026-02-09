# DBPlus

**A database-agnostic Python library for unified SQL access.**

DBPlus is an interface layer between the several Python database interfaces and your program. It makes SQL access from your program database-agnostic, meaning the same code can run unmodified on several databases. All you need to change is the database URL.

!!! warning
    This software is not yet production ready and still changing. Please wait until version 1.0 for production use.

## Supported Databases

| Database   | Driver Package          | URL Scheme   |
|------------|-------------------------|--------------|
| SQLite     | Built-in (no install)   | `sqlite`     |
| PostgreSQL | `psycopg2`              | `postgres`   |
| MySQL      | `mysql-connector-python` | `mysql`     |
| Oracle     | `oracledb`              | `oracle`     |
| IBM DB2    | `ibm_db`                | `db2`        |

## Quick Start

```python
from dbplus import Database

# Connect to a database
db = Database('sqlite:///mydata.db')

# Query with named parameters
rows = db.query('SELECT * FROM users WHERE age > :age', age=21)
for row in rows:
    print(row.name, row.age)

# Execute statements (INSERT, UPDATE, DELETE)
affected = db.execute('UPDATE users SET active = ? WHERE id = ?', 1, 42)

# Transactions
with db.transaction():
    db.execute('INSERT INTO orders VALUES (?, ?)', 1, 'Widget')
    db.execute('UPDATE inventory SET qty = qty - 1 WHERE item = ?', 'Widget')
```

## Key Features

- **Database-agnostic** - Write SQL once, run on any supported database
- **Unified parameter binding** - Use `:named` or `?` positional parameters on all databases
- **Lazy result sets** - Rows are fetched on demand, not all at once
- **Rich record access** - Access columns by name, index, or attribute
- **Transaction support** - Context manager with automatic commit/rollback
- **Pandas integration** - Convert results directly to DataFrames
- **Pydantic support** - Map rows to Pydantic models
- **CSV import/export** - Bulk data transfer with `copy_to` and `copy_from`
- **SQL file management** - Load named queries from `.sql` files with QueryStore

## Example with Output

```python
from dbplus import Database

db = Database('DB2://db2demo:demodb2@192.168.1.222:50000/sample')

rows = db.query(
    'SELECT * FROM emp WHERE edlevel=:edlevel AND workdept=:wd',
    edlevel=18, wd='A00'
)
print(rows)
```

Output:

| empno  | firstnme  | midinit | lastname  | workdept | phoneno | hiredate   | job      | edlevel | sex | birthdate  | salary    | bonus   | comm    |
|--------|-----------|---------|-----------|----------|---------|------------|----------|---------|-----|------------|-----------|---------|---------|
| 000010 | CHRISTINE | I       | HAAS      | A00      | 3978    | 1995-01-01 | PRES     | 18      | F   | 1963-08-24 | 152750.00 | 1000.00 | 4220.00 |
| 200010 | DIAN      | J       | HEMMINGER | A00      | 3978    | 1995-01-01 | SALESREP | 18      | F   | 1973-08-14 | 46500.00  | 1000.00 | 4220.00 |
