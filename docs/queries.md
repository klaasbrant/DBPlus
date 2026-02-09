# Querying Data

## The `query()` Method

Use `db.query()` to execute SELECT statements. It returns a `RecordCollection` containing the result rows.

```python
rows = db.query('SELECT * FROM employees')
```

## Parameter Binding

DBPlus provides a unified parameter syntax that works across all database backends. You never need to worry about whether your database uses `?`, `%s`, or `:param` natively.

### Named Parameters

Use `:param_name` syntax to bind keyword arguments:

```python
rows = db.query(
    'SELECT * FROM employees WHERE department = :dept AND salary > :min_salary',
    dept='Engineering',
    min_salary=50000
)
```

### Positional Parameters

Use `?` to bind positional arguments:

```python
rows = db.query(
    'SELECT * FROM employees WHERE department = ? AND salary > ?',
    'Engineering',
    50000
)
```

### List/Tuple Expansion

Lists and tuples are automatically expanded into comma-separated placeholders, making `IN` clauses easy:

```python
rows = db.query(
    'SELECT * FROM employees WHERE department IN (:depts)',
    depts=['Engineering', 'Sales', 'Marketing']
)
# Expands to: WHERE department IN (?, ?, ?)
```

### Dictionary Parameters

You can pass a dictionary as a positional argument to provide named parameters:

```python
params = {'dept': 'Engineering', 'min_salary': 50000}
rows = db.query(
    'SELECT * FROM employees WHERE department = :dept AND salary > :min_salary',
    params
)
```

## Working with Results

### Iteration

`RecordCollection` is iterable. Rows are fetched lazily from the database cursor:

```python
rows = db.query('SELECT * FROM employees')
for row in rows:
    print(row.firstname, row.lastname)
```

### Indexing and Slicing

Access individual rows by index, or get a subset with slicing:

```python
rows = db.query('SELECT * FROM employees')

first = rows[0]        # First row
last = rows[-1]        # Last row (fetches all rows)
subset = rows[2:5]     # Rows 2, 3, 4 (returns a new RecordCollection)
```

### Length

```python
rows = db.query('SELECT * FROM employees')
print(len(rows))  # Number of rows fetched so far
rows.all()         # Fetch all remaining rows
print(len(rows))  # Total number of rows
```

!!! note
    `len()` returns the count of rows fetched so far, not the total result set size. Call `rows.all()` first if you need the complete count.

### Fetching All Rows

The `all()` method fetches all remaining rows and returns them as a list:

```python
rows = db.query('SELECT * FROM employees')

all_records = rows.all()                # List of Record objects
all_dicts = rows.all(as_dict=True)      # List of dictionaries
all_tuples = rows.all(as_tuple=True)    # List of tuples
all_json = rows.all(as_json=True)       # List of JSON strings
```

### Single Row

Use `one()` to get just the first row, with an optional default:

```python
row = db.query('SELECT * FROM employees WHERE id = ?', 42).one()
if row:
    print(row.name)

# With a default value
row = db.query('SELECT * FROM employees WHERE id = ?', 999).one(default=None)
```

### Scalar Value

Use `scalar()` to get the first column of the first row - ideal for `COUNT(*)` and similar queries:

```python
count = db.query('SELECT COUNT(*) FROM employees').scalar()
print(f'There are {count} employees')

# With a default
max_salary = db.query('SELECT MAX(salary) FROM employees WHERE dept = ?', 'XYZ').scalar(default=0)
```

### Printing Results

`RecordCollection` has a built-in table formatter. Just print it or convert it to a string:

```python
rows = db.query('SELECT empno, firstname, lastname FROM employees')
print(rows)
```

Output:

```
empno |firstname|lastname
------|---------|--------
000010|CHRISTINE|HAAS
000020|MICHAEL  |THOMPSON
```

## Conversion Methods

### To Dictionaries

```python
rows = db.query('SELECT * FROM employees')
list_of_dicts = rows.as_dict()
# [{'empno': '000010', 'firstname': 'CHRISTINE', ...}, ...]
```

### To Tuples

```python
rows = db.query('SELECT * FROM employees')
list_of_tuples = rows.as_tuple()
# [('000010', 'CHRISTINE', ...), ...]
```

### To JSON

```python
rows = db.query('SELECT * FROM employees')
list_of_json = rows.as_json()
# ['{"empno": "000010", "firstname": "CHRISTINE", ...}', ...]
```

### To Pandas DataFrame

Requires `pandas` to be installed:

```python
rows = db.query('SELECT * FROM employees')
df = rows.as_DataFrame()
print(df.to_csv())
```

### To Pydantic Models

Requires `pydantic` to be installed:

```python
from pydantic import BaseModel

class Employee(BaseModel):
    firstnme: str
    lastname: str

rows = db.query('SELECT firstnme, lastname FROM employees')

# Single row
emp = rows[0].as_model(Employee)
print(emp.firstnme, emp.lastname)

# All rows
employees = rows.as_model(Employee)
for emp in employees:
    print(emp.firstnme, emp.lastname)
```

## Result Metadata

Access cursor description information (column names, types, etc.) via the `description` property:

```python
rows = db.query('SELECT * FROM employees')
print(rows.description)
```

## Pending State

A `RecordCollection` tracks whether all rows have been fetched from the database cursor:

```python
rows = db.query('SELECT * FROM employees')
print(rows.pending)  # True - rows not yet fully fetched

rows.all()
print(rows.pending)  # False - all rows fetched
```

## Multiple Result Sets

Some stored procedures return multiple result sets. Use `next_result()` to advance to the next one:

```python
result, params = db.callproc('my_procedure')
print(result)                    # First result set

next_rs = result.next_result()   # Second result set
print(next_rs)
```
