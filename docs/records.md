# Record and RecordCollection

## Record

A `Record` represents a single row from a query result. It provides multiple ways to access column values.

### Accessing Values

```python
row = db.query('SELECT empno, firstname, lastname FROM employees').one()

# By column name (dictionary-style)
print(row['firstname'])

# By attribute
print(row.firstname)

# By column index (0-based)
print(row[0])  # empno
print(row[1])  # firstname

# With a default value
print(row.get('middle_name', 'N/A'))
```

### Getting Keys and Values

```python
row = db.query('SELECT empno, firstname, lastname FROM employees').one()

print(row.keys())    # ['empno', 'firstname', 'lastname']
print(row.values())  # ['000010', 'CHRISTINE', 'HAAS']
```

### Conversion Methods

```python
row = db.query('SELECT empno, firstname FROM employees').one()

# To dictionary
d = row.as_dict()
# {'empno': '000010', 'firstname': 'CHRISTINE'}

# To tuple
t = row.as_tuple()
# ('000010', 'CHRISTINE')

# To list
l = row.as_list()
# ['000010', 'CHRISTINE']

# To JSON string
j = row.as_json()
# '{"empno": "000010", "firstname": "CHRISTINE"}'

# To JSON with formatting options
j = row.as_json(indent=2)
```

### Pydantic Model Conversion

Map a record to a Pydantic model:

```python
from pydantic import BaseModel

class Employee(BaseModel):
    empno: str
    firstname: str
    lastname: str

row = db.query('SELECT empno, firstname, lastname FROM employees').one()
emp = row.as_model(Employee)
print(emp.firstname)  # CHRISTINE
```

The `as_model()` method also works with Python dataclasses or any class that accepts keyword arguments in its constructor.

### String Representation

```python
row = db.query('SELECT empno, firstname FROM employees').one()
print(row)
# <Record {"empno": "000010", "firstname": "CHRISTINE"}>
```

## RecordCollection

A `RecordCollection` is a lazy container of `Record` objects returned by `db.query()`. Rows are fetched from the database cursor on demand.

### Lazy Evaluation

Rows are only fetched when accessed. This means you can start processing results before the entire query has completed:

```python
rows = db.query('SELECT * FROM large_table')
# No rows fetched yet

first = rows[0]    # Fetches the first row
# Only one row fetched

for row in rows:   # Fetches remaining rows one by one
    process(row)
```

### Iteration

```python
rows = db.query('SELECT * FROM employees')
for row in rows:
    print(row.firstname, row.lastname)
```

### Indexing

```python
rows = db.query('SELECT * FROM employees')

first = rows[0]     # First row
third = rows[2]     # Third row
last = rows[-1]     # Last row (requires fetching all rows)
```

### Slicing

Slicing returns a new `RecordCollection`:

```python
rows = db.query('SELECT * FROM employees')

first_five = rows[0:5]     # First 5 rows
from_third = rows[2:]      # From third row onward (fetches all)
last_three = rows[-3:]     # Last 3 rows (fetches all)

for row in first_five:
    print(row.firstname)
```

### Length

```python
rows = db.query('SELECT * FROM employees')

# len() returns count of rows fetched so far
print(len(rows))  # 0 (nothing fetched yet)

rows.all()
print(len(rows))  # Total row count
```

### Bulk Conversion

```python
rows = db.query('SELECT * FROM employees')

# All as Record objects
records = rows.all()

# All as dictionaries
dicts = rows.all(as_dict=True)
# or: dicts = rows.as_dict()

# All as tuples
tuples = rows.all(as_tuple=True)
# or: tuples = rows.as_tuple()

# All as JSON strings
json_list = rows.all(as_json=True)
# or: json_list = rows.as_json()
```

### DataFrame Conversion

Convert the entire result set to a Pandas DataFrame:

```python
rows = db.query('SELECT * FROM employees')
df = rows.as_DataFrame()

# Use any Pandas operations
print(df.describe())
print(df.to_csv())
print(df.groupby('department').mean())
```

Requires `pandas` to be installed. Raises `NotImplementedError` with an installation hint if Pandas is missing.

### Pydantic Model Conversion

Convert all rows to a list of Pydantic model instances:

```python
from pydantic import BaseModel

class Employee(BaseModel):
    firstnme: str
    lastname: str

rows = db.query('SELECT firstnme, lastname FROM employees')
employees = rows.as_model(Employee)

for emp in employees:
    print(f'{emp.firstnme} {emp.lastname}')
```

### Convenience Methods

#### `one(default=None)`

Returns the first record, or `default` if the result set is empty:

```python
row = db.query('SELECT * FROM employees WHERE id = ?', 42).one()
if row is None:
    print('Employee not found')
```

#### `scalar(default=None)`

Returns the first column of the first row. Ideal for aggregate queries:

```python
count = db.query('SELECT COUNT(*) FROM employees').scalar()
name = db.query('SELECT firstname FROM employees WHERE id = ?', 42).scalar(default='Unknown')
```

!!! note
    `scalar()` automatically closes the cursor after reading the value.

### Table Display

When printed, a `RecordCollection` renders as a formatted table:

```python
rows = db.query('SELECT empno, firstname, lastname FROM employees')
print(rows)
```

```
empno |firstname|lastname
------|---------|--------
000010|CHRISTINE|HAAS
000020|MICHAEL  |THOMPSON
```

### Closing a RecordCollection

If you're done with a result set before all rows have been consumed, close it to release the database cursor:

```python
rows = db.query('SELECT * FROM large_table')
first = rows[0]
rows.close()  # Release the cursor
```

If all rows have been fetched (via iteration, `all()`, or indexing past the end), the cursor is closed automatically.
