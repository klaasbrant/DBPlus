# CSV Import and Export

DBPlus provides built-in methods for exporting query results to CSV files and importing CSV files into database tables.

## Exporting to CSV (`copy_to`)

Export a table or query result to a CSV file:

```python
db.copy_to('output.csv', 'employees')
```

### Parameters

| Parameter  | Type   | Default  | Description                              |
|------------|--------|----------|------------------------------------------|
| `file`     | `str`  | Required | Output file path                         |
| `table`    | `str`  | Required | Table name to export                     |
| `sep`      | `str`  | `\t`     | Field separator (delimiter)              |
| `null`     | `str`  | `\x00`   | String to represent NULL values          |
| `columns`  | `list` | `None`   | List of column names (default: all `*`)  |
| `header`   | `bool` | `False`  | Write column names as the first row      |
| `append`   | `bool` | `False`  | Append to file instead of overwriting    |
| `recsep`   | `str`  | `\n`     | Record separator (line ending)           |

### Return Value

Returns the number of rows written.

### Examples

```python
# Basic export (tab-separated, no header)
rows_written = db.copy_to('employees.tsv', 'employees')
print(f'{rows_written} rows exported')

# CSV with header
db.copy_to('employees.csv', 'employees', sep=',', header=True)

# Export specific columns
db.copy_to('names.csv', 'employees', sep=',', header=True,
           columns=['firstname', 'lastname', 'email'])

# Append to existing file
db.copy_to('all_data.csv', 'employees', sep=',', append=True)

# Custom NULL representation
db.copy_to('data.csv', 'employees', sep=',', null='NULL')
```

## Importing from CSV (`copy_from`)

Import data from a CSV file into a database table:

```python
db.copy_from('data.csv', 'employees')
```

### Parameters

| Parameter  | Type   | Default  | Description                              |
|------------|--------|----------|------------------------------------------|
| `file`     | `str`  | Required | Input file path                          |
| `table`    | `str`  | Required | Target table name                        |
| `sep`      | `str`  | `\t`     | Field separator (delimiter)              |
| `recsep`   | `str`  | `\n`     | Record separator (line ending)           |
| `header`   | `bool` | `False`  | Skip the first row (header line)         |
| `null`     | `str`  | `\x00`   | String that represents NULL values       |
| `batch`    | `int`  | `500`    | Number of rows to insert per batch       |
| `columns`  | `list` | `None`   | List of target column names              |

### Return Value

Returns the number of rows read from the file.

### Examples

```python
# Basic import (tab-separated, no header)
rows_read = db.copy_from('employees.tsv', 'employees')
print(f'{rows_read} rows imported')

# CSV with header row
db.copy_from('employees.csv', 'employees', sep=',', header=True)

# Import into specific columns
db.copy_from('names.csv', 'employees', sep=',', header=True,
             columns=['firstname', 'lastname', 'email'])

# Adjust batch size for large files
db.copy_from('large_data.csv', 'employees', sep=',', batch=1000)

# Custom NULL handling
db.copy_from('data.csv', 'employees', sep=',', null='NULL')
```

### Batch Processing

The `copy_from` method inserts rows in batches (default 500 rows per batch) for performance. Each batch is a single `INSERT ... VALUES` statement with multiple rows. Adjust the `batch` parameter based on your data and database:

```python
# Smaller batches for rows with many columns or large values
db.copy_from('wide_table.csv', 'wide_table', batch=100)

# Larger batches for simple data
db.copy_from('simple_data.csv', 'simple_table', batch=2000)
```

## Round-Trip Example

Export from one database and import into another:

```python
from dbplus import Database

# Export from source
source = Database('db2://user:pass@host1:50000/production')
source.copy_to('employees.csv', 'employees', sep=',', header=True)
source.close()

# Import into target
target = Database('postgres://user:pass@host2:5432/staging')
target.copy_from('employees.csv', 'employees', sep=',', header=True)
target.close()
```
