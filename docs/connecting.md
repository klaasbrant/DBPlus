# Connecting to Databases

## Database URLs

DBPlus uses URL-style connection strings to connect to databases. The general format is:

```
driver://user:password@host:port/database
```

### URL Examples

```python
from dbplus import Database

# SQLite (local file)
db = Database('sqlite:///path/to/database.db')

# PostgreSQL
db = Database('postgres://myuser:mypassword@localhost:5432/mydb')

# MySQL
db = Database('mysql://myuser:mypassword@localhost:3306/mydb')

# Oracle
db = Database('oracle://myuser:mypassword@localhost:1521/xe')

# IBM DB2 (remote)
db = Database('db2://myuser:mypassword@192.168.1.100:50000/sample')

# IBM DB2 (local catalog, no user/password)
db = Database('db2:///sample')
```

The driver name in the URL is case-insensitive (`sqlite`, `SQLite`, and `SQLITE` all work).

### SQLite Path Formats

SQLite URLs use three slashes before the path. For an absolute path, this results in four slashes on Unix-like systems:

```python
# Relative path
db = Database('sqlite:///mydata.db')

# Absolute path (Windows)
db = Database('sqlite:///C:/data/mydata.db')

# Absolute path (Linux/Mac)
db = Database('sqlite:////home/user/data/mydata.db')
```

## Environment Variable

If no URL is passed to the constructor, DBPlus falls back to the `DATABASE_URL` environment variable:

```python
import os
os.environ['DATABASE_URL'] = 'sqlite:///mydata.db'

db = Database()  # Uses DATABASE_URL
```

## Connection Lifecycle

### Manual Management

```python
db = Database('sqlite:///mydata.db')  # Opens automatically
# ... use the database ...
db.close()                             # Close when done
```

### Context Manager

The `Database` object supports the `with` statement for automatic cleanup:

```python
with Database('sqlite:///mydata.db') as db:
    rows = db.query('SELECT * FROM users')
    # ...
# Connection is automatically closed here
```

### Connection State

```python
db.is_connected()    # Returns True if connected
db.ensure_connected()  # Reconnects if disconnected
db.close()           # Close the connection
db.open()            # Reopen a closed connection
```

## Logging

DBPlus uses Python's standard `logging` module under the logger name `dbplus`. Enable it to see connection details, SQL statements, and parameter values:

```python
import logging
logging.basicConfig(level=logging.INFO)

db = Database('sqlite:///mydata.db')
# Logs: --> Using Database driver: SQLITE
# Logs: --> Database connected
```

Set `level=logging.DEBUG` for more detailed output including parameter binding and timing information.
