# Database Drivers

Each database backend is implemented as a driver module in `dbplus/drivers/`. All drivers implement the same `BaseDriver` interface, ensuring consistent behavior across databases.

## Driver Selection

The driver is selected automatically based on the URL scheme. The scheme name (case-insensitive) maps directly to the driver module:

| URL Scheme  | Driver Module | Native Library         | Placeholder |
|-------------|---------------|------------------------|-------------|
| `sqlite`    | `SQLITE.py`   | `sqlite3` (built-in)   | `?`         |
| `postgres`  | `POSTGRES.py` | `psycopg2`             | `%s`        |
| `mysql`     | `MYSQL.py`    | `mysql.connector`      | `%s`        |
| `oracle`    | `ORACLE.py`   | `oracledb`             | `:`         |
| `db2`       | `DB2.py`      | `ibm_db`               | `?`         |

You never need to work with placeholders directly. DBPlus's `Statement` class automatically translates your `:named` and `?` positional parameters into the appropriate driver placeholder.

## Accessing the Driver

You can access the underlying driver object for driver-specific functionality:

```python
driver = db.get_driver()
```

Additionally, `Database` proxies unknown method calls to the driver, so you can call driver-specific methods directly on the `Database` object:

```python
# DB2-specific: get server information
info = db.get_server_info()
print(info.DB_NAME)

# DB2-specific: list columns
cols = db.columns(None, '%', 'EMPLOYEES', '')
print(cols)
```

## SQLite

**Library:** `sqlite3` (built-in)

**Connection URL:**
```
sqlite:///path/to/database.db
```

SQLite is the simplest driver to use since it requires no additional installation. The database is a local file.

**Features:**

- Autocommit mode is enabled by default
- Row factory produces dictionary-style rows
- Supports `last_insert_id()` via `lastrowid`

**Default port:** N/A (file-based)

## PostgreSQL

**Library:** `psycopg2`

**Connection URL:**
```
postgres://user:password@host:port/database
```

**Features:**

- Cursor-based row fetching
- `bytearray` values are automatically decoded to UTF-8 strings
- Supports `callproc()` for stored procedures

**Default port:** 5432

## MySQL

**Library:** `mysql-connector-python`

**Connection URL:**
```
mysql://user:password@host:port/database
```

**Features:**

- Detailed connection error messages (access denied, database not found)
- `bytearray` values are automatically decoded to UTF-8 strings
- Supports `callproc()` for stored procedures

**Default port:** 3306

## Oracle

**Library:** `oracledb`

**Connection URL:**
```
oracle://user:password@host:port/service_name
```

**Features:**

- Uses native `:param` binding (kwargs passed directly to Oracle)
- DSN format: `host:port/service_name`
- Supports `callproc()` for stored procedures

**Default port:** 1521

!!! note
    Oracle uses `:param` placeholders natively, so named parameters are passed directly to the Oracle driver without rewriting. This means Oracle queries must use `:param` style exclusively.

## IBM DB2

**Library:** `ibm_db`

**Connection URL:**
```
db2://user:password@host:port/database
```

For a local cataloged database (no authentication):
```
db2:///database_name
```

**Features:**

- Persistent connections via `ibm_db.pconnect()`
- Column names returned in lowercase (`ATTR_CASE: CASE_LOWER`)
- `last_insert_id()` via `IDENTITY_VAL_LOCAL()`
- `callproc()` with multiple result sets (`next_result()`)
- `columns()` for table introspection
- `get_server_info()` for database server metadata
- `execute_many()` for batch inserts
- `describe_cursor()` for detailed column metadata
- Windows `clidriver` DLL auto-detection

**Default port:** 50000

**DB2-specific methods available on the Database object:**

```python
db = Database('db2://user:pass@host:50000/sample')

# Server information
info = db.get_server_info()
print(info.DB_NAME, info.DBMS_VER)

# Table column information
cols = db.columns(None, 'SCHEMA', 'TABLE_NAME', '')
for col in cols:
    print(col.column_name, col.type_name)
```

## Connection String Formats

### DB2 Remote Connection

For remote DB2 connections, the driver builds a connection string:
```
DATABASE=sample;UID=user;PWD=pass;HOSTNAME=192.168.1.100;PORT=50000;PROTOCOL=TCPIP;
```

### DB2 Local (Cataloged) Connection

For local connections (where host is `localhost` or omitted), the driver uses DSN format:
```
DSN=sample;UID=;PWD=;
```
