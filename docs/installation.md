# Installation

## Installing DBPlus

From PyPI:

```bash
pip install dbplus
```

From GitHub:

```bash
git clone https://github.com/klaasbrant/DBPlus.git
cd DBPlus
pip install .
```

## Installing Database Drivers

DBPlus has no required dependencies. You only need to install the driver for the database(s) you intend to use.

### SQLite

No installation required. SQLite support is built into Python via the `sqlite3` module.

### PostgreSQL

```bash
pip install psycopg2
```

Or the binary-only package (no C compiler needed):

```bash
pip install psycopg2-binary
```

### MySQL

```bash
pip install mysql-connector-python
```

### Oracle

```bash
pip install oracledb
```

### IBM DB2

```bash
pip install ibm_db
```

!!! note "Windows users"
    On Windows, DBPlus automatically locates the `clidriver/bin` directory required by `ibm_db`. It checks the `IBM_DB_HOME` environment variable first, then searches Python's `site-packages` directory. If neither is found, a `FileNotFoundError` is raised.

## Optional Dependencies

These are not required but enable additional features:

| Package   | Feature                                      |
|-----------|----------------------------------------------|
| `pandas`  | `RecordCollection.as_DataFrame()` conversion |
| `pydantic`| `Record.as_model()` and `RecordCollection.as_model()` conversion |
