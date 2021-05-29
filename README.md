# Introduction 
DBPlus is a interface layer between the several python database interfaces and your program. It makes the SQL access from your program database-agnostic meaning the same code can run unmodified on several databases. All you need to change is the database URL. Of course if you use specific SQL that will only work on a certain database DBPlus can not change this.

# Installation
The latest stable release from pypi: pip install dbplus

From github: Clone the repository using git and issue "pip install ."

*Please note* that DBPlus requires you to install the clients and their pre-req's:

- DB2: ibm_db
- SQLite: builtin into python (no client required)
- MySQL: Mysql Connector
- Oracle: CX_Oracle
- Postgresql: psycopg2 

Documentation : [![Documentation Status](https://readthedocs.org/projects/dbplus/badge/?version=latest)](https://dbplus.readthedocs.io/en/latest/?badge=latest)

# What's next?
- Add tests / bug fixing
- Add more documentation / examples
- more cool stuff and of course your suggestions are welcome