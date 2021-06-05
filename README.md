# Introduction 
DBPlus is a interface layer between the several python database interfaces and your program. It makes the SQL access from your program database-agnostic meaning the same code can run unmodified on several databases. All you need to change is the database URL. Of course if you use specific SQL that will only work on a certain database DBPlus can not change this.

## Installation
The latest stable release from pypi: pip install dbplus

From github: Clone the repository using git and issue "pip install ."

*Please note* that DBPlus requires you to install the clients and their pre-req's:

- DB2: ibm_db
- SQLite: builtin into python (no client required)
- MySQL: Mysql Connector
- Oracle: CX_Oracle
- Postgresql: psycopg2 

Documentation : https://dbplus.readthedocs.io/en/latest/ [![Documentation Status](https://readthedocs.org/projects/dbplus/badge/?version=latest)](https://dbplus.readthedocs.io/en/latest/?badge=latest)

## Example

```python
from dbplus import Database

# Examples of database urls

#db = Database('SQLite:///test.db')  # driver included in python
#db = Database('Postgres://<user>:<password>@127.0.0.1:5432/dvdrental') # requires psycopg2
#db = Database('MySQL://<user>:<password>@127.0.0.1:3306/test') # requires Mysql Connector
#db = Database('Oracle://<user>:<password>@127.0.0.1:1521/xe') # requires CX_Oracle

db = Database('DB2://db2demo:demodb2@192.168.1.222:50000/sample') # requires ibm_db

# Using named variables in query

rows = db.query('select * from klaas.emp where edlevel=:edlevel and workdept=:wd',edlevel=18,wd='A00')
print(rows,'\n')
print('rows[1]={}\n'.format(rows[1]))
df=rows.as_DataFrame()
print('csv to stdout, check the many options with dataframes!  \n',df.to_csv())

# Full transaction support

with db.transaction():
    # DELETE
    num = db.execute('DELETE FROM klaas.texample')
    print('Rows deleted from klaas.texample={} \n'.format(num))
    # INSERT
    for i in range(1, 11):
        db.execute('INSERT INTO klaas.texample VALUES (?,?)', i, i)
    # UPDATE
    num = db.execute('UPDATE klaas.texample SET col2 = col2+100  WHERE col1 > ?', 5)
    print ('Rows updated in klaas.texample={} \n'.format(num))

# transaction is now commited

print(db.query('select * from klaas.texample'))

```

Output from example above:

| empno | firstnme  | midinit | lastname  | workdept | phoneno | hiredate   | job      | edlevel | sex  | birthdate  | salary    | bonus   | comm    |
| ------- | --------- | ------- | --------- | -------- | ------- | ---------- | -------- | ------- | ---- | ---------- | --------- | ------- | ------- |
| 000010  | CHRISTINE | I       | HAAS      | A00      | 3978    | 1995-01-01 | PRES     | 18      | F    | 1963-08-24 | 152750.00 | 1000.00 | 4220.00 |
| 200010  | DIAN      | J       | HEMMINGER | A00      | 3978    | 1995-01-01 | SALESREP | 18      | F    | 1973-08-14 | 46500.00  | 1000.00 | 4220.00 |

rows[1]=<Record {"empno": "200010", "firstnme": "DIAN", "midinit": "J", "lastname": "HEMMINGER", "workdept": "A00", "phoneno": "3978", "hiredate": "1995-01-01", "job": "SALESREP", "edlevel": 18, "sex": "F", "birthdate": "1973-08-14", "salary": "46500.00", "bonus": "1000.00", "comm": "4220.00"}>

csv to stdout, check the many options with dataframes!
 ,birthdate,bonus,comm,edlevel,empno,firstnme,hiredate,job,lastname,midinit,phoneno,salary,sex,workdept
0,1963-08-24,1000.00,4220.00,18,000010,CHRISTINE,1995-01-01,PRES    ,HAAS,I,3978,152750.00,F,A00
1,1973-08-14,1000.00,4220.00,18,200010,DIAN,1995-01-01,SALESREP,HEMMINGER,J,3978,46500.00,F,A00

Rows deleted from klaas.texample=10

Rows updated in klaas.texample=5

| col1 | col2 |
| ---- | ---- |
| 1    | 1    |
| 2    | 2    |
| 3    | 3    |
| 4    | 4    |
| 5    | 5    |
| 6    | 106  |
| 7    | 107  |
| 8    | 108  |
| 9    | 109  |
| 10   | 110  |



## What's next?
- Add tests / bug fixing
- Add more documentation / examples
- more cool stuff and of course your suggestions are welcome