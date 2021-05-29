from setuptools import setup

setup(name='DBPlus',
      version='0.2',
      description='Database-agnostic SQL Interface for Postresql, MySQL, SQLite, DB2 and more',
      url='https://github.com/klaasbrant/DBPlus',
      author='Klaas Brant',
      author_email='kbrant@kbce.com',
      license='ISC',
      packages=['dbplus','dbplus.drivers'],
      install_requires=[],
      zip_safe=False)
