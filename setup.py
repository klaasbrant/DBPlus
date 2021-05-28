from setuptools import setup

setup(name='DBPlus',
      version='0.1.1',
      description='Generic Database Interface (DB2, MySQL, SQLite and more)',
      url='https://github.com/klaasbrant/DBPlus',
      author='Klaas Brant',
      author_email='kbrant@kbce.com',
      license='ISC',
      packages=['dbplus','dbplus.drivers'],
      install_requires=[],
      zip_safe=False)
