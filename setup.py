from setuptools import setup

setup(name='dbplus',
      version='0.1',
      description='Database Interface for DB2',
      url='http://github.com/kbce/dbplus',
      author='Klaas Brant',
      author_email='kbrant@kbce.com',
      license='MIT',
      packages=['dbplus','dbplus.drivers'],
      install_requires=[],
      zip_safe=False)