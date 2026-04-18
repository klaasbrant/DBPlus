# read the contents of your README file
from pathlib import Path

from setuptools import setup

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="dbplus",
    version="0.8.1",
    description="Database-agnostic SQL Interface for Postresql, MySQL, SQLite, DB2 and more",
    url="https://github.com/klaasbrant/DBPlus",
    author="Klaas Brant",
    author_email="kbrant@kbce.com",
    license="ISC",
    packages=["dbplus", "dbplus.drivers"],
    package_data={"dbplus": ["py.typed"]},
    install_requires=[],
    extras_require={"dev": ["pytest"]},
    zip_safe=False,
    long_description=long_description,
    long_description_content_type="text/markdown",
)
