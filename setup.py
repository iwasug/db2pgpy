from setuptools import setup, find_packages

setup(
    name="db2pgpy",
    version="1.0.0",
    description="CLI tool for migrating DB2 databases to PostgreSQL",
    author="Your Name",
    packages=find_packages(),
    install_requires=[
        "ibm_db>=3.1.0",
        "psycopg2-binary>=2.9.0",
        "PyYAML>=6.0",
        "click>=8.0.0",
        "tqdm>=4.65.0",
        "colorama>=0.4.6",
        "jsonschema>=4.17.0",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-mock>=3.10.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "db2pgpy=db2pgpy.cli:main",
        ],
    },
    python_requires=">=3.8",
)
