"""Database connectors for DB2 and PostgreSQL."""

__all__ = []

# PostgreSQL connector (always available)
try:
    from .postgres import PostgresConnector
    __all__.append("PostgresConnector")
except ImportError:
    PostgresConnector = None

# DB2 connector (requires ibm_db package)
try:
    from .db2 import DB2Connector
    __all__.append("DB2Connector")
except ImportError:
    DB2Connector = None
