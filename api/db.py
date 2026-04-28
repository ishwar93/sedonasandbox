"""
db.py
Shared databricks-sql-connector connection for the FastAPI backend.
Reads credentials from the root .env file.
"""

import os
from databricks import sql
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

DATABRICKS_HOST      = os.environ['DATABRICKS_HOST']
DATABRICKS_HTTP_PATH = os.environ['DATABRICKS_HTTP_PATH']
DATABRICKS_TOKEN     = os.environ['DATABRICKS_TOKEN']


def get_connection():
    """Return a new Databricks SQL connection."""
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )


def query(sql_str: str) -> list[dict]:
    """Execute a SQL string and return rows as list of dicts."""
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_str)
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
