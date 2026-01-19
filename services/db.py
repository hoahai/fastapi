# services/db.py

import mysql.connector

from services.utils import load_env
from services.tenant import get_env

load_env()


def get_connection():
    return mysql.connector.connect(
        host=get_env("DB_HOST"),
        port=int(get_env("DB_PORT", "3306")),
        user=get_env("DB_USER"),
        password=get_env("DB_PASSWORD"),
        database=get_env("DB_NAME"),
        ssl_disabled=False
    )


def fetch_all(query: str, params: tuple | None = None) -> list[dict]:
    """
    Generic SELECT query executor.
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(query, params)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()
