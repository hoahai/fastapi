# shared/db.py

from typing import Callable, TypeVar

import mysql.connector

from shared.utils import load_env
from shared.tenant import get_env

load_env()

T = TypeVar("T")


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


def run_transaction(
    work: Callable[[mysql.connector.cursor.MySQLCursor], T],
    *,
    cursor_kwargs: dict | None = None,
) -> T:
    conn = get_connection()
    cursor = conn.cursor(**(cursor_kwargs or {}))
    try:
        conn.start_transaction()
        result = work(cursor)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def execute_write(query: str, params: tuple | None = None) -> int:
    def _work(cursor: mysql.connector.cursor.MySQLCursor) -> int:
        cursor.execute(query, params)
        return cursor.rowcount

    return run_transaction(_work)


def execute_many(query: str, rows: list[tuple]) -> int:
    if not rows:
        return 0

    def _work(cursor: mysql.connector.cursor.MySQLCursor) -> int:
        cursor.executemany(query, rows)
        return cursor.rowcount

    return run_transaction(_work)
