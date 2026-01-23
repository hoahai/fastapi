# shared/db.py

from typing import Callable, TypeVar
import hashlib
import threading

import mysql.connector
from mysql.connector import pooling

from shared.utils import load_env
from shared.tenant import get_env

load_env()

T = TypeVar("T")

_POOL_LOCK = threading.Lock()
_POOLS: dict[str, pooling.MySQLConnectionPool] = {}


def get_connection():
    if not _pooling_enabled():
        return mysql.connector.connect(**_build_connection_kwargs())
    return _get_pool().get_connection()


def _pooling_enabled() -> bool:
    value = get_env("DB_POOL_ENABLED", "true")
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _pool_size() -> int:
    raw = get_env("DB_POOL_SIZE", "5")
    try:
        size = int(str(raw).strip())
    except (TypeError, ValueError):
        size = 5
    return max(size, 1)


def _pool_reset_session() -> bool:
    value = get_env("DB_POOL_RESET_SESSION", "true")
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _build_connection_kwargs() -> dict:
    return {
        "host": get_env("DB_HOST"),
        "port": int(get_env("DB_PORT", "3306")),
        "user": get_env("DB_USER"),
        "password": get_env("DB_PASSWORD"),
        "database": get_env("DB_NAME"),
        "ssl_disabled": False,
    }


def _pool_key() -> str:
    params = _build_connection_kwargs()
    signature = (
        f"{params.get('host')}|{params.get('port')}|{params.get('user')}|"
        f"{params.get('password')}|{params.get('database')}|{params.get('ssl_disabled')}"
    )
    digest = hashlib.md5(signature.encode("utf-8")).hexdigest()[:12]
    return f"db_{digest}"


def _get_pool() -> pooling.MySQLConnectionPool:
    key = _pool_key()
    with _POOL_LOCK:
        pool = _POOLS.get(key)
        if pool is None:
            pool = pooling.MySQLConnectionPool(
                pool_name=key,
                pool_size=_pool_size(),
                pool_reset_session=_pool_reset_session(),
                **_build_connection_kwargs(),
            )
            _POOLS[key] = pool
        return pool


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
