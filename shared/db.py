# shared/db.py

from typing import Callable, TypeVar
import hashlib
import threading
import time
import random

import mysql.connector
from mysql.connector import pooling
from mysql.connector.errors import PoolError

from shared.utils import load_env
from shared.tenant import get_env

load_env()

T = TypeVar("T")

_POOL_LOCK = threading.Lock()
_POOLS: dict[str, pooling.MySQLConnectionPool] = {}


def get_connection():
    if not _pooling_enabled():
        return mysql.connector.connect(**_build_connection_kwargs())
    pool = _get_pool()
    timeout_ms = _pool_acquire_timeout_ms()
    backoff_ms = _pool_acquire_backoff_ms()
    max_backoff_ms = _pool_acquire_max_backoff_ms()
    deadline = (
        time.monotonic() + (timeout_ms / 1000)
        if timeout_ms > 0
        else None
    )
    attempt = 0

    while True:
        try:
            return pool.get_connection()
        except PoolError as exc:
            message = str(exc).lower()
            if "exhausted" not in message and "failed getting connection" not in message:
                raise
            if deadline is not None and time.monotonic() >= deadline:
                raise
            attempt += 1
            sleep_ms = min(
                max_backoff_ms,
                backoff_ms * (1 + attempt * 0.2),
            )
            sleep_ms *= random.uniform(0.75, 1.25)
            time.sleep(max(sleep_ms, 1) / 1000)


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


def _pool_acquire_timeout_ms() -> int:
    raw = get_env("DB_POOL_ACQUIRE_TIMEOUT_MS", "2000")
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        value = 2000
    return max(value, 0)


def _pool_acquire_backoff_ms() -> int:
    raw = get_env("DB_POOL_ACQUIRE_BACKOFF_MS", "50")
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        value = 50
    return max(value, 1)


def _pool_acquire_max_backoff_ms() -> int:
    raw = get_env("DB_POOL_ACQUIRE_MAX_BACKOFF_MS", "500")
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        value = 500
    return max(value, 1)


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
