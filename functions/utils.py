# functions/utils.py

from __future__ import annotations

from datetime import datetime, date
from contextvars import copy_context
from zoneinfo import ZoneInfo
import calendar
import pytz
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import Callable, Iterable, TypeVar, Optional, Any

from functions.constants import (
    TIMEZONE,
    PARALLEL_MAX_WORKERS,
    PARALLEL_MAX_RETRIES,
    PARALLEL_INITIAL_BACKOFF,
    PARALLEL_MAX_BACKOFF,
    PARALLEL_TASK_TIMEOUT,
    PARALLEL_JITTER_MIN,
    PARALLEL_JITTER_MAX,
    PARALLEL_RATE_LIMIT,
    PARALLEL_RATE_INTERVAL,
)

from functions.logger import (
    get_logger,
    enable_console_logging,
    disable_console_logging,
)

T = TypeVar("T")
R = TypeVar("R")

ParallelTask = tuple[Callable[..., R], tuple[Any, ...]]

logger = get_logger("Utils")

# ======================================================
# DATE HELPERS
# ======================================================


def get_current_period() -> dict:
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)

    year = now.year
    month = now.month

    start_date = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = date(year, month, last_day)

    return {
        "year": year,
        "month": month,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


# ======================================================
# RATE LIMIT
# ======================================================


class RateLimitBucket:
    def __init__(self, rate: int, per: float) -> None:
        self.rate = rate
        self.per = per
        self.tokens = float(rate)
        self.updated_at = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated_at

                refill = (elapsed / self.per) * self.rate
                if refill > 0:
                    self.tokens = min(self.rate, self.tokens + refill)
                    self.updated_at = now

                if self.tokens >= 1:
                    self.tokens -= 1
                    return

            time.sleep(0.01)


GLOBAL_RATE_BUCKET: Optional[RateLimitBucket] = (
    RateLimitBucket(PARALLEL_RATE_LIMIT, PARALLEL_RATE_INTERVAL)
    if PARALLEL_RATE_LIMIT > 0
    else None
)

# ======================================================
# VALIDATION
# ======================================================


def _validate_task(task: ParallelTask) -> None:
    if not isinstance(task, tuple) or len(task) != 2:
        raise TypeError("Task must be (callable, args_tuple)")

    func, args = task

    if not callable(func):
        raise TypeError("Task function must be callable")

    if func.__name__ == "<lambda>":
        raise TypeError("Lambdas are forbidden")

    if not isinstance(args, tuple):
        raise TypeError("Args must be tuple")


# ======================================================
# SAFE PARAM LOGGING
# ======================================================


def _safe_serialize_args(args: tuple[Any, ...]) -> list[Any]:
    out: list[Any] = []
    for a in args:
        if isinstance(a, (str, int, float, bool)) or a is None:
            out.append(a)
        elif isinstance(a, list):
            out.append({"type": "list", "length": len(a), "sample": a[:3]})
        elif isinstance(a, dict):
            out.append({"type": "dict", "keys": list(a.keys())[:10]})
        else:
            out.append({"type": type(a).__name__})
    return out


def _safe_serialize_result(result: Any, *, max_str: int = 2000) -> Any:
    if isinstance(result, (str, int, float, bool)) or result is None:
        if isinstance(result, str) and len(result) > max_str:
            return result[:max_str] + "...(truncated)"
        return result
    if isinstance(result, list):
        return {"type": "list", "length": len(result), "sample": result[:3]}
    if isinstance(result, dict):
        return {"type": "dict", "keys": list(result.keys())[:20]}
    return {"type": type(result).__name__}


# ======================================================
# TASK EXECUTION
# ======================================================


def _run_with_retry(
    func: Callable[..., R],
    args: tuple[Any, ...],
    *,
    api_name: str,
) -> R:
    attempts = 0
    start = time.monotonic()

    while True:
        attempts += 1
        try:
            if GLOBAL_RATE_BUCKET:
                GLOBAL_RATE_BUCKET.acquire()

            if PARALLEL_JITTER_MAX > 0:
                time.sleep(
                    random.uniform(
                        PARALLEL_JITTER_MIN,
                        PARALLEL_JITTER_MAX,
                    )
                )

            result = func(*args)
            duration = time.monotonic() - start

            logger.debug(
                "Task summary",
                extra={
                    "extra_fields": {
                        "api": api_name,
                        "function": func.__name__,
                        "params": _safe_serialize_args(args),
                        "result": _safe_serialize_result(result),
                        "status": "success",
                        "attempts": attempts,
                        "duration_ms": int(duration * 1000),
                    }
                },
            )

            return result

        except Exception as exc:
            if attempts >= PARALLEL_MAX_RETRIES:
                duration = time.monotonic() - start

                logger.error(
                    "Task summary",
                    extra={
                        "extra_fields": {
                            "api": api_name,
                            "function": func.__name__,
                            "params": _safe_serialize_args(args),
                            "status": "failed",
                            "attempts": attempts,
                            "duration_ms": int(duration * 1000),
                            "error": str(exc),
                        }
                    },
                )
                raise

            backoff = min(
                PARALLEL_INITIAL_BACKOFF * (2 ** (attempts - 1)),
                PARALLEL_MAX_BACKOFF,
            )
            time.sleep(backoff)


# ======================================================
# PARALLEL EXECUTION
# ======================================================


def run_parallel(
    *,
    tasks: Iterable[ParallelTask],
    api_name: str = "default",
    max_workers: int = PARALLEL_MAX_WORKERS,
    timeout: int = PARALLEL_TASK_TIMEOUT,
    log_to_console: bool = False,
) -> list[R]:

    task_list = list(tasks)
    if not task_list:
        return []

    for t in task_list:
        _validate_task(t)

    if log_to_console:
        enable_console_logging(logger)
    else:
        disable_console_logging(logger)

    results: list[R] = [None] * len(task_list)

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(
                    copy_context().run,
                    _run_with_retry,
                    func,
                    args,
                    api_name=api_name,
                ): idx
                for idx, (func, args) in enumerate(task_list)
            }

            for future in as_completed(future_map):
                idx = future_map[future]
                results[idx] = future.result(timeout=timeout)

    finally:
        disable_console_logging(logger)

    return results


# ======================================================
# FLATTEN
# ======================================================


def run_parallel_flatten(
    *,
    tasks: Iterable[ParallelTask],
    api_name: str = "default",
    **kwargs,
) -> list[Any]:

    results = run_parallel(
        tasks=tasks,
        api_name=api_name,
        **kwargs,
    )

    flattened: list[Any] = []
    for r in results:
        if isinstance(r, list):
            flattened.extend(r)
        else:
            flattened.append(r)

    return flattened


# ======================================================
# ROUTE META
# ======================================================


def format_hms(seconds: float) -> str:
    total_ms = int(seconds * 1000)
    s, ms = divmod(total_ms, 1000)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h:02}:{m:02}:{s:02}.{ms:03}"


def with_meta(*, data: dict | list, start_time: float) -> dict:
    duration = time.perf_counter() - start_time

    return {
        "meta": {
            "timestamp": datetime.now(ZoneInfo(TIMEZONE)).isoformat(),
            "duration_ms": int(duration * 1000),
            "duration_hms": format_hms(duration),
        },
        "data": data,
    }
