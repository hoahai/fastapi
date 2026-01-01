# functions/utils.py

from __future__ import annotations

from datetime import datetime, date
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

from functions.logger import get_logger
from functions.metrics import GLOBAL_METRICS


logger = get_logger("parallel")

T = TypeVar("T")
R = TypeVar("R")

# =====================================================================
# DATE HELPERS
# =====================================================================

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

# =====================================================================
# RATE LIMIT BUCKET
# =====================================================================

class RateLimitBucket:
    """
    Token bucket rate limiter.
    Example: rate=5, per=1.0 → 5 requests / second
    """

    def __init__(self, rate: int, per: float) -> None:
        self.rate = rate
        self.per = per
        self.tokens = float(rate)
        self.updated_at = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> None:
        wait_start: Optional[float] = None

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
                    if wait_start is not None:
                        GLOBAL_METRICS.observe(
                            "rate_limit.wait_seconds",
                            time.monotonic() - wait_start,
                        )
                    return

            if wait_start is None:
                wait_start = time.monotonic()
                GLOBAL_METRICS.inc("rate_limit.waits")

            time.sleep(0.01)


GLOBAL_RATE_BUCKET: Optional[RateLimitBucket] = None

if PARALLEL_RATE_LIMIT > 0:
    GLOBAL_RATE_BUCKET = RateLimitBucket(
        rate=PARALLEL_RATE_LIMIT,
        per=PARALLEL_RATE_INTERVAL,
    )

# =====================================================================
# SAFE TASK DEFINITION
# =====================================================================

ParallelTask = tuple[Callable[..., R], tuple[Any, ...]]

def _validate_task(task: ParallelTask) -> None:
    if not isinstance(task, tuple) or len(task) != 2:
        raise TypeError(
            "Each task must be a tuple: (callable, args_tuple)"
        )

    func, args = task

    if not callable(func):
        raise TypeError(f"Task function {func!r} is not callable")

    if func.__name__ == "<lambda>":
        raise TypeError(
            "Lambdas are forbidden in run_parallel (unsafe with threads)"
        )

    if not isinstance(args, tuple):
        raise TypeError(
            f"Arguments for {func.__name__} must be a tuple"
        )

# =====================================================================
# RETRY + EXECUTION
# =====================================================================

def _run_with_retry(
    func: Callable[..., R],
    args: tuple[Any, ...],
    *,
    api_name: str,
) -> R:
    attempt = 0
    start = time.monotonic()

    while True:
        attempt += 1
        try:
            if GLOBAL_RATE_BUCKET:
                GLOBAL_RATE_BUCKET.acquire()

            # Jitter (pre-call)
            if PARALLEL_JITTER_MAX > 0:
                time.sleep(
                    random.uniform(
                        PARALLEL_JITTER_MIN,
                        PARALLEL_JITTER_MAX,
                    )
                )

            GLOBAL_METRICS.inc(f"{api_name}.tasks.started")

            result = func(*args)

            duration = time.monotonic() - start
            GLOBAL_METRICS.inc(f"{api_name}.tasks.success")
            GLOBAL_METRICS.observe(
                f"{api_name}.tasks.duration",
                duration,
            )

            logger.info(
                "Task completed",
                extra={
                    "api": api_name,
                    "function": func.__name__,
                    "attempts": attempt,
                    "duration_ms": int(duration * 1000),
                },
            )

            return result

        except Exception as exc:
            GLOBAL_METRICS.inc(f"{api_name}.tasks.errors")
            GLOBAL_METRICS.inc(f"{api_name}.tasks.retries")

            logger.warning(
                "Task failed",
                extra={
                    "api": api_name,
                    "function": func.__name__,
                    "attempt": attempt,
                    "error": str(exc),
                },
            )

            if attempt >= PARALLEL_MAX_RETRIES:
                logger.error(
                    "Task permanently failed",
                    extra={
                        "api": api_name,
                        "function": func.__name__,
                        "error": str(exc),
                    },
                )
                raise

            backoff = min(
                PARALLEL_INITIAL_BACKOFF * (2 ** (attempt - 1)),
                PARALLEL_MAX_BACKOFF,
            )
            time.sleep(backoff)

# =====================================================================
# PARALLEL EXECUTION (SAFE, GUARDED)
# =====================================================================

def run_parallel(
    *,
    tasks: Iterable[ParallelTask],
    api_name: str = "default",
    max_workers: int = PARALLEL_MAX_WORKERS,
    timeout: int = PARALLEL_TASK_TIMEOUT,
) -> list[R]:
    """
    Safely execute tasks in parallel.

    STRICT CONTRACT:
    - tasks MUST be [(callable, args_tuple), ...]
    - lambdas are forbidden
    - closures are forbidden
    """

    task_list = list(tasks)
    if not task_list:
        return []

    for task in task_list:
        _validate_task(task)

    batch_start = time.monotonic()
    GLOBAL_METRICS.inc(f"{api_name}.batches.started")

    results: list[R] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                _run_with_retry,
                func,
                args,
                api_name=api_name,
            ): (func, args)
            for func, args in task_list
        }

        for future in as_completed(future_map):
            func, args = future_map[future]

            try:
                results.append(future.result(timeout=timeout))

            except TimeoutError:
                GLOBAL_METRICS.inc(f"{api_name}.tasks.timeout")
                raise TimeoutError(
                    f"{func.__name__}{args} timed out after {timeout}s"
                )

            except Exception as exc:
                raise RuntimeError(
                    f"Parallel task {func.__name__}{args} failed"
                ) from exc

    duration = time.monotonic() - batch_start
    GLOBAL_METRICS.inc(f"{api_name}.batches.completed")
    GLOBAL_METRICS.observe(
        f"{api_name}.batches.duration",
        duration,
    )

    logger.info(
        "Batch completed",
        extra={
            "api": api_name,
            "tasks": len(task_list),
            "duration_ms": int(duration * 1000),
        },
    )

    return results

# =====================================================================
# PARALLEL FLATTEN HELPER
# =====================================================================

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
    