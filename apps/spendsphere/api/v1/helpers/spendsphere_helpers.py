from contextlib import contextmanager
import json
import os
from pathlib import Path
from threading import Lock

from fastapi import HTTPException

from apps.spendsphere.api.v1.helpers.db_queries import get_accounts

try:
    import fcntl
except Exception:  # pragma: no cover - fallback for non-posix
    fcntl = None

_CACHE_BASE_PATH = Path(
    os.getenv(
        "SPENDSPHERE_ACCOUNT_CODE_CACHE_PATH",
        Path(__file__).resolve().parents[2] / "account_code_cache.json",
    )
)
_ACCOUNT_CODE_CACHE_LOCK = Lock()


def _normalize_account_codes(account_codes: str | list[str] | None) -> list[str]:
    if account_codes is None:
        return []
    if isinstance(account_codes, str):
        candidates = [account_codes]
    elif isinstance(account_codes, list):
        candidates = account_codes
    else:
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        code = candidate.strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def _get_cache_path(include_all: bool) -> Path:
    if include_all:
        return _CACHE_BASE_PATH.with_name(
            f"{_CACHE_BASE_PATH.stem}_all{_CACHE_BASE_PATH.suffix}"
        )
    return _CACHE_BASE_PATH


@contextmanager
def _cache_file_lock(cache_path: Path):
    if fcntl is None:
        yield
        return
    lock_path = cache_path.with_suffix(f"{cache_path.suffix}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _load_cache(cache_path: Path) -> dict[str, dict]:
    try:
        with open(cache_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError, ValueError):
        return {}

    if not isinstance(data, dict):
        return {}

    normalized: dict[str, dict] = {}
    for code, account in data.items():
        if not isinstance(code, str) or not isinstance(account, dict):
            continue
        normalized[code.strip().upper()] = account
    return normalized


def _write_cache(cache_path: Path, cache: dict[str, dict]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = cache_path.with_suffix(f"{cache_path.suffix}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(cache, handle)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, cache_path)


def validate_account_codes(
    account_codes: str | list[str] | None,
    *,
    include_all: bool = False,
) -> list[dict]:
    """
    Validate accountCodes against DB.

    Rules:
    - None / ""     -> all accounts
    - "TAAA"        -> single account
    - ["TAAA","X"]  -> multiple accounts
    """

    if not account_codes:
        return get_accounts(account_codes, include_all=include_all)

    requested_codes = _normalize_account_codes(account_codes)
    if not requested_codes:
        return get_accounts(account_codes, include_all=include_all)

    requested_set = set(requested_codes)
    cache_path = _get_cache_path(include_all)

    with _ACCOUNT_CODE_CACHE_LOCK:
        with _cache_file_lock(cache_path):
            cache = _load_cache(cache_path)

    cached_accounts = {code: cache[code] for code in requested_set if code in cache}
    missing_codes = requested_set - set(cached_accounts.keys())

    source_accounts: dict[str, dict]
    if missing_codes:
        db_accounts = get_accounts(None, include_all=include_all)
        db_accounts_map = {a["code"].upper(): a for a in db_accounts}
        with _ACCOUNT_CODE_CACHE_LOCK:
            with _cache_file_lock(cache_path):
                _write_cache(cache_path, db_accounts_map)
        source_accounts = db_accounts_map
    else:
        source_accounts = cache

    found_codes = set(source_accounts.keys())
    missing = sorted(requested_set - found_codes)

    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Invalid accountCodes",
                "invalid_codes": missing,
                "valid_codes": sorted(found_codes),
            },
        )

    # Preserve a deterministic order based on the input list.
    ordered_accounts: list[dict] = []
    for code in requested_codes:
        account = source_accounts.get(code)
        if account:
            ordered_accounts.append(account)

    return ordered_accounts


def require_account_code(account_code: str) -> str:
    if not account_code or not account_code.strip():
        raise HTTPException(status_code=400, detail="account_code is required")
    return account_code.strip().upper()


def should_validate_account_codes(account_codes: str | list[str] | None) -> bool:
    if account_codes is None:
        return False
    if isinstance(account_codes, str) and not account_codes.strip():
        return False
    return not (isinstance(account_codes, list) and len(account_codes) == 0)


def normalize_query_params(params: object) -> dict[str, object] | None:
    if not params:
        return None
    result: dict[str, object] = {}
    try:
        items = params.multi_items()
    except AttributeError:
        try:
            items = dict(params).items()
        except Exception:
            return None
    for key, value in items:
        if key in result:
            existing = result[key]
            if isinstance(existing, list):
                existing.append(value)
            else:
                result[key] = [existing, value]
        else:
            result[key] = value
    return result
