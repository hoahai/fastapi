from datetime import datetime
import ast
import json
import os
from pathlib import Path
from threading import Lock
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from apps.spendsphere.api.v1.helpers.db_queries import get_accounts
from shared.file_cache import FileCache, normalize_tenant_key
from shared.tenant import get_env, get_tenant_id, get_timezone

_CACHE_BASE_PATH = Path(
    os.getenv(
        "SPENDSPHERE_ACCOUNT_CODE_CACHE_PATH",
        Path(__file__).resolve().parents[5] / "caches.json",
    )
)
_ACCOUNT_CODES_KEY = "account_codes"
_GOOGLE_ADS_CLIENTS_KEY = "google_ads_clients"
_GOOGLE_ADS_BUDGETS_KEY = "google_ads_budgets"
_GOOGLE_ADS_CAMPAIGNS_KEY = "google_ads_campaigns"
_GOOGLE_SHEETS_KEY = "google_sheets"
_GOOGLE_ADS_CLIENTS_CACHE_TTL_ENV = "SPENDSPHERE_GOOGLE_ADS_CLIENTS_CACHE_TTL_SECONDS"
_GOOGLE_ADS_CLIENTS_CACHE_TTL_FALLBACK_ENV = "ttl_time"
_DEFAULT_SPENDSPHERE_CACHE_TTL_SECONDS = 86400
_DEFAULT_GOOGLE_ADS_RESOURCE_CACHE_TTL_SECONDS = 300
_ACCOUNT_CODES_SCOPE_ACTIVE = "active"
_ACCOUNT_CODES_SCOPE_ALL = "all"
_CACHE_STORES: dict[str, FileCache] = {}
_CACHE_STORES_LOCK = Lock()


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
    return _CACHE_BASE_PATH


def _get_cache_store(cache_path: Path) -> FileCache:
    key = str(cache_path)
    with _CACHE_STORES_LOCK:
        store = _CACHE_STORES.get(key)
        if store is None:
            store = FileCache(cache_path)
            _CACHE_STORES[key] = store
    return store


def _normalize_account_map(raw: object) -> dict[str, dict]:
    if not isinstance(raw, dict):
        return {}
    normalized: dict[str, dict] = {}
    for code, account in raw.items():
        if not isinstance(code, str) or not isinstance(account, dict):
            continue
        normalized[code.strip().upper()] = account
    return normalized


def _extract_legacy_all_accounts(raw: object) -> dict[str, dict]:
    if not isinstance(raw, dict):
        return {}
    candidate = raw.get(_ACCOUNT_CODES_KEY, raw)
    if not isinstance(candidate, dict):
        return {}

    if "default" in candidate and isinstance(candidate.get("default"), dict):
        default_entry = candidate.get("default") or {}
        if _ACCOUNT_CODES_SCOPE_ALL in default_entry and isinstance(
            default_entry.get(_ACCOUNT_CODES_SCOPE_ALL), dict
        ):
            scope_entry = default_entry.get(_ACCOUNT_CODES_SCOPE_ALL) or {}
            candidate = scope_entry.get("accounts", scope_entry)
        else:
            candidate = default_entry.get("accounts", default_entry)

    return _normalize_account_map(candidate)


def _is_legacy_account_map(raw: object) -> bool:
    if not isinstance(raw, dict):
        return False
    if not raw:
        return False
    for value in raw.values():
        if isinstance(value, dict) and ("code" in value or "name" in value):
            return True
    return False


def _normalize_account_codes_cache(raw: object) -> dict[str, dict[str, dict]]:
    if not isinstance(raw, dict):
        return {}

    if _is_legacy_account_map(raw):
        return {
            "default": {
                _ACCOUNT_CODES_SCOPE_ACTIVE: {
                    "accounts": _normalize_account_map(raw)
                }
            }
        }

    normalized: dict[str, dict[str, dict]] = {}
    for tenant_key, accounts in raw.items():
        if not isinstance(tenant_key, str):
            continue
        if isinstance(accounts, dict) and (
            _ACCOUNT_CODES_SCOPE_ACTIVE in accounts
            or _ACCOUNT_CODES_SCOPE_ALL in accounts
        ):
            tenant_entry: dict[str, dict] = {}
            for scope in (_ACCOUNT_CODES_SCOPE_ACTIVE, _ACCOUNT_CODES_SCOPE_ALL):
                scope_data = accounts.get(scope)
                if scope_data is None:
                    continue
                scope_entry: dict[str, object] = {}
                if isinstance(scope_data, dict) and "accounts" in scope_data:
                    scope_entry["accounts"] = _normalize_account_map(
                        scope_data.get("accounts")
                    )
                    if isinstance(scope_data.get("updated_at"), str):
                        scope_entry["updated_at"] = scope_data.get("updated_at")
                else:
                    scope_entry["accounts"] = _normalize_account_map(scope_data)
                if scope_entry["accounts"] or "updated_at" in scope_entry:
                    tenant_entry[scope] = scope_entry
            if tenant_entry:
                normalized[tenant_key] = tenant_entry
        elif isinstance(accounts, dict) and "accounts" in accounts:
            entry_accounts = _normalize_account_map(accounts.get("accounts"))
            entry: dict[str, dict] = {"accounts": entry_accounts}
            if isinstance(accounts.get("updated_at"), str):
                entry["updated_at"] = accounts.get("updated_at")
            normalized[tenant_key] = {_ACCOUNT_CODES_SCOPE_ACTIVE: entry}
        else:
            normalized[tenant_key] = {
                _ACCOUNT_CODES_SCOPE_ACTIVE: {
                    "accounts": _normalize_account_map(accounts)
                }
            }
    return normalized


def _load_cache_root(cache_store: FileCache) -> dict[str, object]:
    data = cache_store.load_root()
    if not isinstance(data, dict):
        data = {}

    if (
        _ACCOUNT_CODES_KEY in data
        or _GOOGLE_ADS_CLIENTS_KEY in data
        or _GOOGLE_SHEETS_KEY in data
    ):
        root = data
    else:
        root = {_ACCOUNT_CODES_KEY: data}

    account_data = root.get(_ACCOUNT_CODES_KEY)
    account_cache = _normalize_account_codes_cache(account_data)

    google_ads = root.get(_GOOGLE_ADS_CLIENTS_KEY)
    if not isinstance(google_ads, dict):
        google_ads = {}

    google_ads_budgets = root.get(_GOOGLE_ADS_BUDGETS_KEY)
    if not isinstance(google_ads_budgets, dict):
        google_ads_budgets = {}

    google_ads_campaigns = root.get(_GOOGLE_ADS_CAMPAIGNS_KEY)
    if not isinstance(google_ads_campaigns, dict):
        google_ads_campaigns = {}

    google_sheets = root.get(_GOOGLE_SHEETS_KEY)
    if not isinstance(google_sheets, dict):
        google_sheets = {}

    return {
        _ACCOUNT_CODES_KEY: account_cache,
        _GOOGLE_ADS_CLIENTS_KEY: google_ads,
        _GOOGLE_ADS_BUDGETS_KEY: google_ads_budgets,
        _GOOGLE_ADS_CAMPAIGNS_KEY: google_ads_campaigns,
        _GOOGLE_SHEETS_KEY: google_sheets,
    }


def _write_cache_root(cache_store: FileCache, cache: dict[str, object]) -> None:
    cache_store.write_root(cache)


def _write_cache(cache_path: Path, cache: dict[str, dict]) -> None:
    cache_store = _get_cache_store(cache_path)
    root = _load_cache_root(cache_store)
    root[_ACCOUNT_CODES_KEY] = {
        "default": {
            _ACCOUNT_CODES_SCOPE_ACTIVE: {
                "accounts": cache,
                "updated_at": datetime.now(ZoneInfo(get_timezone())).isoformat(),
            }
        }
    }
    _write_cache_root(cache_store, root)


def _normalize_tenant_cache_key(tenant_id: str | None) -> str:
    return normalize_tenant_key(tenant_id)


def _parse_cache_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=ZoneInfo(get_timezone()))
    return parsed


def _get_account_cache_entry(
    tenant_cache: dict,
    *,
    include_all: bool,
) -> tuple[dict[str, dict], str | None]:
    if not isinstance(tenant_cache, dict):
        return {}, None
    scope_key = (
        _ACCOUNT_CODES_SCOPE_ALL if include_all else _ACCOUNT_CODES_SCOPE_ACTIVE
    )
    entry = tenant_cache.get(scope_key)
    if not isinstance(entry, dict):
        if "accounts" in tenant_cache:
            entry = tenant_cache
        else:
            return {}, None

    accounts = entry.get("accounts")
    if not isinstance(accounts, dict):
        accounts = {}
    updated_at = entry.get("updated_at") if isinstance(entry.get("updated_at"), str) else None
    return _normalize_account_map(accounts), updated_at


def _parse_cache_config(raw: object) -> dict[str, object]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return {}
    cleaned = raw.strip()
    if not cleaned:
        return {}
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(cleaned)
        except (ValueError, SyntaxError):
            return {}
    return parsed if isinstance(parsed, dict) else {}


def _get_cache_config() -> dict[str, object]:
    raw = get_env("CACHE") or get_env("cache")
    config = _parse_cache_config(raw)
    normalized: dict[str, object] = {}
    for key, value in config.items():
        if not isinstance(key, str):
            continue
        normalized[key.strip().lower()] = value
    return normalized


def _parse_ttl_value(raw: object) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        value = raw
    else:
        cleaned = str(raw).strip()
        if not cleaned:
            return None
        try:
            value = int(cleaned)
        except ValueError:
            return None
    if value < 0:
        return None
    return value


def _get_cache_override(keys: tuple[str, ...]) -> int | None:
    config = _get_cache_config()
    for key in keys:
        raw = config.get(key)
        value = _parse_ttl_value(raw)
        if value is not None:
            return value
    return None


def get_spendsphere_cache_ttl_seconds() -> int:
    value = _get_cache_override(("ttl_time", "ttl"))
    if value is None:
        value = _parse_ttl_value(get_env(_GOOGLE_ADS_CLIENTS_CACHE_TTL_FALLBACK_ENV))
    if value is None:
        return _DEFAULT_SPENDSPHERE_CACHE_TTL_SECONDS
    return value


def get_account_codes_cache_ttl_seconds() -> int:
    value = _get_cache_override(
        (
            "account_codes_ttl_time",
            "account_code_ttl_time",
            "accountcode_ttl_time",
        )
    )
    if value is None:
        return get_spendsphere_cache_ttl_seconds()
    return value


def get_google_ads_clients_cache_ttl_seconds() -> int:
    raw = get_env(_GOOGLE_ADS_CLIENTS_CACHE_TTL_ENV)
    value = _parse_ttl_value(raw)
    if value is not None:
        return value
    value = _get_cache_override(
        (
            "google_ads_clients_ttl_time",
            "google_ads_client_ttl_time",
            "google_ads_ttl_time",
            "googleadsclients_ttl_time",
        )
    )
    if value is None:
        return get_spendsphere_cache_ttl_seconds()
    return value


def get_google_sheet_cache_ttl_seconds() -> int:
    value = _get_cache_override(
        (
            "google_sheet_ttl_time",
            "google_sheets_ttl_time",
            "googlesheet_ttl_time",
        )
    )
    if value is None:
        return get_spendsphere_cache_ttl_seconds()
    return value


def get_google_ads_budgets_cache_ttl_seconds() -> int:
    value = _get_cache_override(
        (
            "google_ads_budgets_ttl_time",
            "google_ads_budget_ttl_time",
            "googleadsbudgets_ttl_time",
        )
    )
    if value is None:
        return _DEFAULT_GOOGLE_ADS_RESOURCE_CACHE_TTL_SECONDS
    return value


def get_google_ads_campaigns_cache_ttl_seconds() -> int:
    value = _get_cache_override(
        (
            "google_ads_campaigns_ttl_time",
            "google_ads_campaign_ttl_time",
            "googleadscampaigns_ttl_time",
        )
    )
    if value is None:
        return _DEFAULT_GOOGLE_ADS_RESOURCE_CACHE_TTL_SECONDS
    return value


def _write_account_codes_cache(
    cache_path: Path,
    *,
    tenant_key: str,
    accounts: dict[str, dict],
    include_all: bool,
) -> None:
    cache_store = _get_cache_store(cache_path)
    root = _load_cache_root(cache_store)
    account_cache = root.get(_ACCOUNT_CODES_KEY)
    if not isinstance(account_cache, dict):
        account_cache = {}
    tenant_entry = account_cache.get(tenant_key)
    if not isinstance(tenant_entry, dict):
        tenant_entry = {}
    scope_key = (
        _ACCOUNT_CODES_SCOPE_ALL if include_all else _ACCOUNT_CODES_SCOPE_ACTIVE
    )
    tenant_entry[scope_key] = {
        "accounts": accounts,
        "updated_at": datetime.now(ZoneInfo(get_timezone())).isoformat(),
    }
    account_cache[tenant_key] = tenant_entry
    root[_ACCOUNT_CODES_KEY] = account_cache
    _write_cache_root(cache_store, root)


def get_google_ads_clients_cache_entry(
    *,
    tenant_id: str | None = None,
) -> tuple[list[dict] | None, bool]:
    tenant_key = _normalize_tenant_cache_key(tenant_id or get_tenant_id())
    cache_path = _get_cache_path(include_all=False)
    cache_store = _get_cache_store(cache_path)

    with cache_store.lock():
        root = _load_cache_root(cache_store)

    google_ads = root.get(_GOOGLE_ADS_CLIENTS_KEY)
    if not isinstance(google_ads, dict):
        return None, False

    entry = google_ads.get(tenant_key)
    if not isinstance(entry, dict):
        return None, False

    clients = entry.get("clients")
    if not isinstance(clients, list):
        return None, False

    ttl_seconds = get_google_ads_clients_cache_ttl_seconds()
    updated_at = _parse_cache_datetime(entry.get("updated_at"))
    if ttl_seconds <= 0:
        return clients, False
    if updated_at is None:
        return clients, True

    now = datetime.now(ZoneInfo(get_timezone()))
    age_seconds = (now - updated_at).total_seconds()
    return clients, age_seconds > ttl_seconds


def set_google_ads_clients_cache(
    clients: list[dict],
    *,
    tenant_id: str | None = None,
) -> None:
    tenant_key = _normalize_tenant_cache_key(tenant_id or get_tenant_id())
    cache_path = _get_cache_path(include_all=False)
    cache_store = _get_cache_store(cache_path)

    with cache_store.lock():
        root = _load_cache_root(cache_store)
        google_ads = root.get(_GOOGLE_ADS_CLIENTS_KEY)
        if not isinstance(google_ads, dict):
            google_ads = {}
        google_ads[tenant_key] = {
            "clients": clients,
            "updated_at": datetime.now(ZoneInfo(get_timezone())).isoformat(),
        }
        root[_GOOGLE_ADS_CLIENTS_KEY] = google_ads
        _write_cache_root(cache_store, root)


def get_google_ads_budgets_cache_entries(
    account_codes: list[str] | None,
    *,
    tenant_id: str | None = None,
) -> tuple[dict[str, list[dict]], set[str]]:
    codes = _normalize_account_codes(account_codes)
    if not codes:
        return {}, set()

    tenant_key = _normalize_tenant_cache_key(tenant_id or get_tenant_id())
    cache_path = _get_cache_path(include_all=False)
    cache_store = _get_cache_store(cache_path)

    with cache_store.lock():
        root = _load_cache_root(cache_store)

    budgets_cache = root.get(_GOOGLE_ADS_BUDGETS_KEY)
    if not isinstance(budgets_cache, dict):
        return {}, set(codes)

    tenant_entry = budgets_cache.get(tenant_key)
    if not isinstance(tenant_entry, dict):
        return {}, set(codes)

    ttl_seconds = get_google_ads_budgets_cache_ttl_seconds()
    now = datetime.now(ZoneInfo(get_timezone()))

    cached: dict[str, list[dict]] = {}
    missing: set[str] = set()

    for code in codes:
        entry = tenant_entry.get(code)
        if not isinstance(entry, dict):
            missing.add(code)
            continue
        budgets = entry.get("budgets")
        if not isinstance(budgets, list):
            missing.add(code)
            continue
        if ttl_seconds <= 0:
            cached[code] = budgets
            continue
        updated_at = _parse_cache_datetime(entry.get("updated_at"))
        if updated_at is None:
            missing.add(code)
            continue
        age_seconds = (now - updated_at).total_seconds()
        if age_seconds > ttl_seconds:
            missing.add(code)
            continue
        cached[code] = budgets

    return cached, missing


def set_google_ads_budgets_cache(
    account_code: str,
    budgets: list[dict],
    *,
    tenant_id: str | None = None,
) -> None:
    if not account_code or not isinstance(account_code, str):
        return
    code = account_code.strip().upper()
    if not code:
        return

    tenant_key = _normalize_tenant_cache_key(tenant_id or get_tenant_id())
    cache_path = _get_cache_path(include_all=False)
    cache_store = _get_cache_store(cache_path)

    with cache_store.lock():
        root = _load_cache_root(cache_store)
        budgets_cache = root.get(_GOOGLE_ADS_BUDGETS_KEY)
        if not isinstance(budgets_cache, dict):
            budgets_cache = {}
        tenant_entry = budgets_cache.get(tenant_key)
        if not isinstance(tenant_entry, dict):
            tenant_entry = {}
        tenant_entry[code] = {
            "budgets": budgets,
            "updated_at": datetime.now(ZoneInfo(get_timezone())).isoformat(),
        }
        budgets_cache[tenant_key] = tenant_entry
        root[_GOOGLE_ADS_BUDGETS_KEY] = budgets_cache
        _write_cache_root(cache_store, root)


def get_google_ads_campaigns_cache_entries(
    account_codes: list[str] | None,
    *,
    tenant_id: str | None = None,
) -> tuple[dict[str, list[dict]], set[str]]:
    codes = _normalize_account_codes(account_codes)
    if not codes:
        return {}, set()

    tenant_key = _normalize_tenant_cache_key(tenant_id or get_tenant_id())
    cache_path = _get_cache_path(include_all=False)
    cache_store = _get_cache_store(cache_path)

    with cache_store.lock():
        root = _load_cache_root(cache_store)

    campaigns_cache = root.get(_GOOGLE_ADS_CAMPAIGNS_KEY)
    if not isinstance(campaigns_cache, dict):
        return {}, set(codes)

    tenant_entry = campaigns_cache.get(tenant_key)
    if not isinstance(tenant_entry, dict):
        return {}, set(codes)

    ttl_seconds = get_google_ads_campaigns_cache_ttl_seconds()
    now = datetime.now(ZoneInfo(get_timezone()))

    cached: dict[str, list[dict]] = {}
    missing: set[str] = set()

    for code in codes:
        entry = tenant_entry.get(code)
        if not isinstance(entry, dict):
            missing.add(code)
            continue
        campaigns = entry.get("campaigns")
        if not isinstance(campaigns, list):
            missing.add(code)
            continue
        if ttl_seconds <= 0:
            cached[code] = campaigns
            continue
        updated_at = _parse_cache_datetime(entry.get("updated_at"))
        if updated_at is None:
            missing.add(code)
            continue
        age_seconds = (now - updated_at).total_seconds()
        if age_seconds > ttl_seconds:
            missing.add(code)
            continue
        cached[code] = campaigns

    return cached, missing


def set_google_ads_campaigns_cache(
    account_code: str,
    campaigns: list[dict],
    *,
    tenant_id: str | None = None,
) -> None:
    if not account_code or not isinstance(account_code, str):
        return
    code = account_code.strip().upper()
    if not code:
        return

    tenant_key = _normalize_tenant_cache_key(tenant_id or get_tenant_id())
    cache_path = _get_cache_path(include_all=False)
    cache_store = _get_cache_store(cache_path)

    with cache_store.lock():
        root = _load_cache_root(cache_store)
        campaigns_cache = root.get(_GOOGLE_ADS_CAMPAIGNS_KEY)
        if not isinstance(campaigns_cache, dict):
            campaigns_cache = {}
        tenant_entry = campaigns_cache.get(tenant_key)
        if not isinstance(tenant_entry, dict):
            tenant_entry = {}
        tenant_entry[code] = {
            "campaigns": campaigns,
            "updated_at": datetime.now(ZoneInfo(get_timezone())).isoformat(),
        }
        campaigns_cache[tenant_key] = tenant_entry
        root[_GOOGLE_ADS_CAMPAIGNS_KEY] = campaigns_cache
        _write_cache_root(cache_store, root)


def get_google_sheet_cache_entry(
    sheet_key: str,
    *,
    config_hash: str | None = None,
    tenant_id: str | None = None,
) -> tuple[list[dict] | None, bool]:
    tenant_key = _normalize_tenant_cache_key(tenant_id or get_tenant_id())
    cache_path = _get_cache_path(include_all=False)
    cache_store = _get_cache_store(cache_path)

    with cache_store.lock():
        root = _load_cache_root(cache_store)

    google_sheets = root.get(_GOOGLE_SHEETS_KEY)
    if not isinstance(google_sheets, dict):
        return None, False

    tenant_entry = google_sheets.get(tenant_key)
    if not isinstance(tenant_entry, dict):
        return None, False

    entry = tenant_entry.get(sheet_key)
    if not isinstance(entry, dict):
        return None, False

    rows = entry.get("rows")
    if not isinstance(rows, list):
        return None, False

    entry_hash = entry.get("config_hash") if isinstance(entry.get("config_hash"), str) else None
    hash_mismatch = bool(config_hash and entry_hash and entry_hash != config_hash)

    ttl_seconds = get_google_sheet_cache_ttl_seconds()
    updated_at = _parse_cache_datetime(entry.get("updated_at"))
    if ttl_seconds <= 0:
        return rows, hash_mismatch
    if updated_at is None:
        return rows, True

    now = datetime.now(ZoneInfo(get_timezone()))
    age_seconds = (now - updated_at).total_seconds()
    is_stale = age_seconds > ttl_seconds or hash_mismatch
    return rows, is_stale


def set_google_sheet_cache(
    sheet_key: str,
    rows: list[dict],
    *,
    config_hash: str | None = None,
    tenant_id: str | None = None,
) -> None:
    tenant_key = _normalize_tenant_cache_key(tenant_id or get_tenant_id())
    cache_path = _get_cache_path(include_all=False)
    cache_store = _get_cache_store(cache_path)

    with cache_store.lock():
        root = _load_cache_root(cache_store)
        google_sheets = root.get(_GOOGLE_SHEETS_KEY)
        if not isinstance(google_sheets, dict):
            google_sheets = {}
        tenant_entry = google_sheets.get(tenant_key)
        if not isinstance(tenant_entry, dict):
            tenant_entry = {}
        tenant_entry[sheet_key] = {
            "rows": rows,
            "updated_at": datetime.now(ZoneInfo(get_timezone())).isoformat(),
            "config_hash": config_hash,
        }
        google_sheets[tenant_key] = tenant_entry
        root[_GOOGLE_SHEETS_KEY] = google_sheets
        _write_cache_root(cache_store, root)


def refresh_account_codes_cache(
    *,
    include_all: bool,
    tenant_id: str | None = None,
) -> list[dict]:
    accounts = get_accounts(None, include_all=include_all)
    accounts_map = {a["code"].upper(): a for a in accounts}
    cache_path = _get_cache_path(include_all)
    tenant_key = _normalize_tenant_cache_key(tenant_id or get_tenant_id())
    cache_store = _get_cache_store(cache_path)

    with cache_store.lock():
        _write_account_codes_cache(
            cache_path,
            tenant_key=tenant_key,
            accounts=accounts_map,
            include_all=include_all,
        )

    return accounts


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
    tenant_key = _normalize_tenant_cache_key(get_tenant_id())
    cache_store = _get_cache_store(cache_path)

    with cache_store.lock():
        root = _load_cache_root(cache_store)

    account_cache = root.get(_ACCOUNT_CODES_KEY)
    if not isinstance(account_cache, dict):
        account_cache = {}
    tenant_cache = account_cache.get(tenant_key)
    if not isinstance(tenant_cache, dict):
        tenant_cache = {}

    tenant_accounts, tenant_updated_at = _get_account_cache_entry(
        tenant_cache,
        include_all=include_all,
    )

    ttl_seconds = get_account_codes_cache_ttl_seconds()
    updated_at = _parse_cache_datetime(tenant_updated_at)
    is_stale = False
    if ttl_seconds > 0:
        if updated_at is None:
            is_stale = True
        else:
            now = datetime.now(ZoneInfo(get_timezone()))
            age_seconds = (now - updated_at).total_seconds()
            is_stale = age_seconds > ttl_seconds

    cached_accounts = {
        code: tenant_accounts[code]
        for code in requested_set
        if code in tenant_accounts
    }
    missing_codes = requested_set - set(cached_accounts.keys())

    source_accounts: dict[str, dict]
    if missing_codes or is_stale:
        db_accounts = refresh_account_codes_cache(
            include_all=include_all,
            tenant_id=tenant_key,
        )
        source_accounts = {a["code"].upper(): a for a in db_accounts}
    else:
        source_accounts = tenant_accounts

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
