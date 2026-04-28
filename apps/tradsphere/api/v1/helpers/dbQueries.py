from __future__ import annotations

import re

from shared.db import execute_many, fetch_all, run_transaction
from shared.normalization import (
    normalize_compact_token as _normalize_compact_token,
    normalize_input_text as _normalize_input_text,
    normalize_optional_input_text as _normalize_optional_input_text,
)
from shared.tenantDataCache import (
    get_tenant_shared_cache_value,
    set_tenant_shared_cache_value,
)

from apps.tradsphere.api.v1.helpers.config import (
    get_db_read_cache_ttl_seconds,
    get_db_tables,
)


_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")
_DB_READ_CACHE_BUCKET = "db_reads"
_DB_READ_CACHE_PREFIX = "tradsphere_db_reads::"
_SCHEDULE_EXISTS_CACHE_BUCKET = "db_reads"
_SCHEDULE_EXISTS_CACHE_PREFIX = "tradsphere_validation::schedule_has_estnum::"


def _quote_identifier(name: str) -> str:
    cleaned = str(name or "").strip()
    if not _IDENTIFIER_RE.fullmatch(cleaned):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return f"`{cleaned}`"


def _quote_table_name(table_name: str) -> str:
    parts = [part.strip() for part in str(table_name or "").split(".") if part.strip()]
    if not parts:
        raise ValueError("Invalid table name")
    return ".".join(_quote_identifier(part) for part in parts)


def _normalize_bool(value: object, *, default: bool = True) -> int:
    if value is None:
        return 1 if default else 0
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if bool(value) else 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return 1
        if text in {"0", "false", "no", "n", "off"}:
            return 0
    raise ValueError("active must be boolean-like")


def _normalize_account_code(value: object) -> str:
    return _normalize_compact_token(value).upper()


def _normalize_media_type(value: object) -> str:
    return _normalize_compact_token(value).upper()


def _normalize_contact_type(value: object) -> str:
    return _normalize_compact_token(value).upper()


def _normalize_email(value: object) -> str:
    return _normalize_compact_token(value).lower()


def _build_in_placeholders(values: list[object]) -> str:
    if not values:
        raise ValueError("Cannot build IN placeholder for empty values")
    return ", ".join(["%s"] * len(values))


def _cache_ttl_seconds(*, ttl_key: str) -> int:
    return max(int(get_db_read_cache_ttl_seconds(key=ttl_key)), 0)


def _normalized_text_cache_values(values: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return sorted(normalized)


def _normalized_int_cache_values(values: list[int]) -> list[int]:
    seen: set[int] = set()
    normalized: list[int] = []
    for value in values:
        parsed = int(value)
        if parsed in seen:
            continue
        seen.add(parsed)
        normalized.append(parsed)
    return sorted(normalized)


def _build_db_read_cache_key(
    cache_scope: str,
    *parts: str,
) -> str:
    cleaned_parts = [str(part or "").strip() for part in parts]
    return _DB_READ_CACHE_PREFIX + str(cache_scope).strip() + "::" + "::".join(cleaned_parts)


def _get_cached_list(
    cache_key: str,
    *,
    ttl_key: str,
) -> list[dict] | None:
    cached_value, cache_hit = get_tenant_shared_cache_value(
        bucket=_DB_READ_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_cache_ttl_seconds(ttl_key=ttl_key),
    )
    if cache_hit and isinstance(cached_value, list):
        return cached_value
    return None


def _get_cached_dict(
    cache_key: str,
    *,
    ttl_key: str,
) -> dict | None:
    cached_value, cache_hit = get_tenant_shared_cache_value(
        bucket=_DB_READ_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_cache_ttl_seconds(ttl_key=ttl_key),
    )
    if cache_hit and isinstance(cached_value, dict):
        return cached_value
    return None


def _set_cached_value(cache_key: str, value: object) -> None:
    set_tenant_shared_cache_value(
        bucket=_DB_READ_CACHE_BUCKET,
        cache_key=cache_key,
        value=value,
    )


def get_accounts(
    *,
    account_codes: list[str] | None = None,
    active_only: bool = False,
) -> list[dict]:
    tables = get_db_tables()
    accounts_table = _quote_table_name(tables["ACCOUNTS"])
    master_accounts_table = _quote_table_name(tables["MASTERACCOUNTS"])

    where_clauses: list[str] = []
    params: list[object] = []

    normalized_codes = [_normalize_account_code(code) for code in (account_codes or [])]
    normalized_codes = _normalized_text_cache_values(
        [code for code in normalized_codes if code]
    )
    if normalized_codes:
        placeholders = _build_in_placeholders(normalized_codes)
        where_clauses.append(f"UPPER(t.accountCode) IN ({placeholders})")
        params.extend(normalized_codes)

    if active_only:
        where_clauses.append("COALESCE(m.active, 0) = 1")

    cache_key = _build_db_read_cache_key(
        "accounts",
        f"accounts_table={accounts_table}",
        f"master_accounts_table={master_accounts_table}",
        f"active_only={int(bool(active_only))}",
        "account_codes=" + (",".join(normalized_codes) if normalized_codes else "*"),
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_accounts_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT DISTINCT "
        "t.accountCode AS accountCode, "
        "t.billingType AS billingType, "
        "t.market AS market, "
        "t.note AS note, "
        "m.name AS name, "
        "m.logoUrl AS logoUrl, "
        "COALESCE(m.active, 0) AS active "
        f"FROM {accounts_table} t "
        f"LEFT JOIN {master_accounts_table} m "
        "ON UPPER(m.code) = UPPER(t.accountCode)"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY t.accountCode ASC"
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def insert_accounts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    accounts_table = _quote_table_name(tables["ACCOUNTS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        account_code = _normalize_account_code(item.get("accountCode"))
        if not account_code:
            raise ValueError("accountCode is required")
        billing_type = _normalize_input_text(item.get("billingType") or "Calendar") or "Calendar"
        market = _normalize_optional_input_text(item.get("market"))
        note = _normalize_optional_input_text(item.get("note"))
        values.append((account_code, billing_type, market, note))

    query = (
        f"INSERT INTO {accounts_table} (accountCode, billingType, market, note) "
        "VALUES (%s, %s, %s, %s)"
    )
    return execute_many(query, values)


def update_accounts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    accounts_table = _quote_table_name(tables["ACCOUNTS"])

    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        account_code = _normalize_account_code(item.get("accountCode"))
        if not account_code:
            raise ValueError("accountCode is required for update")

        fields: list[str] = []
        params: list[object] = []

        if "billingType" in item:
            billing_type = _normalize_input_text(item.get("billingType"))
            if not billing_type:
                raise ValueError("billingType cannot be empty")
            fields.append("billingType = %s")
            params.append(billing_type)

        if "market" in item:
            market = _normalize_optional_input_text(item.get("market"))
            fields.append("market = %s")
            params.append(market)

        if "note" in item:
            note = _normalize_optional_input_text(item.get("note"))
            fields.append("note = %s")
            params.append(note)

        if not fields:
            raise ValueError(
                f"No updatable fields provided for accountCode '{account_code}'"
            )

        params.append(account_code)
        query = f"UPDATE {accounts_table} SET " + ", ".join(fields) + " WHERE accountCode = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    return run_transaction(_work)


def get_est_nums(
    *,
    est_nums: list[int] | None = None,
    account_codes: list[str] | None = None,
) -> list[dict]:
    tables = get_db_tables()
    est_nums_table = _quote_table_name(tables["ESTNUMS"])

    where_clauses: list[str] = []
    params: list[object] = []

    normalized_est_nums = _normalized_int_cache_values(
        [int(item) for item in (est_nums or [])]
    )
    if normalized_est_nums:
        placeholders = _build_in_placeholders(normalized_est_nums)
        where_clauses.append(f"estNum IN ({placeholders})")
        params.extend(normalized_est_nums)

    normalized_account_codes = _normalized_text_cache_values([
        _normalize_account_code(item) for item in (account_codes or [])
    ])
    normalized_account_codes = [item for item in normalized_account_codes if item]
    if normalized_account_codes:
        placeholders = _build_in_placeholders(normalized_account_codes)
        where_clauses.append(f"UPPER(accountCode) IN ({placeholders})")
        params.extend(normalized_account_codes)

    cache_key = _build_db_read_cache_key(
        "est_nums",
        f"est_nums_table={est_nums_table}",
        "est_nums=" + (",".join(map(str, normalized_est_nums)) if normalized_est_nums else "*"),
        "account_codes="
        + (",".join(normalized_account_codes) if normalized_account_codes else "*"),
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_est_nums_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT estNum, accountCode, flightStart, flightEnd, mediaType, buyer, note "
        f"FROM {est_nums_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY estNum ASC"
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def get_scheduled_est_nums(est_nums: list[int]) -> set[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for item in est_nums or []:
        value = int(item)
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    if not normalized:
        return set()

    ttl_seconds = max(
        int(get_db_read_cache_ttl_seconds(key="db_schedule_exists_ttl_time")),
        0,
    )
    matched: set[int] = set()
    cache_misses: list[int] = []

    for est_num in normalized:
        cache_key = f"{_SCHEDULE_EXISTS_CACHE_PREFIX}{est_num}"
        cached_value, cache_hit = get_tenant_shared_cache_value(
            bucket=_SCHEDULE_EXISTS_CACHE_BUCKET,
            cache_key=cache_key,
            ttl_seconds=ttl_seconds,
        )
        if not cache_hit:
            cache_misses.append(est_num)
            continue

        exists = False
        if isinstance(cached_value, bool):
            exists = cached_value
        elif isinstance(cached_value, (int, float)):
            exists = bool(cached_value)
        elif isinstance(cached_value, str):
            exists = cached_value.strip().lower() in {"1", "true", "yes", "y", "on"}

        if exists:
            matched.add(est_num)

    if not cache_misses:
        return matched

    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    chunk_size = 500
    found_in_db: set[int] = set()

    for start in range(0, len(cache_misses), chunk_size):
        chunk = cache_misses[start : start + chunk_size]
        placeholders = _build_in_placeholders(chunk)
        query = (
            "SELECT DISTINCT estNum "
            f"FROM {schedules_table} "
            f"WHERE estNum IN ({placeholders})"
        )
        rows = fetch_all(query, tuple(chunk))
        for row in rows:
            raw_value = row.get("estNum")
            if raw_value is None:
                continue
            found_in_db.add(int(raw_value))

    for est_num in cache_misses:
        exists = est_num in found_in_db
        set_tenant_shared_cache_value(
            bucket=_SCHEDULE_EXISTS_CACHE_BUCKET,
            cache_key=f"{_SCHEDULE_EXISTS_CACHE_PREFIX}{est_num}",
            value=exists,
        )
        if exists:
            matched.add(est_num)

    return matched


def insert_est_nums(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        est_num_raw = item.get("estNum")
        if est_num_raw is None:
            raise ValueError("estNum is required")
        est_num = int(est_num_raw)
        account_code = _normalize_account_code(item.get("accountCode"))
        if not account_code:
            raise ValueError("accountCode is required")
        flight_start = _normalize_input_text(item.get("flightStart"))
        if not flight_start:
            raise ValueError("flightStart is required")
        flight_end = _normalize_input_text(item.get("flightEnd"))
        if not flight_end:
            raise ValueError("flightEnd is required")
        media_type = _normalize_media_type(item.get("mediaType"))
        if not media_type:
            raise ValueError("mediaType is required")
        buyer = _normalize_input_text(item.get("buyer"))
        if not buyer:
            raise ValueError("buyer is required")
        note = _normalize_optional_input_text(item.get("note"))
        values.append((est_num, account_code, flight_start, flight_end, media_type, buyer, note))

    query = (
        f"INSERT INTO {est_nums_table} (estNum, accountCode, flightStart, flightEnd, mediaType, buyer, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )
    return execute_many(query, values)


def update_est_nums(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        est_num_raw = item.get("estNum")
        if est_num_raw is None:
            raise ValueError("estNum is required for update")
        est_num = int(est_num_raw)

        fields: list[str] = []
        params: list[object] = []

        if "accountCode" in item:
            account_code = _normalize_account_code(item.get("accountCode"))
            if not account_code:
                raise ValueError("accountCode cannot be empty")
            fields.append("accountCode = %s")
            params.append(account_code)

        if "mediaType" in item:
            media_type = _normalize_media_type(item.get("mediaType"))
            if not media_type:
                raise ValueError("mediaType cannot be empty")
            fields.append("mediaType = %s")
            params.append(media_type)

        if "flightStart" in item:
            flight_start = _normalize_input_text(item.get("flightStart"))
            if not flight_start:
                raise ValueError("flightStart cannot be empty")
            fields.append("flightStart = %s")
            params.append(flight_start)

        if "flightEnd" in item:
            flight_end = _normalize_input_text(item.get("flightEnd"))
            if not flight_end:
                raise ValueError("flightEnd cannot be empty")
            fields.append("flightEnd = %s")
            params.append(flight_end)

        if "buyer" in item:
            buyer = _normalize_input_text(item.get("buyer"))
            if not buyer:
                raise ValueError("buyer cannot be empty")
            fields.append("buyer = %s")
            params.append(buyer)

        if "note" in item:
            fields.append("note = %s")
            params.append(_normalize_optional_input_text(item.get("note")))

        if not fields:
            raise ValueError(f"No updatable fields provided for estNum '{est_num}'")

        params.append(est_num)
        query = f"UPDATE {est_nums_table} SET " + ", ".join(fields) + " WHERE estNum = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    return run_transaction(_work)


def get_schedules(
    *,
    ids: list[str] | None = None,
    schedule_ids: list[str] | None = None,
    est_nums: list[int] | None = None,
    billing_codes: list[str] | None = None,
    media_types: list[str] | None = None,
    station_codes: list[str] | None = None,
    broadcast_month: int | None = None,
    broadcast_year: int | None = None,
    start_date_from: str | None = None,
    start_date_to: str | None = None,
    end_date_from: str | None = None,
    end_date_to: str | None = None,
) -> list[dict]:
    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])

    where_clauses: list[str] = []
    params: list[object] = []

    normalized_ids = _normalized_text_cache_values(
        [str(item or "").strip() for item in (ids or [])]
    )
    normalized_ids = [item for item in normalized_ids if item]
    if normalized_ids:
        placeholders = _build_in_placeholders(normalized_ids)
        where_clauses.append(f"id IN ({placeholders})")
        params.extend(normalized_ids)

    normalized_schedule_ids = _normalized_text_cache_values(
        [str(item or "").strip() for item in (schedule_ids or [])]
    )
    normalized_schedule_ids = [item for item in normalized_schedule_ids if item]
    if normalized_schedule_ids:
        placeholders = _build_in_placeholders(normalized_schedule_ids)
        where_clauses.append(f"scheduleId IN ({placeholders})")
        params.extend(normalized_schedule_ids)

    normalized_est_nums = _normalized_int_cache_values(
        [int(item) for item in (est_nums or [])]
    )
    if normalized_est_nums:
        placeholders = _build_in_placeholders(normalized_est_nums)
        where_clauses.append(f"estNum IN ({placeholders})")
        params.extend(normalized_est_nums)

    normalized_billing_codes = _normalized_text_cache_values(
        [str(item or "").strip() for item in (billing_codes or [])]
    )
    normalized_billing_codes = [item for item in normalized_billing_codes if item]
    if normalized_billing_codes:
        placeholders = _build_in_placeholders(normalized_billing_codes)
        where_clauses.append(f"billingCode IN ({placeholders})")
        params.extend(normalized_billing_codes)

    normalized_media_types = _normalized_text_cache_values(
        [_normalize_media_type(item) for item in (media_types or [])]
    )
    normalized_media_types = [item for item in normalized_media_types if item]
    if normalized_media_types:
        placeholders = _build_in_placeholders(normalized_media_types)
        where_clauses.append(f"UPPER(mediaType) IN ({placeholders})")
        params.extend(normalized_media_types)

    normalized_station_codes = _normalized_text_cache_values([
        _normalize_account_code(item) for item in (station_codes or [])
    ])
    normalized_station_codes = [item for item in normalized_station_codes if item]
    if normalized_station_codes:
        placeholders = _build_in_placeholders(normalized_station_codes)
        where_clauses.append(f"UPPER(stationCode) IN ({placeholders})")
        params.extend(normalized_station_codes)

    if broadcast_month is not None:
        where_clauses.append("broadcastMonth = %s")
        params.append(int(broadcast_month))

    if broadcast_year is not None:
        where_clauses.append("broadcastYear = %s")
        params.append(int(broadcast_year))

    if start_date_from is not None:
        where_clauses.append("startDate >= %s")
        params.append(str(start_date_from))
    if start_date_to is not None:
        where_clauses.append("startDate <= %s")
        params.append(str(start_date_to))
    if end_date_from is not None:
        where_clauses.append("endDate >= %s")
        params.append(str(end_date_from))
    if end_date_to is not None:
        where_clauses.append("endDate <= %s")
        params.append(str(end_date_to))

    cache_key = _build_db_read_cache_key(
        "schedules",
        f"schedules_table={schedules_table}",
        "ids=" + (",".join(normalized_ids) if normalized_ids else "*"),
        "schedule_ids=" + (",".join(normalized_schedule_ids) if normalized_schedule_ids else "*"),
        "est_nums=" + (",".join(map(str, normalized_est_nums)) if normalized_est_nums else "*"),
        "billing_codes="
        + (",".join(normalized_billing_codes) if normalized_billing_codes else "*"),
        "media_types=" + (",".join(normalized_media_types) if normalized_media_types else "*"),
        "station_codes="
        + (",".join(normalized_station_codes) if normalized_station_codes else "*"),
        f"broadcast_month={int(broadcast_month) if broadcast_month is not None else '*'}",
        f"broadcast_year={int(broadcast_year) if broadcast_year is not None else '*'}",
        f"start_date_from={str(start_date_from) if start_date_from is not None else '*'}",
        f"start_date_to={str(start_date_to) if start_date_to is not None else '*'}",
        f"end_date_from={str(end_date_from) if end_date_from is not None else '*'}",
        f"end_date_to={str(end_date_to) if end_date_to is not None else '*'}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_schedules_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    query = (
        "SELECT "
        "id, scheduleId, lineNum, estNum, billingCode, mediaType, stationCode, "
        "broadcastMonth, broadcastYear, startDate, endDate, totalSpot, totalGross, rateGross, "
        "length, runtime, programName, days, daypart, rtg "
        f"FROM {schedules_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY broadcastYear ASC, broadcastMonth ASC, startDate ASC, id ASC"
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def get_schedules_by_match_keys(match_keys: list[str]) -> list[dict]:
    normalized_match_keys = [str(item or "").strip() for item in (match_keys or [])]
    normalized_match_keys = [item for item in normalized_match_keys if item]
    if not normalized_match_keys:
        return []

    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    placeholders = _build_in_placeholders(normalized_match_keys)
    query = (
        "SELECT id, matchKey "
        f"FROM {schedules_table} "
        f"WHERE matchKey IN ({placeholders})"
    )
    return fetch_all(query, tuple(normalized_match_keys))


def insert_schedules(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    values: list[tuple[object, ...]] = []
    for item in items:
        schedule_row_id = _normalize_input_text(item.get("id"))
        if not schedule_row_id:
            raise ValueError("id is required")
        schedule_business_id = _normalize_input_text(item.get("scheduleId"))
        if not schedule_business_id:
            raise ValueError("scheduleId is required")
        line_num = item.get("lineNum")
        if line_num is None:
            raise ValueError("lineNum is required")
        est_num = item.get("estNum")
        if est_num is None:
            raise ValueError("estNum is required")
        billing_code = _normalize_input_text(item.get("billingCode"))
        if not billing_code:
            raise ValueError("billingCode is required")
        media_type = _normalize_media_type(item.get("mediaType"))
        if not media_type:
            raise ValueError("mediaType is required")
        station_code = _normalize_account_code(item.get("stationCode"))
        if not station_code:
            raise ValueError("stationCode is required")
        broadcast_month = item.get("broadcastMonth")
        if broadcast_month is None:
            raise ValueError("broadcastMonth is required")
        broadcast_year = item.get("broadcastYear")
        if broadcast_year is None:
            raise ValueError("broadcastYear is required")
        start_date = _normalize_input_text(item.get("startDate"))
        if not start_date:
            raise ValueError("startDate is required")
        end_date = _normalize_input_text(item.get("endDate"))
        if not end_date:
            raise ValueError("endDate is required")
        total_spot = item.get("totalSpot")
        if total_spot is None:
            raise ValueError("totalSpot is required")
        total_gross = item.get("totalGross")
        if total_gross is None:
            raise ValueError("totalGross is required")
        rate_gross = item.get("rateGross")
        if rate_gross is None:
            raise ValueError("rateGross is required")
        length = item.get("length")
        if length is None:
            raise ValueError("length is required")
        runtime = _normalize_input_text(item.get("runtime"))
        if not runtime:
            raise ValueError("runtime is required")
        days = _normalize_input_text(item.get("days"))
        if not days:
            raise ValueError("days is required")
        daypart = _normalize_input_text(item.get("daypart"))
        if not daypart:
            raise ValueError("daypart is required")
        match_key = _normalize_input_text(item.get("matchKey"))
        if not match_key:
            raise ValueError("matchKey is required")
        values.append(
            (
                schedule_row_id,
                schedule_business_id,
                int(line_num),
                int(est_num),
                billing_code,
                media_type,
                station_code,
                int(broadcast_month),
                int(broadcast_year),
                start_date,
                end_date,
                int(total_spot),
                total_gross,
                rate_gross,
                int(length),
                runtime,
                _normalize_optional_input_text(item.get("programName")),
                days,
                daypart,
                item.get("rtg"),
                match_key,
            )
        )

    query = (
        f"INSERT INTO {schedules_table} "
        "(id, scheduleId, lineNum, estNum, billingCode, mediaType, stationCode, broadcastMonth, broadcastYear, "
        "startDate, endDate, totalSpot, totalGross, rateGross, length, runtime, programName, days, daypart, rtg, matchKey) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "scheduleId = VALUES(scheduleId), "
        "lineNum = VALUES(lineNum), "
        "estNum = VALUES(estNum), "
        "billingCode = VALUES(billingCode), "
        "mediaType = VALUES(mediaType), "
        "stationCode = VALUES(stationCode), "
        "broadcastMonth = VALUES(broadcastMonth), "
        "broadcastYear = VALUES(broadcastYear), "
        "startDate = VALUES(startDate), "
        "endDate = VALUES(endDate), "
        "totalSpot = VALUES(totalSpot), "
        "totalGross = VALUES(totalGross), "
        "rateGross = VALUES(rateGross), "
        "length = VALUES(length), "
        "runtime = VALUES(runtime), "
        "programName = VALUES(programName), "
        "days = VALUES(days), "
        "daypart = VALUES(daypart), "
        "rtg = VALUES(rtg), "
        "matchKey = VALUES(matchKey)"
    )
    return execute_many(query, values)


def update_schedules(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        schedule_row_id = _normalize_input_text(item.get("id"))
        if not schedule_row_id:
            raise ValueError("id is required for schedules update")
        fields: list[str] = []
        params: list[object] = []

        if "scheduleId" in item:
            schedule_business_id = _normalize_input_text(item.get("scheduleId"))
            if not schedule_business_id:
                raise ValueError("scheduleId cannot be empty")
            fields.append("scheduleId = %s")
            params.append(schedule_business_id)
        if "lineNum" in item:
            line_num = item.get("lineNum")
            if line_num is None:
                raise ValueError("lineNum cannot be null")
            fields.append("lineNum = %s")
            params.append(int(line_num))
        if "estNum" in item:
            est_num = item.get("estNum")
            if est_num is None:
                raise ValueError("estNum cannot be null")
            fields.append("estNum = %s")
            params.append(int(est_num))
        if "billingCode" in item:
            billing_code = _normalize_input_text(item.get("billingCode"))
            if not billing_code:
                raise ValueError("billingCode cannot be empty")
            fields.append("billingCode = %s")
            params.append(billing_code)
        if "mediaType" in item:
            media_type = _normalize_media_type(item.get("mediaType"))
            if not media_type:
                raise ValueError("mediaType cannot be empty")
            fields.append("mediaType = %s")
            params.append(media_type)
        if "stationCode" in item:
            station_code = _normalize_account_code(item.get("stationCode"))
            if not station_code:
                raise ValueError("stationCode cannot be empty")
            fields.append("stationCode = %s")
            params.append(station_code)
        if "broadcastMonth" in item:
            broadcast_month = item.get("broadcastMonth")
            if broadcast_month is None:
                raise ValueError("broadcastMonth cannot be null")
            fields.append("broadcastMonth = %s")
            params.append(int(broadcast_month))
        if "broadcastYear" in item:
            broadcast_year = item.get("broadcastYear")
            if broadcast_year is None:
                raise ValueError("broadcastYear cannot be null")
            fields.append("broadcastYear = %s")
            params.append(int(broadcast_year))
        if "startDate" in item:
            start_date = _normalize_input_text(item.get("startDate"))
            if not start_date:
                raise ValueError("startDate cannot be empty")
            fields.append("startDate = %s")
            params.append(start_date)
        if "endDate" in item:
            end_date = _normalize_input_text(item.get("endDate"))
            if not end_date:
                raise ValueError("endDate cannot be empty")
            fields.append("endDate = %s")
            params.append(end_date)
        if "totalSpot" in item:
            total_spot = item.get("totalSpot")
            if total_spot is None:
                raise ValueError("totalSpot cannot be null")
            fields.append("totalSpot = %s")
            params.append(int(total_spot))
        if "totalGross" in item:
            total_gross = item.get("totalGross")
            if total_gross is None:
                raise ValueError("totalGross cannot be null")
            fields.append("totalGross = %s")
            params.append(total_gross)
        if "rateGross" in item:
            rate_gross = item.get("rateGross")
            if rate_gross is None:
                raise ValueError("rateGross cannot be null")
            fields.append("rateGross = %s")
            params.append(rate_gross)
        if "length" in item:
            length = item.get("length")
            if length is None:
                raise ValueError("length cannot be null")
            fields.append("length = %s")
            params.append(int(length))
        if "runtime" in item:
            runtime = _normalize_input_text(item.get("runtime"))
            if not runtime:
                raise ValueError("runtime cannot be empty")
            fields.append("runtime = %s")
            params.append(runtime)
        if "programName" in item:
            fields.append("programName = %s")
            params.append(_normalize_optional_input_text(item.get("programName")))
        if "days" in item:
            days = _normalize_input_text(item.get("days"))
            if not days:
                raise ValueError("days cannot be empty")
            fields.append("days = %s")
            params.append(days)
        if "daypart" in item:
            daypart = _normalize_input_text(item.get("daypart"))
            if not daypart:
                raise ValueError("daypart cannot be empty")
            fields.append("daypart = %s")
            params.append(daypart)
        if "rtg" in item:
            fields.append("rtg = %s")
            params.append(item.get("rtg"))
        if "matchKey" in item:
            match_key = _normalize_input_text(item.get("matchKey"))
            if not match_key:
                raise ValueError("matchKey cannot be empty")
            fields.append("matchKey = %s")
            params.append(match_key)

        if not fields:
            raise ValueError(f"No updatable fields provided for schedule id '{schedule_row_id}'")

        params.append(schedule_row_id)
        query = f"UPDATE {schedules_table} SET " + ", ".join(fields) + " WHERE id = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    return run_transaction(_work)


def get_schedule_weeks(
    *,
    ids: list[int] | None = None,
    schedule_ids: list[str] | None = None,
    week_start_from: str | None = None,
    week_start_to: str | None = None,
    week_end_from: str | None = None,
    week_end_to: str | None = None,
) -> list[dict]:
    tables = get_db_tables()
    schedules_weeks_table = _quote_table_name(tables["SCHEDULESWEEKS"])
    where_clauses: list[str] = []
    params: list[object] = []

    normalized_ids = [int(item) for item in (ids or [])]
    if normalized_ids:
        placeholders = _build_in_placeholders(normalized_ids)
        where_clauses.append(f"id IN ({placeholders})")
        params.extend(normalized_ids)

    normalized_schedule_ids = [str(item or "").strip() for item in (schedule_ids or [])]
    normalized_schedule_ids = [item for item in normalized_schedule_ids if item]
    if normalized_schedule_ids:
        placeholders = _build_in_placeholders(normalized_schedule_ids)
        where_clauses.append(f"scheduleId IN ({placeholders})")
        params.extend(normalized_schedule_ids)

    if week_start_from is not None:
        where_clauses.append("weekStart >= %s")
        params.append(str(week_start_from))
    if week_start_to is not None:
        where_clauses.append("weekStart <= %s")
        params.append(str(week_start_to))
    if week_end_from is not None:
        where_clauses.append("weekEnd >= %s")
        params.append(str(week_end_from))
    if week_end_to is not None:
        where_clauses.append("weekEnd <= %s")
        params.append(str(week_end_to))

    query = (
        "SELECT id, scheduleId, weekStart, weekEnd, spots "
        f"FROM {schedules_weeks_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY weekStart ASC, scheduleId ASC, id ASC"
    return fetch_all(query, tuple(params))


def insert_schedule_weeks(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    schedules_weeks_table = _quote_table_name(tables["SCHEDULESWEEKS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        schedule_row_id = _normalize_input_text(item.get("scheduleId"))
        if not schedule_row_id:
            raise ValueError("scheduleId is required")
        week_start = _normalize_input_text(item.get("weekStart"))
        if not week_start:
            raise ValueError("weekStart is required")
        week_end = _normalize_input_text(item.get("weekEnd"))
        if not week_end:
            raise ValueError("weekEnd is required")
        spots = item.get("spots")
        if spots is None:
            raise ValueError("spots is required")
        values.append((schedule_row_id, week_start, week_end, int(spots)))

    query = (
        f"INSERT INTO {schedules_weeks_table} "
        "(scheduleId, weekStart, weekEnd, spots) "
        "VALUES (%s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "weekEnd = VALUES(weekEnd), "
        "spots = VALUES(spots)"
    )
    return execute_many(query, values)


def update_schedule_weeks(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    schedules_weeks_table = _quote_table_name(tables["SCHEDULESWEEKS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        week_row_id = item.get("id")
        if week_row_id is None:
            raise ValueError("id is required for schedule weeks update")
        fields: list[str] = []
        params: list[object] = []

        if "scheduleId" in item:
            schedule_row_id = _normalize_input_text(item.get("scheduleId"))
            if not schedule_row_id:
                raise ValueError("scheduleId cannot be empty")
            fields.append("scheduleId = %s")
            params.append(schedule_row_id)
        if "weekStart" in item:
            week_start = _normalize_input_text(item.get("weekStart"))
            if not week_start:
                raise ValueError("weekStart cannot be empty")
            fields.append("weekStart = %s")
            params.append(week_start)
        if "weekEnd" in item:
            week_end = _normalize_input_text(item.get("weekEnd"))
            if not week_end:
                raise ValueError("weekEnd cannot be empty")
            fields.append("weekEnd = %s")
            params.append(week_end)
        if "spots" in item:
            spots = item.get("spots")
            if spots is None:
                raise ValueError("spots cannot be null")
            fields.append("spots = %s")
            params.append(int(spots))

        if not fields:
            raise ValueError(f"No updatable fields provided for schedule week id '{week_row_id}'")

        params.append(int(week_row_id))
        query = f"UPDATE {schedules_weeks_table} SET " + ", ".join(fields) + " WHERE id = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    return run_transaction(_work)


def get_delivery_methods(*, ids: list[int] | None = None) -> list[dict]:
    tables = get_db_tables()
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    where_clauses: list[str] = []
    params: list[object] = []
    normalized_ids = [int(item) for item in (ids or [])]
    if normalized_ids:
        placeholders = _build_in_placeholders(normalized_ids)
        where_clauses.append(f"id IN ({placeholders})")
        params.extend(normalized_ids)

    query = (
        "SELECT id, name, url, username, deadline, note "
        f"FROM {delivery_methods_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY id ASC"
    return fetch_all(query, tuple(params))


def insert_delivery_methods(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        name = _normalize_input_text(item.get("name"))
        if not name:
            raise ValueError("name is required")
        url = _normalize_input_text(item.get("url"))
        if not url:
            raise ValueError("url is required")
        username = _normalize_input_text(item.get("username"))
        if not username:
            raise ValueError("username is required")
        deadline = _normalize_input_text(item.get("deadline") or "10 AM") or "10 AM"
        password = item.get("password")
        note = _normalize_optional_input_text(item.get("note"))
        values.append((name, url, username, password, deadline, note))

    query = (
        f"INSERT INTO {delivery_methods_table} "
        "(name, url, username, password, deadline, note) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "name = VALUES(name), "
        "password = VALUES(password), "
        "note = VALUES(note)"
    )
    return execute_many(query, values)


def get_stations(
    *,
    codes: list[str] | None = None,
    account_codes: list[str] | None = None,
    est_nums: list[int] | None = None,
    station_name: str | None = None,
    delivery_method_detail: bool = True,
) -> list[dict]:
    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    normalized_codes = _normalized_text_cache_values(
        [_normalize_account_code(code) for code in (codes or [])]
    )
    normalized_codes = [code for code in normalized_codes if code]

    where_clauses: list[str] = []
    params: list[object] = []

    if normalized_codes:
        code_placeholders = _build_in_placeholders(normalized_codes)
        where_clauses.append(f"UPPER(s.code) IN ({code_placeholders})")
        params.extend(normalized_codes)

    normalized_account_codes = _normalized_text_cache_values([
        _normalize_account_code(item) for item in (account_codes or [])
    ])
    normalized_account_codes = [item for item in normalized_account_codes if item]

    normalized_est_nums = _normalized_int_cache_values(
        [int(item) for item in (est_nums or [])]
    )
    normalized_station_name = str(station_name or "").strip().lower()

    if normalized_est_nums or normalized_account_codes:
        exists_clauses = ["UPPER(sc.stationCode) = UPPER(s.code)"]
        exists_params: list[object] = []

        if normalized_est_nums:
            placeholders = _build_in_placeholders(normalized_est_nums)
            exists_clauses.append(f"sc.estNum IN ({placeholders})")
            exists_params.extend(normalized_est_nums)

        exists_from = f"FROM {schedules_table} sc "
        if normalized_account_codes:
            placeholders = _build_in_placeholders(normalized_account_codes)
            exists_from += f"JOIN {est_nums_table} en ON en.estNum = sc.estNum "
            exists_clauses.append(f"UPPER(en.accountCode) IN ({placeholders})")
            exists_params.extend(normalized_account_codes)

        where_clauses.append(
            "EXISTS (SELECT 1 "
            + exists_from
            + "WHERE "
            + " AND ".join(exists_clauses)
            + ")"
        )
        params.extend(exists_params)

    if normalized_station_name:
        where_clauses.append("LOWER(COALESCE(s.name, '')) LIKE %s")
        params.append(f"%{normalized_station_name}%")

    if not where_clauses:
        return []

    cache_key = _build_db_read_cache_key(
        "stations",
        f"stations_table={stations_table}",
        f"delivery_methods_table={delivery_methods_table}",
        f"schedules_table={schedules_table}",
        f"est_nums_table={est_nums_table}",
        "codes=" + (",".join(normalized_codes) if normalized_codes else "*"),
        "account_codes="
        + (",".join(normalized_account_codes) if normalized_account_codes else "*"),
        "est_nums=" + (",".join(map(str, normalized_est_nums)) if normalized_est_nums else "*"),
        f"station_name={normalized_station_name or '*'}",
        f"delivery_method_detail={int(bool(delivery_method_detail))}",
    )
    cached_rows = _get_cached_list(
        cache_key,
        ttl_key="db_stations_ttl_time",
    )
    if cached_rows is not None:
        return cached_rows

    select_fields = (
        "s.code AS code, "
        "s.name AS name, "
        "s.affiliation AS affiliation, "
        "s.mediaType AS mediaType, "
        "CASE WHEN UPPER(s.mediaType) = 'CA' THEN s.syscode ELSE NULL END AS syscode, "
        "s.language AS language, "
        "s.ownership AS ownership, "
        "s.deliveryMethodId AS deliveryMethodId, "
        "s.note AS note, "
        "d.name AS deliveryMethodName"
    )
    from_clause = (
        f"FROM {stations_table} s "
        f"LEFT JOIN {delivery_methods_table} d ON s.deliveryMethodId = d.id "
    )
    if delivery_method_detail:
        select_fields += (
            ", d.url AS deliveryMethodUrl, "
            "d.username AS deliveryMethodUsername, "
            "d.deadline AS deliveryMethodDeadline, "
            "d.note AS deliveryMethodNote"
        )

    query = (
        "SELECT "
        + select_fields
        + " "
        + from_clause
        + "WHERE "
        + " AND ".join(where_clauses)
        + " ORDER BY s.code ASC"
    )
    rows = fetch_all(query, tuple(params))
    _set_cached_value(cache_key, rows)
    return rows


def get_station_media_types(*, codes: list[str]) -> dict[str, str]:
    normalized_codes = _normalized_text_cache_values(
        [_normalize_account_code(code) for code in (codes or [])]
    )
    normalized_codes = [code for code in normalized_codes if code]
    if not normalized_codes:
        return {}

    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    cache_key = _build_db_read_cache_key(
        "station_media_types",
        f"stations_table={stations_table}",
        "codes=" + ",".join(normalized_codes),
    )
    cached_mapped = _get_cached_dict(
        cache_key,
        ttl_key="db_station_media_types_ttl_time",
    )
    if cached_mapped is not None:
        normalized_mapped: dict[str, str] = {}
        for code, media_type in cached_mapped.items():
            code_text = str(code or "").strip().upper()
            media_type_text = str(media_type or "").strip().upper()
            if not code_text or not media_type_text:
                continue
            normalized_mapped[code_text] = media_type_text
        return normalized_mapped

    placeholders = _build_in_placeholders(normalized_codes)
    query = (
        "SELECT UPPER(code) AS code, UPPER(mediaType) AS mediaType "
        f"FROM {stations_table} "
        f"WHERE UPPER(code) IN ({placeholders})"
    )
    rows = fetch_all(query, tuple(normalized_codes))

    mapped: dict[str, str] = {}
    for row in rows:
        code = str(row.get("code") or "").strip().upper()
        media_type = str(row.get("mediaType") or "").strip().upper()
        if not code or not media_type:
            continue
        mapped[code] = media_type
    _set_cached_value(cache_key, mapped)
    return mapped


def get_station_account_codes(
    *,
    station_codes: list[str],
    account_codes: list[str] | None = None,
    est_nums: list[int] | None = None,
) -> dict[str, list[str]]:
    normalized_station_codes = [_normalize_account_code(code) for code in station_codes]
    normalized_station_codes = [code for code in normalized_station_codes if code]
    if not normalized_station_codes:
        return {}

    normalized_account_codes = [
        _normalize_account_code(item) for item in (account_codes or [])
    ]
    normalized_account_codes = [item for item in normalized_account_codes if item]
    normalized_est_nums = [int(item) for item in (est_nums or [])]

    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    est_nums_table = _quote_table_name(tables["ESTNUMS"])

    where_clauses: list[str] = []
    params: list[object] = []

    station_placeholders = _build_in_placeholders(normalized_station_codes)
    where_clauses.append(f"UPPER(sc.stationCode) IN ({station_placeholders})")
    params.extend(normalized_station_codes)

    if normalized_est_nums:
        placeholders = _build_in_placeholders(normalized_est_nums)
        where_clauses.append(f"sc.estNum IN ({placeholders})")
        params.extend(normalized_est_nums)

    if normalized_account_codes:
        placeholders = _build_in_placeholders(normalized_account_codes)
        where_clauses.append(f"UPPER(en.accountCode) IN ({placeholders})")
        params.extend(normalized_account_codes)

    query = (
        "SELECT DISTINCT "
        "UPPER(sc.stationCode) AS stationCode, "
        "UPPER(en.accountCode) AS accountCode "
        f"FROM {schedules_table} sc "
        f"JOIN {est_nums_table} en ON en.estNum = sc.estNum "
        "WHERE " + " AND ".join(where_clauses) + " "
        "ORDER BY stationCode ASC, accountCode ASC"
    )
    rows = fetch_all(query, tuple(params))

    grouped: dict[str, list[str]] = {}
    for row in rows:
        station_code = str(row.get("stationCode") or "").strip().upper()
        account_code = str(row.get("accountCode") or "").strip().upper()
        if not station_code or not account_code:
            continue
        grouped.setdefault(station_code, []).append(account_code)
    return grouped


def insert_stations(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        code = _normalize_account_code(item.get("code"))
        if not code:
            raise ValueError("code is required")
        name = _normalize_input_text(item.get("name"))
        if not name:
            raise ValueError("name is required")
        media_type = _normalize_media_type(item.get("mediaType"))
        if not media_type:
            raise ValueError("mediaType is required")
        language = _normalize_input_text(item.get("language"))
        if not language:
            raise ValueError("language is required")
        delivery_method_id = item.get("deliveryMethodId")
        if delivery_method_id is None:
            raise ValueError("deliveryMethodId is required")
        syscode = item.get("syscode")
        if syscode is not None:
            syscode = int(syscode)
            if syscode < 0:
                raise ValueError("syscode must be an unsigned integer")
        values.append(
            (
                code,
                name,
                _normalize_optional_input_text(item.get("affiliation")),
                media_type,
                syscode,
                language,
                _normalize_optional_input_text(item.get("ownership")),
                int(delivery_method_id),
                _normalize_optional_input_text(item.get("note")),
            )
        )

    query = (
        f"INSERT INTO {stations_table} "
        "(code, name, affiliation, mediaType, syscode, language, ownership, deliveryMethodId, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "name = VALUES(name), "
        "affiliation = VALUES(affiliation), "
        "mediaType = VALUES(mediaType), "
        "syscode = VALUES(syscode), "
        "language = VALUES(language), "
        "ownership = VALUES(ownership), "
        "deliveryMethodId = VALUES(deliveryMethodId), "
        "note = VALUES(note)"
    )
    return execute_many(query, values)


def update_stations(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        code = _normalize_account_code(item.get("code"))
        if not code:
            raise ValueError("code is required for stations update")
        fields: list[str] = []
        params: list[object] = []
        if "name" in item:
            name = _normalize_input_text(item.get("name"))
            if not name:
                raise ValueError("name cannot be empty")
            fields.append("name = %s")
            params.append(name)
        if "affiliation" in item:
            fields.append("affiliation = %s")
            params.append(_normalize_optional_input_text(item.get("affiliation")))
        if "mediaType" in item:
            media_type = _normalize_media_type(item.get("mediaType"))
            if not media_type:
                raise ValueError("mediaType cannot be empty")
            fields.append("mediaType = %s")
            params.append(media_type)
        if "syscode" in item:
            syscode = item.get("syscode")
            if syscode is None:
                fields.append("syscode = %s")
                params.append(None)
            else:
                parsed_syscode = int(syscode)
                if parsed_syscode < 0:
                    raise ValueError("syscode must be an unsigned integer")
                fields.append("syscode = %s")
                params.append(parsed_syscode)
        if "language" in item:
            language = _normalize_input_text(item.get("language"))
            if not language:
                raise ValueError("language cannot be empty")
            fields.append("language = %s")
            params.append(language)
        if "ownership" in item:
            fields.append("ownership = %s")
            params.append(_normalize_optional_input_text(item.get("ownership")))
        if "deliveryMethodId" in item:
            delivery_method_id = item.get("deliveryMethodId")
            if delivery_method_id is None:
                raise ValueError("deliveryMethodId cannot be null")
            fields.append("deliveryMethodId = %s")
            params.append(int(delivery_method_id))
        if "note" in item:
            fields.append("note = %s")
            params.append(_normalize_optional_input_text(item.get("note")))
        if not fields:
            raise ValueError(f"No updatable fields provided for station '{code}'")
        params.append(code)
        query = f"UPDATE {stations_table} SET " + ", ".join(fields) + " WHERE code = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    return run_transaction(_work)


def update_delivery_methods(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        delivery_method_id = item.get("id")
        if delivery_method_id is None:
            raise ValueError("id is required for delivery method update")
        fields: list[str] = []
        params: list[object] = []
        if "name" in item:
            name = _normalize_input_text(item.get("name"))
            if not name:
                raise ValueError("name cannot be empty")
            fields.append("name = %s")
            params.append(name)
        if "url" in item:
            url = _normalize_input_text(item.get("url"))
            if not url:
                raise ValueError("url cannot be empty")
            fields.append("url = %s")
            params.append(url)
        if "username" in item:
            username = _normalize_input_text(item.get("username"))
            if not username:
                raise ValueError("username cannot be empty")
            fields.append("username = %s")
            params.append(username)
        if "password" in item:
            fields.append("password = %s")
            params.append(item.get("password"))
        if "deadline" in item:
            deadline = _normalize_input_text(item.get("deadline"))
            if not deadline:
                raise ValueError("deadline cannot be empty")
            fields.append("deadline = %s")
            params.append(deadline)
        if "note" in item:
            fields.append("note = %s")
            params.append(_normalize_optional_input_text(item.get("note")))
        if not fields:
            raise ValueError(
                f"No updatable fields provided for delivery method id '{delivery_method_id}'"
            )
        params.append(int(delivery_method_id))
        query = f"UPDATE {delivery_methods_table} SET " + ", ".join(fields) + " WHERE id = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    return run_transaction(_work)


def get_contacts(
    *,
    emails: list[str] | None = None,
    name: str | None = None,
    contact_type: str | None = None,
    active: bool | None = None,
) -> list[dict]:
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])
    where_clauses: list[str] = []
    params: list[object] = []

    normalized_emails = [_normalize_email(item) for item in (emails or [])]
    normalized_emails = [item for item in normalized_emails if item]
    if normalized_emails:
        email_like_clauses: list[str] = []
        for email_value in normalized_emails:
            email_like_clauses.append("LOWER(COALESCE(c.email, '')) LIKE %s")
            params.append(f"%{email_value}%")
        where_clauses.append("(" + " OR ".join(email_like_clauses) + ")")

    normalized_name = str(name or "").strip().lower()
    if normalized_name:
        like_value = f"%{normalized_name}%"
        where_clauses.append(
            "("
            "LOWER(COALESCE(c.firstName, '')) LIKE %s "
            "OR LOWER(COALESCE(c.lastName, '')) LIKE %s "
            "OR LOWER(CONCAT_WS(' ', COALESCE(c.firstName, ''), COALESCE(c.lastName, ''))) LIKE %s"
            ")"
        )
        params.extend([like_value, like_value, like_value])

    normalized_contact_type = _normalize_contact_type(contact_type) if contact_type else ""
    if normalized_contact_type:
        where_clauses.append("UPPER(sc.contactType) = %s")
        params.append(normalized_contact_type)

    if active is not None:
        where_clauses.append("c.active = %s")
        params.append(1 if active else 0)

    query = (
        "SELECT "
        "c.id AS id, "
        "c.firstName AS firstName, "
        "c.lastName AS lastName, "
        "c.company AS company, "
        "c.jobTitle AS jobTitle, "
        "c.office AS office, "
        "c.cell AS cell, "
        "c.email AS email, "
        "c.active AS active, "
        "c.note AS note, "
        "GROUP_CONCAT(DISTINCT UPPER(sc.stationCode) ORDER BY UPPER(sc.stationCode) SEPARATOR ',') "
        "AS stationCodes "
        f"FROM {contacts_table} c "
        f"LEFT JOIN {stations_contacts_table} sc ON sc.contactId = c.id AND sc.active = 1"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += (
        " GROUP BY c.id, c.firstName, c.lastName, c.company, c.jobTitle, c.office, c.cell, "
        "c.email, c.active, c.note ORDER BY c.id ASC"
    )
    return fetch_all(query, tuple(params))


def get_contacts_by_station_codes(
    *,
    station_codes: list[str],
    contact_types: list[str] | None = None,
) -> list[dict]:
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])

    normalized_station_codes = [_normalize_account_code(code) for code in station_codes]
    normalized_station_codes = [code for code in normalized_station_codes if code]
    if not normalized_station_codes:
        return []

    params: list[object] = []
    where_clauses: list[str] = []

    station_placeholders = _build_in_placeholders(normalized_station_codes)
    where_clauses.append(f"UPPER(sc.stationCode) IN ({station_placeholders})")
    params.extend(normalized_station_codes)

    normalized_contact_types = [
        _normalize_contact_type(item) for item in (contact_types or [])
    ]
    normalized_contact_types = [item for item in normalized_contact_types if item]
    if normalized_contact_types:
        type_placeholders = _build_in_placeholders(normalized_contact_types)
        where_clauses.append(f"UPPER(sc.contactType) IN ({type_placeholders})")
        params.extend(normalized_contact_types)

    where_clauses.append("sc.active = 1")
    where_clauses.append("c.active = 1")

    query = (
        "SELECT "
        "sc.stationCode AS stationCode, "
        "c.id AS id, "
        "c.email AS email, "
        "c.firstName AS firstName, "
        "c.lastName AS lastName, "
        "c.company AS company, "
        "c.jobTitle AS jobTitle, "
        "c.office AS office, "
        "c.cell AS cell, "
        "c.active AS active, "
        "c.note AS note, "
        "sc.contactType AS contactType, "
        "sc.primaryContact AS primaryContact, "
        "sc.note AS contactTypeNote "
        f"FROM {stations_contacts_table} sc "
        f"INNER JOIN {contacts_table} c ON sc.contactId = c.id "
        "WHERE "
        + " AND ".join(where_clauses)
        + " ORDER BY sc.stationCode ASC, sc.primaryContact DESC, c.id ASC"
    )
    return fetch_all(query, tuple(params))


def insert_contacts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        email = _normalize_email(item.get("email"))
        if not email:
            raise ValueError("email is required")
        first_name = item.get("firstName")
        if first_name is None:
            first_name = ""
        else:
            first_name = _normalize_input_text(first_name)
        values.append(
            (
                first_name,
                _normalize_optional_input_text(item.get("lastName")),
                _normalize_optional_input_text(item.get("company")),
                _normalize_optional_input_text(item.get("jobTitle")),
                _normalize_optional_input_text(item.get("office")),
                _normalize_optional_input_text(item.get("cell")),
                email,
                _normalize_bool(item.get("active"), default=True),
                _normalize_optional_input_text(item.get("note")),
            )
        )
    query = (
        f"INSERT INTO {contacts_table} "
        "(firstName, lastName, company, jobTitle, office, cell, email, active, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    return execute_many(query, values)


def update_contacts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        contact_id = item.get("id")
        if contact_id is None:
            raise ValueError("id is required for contacts update")
        fields: list[str] = []
        params: list[object] = []
        if "email" in item:
            email = _normalize_email(item.get("email"))
            if not email:
                raise ValueError("email cannot be empty")
            fields.append("email = %s")
            params.append(email)
        if "firstName" in item:
            fields.append("firstName = %s")
            first_name = item.get("firstName")
            if first_name is None:
                first_name = ""
            else:
                first_name = _normalize_input_text(first_name)
            params.append(first_name)
        if "lastName" in item:
            fields.append("lastName = %s")
            params.append(_normalize_optional_input_text(item.get("lastName")))
        if "company" in item:
            fields.append("company = %s")
            params.append(_normalize_optional_input_text(item.get("company")))
        if "jobTitle" in item:
            fields.append("jobTitle = %s")
            params.append(_normalize_optional_input_text(item.get("jobTitle")))
        if "office" in item:
            fields.append("office = %s")
            params.append(_normalize_optional_input_text(item.get("office")))
        if "cell" in item:
            fields.append("cell = %s")
            params.append(_normalize_optional_input_text(item.get("cell")))
        if "active" in item:
            fields.append("active = %s")
            params.append(_normalize_bool(item.get("active"), default=True))
        if "note" in item:
            fields.append("note = %s")
            params.append(_normalize_optional_input_text(item.get("note")))
        if not fields:
            raise ValueError(f"No updatable fields provided for contact id '{contact_id}'")
        params.append(int(contact_id))
        query = f"UPDATE {contacts_table} SET " + ", ".join(fields) + " WHERE id = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    return run_transaction(_work)


def find_existing_emails(
    *,
    emails: list[str],
    exclude_ids: list[int] | None = None,
) -> list[dict]:
    normalized_emails = [_normalize_email(item) for item in emails]
    normalized_emails = [item for item in normalized_emails if item]
    if not normalized_emails:
        return []
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    placeholders = _build_in_placeholders(normalized_emails)
    params: list[object] = list(normalized_emails)
    where_clauses = [f"LOWER(email) IN ({placeholders})"]

    normalized_exclude_ids = [int(item) for item in (exclude_ids or [])]
    if normalized_exclude_ids:
        exclude_placeholders = _build_in_placeholders(normalized_exclude_ids)
        where_clauses.append(f"id NOT IN ({exclude_placeholders})")
        params.extend(normalized_exclude_ids)

    query = (
        "SELECT id, email "
        f"FROM {contacts_table} "
        "WHERE " + " AND ".join(where_clauses)
    )
    return fetch_all(query, tuple(params))


def get_stations_contacts(
    *,
    ids: list[int] | None = None,
    station_codes: list[str] | None = None,
    contact_ids: list[int] | None = None,
    active: bool | None = None,
) -> list[dict]:
    tables = get_db_tables()
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])
    where_clauses: list[str] = []
    params: list[object] = []

    normalized_ids = [int(item) for item in (ids or [])]
    if normalized_ids:
        placeholders = _build_in_placeholders(normalized_ids)
        where_clauses.append(f"id IN ({placeholders})")
        params.extend(normalized_ids)

    normalized_station_codes = [
        _normalize_account_code(item) for item in (station_codes or [])
    ]
    normalized_station_codes = [item for item in normalized_station_codes if item]
    if normalized_station_codes:
        placeholders = _build_in_placeholders(normalized_station_codes)
        where_clauses.append(f"UPPER(stationCode) IN ({placeholders})")
        params.extend(normalized_station_codes)

    normalized_contact_ids = [int(item) for item in (contact_ids or [])]
    if normalized_contact_ids:
        placeholders = _build_in_placeholders(normalized_contact_ids)
        where_clauses.append(f"contactId IN ({placeholders})")
        params.extend(normalized_contact_ids)

    if active is not None:
        where_clauses.append("active = %s")
        params.append(1 if active else 0)

    query = (
        "SELECT id, stationCode, contactId, contactType, primaryContact, note, active "
        f"FROM {stations_contacts_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY id ASC"
    return fetch_all(query, tuple(params))


def insert_stations_contacts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])
    values: list[tuple[object, ...]] = []
    for item in items:
        station_code = _normalize_account_code(item.get("stationCode"))
        if not station_code:
            raise ValueError("stationCode is required")
        contact_id = item.get("contactId")
        if contact_id is None:
            raise ValueError("contactId is required")
        contact_type = _normalize_contact_type(item.get("contactType"))
        if not contact_type:
            raise ValueError("contactType is required")
        values.append(
            (
                station_code,
                int(contact_id),
                contact_type,
                _normalize_bool(item.get("primaryContact"), default=False),
                _normalize_optional_input_text(item.get("note")),
                _normalize_bool(item.get("active"), default=True),
            )
        )
    query = (
        f"INSERT INTO {stations_contacts_table} "
        "(stationCode, contactId, contactType, primaryContact, note, active) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "contactType = VALUES(contactType), "
        "primaryContact = VALUES(primaryContact), "
        "note = VALUES(note), "
        "active = VALUES(active)"
    )
    return execute_many(query, values)


def update_stations_contacts(items: list[dict]) -> int:
    if not items:
        return 0
    tables = get_db_tables()
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])
    statements: list[tuple[str, tuple[object, ...]]] = []
    for item in items:
        row_id = item.get("id")
        if row_id is None:
            raise ValueError("id is required for stationsContacts update")
        fields: list[str] = []
        params: list[object] = []
        if "stationCode" in item:
            station_code = _normalize_account_code(item.get("stationCode"))
            if not station_code:
                raise ValueError("stationCode cannot be empty")
            fields.append("stationCode = %s")
            params.append(station_code)
        if "contactId" in item:
            contact_id = item.get("contactId")
            if contact_id is None:
                raise ValueError("contactId cannot be null")
            fields.append("contactId = %s")
            params.append(int(contact_id))
        if "contactType" in item:
            contact_type = _normalize_contact_type(item.get("contactType"))
            if not contact_type:
                raise ValueError("contactType cannot be empty")
            fields.append("contactType = %s")
            params.append(contact_type)
        if "primaryContact" in item:
            fields.append("primaryContact = %s")
            params.append(_normalize_bool(item.get("primaryContact"), default=False))
        if "note" in item:
            fields.append("note = %s")
            params.append(_normalize_optional_input_text(item.get("note")))
        if "active" in item:
            fields.append("active = %s")
            params.append(_normalize_bool(item.get("active"), default=True))
        if not fields:
            raise ValueError(
                f"No updatable fields provided for stationsContacts id '{row_id}'"
            )
        params.append(int(row_id))
        query = (
            f"UPDATE {stations_contacts_table} SET "
            + ", ".join(fields)
            + " WHERE id = %s"
        )
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += int(cursor.rowcount or 0)
        return updated

    return run_transaction(_work)


def get_all_tradsphere_account_codes() -> list[str]:
    tables = get_db_tables()
    accounts_table = _quote_table_name(tables["ACCOUNTS"])
    rows = fetch_all(f"SELECT accountCode FROM {accounts_table}")
    out: list[str] = []
    for row in rows:
        code = _normalize_account_code(row.get("accountCode"))
        if code:
            out.append(code)
    return sorted(set(out))


def get_all_master_account_codes() -> list[str]:
    tables = get_db_tables()
    master_accounts_table = _quote_table_name(tables["MASTERACCOUNTS"])
    rows = fetch_all(f"SELECT code FROM {master_accounts_table}")
    out: list[str] = []
    for row in rows:
        code = _normalize_account_code(row.get("code"))
        if code:
            out.append(code)
    return sorted(set(out))


def get_all_station_codes() -> list[str]:
    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    rows = fetch_all(f"SELECT code FROM {stations_table}")
    out: list[str] = []
    for row in rows:
        code = _normalize_account_code(row.get("code"))
        if code:
            out.append(code)
    return sorted(set(out))


def get_all_delivery_method_ids() -> list[int]:
    tables = get_db_tables()
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    rows = fetch_all(f"SELECT id FROM {delivery_methods_table}")
    out: list[int] = []
    for row in rows:
        raw = row.get("id")
        if raw is None:
            continue
        out.append(int(raw))
    return sorted(set(out))


def get_all_contact_ids() -> list[int]:
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    rows = fetch_all(f"SELECT id FROM {contacts_table}")
    out: list[int] = []
    for row in rows:
        raw = row.get("id")
        if raw is None:
            continue
        out.append(int(raw))
    return sorted(set(out))


def get_all_stations_contacts_ids() -> list[int]:
    tables = get_db_tables()
    stations_contacts_table = _quote_table_name(tables["STATIONSCONTACTS"])
    rows = fetch_all(f"SELECT id FROM {stations_contacts_table}")
    out: list[int] = []
    for row in rows:
        raw = row.get("id")
        if raw is None:
            continue
        out.append(int(raw))
    return sorted(set(out))


def get_all_est_nums() -> list[int]:
    tables = get_db_tables()
    est_nums_table = _quote_table_name(tables["ESTNUMS"])
    rows = fetch_all(f"SELECT estNum FROM {est_nums_table}")
    out: list[int] = []
    for row in rows:
        raw = row.get("estNum")
        if raw is None:
            continue
        out.append(int(raw))
    return sorted(set(out))


def get_all_schedule_ids() -> list[str]:
    tables = get_db_tables()
    schedules_table = _quote_table_name(tables["SCHEDULES"])
    rows = fetch_all(f"SELECT id FROM {schedules_table}")
    out: list[str] = []
    for row in rows:
        schedule_row_id = str(row.get("id") or "").strip()
        if schedule_row_id:
            out.append(schedule_row_id)
    return sorted(set(out))
