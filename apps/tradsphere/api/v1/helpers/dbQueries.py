from __future__ import annotations

import re

from shared.db import execute_many, fetch_all, run_transaction

from apps.tradsphere.api.v1.helpers.config import get_db_tables


_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


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
    return str(value or "").strip().upper()


def _normalize_media_type(value: object) -> str:
    return str(value or "").strip().upper()


def _normalize_contact_type(value: object) -> str:
    return str(value or "").strip().upper()


def _normalize_email(value: object) -> str:
    return str(value or "").strip().lower()


def _build_in_placeholders(values: list[object]) -> str:
    if not values:
        raise ValueError("Cannot build IN placeholder for empty values")
    return ", ".join(["%s"] * len(values))


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
    normalized_codes = [code for code in normalized_codes if code]
    if normalized_codes:
        placeholders = _build_in_placeholders(normalized_codes)
        where_clauses.append(f"UPPER(t.accountCode) IN ({placeholders})")
        params.extend(normalized_codes)

    if active_only:
        where_clauses.append("COALESCE(m.active, 0) = 1")

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

    return fetch_all(query, tuple(params))


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
        billing_type = str(item.get("billingType") or "Calendar").strip() or "Calendar"
        market = str(item.get("market") or "").strip() or None
        note = str(item.get("note") or "").strip() or None
        values.append((account_code, billing_type, market, note))

    query = (
        f"INSERT INTO {accounts_table} (accountCode, billingType, market, note) "
        "VALUES (%s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "billingType = VALUES(billingType), "
        "market = VALUES(market), "
        "note = VALUES(note)"
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
            billing_type = str(item.get("billingType") or "").strip()
            if not billing_type:
                raise ValueError("billingType cannot be empty")
            fields.append("billingType = %s")
            params.append(billing_type)

        if "market" in item:
            market = str(item.get("market") or "").strip() or None
            fields.append("market = %s")
            params.append(market)

        if "note" in item:
            note = str(item.get("note") or "").strip() or None
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
    media_types: list[str] | None = None,
) -> list[dict]:
    tables = get_db_tables()
    est_nums_table = _quote_table_name(tables["ESTNUMS"])

    where_clauses: list[str] = []
    params: list[object] = []

    normalized_est_nums = [int(item) for item in (est_nums or [])]
    if normalized_est_nums:
        placeholders = _build_in_placeholders(normalized_est_nums)
        where_clauses.append(f"estNum IN ({placeholders})")
        params.extend(normalized_est_nums)

    normalized_account_codes = [
        _normalize_account_code(item) for item in (account_codes or [])
    ]
    normalized_account_codes = [item for item in normalized_account_codes if item]
    if normalized_account_codes:
        placeholders = _build_in_placeholders(normalized_account_codes)
        where_clauses.append(f"UPPER(accountCode) IN ({placeholders})")
        params.extend(normalized_account_codes)

    normalized_media_types = [_normalize_media_type(item) for item in (media_types or [])]
    normalized_media_types = [item for item in normalized_media_types if item]
    if normalized_media_types:
        placeholders = _build_in_placeholders(normalized_media_types)
        where_clauses.append(f"UPPER(mediaType) IN ({placeholders})")
        params.extend(normalized_media_types)

    query = (
        "SELECT estNum, accountCode, flightStart, flightEnd, mediaType, buyer, note "
        f"FROM {est_nums_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY estNum ASC"
    return fetch_all(query, tuple(params))


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
        flight_start = str(item.get("flightStart") or "").strip()
        if not flight_start:
            raise ValueError("flightStart is required")
        flight_end = str(item.get("flightEnd") or "").strip()
        if not flight_end:
            raise ValueError("flightEnd is required")
        media_type = _normalize_media_type(item.get("mediaType"))
        if not media_type:
            raise ValueError("mediaType is required")
        buyer = str(item.get("buyer") or "").strip()
        if not buyer:
            raise ValueError("buyer is required")
        note = str(item.get("note") or "").strip() or None
        values.append((est_num, account_code, flight_start, flight_end, media_type, buyer, note))

    query = (
        f"INSERT INTO {est_nums_table} (estNum, accountCode, flightStart, flightEnd, mediaType, buyer, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "accountCode = VALUES(accountCode), "
        "flightStart = VALUES(flightStart), "
        "flightEnd = VALUES(flightEnd), "
        "mediaType = VALUES(mediaType), "
        "buyer = VALUES(buyer), "
        "note = VALUES(note)"
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
            flight_start = str(item.get("flightStart") or "").strip()
            if not flight_start:
                raise ValueError("flightStart cannot be empty")
            fields.append("flightStart = %s")
            params.append(flight_start)

        if "flightEnd" in item:
            flight_end = str(item.get("flightEnd") or "").strip()
            if not flight_end:
                raise ValueError("flightEnd cannot be empty")
            fields.append("flightEnd = %s")
            params.append(flight_end)

        if "buyer" in item:
            buyer = str(item.get("buyer") or "").strip()
            if not buyer:
                raise ValueError("buyer cannot be empty")
            fields.append("buyer = %s")
            params.append(buyer)

        if "note" in item:
            fields.append("note = %s")
            params.append(str(item.get("note") or "").strip() or None)

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
        name = str(item.get("name") or "").strip()
        if not name:
            raise ValueError("name is required")
        url = str(item.get("url") or "").strip()
        if not url:
            raise ValueError("url is required")
        username = str(item.get("username") or "").strip()
        if not username:
            raise ValueError("username is required")
        deadline = str(item.get("deadline") or "").strip() or "10 AM"
        password = item.get("password")
        note = str(item.get("note") or "").strip() or None
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
    codes: list[str],
    media_types: list[str] | None = None,
    languages: list[str] | None = None,
) -> list[dict]:
    tables = get_db_tables()
    stations_table = _quote_table_name(tables["STATIONS"])
    delivery_methods_table = _quote_table_name(tables["DELIVERYMETHODS"])
    normalized_codes = [_normalize_account_code(code) for code in codes]
    normalized_codes = [code for code in normalized_codes if code]
    if not normalized_codes:
        return []

    where_clauses: list[str] = []
    params: list[object] = []

    code_placeholders = _build_in_placeholders(normalized_codes)
    where_clauses.append(f"UPPER(s.code) IN ({code_placeholders})")
    params.extend(normalized_codes)

    normalized_media_types = [_normalize_media_type(item) for item in (media_types or [])]
    normalized_media_types = [item for item in normalized_media_types if item]
    if normalized_media_types:
        placeholders = _build_in_placeholders(normalized_media_types)
        where_clauses.append(f"UPPER(s.mediaType) IN ({placeholders})")
        params.extend(normalized_media_types)

    normalized_languages = [str(item or "").strip().upper() for item in (languages or [])]
    normalized_languages = [item for item in normalized_languages if item]
    if normalized_languages:
        placeholders = _build_in_placeholders(normalized_languages)
        where_clauses.append(f"UPPER(s.language) IN ({placeholders})")
        params.extend(normalized_languages)

    query = (
        "SELECT "
        "s.code AS code, "
        "s.name AS name, "
        "s.affiliation AS affiliation, "
        "s.mediaType AS mediaType, "
        "s.syscode AS syscode, "
        "s.language AS language, "
        "s.ownership AS ownership, "
        "s.deliveryMethodId AS deliveryMethodId, "
        "s.note AS note, "
        "d.name AS deliveryMethodName, "
        "d.url AS deliveryMethodUrl, "
        "d.username AS deliveryMethodUsername, "
        "d.deadline AS deliveryMethodDeadline, "
        "d.note AS deliveryMethodNote "
        f"FROM {stations_table} s "
        f"LEFT JOIN {delivery_methods_table} d "
        "ON s.deliveryMethodId = d.id "
        "WHERE "
        + " AND ".join(where_clauses)
        + " ORDER BY s.code ASC"
    )
    return fetch_all(query, tuple(params))


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
        name = str(item.get("name") or "").strip()
        if not name:
            raise ValueError("name is required")
        media_type = _normalize_media_type(item.get("mediaType"))
        if not media_type:
            raise ValueError("mediaType is required")
        language = str(item.get("language") or "").strip()
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
                str(item.get("affiliation") or "").strip() or None,
                media_type,
                syscode,
                language,
                str(item.get("ownership") or "").strip() or None,
                int(delivery_method_id),
                str(item.get("note") or "").strip() or None,
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
            name = str(item.get("name") or "").strip()
            if not name:
                raise ValueError("name cannot be empty")
            fields.append("name = %s")
            params.append(name)
        if "affiliation" in item:
            fields.append("affiliation = %s")
            params.append(str(item.get("affiliation") or "").strip() or None)
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
            language = str(item.get("language") or "").strip()
            if not language:
                raise ValueError("language cannot be empty")
            fields.append("language = %s")
            params.append(language)
        if "ownership" in item:
            fields.append("ownership = %s")
            params.append(str(item.get("ownership") or "").strip() or None)
        if "deliveryMethodId" in item:
            delivery_method_id = item.get("deliveryMethodId")
            if delivery_method_id is None:
                raise ValueError("deliveryMethodId cannot be null")
            fields.append("deliveryMethodId = %s")
            params.append(int(delivery_method_id))
        if "note" in item:
            fields.append("note = %s")
            params.append(str(item.get("note") or "").strip() or None)
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
            name = str(item.get("name") or "").strip()
            if not name:
                raise ValueError("name cannot be empty")
            fields.append("name = %s")
            params.append(name)
        if "url" in item:
            url = str(item.get("url") or "").strip()
            if not url:
                raise ValueError("url cannot be empty")
            fields.append("url = %s")
            params.append(url)
        if "username" in item:
            username = str(item.get("username") or "").strip()
            if not username:
                raise ValueError("username cannot be empty")
            fields.append("username = %s")
            params.append(username)
        if "password" in item:
            fields.append("password = %s")
            params.append(item.get("password"))
        if "deadline" in item:
            deadline = str(item.get("deadline") or "").strip()
            if not deadline:
                raise ValueError("deadline cannot be empty")
            fields.append("deadline = %s")
            params.append(deadline)
        if "note" in item:
            fields.append("note = %s")
            params.append(str(item.get("note") or "").strip() or None)
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
    active: bool | None = None,
) -> list[dict]:
    tables = get_db_tables()
    contacts_table = _quote_table_name(tables["CONTACTS"])
    where_clauses: list[str] = []
    params: list[object] = []
    normalized_emails = [_normalize_email(item) for item in (emails or [])]
    normalized_emails = [item for item in normalized_emails if item]
    if normalized_emails:
        placeholders = _build_in_placeholders(normalized_emails)
        where_clauses.append(f"LOWER(email) IN ({placeholders})")
        params.extend(normalized_emails)
    if active is not None:
        where_clauses.append("active = %s")
        params.append(1 if active else 0)

    query = (
        "SELECT id, firstName, lastName, company, jobTitle, office, cell, email, active, note "
        f"FROM {contacts_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY id ASC"
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
        email = str(item.get("email") or "").strip()
        if not email:
            raise ValueError("email is required")
        first_name = item.get("firstName")
        if first_name is None:
            first_name = ""
        else:
            first_name = str(first_name).strip()
        values.append(
            (
                first_name,
                str(item.get("lastName") or "").strip() or None,
                str(item.get("company") or "").strip() or None,
                str(item.get("jobTitle") or "").strip() or None,
                str(item.get("office") or "").strip() or None,
                str(item.get("cell") or "").strip() or None,
                email,
                _normalize_bool(item.get("active"), default=True),
                str(item.get("note") or "").strip() or None,
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
            email = str(item.get("email") or "").strip()
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
                first_name = str(first_name).strip()
            params.append(first_name)
        if "lastName" in item:
            fields.append("lastName = %s")
            params.append(str(item.get("lastName") or "").strip() or None)
        if "company" in item:
            fields.append("company = %s")
            params.append(str(item.get("company") or "").strip() or None)
        if "jobTitle" in item:
            fields.append("jobTitle = %s")
            params.append(str(item.get("jobTitle") or "").strip() or None)
        if "office" in item:
            fields.append("office = %s")
            params.append(str(item.get("office") or "").strip() or None)
        if "cell" in item:
            fields.append("cell = %s")
            params.append(str(item.get("cell") or "").strip() or None)
        if "active" in item:
            fields.append("active = %s")
            params.append(_normalize_bool(item.get("active"), default=True))
        if "note" in item:
            fields.append("note = %s")
            params.append(str(item.get("note") or "").strip() or None)
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
    contact_types: list[str] | None = None,
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

    normalized_contact_types = [
        _normalize_contact_type(item) for item in (contact_types or [])
    ]
    normalized_contact_types = [item for item in normalized_contact_types if item]
    if normalized_contact_types:
        placeholders = _build_in_placeholders(normalized_contact_types)
        where_clauses.append(f"UPPER(contactType) IN ({placeholders})")
        params.extend(normalized_contact_types)

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
                item.get("note"),
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
            params.append(item.get("note"))
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
