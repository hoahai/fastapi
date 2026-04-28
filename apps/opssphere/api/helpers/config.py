from __future__ import annotations

import ast
import json
import re
import threading
from datetime import datetime

from shared.tenant import (
    TenantConfigValidationError,
    get_app_scoped_env,
    get_tenant_id,
    get_timezone,
)

APP_NAME = "OpsSphere"
_GOOGLE_ACCOUNTS_CONFIG_KEY_PREFIX = "opssphere.google_accounts"
_GA4_CONFIG_KEY_PREFIX = "opssphere.ga4"
_GA4_PROPERTIES_KEY = f"{_GA4_CONFIG_KEY_PREFIX}.properties"
_PUBLIC_CONFIG_KEY_PREFIX = "opssphere.public"
_PROPERTY_ID_RE = re.compile(r"^\d+$")
_PUBLIC_SIGNED_REPORT_TTL_DAYS_DEFAULT = 30
_PUBLIC_SIGNED_REPORT_TTL_DAYS_MIN = 1
_PUBLIC_SIGNED_REPORT_TTL_DAYS_MAX = 365
_DATE_FORMAT = "%Y-%m-%d"

_VALIDATED_TENANTS: set[str] = set()
_VALIDATION_LOCK = threading.Lock()


def _parse_raw_value(raw: str, key: str, expected_type):
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError) as exc:
            raise TenantConfigValidationError(app_name=APP_NAME, invalid=[key]) from exc

    if not isinstance(parsed, expected_type):
        raise TenantConfigValidationError(app_name=APP_NAME, invalid=[key])
    return parsed


def _get_google_accounts_raw() -> str | None:
    return get_app_scoped_env(APP_NAME, "GOOGLE_ACCOUNTS")


def _get_ga4_raw() -> str | None:
    return get_app_scoped_env(APP_NAME, "GA4")


def _get_public_raw() -> str | None:
    return get_app_scoped_env(APP_NAME, "PUBLIC")


def _has_opssphere_config() -> bool:
    return any(
        raw is not None and str(raw).strip() != ""
        for raw in (_get_google_accounts_raw(), _get_ga4_raw())
    )


def _validate_google_accounts_config(
    parsed: dict,
    *,
    key_prefix: str,
) -> tuple[list[str], list[str]]:
    missing: list[str] = []
    invalid: list[str] = []

    normalized = {str(k).strip().lower(): v for k, v in parsed.items()}
    json_key_file_path = normalized.get("json_key_file_path")
    google_app_creds = normalized.get("google_application_credentials")
    has_json_key_file_path = (
        isinstance(json_key_file_path, str) and bool(json_key_file_path.strip())
    )
    has_google_app_creds = (
        isinstance(google_app_creds, str) and bool(google_app_creds.strip())
    )

    if not has_json_key_file_path and not has_google_app_creds:
        missing.append(
            f"{key_prefix}.json_key_file_path|GOOGLE_APPLICATION_CREDENTIALS"
        )
    if json_key_file_path is not None and not has_json_key_file_path:
        invalid.append(f"{key_prefix}.json_key_file_path")
    if google_app_creds is not None and not has_google_app_creds:
        invalid.append(f"{key_prefix}.GOOGLE_APPLICATION_CREDENTIALS")

    return missing, invalid


def _parse_property_config(
    value: object,
    *,
    account_code: str,
) -> tuple[dict[str, object], list[str], list[str]]:
    missing: list[str] = []
    invalid: list[str] = []

    key_prefix = f"{_GA4_PROPERTIES_KEY}.{account_code}"
    property_id = ""
    timezone = ""
    click_text_dimension = ""
    click_text_dimension_candidates: list[str] = []
    sub_menu_parent_dimension = ""
    start_date = ""

    if isinstance(value, str):
        property_id = value.strip()
    elif isinstance(value, dict):
        normalized = {str(k).strip().lower(): v for k, v in value.items()}
        property_id = str(
            normalized.get("id")
            or normalized.get("propertyid")
            or normalized.get("property_id")
            or ""
        ).strip()
        raw_timezone = normalized.get("timezone")
        if raw_timezone is not None:
            timezone = str(raw_timezone).strip()
            if not timezone:
                invalid.append(f"{key_prefix}.timezone")

        raw_click_text_dimension = (
            normalized.get("click_text_dimension")
            or normalized.get("clicktextdimension")
        )
        if raw_click_text_dimension is not None:
            click_text_dimension = str(raw_click_text_dimension).strip()
            if not click_text_dimension:
                invalid.append(f"{key_prefix}.click_text_dimension")

        raw_click_text_dimension_candidates = (
            normalized.get("click_text_dimension_candidates")
            or normalized.get("clicktextdimensioncandidates")
        )
        if raw_click_text_dimension_candidates is not None:
            if not isinstance(raw_click_text_dimension_candidates, list):
                invalid.append(f"{key_prefix}.click_text_dimension_candidates")
            else:
                for idx, candidate in enumerate(raw_click_text_dimension_candidates):
                    candidate_text = str(candidate or "").strip()
                    if not candidate_text:
                        invalid.append(
                            f"{key_prefix}.click_text_dimension_candidates[{idx}]"
                        )
                        continue
                    if candidate_text in click_text_dimension_candidates:
                        continue
                    click_text_dimension_candidates.append(candidate_text)

        raw_sub_menu_parent_dimension = (
            normalized.get("sub_menu_parent_dimension")
            or normalized.get("submenuparentdimension")
        )
        if raw_sub_menu_parent_dimension is not None:
            sub_menu_parent_dimension = str(raw_sub_menu_parent_dimension).strip()
            if not sub_menu_parent_dimension:
                invalid.append(f"{key_prefix}.sub_menu_parent_dimension")

        raw_start_date = (
            normalized.get("start_date")
            or normalized.get("startdate")
        )
        if raw_start_date is not None:
            start_date = str(raw_start_date).strip()
            if not _is_valid_iso_date(start_date):
                invalid.append(f"{key_prefix}.start_date")

    else:
        invalid.append(key_prefix)
        return {}, missing, invalid

    if not _PROPERTY_ID_RE.fullmatch(property_id):
        invalid.append(f"{key_prefix}.id")
        return {}, missing, invalid

    config = {
        "property_id": property_id,
        "timezone": timezone,
        "click_text_dimension": click_text_dimension,
        "click_text_dimension_candidates": click_text_dimension_candidates,
        "sub_menu_parent_dimension": sub_menu_parent_dimension,
        "start_date": start_date,
    }
    return config, missing, invalid


def _is_valid_iso_date(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        parsed = datetime.strptime(text, _DATE_FORMAT)
    except ValueError:
        return False
    return parsed.strftime(_DATE_FORMAT) == text


def _parse_properties_map(
    raw: object,
) -> tuple[dict[str, dict[str, object]], list[str], list[str]]:
    missing: list[str] = []
    invalid: list[str] = []
    properties_by_account_code: dict[str, dict[str, object]] = {}

    if raw is None:
        missing.append(_GA4_PROPERTIES_KEY)
        return properties_by_account_code, missing, invalid

    if not isinstance(raw, dict):
        invalid.append(_GA4_PROPERTIES_KEY)
        return properties_by_account_code, missing, invalid

    for account_code_raw, property_id_raw in raw.items():
        account_code = str(account_code_raw or "").strip().upper()
        if not account_code:
            invalid.append(f"{_GA4_PROPERTIES_KEY}.{account_code_raw}")
            continue

        parsed_config, cfg_missing, cfg_invalid = _parse_property_config(
            property_id_raw,
            account_code=account_code,
        )
        missing.extend(cfg_missing)
        invalid.extend(cfg_invalid)
        if parsed_config:
            properties_by_account_code[account_code] = parsed_config

    if not properties_by_account_code:
        missing.append(_GA4_PROPERTIES_KEY)

    return properties_by_account_code, missing, invalid


def _parse_int_in_range(
    value: object,
    *,
    default: int,
    min_value: int,
    max_value: int,
    invalid_key: str,
) -> tuple[int, list[str]]:
    invalid: list[str] = []
    if value is None or str(value).strip() == "":
        return default, invalid

    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        invalid.append(invalid_key)
        return default, invalid

    if parsed < min_value or parsed > max_value:
        invalid.append(invalid_key)
        return default, invalid

    return parsed, invalid


def get_ga4_config() -> dict[str, object]:
    raw = _get_ga4_raw()
    if raw is None or str(raw).strip() == "":
        raise TenantConfigValidationError(app_name=APP_NAME, missing=[_GA4_CONFIG_KEY_PREFIX])

    parsed = _parse_raw_value(str(raw), "OPSSPHERE_GA4", dict)
    normalized = {str(k).strip().lower(): v for k, v in parsed.items()}
    start_date = str(
        normalized.get("start_date")
        or normalized.get("startdate")
        or ""
    ).strip()
    if start_date and not _is_valid_iso_date(start_date):
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            invalid=[f"{_GA4_CONFIG_KEY_PREFIX}.start_date"],
        )
    properties_by_account_code, missing, invalid = _parse_properties_map(
        normalized.get("properties")
    )
    if missing or invalid:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            missing=sorted(set(missing)),
            invalid=sorted(set(invalid)),
        )

    property_ids_by_account_code: dict[str, str] = {}
    property_ids: list[str] = []
    seen: set[str] = set()
    for account_code, config in properties_by_account_code.items():
        property_id = str(config.get("property_id") or "").strip()
        property_ids_by_account_code[account_code] = property_id
        if property_id in seen:
            continue
        seen.add(property_id)
        property_ids.append(property_id)

    return {
        "properties_config_by_account_code": properties_by_account_code,
        "properties_by_account_code": property_ids_by_account_code,
        "property_ids": property_ids,
        "start_date": start_date,
        "timezone": get_timezone(),
    }


def get_public_config() -> dict[str, object]:
    raw = _get_public_raw()
    if raw is None or str(raw).strip() == "":
        return {
            "signed_report_ttl_days": _PUBLIC_SIGNED_REPORT_TTL_DAYS_DEFAULT,
            "signing_secret": "",
        }

    parsed = _parse_raw_value(str(raw), "OPSSPHERE_PUBLIC", dict)
    normalized = {str(k).strip().lower(): v for k, v in parsed.items()}

    ttl_days, invalid = _parse_int_in_range(
        normalized.get("signed_report_ttl_days")
        or normalized.get("signedreportttldays"),
        default=_PUBLIC_SIGNED_REPORT_TTL_DAYS_DEFAULT,
        min_value=_PUBLIC_SIGNED_REPORT_TTL_DAYS_MIN,
        max_value=_PUBLIC_SIGNED_REPORT_TTL_DAYS_MAX,
        invalid_key=f"{_PUBLIC_CONFIG_KEY_PREFIX}.signed_report_ttl_days",
    )
    if invalid:
        raise TenantConfigValidationError(app_name=APP_NAME, invalid=invalid)

    signing_secret = str(
        normalized.get("signing_secret")
        or normalized.get("signed_report_signing_secret")
        or ""
    ).strip()
    return {
        "signed_report_ttl_days": ttl_days,
        "signing_secret": signing_secret,
    }


def validate_tenant_config(tenant_id: str | None = None) -> None:
    tenant_id = tenant_id or get_tenant_id()
    if not tenant_id:
        raise TenantConfigValidationError(app_name=APP_NAME, missing=["tenant_id"])

    with _VALIDATION_LOCK:
        if tenant_id in _VALIDATED_TENANTS:
            return

        missing: list[str] = []
        invalid: list[str] = []

        if not _has_opssphere_config():
            missing.append("opssphere")
        else:
            google_accounts_raw = _get_google_accounts_raw()
            if google_accounts_raw is None or str(google_accounts_raw).strip() == "":
                missing.append(_GOOGLE_ACCOUNTS_CONFIG_KEY_PREFIX)
            else:
                try:
                    google_accounts = _parse_raw_value(
                        str(google_accounts_raw),
                        "OPSSPHERE_GOOGLE_ACCOUNTS",
                        dict,
                    )
                    cfg_missing, cfg_invalid = _validate_google_accounts_config(
                        google_accounts,
                        key_prefix=_GOOGLE_ACCOUNTS_CONFIG_KEY_PREFIX,
                    )
                    missing.extend(cfg_missing)
                    invalid.extend(cfg_invalid)
                except TenantConfigValidationError as exc:
                    missing.extend(exc.missing)
                    invalid.extend(exc.invalid)

            try:
                _ = get_ga4_config()
            except TenantConfigValidationError as exc:
                missing.extend(exc.missing)
                invalid.extend(exc.invalid)
            try:
                _ = get_public_config()
            except TenantConfigValidationError as exc:
                missing.extend(exc.missing)
                invalid.extend(exc.invalid)

        if missing or invalid:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                missing=sorted(set(missing)),
                invalid=sorted(set(invalid)),
            )

        _VALIDATED_TENANTS.add(tenant_id)
