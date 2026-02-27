# api/v1/helpers/ggAd.py

import re
import calendar
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.field_mask_pb2 import FieldMask
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v22.errors.types.errors import GoogleAdsFailure

from shared.utils import (
    get_current_period,
    run_parallel,
    run_parallel_flatten,
    LOCAL_SECRETS_DIR,
)
from shared.tenant import get_env, TenantConfigError
from pathlib import Path
from shared.constants import (
    GGADS_MAX_UPDATES_PER_REQUEST,
    GGADS_MAX_PAUSED_CAMPAIGNS,
    GGADS_MIN_BUDGET,
    GGADS_MAX_BUDGET_MULTIPLIER,
    GGADS_ALLOWED_CAMPAIGN_STATUSES,
)
from shared.logger import get_logger
from apps.spendsphere.api.v1.helpers.account_codes import standardize_account_code
from apps.spendsphere.api.v1.helpers.config import (
    get_adtypes,
    get_google_ads_inactive_prefixes,
    get_google_ads_naming,
    is_google_ads_inactive_name,
)
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    get_google_ads_budgets_cache_entries,
    get_google_ads_campaigns_cache_entries,
    get_google_ads_clients_cache_entry,
    set_google_ads_budgets_cache,
    set_google_ads_campaigns_cache,
    set_google_ads_clients_cache,
)

logger = get_logger("Google Ads")


def _chunked(items: list[dict], size: int):
    if size <= 0:
        yield items
        return
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _is_zzz_name(
    name: str | None,
    inactive_prefixes: tuple[str, ...] | None = None,
) -> bool:
    return is_google_ads_inactive_name(
        name,
        inactive_prefixes=inactive_prefixes,
    )


_DEFAULT_NAMING_TOKEN_PATTERNS = {
    "accountCode": r"[A-Za-z0-9]+",
    "adTypeCode": r"[A-Za-z0-9]+",
    "accountName": r".+",
    "campaignName": r".+",
}
_UNKNOWN_ADTYPE_CODE = "UNK"


def _build_ad_type_pattern(adtypes: dict) -> str:
    codes = [str(code).strip() for code in adtypes.keys() if str(code).strip()]
    if not codes:
        return _DEFAULT_NAMING_TOKEN_PATTERNS["adTypeCode"]
    return "|".join(sorted((re.escape(code) for code in codes), key=len, reverse=True))


def _get_naming_token_patterns(adtypes: dict) -> dict[str, str]:
    naming = get_google_ads_naming()
    token_patterns = dict(_DEFAULT_NAMING_TOKEN_PATTERNS)
    token_patterns["adTypeCode"] = _build_ad_type_pattern(adtypes)

    raw_token_patterns = naming.get("tokenPatterns")
    if isinstance(raw_token_patterns, dict):
        for key, value in raw_token_patterns.items():
            token = str(key).strip()
            pattern = str(value).strip()
            if token and pattern:
                token_patterns[token] = pattern

    return token_patterns


def _compile_format_pattern(
    *,
    section: str,
    format_pattern: str,
    token_patterns: dict[str, str],
) -> re.Pattern:
    seen_tokens: set[str] = set()

    def parse_segment(start_idx: int, *, in_optional: bool) -> tuple[str, int]:
        parts: list[str] = []
        idx = start_idx

        while idx < len(format_pattern):
            ch = format_pattern[idx]

            if ch == "[":
                nested, idx = parse_segment(idx + 1, in_optional=True)
                parts.append(f"(?:{nested})?")
                continue

            if ch == "]":
                if in_optional:
                    return "".join(parts), idx + 1
                raise TenantConfigError(
                    f"Invalid GOOGLE_ADS_NAMING.{section}.format: unexpected ']'"
                )

            if ch == "{":
                end_idx = format_pattern.find("}", idx + 1)
                if end_idx == -1:
                    raise TenantConfigError(
                        f"Invalid GOOGLE_ADS_NAMING.{section}.format: missing '}}'"
                    )

                token = format_pattern[idx + 1 : end_idx].strip()
                if not token:
                    raise TenantConfigError(
                        f"Invalid GOOGLE_ADS_NAMING.{section}.format: empty token"
                    )

                token_pattern = token_patterns.get(token)
                if not token_pattern:
                    raise TenantConfigError(
                        "Invalid GOOGLE_ADS_NAMING."
                        f"{section}.format: unknown token '{token}'"
                    )

                if token in seen_tokens:
                    parts.append(f"(?:{token_pattern})")
                else:
                    parts.append(f"(?P<{token}>{token_pattern})")
                    seen_tokens.add(token)
                idx = end_idx + 1
                continue

            parts.append(re.escape(ch))
            idx += 1

        if in_optional:
            raise TenantConfigError(
                f"Invalid GOOGLE_ADS_NAMING.{section}.format: missing ']'"
            )

        return "".join(parts), idx

    body, _ = parse_segment(0, in_optional=False)
    try:
        return re.compile(f"^{body}$", re.IGNORECASE)
    except re.error as exc:
        raise TenantConfigError(
            f"Invalid GOOGLE_ADS_NAMING.{section}.format pattern: {exc}"
        ) from exc


def _compile_naming_pattern(section: str, adtypes: dict) -> re.Pattern:
    naming = get_google_ads_naming()
    section_config = naming.get(section)
    if not isinstance(section_config, dict):
        raise TenantConfigError(f"Missing GOOGLE_ADS_NAMING.{section} config")

    token_patterns = _get_naming_token_patterns(adtypes)
    regex_value = section_config.get("regex")
    format_value = section_config.get("format")

    if isinstance(regex_value, str) and regex_value.strip():
        try:
            pattern = re.compile(regex_value.strip(), re.IGNORECASE)
        except re.error as exc:
            raise TenantConfigError(
                f"Invalid GOOGLE_ADS_NAMING.{section}.regex: {exc}"
            ) from exc
    elif isinstance(format_value, str) and format_value.strip():
        pattern = _compile_format_pattern(
            section=section,
            format_pattern=format_value.strip(),
            token_patterns=token_patterns,
        )
    else:
        raise TenantConfigError(
            f"Missing GOOGLE_ADS_NAMING.{section}.format or .regex"
        )

    if section == "account" and "accountCode" not in pattern.groupindex:
        raise TenantConfigError(
            "GOOGLE_ADS_NAMING.account must include named group 'accountCode'"
        )

    return pattern


def _normalize_named_groups(match: re.Match[str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in match.groupdict().items():
        if value is None:
            continue
        text = str(value).strip()
        if text:
            normalized[str(key)] = text
    return normalized


def _build_channel_type_to_adtype_map(adtypes: dict) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for ad_type_code, config in adtypes.items():
        code = str(ad_type_code).strip().upper()
        if not code or not isinstance(config, dict):
            continue

        query_value = config.get("adTypeQuery")
        if isinstance(query_value, str) and query_value.strip():
            mapping[query_value.strip().upper()] = code
        elif isinstance(query_value, list):
            for item in query_value:
                channel = str(item).strip().upper()
                if channel:
                    mapping[channel] = code

    return mapping


# =====================
# CLIENT
# =====================


def get_client() -> GoogleAdsClient:
    """
    Create and return a Google Ads client using tenant config.
    """
    developer_token = get_env("developer_token")
    login_customer_id = get_env("login_customer_id")
    json_key_file_path = get_env("json_key_file_path")
    google_app_creds = get_env("GOOGLE_APPLICATION_CREDENTIALS")
    use_proto_plus = get_env("use_proto_plus", "true")

    missing: list[str] = []
    if not developer_token:
        missing.append("developer_token")
    if not login_customer_id:
        missing.append("login_customer_id")
    if not json_key_file_path and not google_app_creds:
        missing.append("json_key_file_path or GOOGLE_APPLICATION_CREDENTIALS")
    if missing:
        raise TenantConfigError(
            "Missing Google Ads tenant config keys: " + ", ".join(missing)
        )

    def _resolve_key_path(raw_path: str) -> Path | None:
        candidate = Path(raw_path)
        if candidate.is_file():
            return candidate
        if not candidate.is_absolute():
            candidate_str = str(candidate)
            if candidate_str.startswith("etc/secrets/"):
                abs_candidate = Path("/") / candidate
                if abs_candidate.is_file():
                    return abs_candidate
            for base in (Path("/etc/secrets"), LOCAL_SECRETS_DIR):
                alt = base / candidate_str
                if alt.is_file():
                    return alt
        return None

    key_path = None
    tried: list[str] = []
    for raw_path in (json_key_file_path, google_app_creds):
        if not raw_path:
            continue
        tried.append(str(raw_path))
        resolved = _resolve_key_path(str(raw_path))
        if resolved is not None:
            key_path = resolved
            break

    if key_path is None:
        tried_display = ", ".join(tried) if tried else "(none)"
        raise TenantConfigError(
            "Google Ads json_key_file_path not found. Tried: " + tried_display
        )

    config = {
        "developer_token": developer_token,
        "login_customer_id": login_customer_id,
        "json_key_file_path": str(key_path),
        "use_proto_plus": str(use_proto_plus).lower()
        in {"1", "true", "yes", "on"},
    }
    return GoogleAdsClient.load_from_dict(config)


# =====================
# ACCOUNTS
# =====================


def get_mcc_accounts() -> list[dict]:
    """
    Get all non-hidden, ENABLED Google Ads accounts under the MCC.
    """
    client = get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          customer_client.id,
          customer_client.descriptive_name,
          customer_client.status
        FROM customer_client
        WHERE customer_client.hidden != TRUE
          AND customer_client.status = 'ENABLED'
    """

    results: list[dict] = []

    try:
        response = ga_service.search(
            customer_id=client.login_customer_id,
            query=query,
        )

        for row in response:
            results.append(
                {
                    "id": str(row.customer_client.id),
                    "name": row.customer_client.descriptive_name,
                    "status": row.customer_client.status.name,
                }
            )

    except GoogleAdsException as ex:
        logger.error(
            "Failed to fetch MCC accounts",
            extra={"extra_fields": {"error": str(ex)}},
        )
        raise RuntimeError(f"Google Ads API error: {ex.failure}") from ex

    return results


def _normalize_account_code_token(value: object) -> str | None:
    return standardize_account_code(value)


def _normalize_account_name_token(value: object) -> str | None:
    cleaned = str(value).strip()
    return cleaned or None


def _get_google_ads_account_overrides() -> tuple[dict[str, dict], dict[str, dict]]:
    naming = get_google_ads_naming()
    raw_overrides = naming.get("accountOverrides")
    if not isinstance(raw_overrides, dict):
        return {}, {}

    def _normalize_override_entry(raw_entry: object) -> dict | None:
        if not isinstance(raw_entry, dict):
            return None

        account_code = _normalize_account_code_token(raw_entry.get("accountCode"))
        if not account_code:
            return None

        normalized: dict[str, str] = {"accountCode": account_code}
        account_name = _normalize_account_name_token(raw_entry.get("accountName"))
        if account_name:
            normalized["accountName"] = account_name
        return normalized

    by_id: dict[str, dict] = {}
    raw_by_id = raw_overrides.get("byId")
    if isinstance(raw_by_id, dict):
        for raw_key, raw_entry in raw_by_id.items():
            key = str(raw_key).strip()
            normalized_entry = _normalize_override_entry(raw_entry)
            if key and normalized_entry:
                by_id[key] = normalized_entry

    by_name: dict[str, dict] = {}
    raw_by_name = raw_overrides.get("byName")
    if isinstance(raw_by_name, dict):
        for raw_key, raw_entry in raw_by_name.items():
            key = str(raw_key).strip().casefold()
            normalized_entry = _normalize_override_entry(raw_entry)
            if key and normalized_entry:
                by_name[key] = normalized_entry

    return by_id, by_name


def _resolve_account_override(
    *,
    account_id: object,
    descriptive_name: str,
    by_id: dict[str, dict],
    by_name: dict[str, dict],
) -> dict | None:
    account_id_key = str(account_id).strip()
    if account_id_key:
        matched_by_id = by_id.get(account_id_key)
        if isinstance(matched_by_id, dict):
            return matched_by_id

    if descriptive_name:
        matched_by_name = by_name.get(descriptive_name.casefold())
        if isinstance(matched_by_name, dict):
            return matched_by_name

    return None


def _parse_single_google_ads_account(
    *,
    account: dict,
    account_name_pattern: re.Pattern,
    overrides_by_id: dict[str, dict],
    overrides_by_name: dict[str, dict],
    inactive_prefixes: tuple[str, ...],
) -> tuple[dict | None, dict | None]:
    descriptive_name = str(account.get("name", "")).strip()
    account_id = account.get("id")
    if not descriptive_name:
        return None, {
            "id": account_id,
            "descriptiveName": account.get("name"),
            "reason": "missing_descriptive_name",
        }

    override = _resolve_account_override(
        account_id=account_id,
        descriptive_name=descriptive_name,
        by_id=overrides_by_id,
        by_name=overrides_by_name,
    )
    if override is not None:
        account_code = _normalize_account_code_token(override.get("accountCode"))
        if not account_code:
            return None, {
                "id": account_id,
                "descriptiveName": descriptive_name,
                "reason": "account_override_code_not_extractable",
                "accountCode": override.get("accountCode"),
            }

        account_name = (
            _normalize_account_name_token(override.get("accountName")) or descriptive_name
        )
        inactive_by_name = _is_zzz_name(
            descriptive_name,
            inactive_prefixes=inactive_prefixes,
        )
        normalized_name = account_name or account_code

        return {
            "id": account_id,
            "descriptiveName": descriptive_name,
            "accountCode": account_code,
            "accountName": normalized_name,
            "code": account_code,
            "name": normalized_name,
            "inactiveByName": inactive_by_name,
            "source": "google_ads_override",
        }, None

    match = account_name_pattern.match(descriptive_name)
    if not match:
        return None, {
            "id": account_id,
            "descriptiveName": descriptive_name,
            "reason": "invalid_name_format",
        }

    groups = _normalize_named_groups(match)
    raw_account_code = groups.get("accountCode")
    account_code = _normalize_account_code_token(raw_account_code)
    if not account_code:
        return None, {
            "id": account_id,
            "descriptiveName": descriptive_name,
            "reason": "account_code_not_extractable",
            "accountCode": raw_account_code,
        }

    account_name = (
        groups.get("accountName")
        or groups.get("campaignName")
        or descriptive_name
    )
    inactive_by_name = _is_zzz_name(
        descriptive_name,
        inactive_prefixes=inactive_prefixes,
    )
    normalized_name = str(account_name).strip() or account_code

    return {
        "id": account_id,
        "descriptiveName": descriptive_name,
        "accountCode": account_code,
        "accountName": normalized_name,
        "code": account_code,
        "name": normalized_name,
        "inactiveByName": inactive_by_name,
        "source": "google_ads",
    }, None


def _dedupe_accounts_by_code(accounts: list[dict]) -> list[dict]:
    deduped: dict[str, dict] = {}
    order: list[str] = []

    for account in accounts:
        code = _normalize_account_code_token(account.get("accountCode"))
        if not code:
            continue

        existing = deduped.get(code)
        if existing is None:
            deduped[code] = account
            order.append(code)
            continue

        # Prefer active-by-name entries over configured inactive-prefix entries.
        if bool(existing.get("inactiveByName")) and not bool(
            account.get("inactiveByName")
        ):
            deduped[code] = account

    return [deduped[code] for code in order]


def _parse_named_google_ads_accounts(raw_accounts: list[dict]) -> list[dict]:
    account_name_pattern = _compile_naming_pattern("account", get_adtypes())
    overrides_by_id, overrides_by_name = _get_google_ads_account_overrides()
    inactive_prefixes = get_google_ads_inactive_prefixes()
    parsed: list[dict] = []

    for acc in raw_accounts:
        parsed_entry, _ = _parse_single_google_ads_account(
            account=acc,
            account_name_pattern=account_name_pattern,
            overrides_by_id=overrides_by_id,
            overrides_by_name=overrides_by_name,
            inactive_prefixes=inactive_prefixes,
        )
        if parsed_entry is None:
            continue

        parsed.append(parsed_entry)

    return _dedupe_accounts_by_code(parsed)


def _parse_named_google_ads_accounts_with_failures(
    raw_accounts: list[dict],
) -> tuple[list[dict], list[dict]]:
    account_name_pattern = _compile_naming_pattern("account", get_adtypes())
    overrides_by_id, overrides_by_name = _get_google_ads_account_overrides()
    inactive_prefixes = get_google_ads_inactive_prefixes()
    parsed: list[dict] = []
    failed: list[dict] = []

    for acc in raw_accounts:
        parsed_entry, failure_entry = _parse_single_google_ads_account(
            account=acc,
            account_name_pattern=account_name_pattern,
            overrides_by_id=overrides_by_id,
            overrides_by_name=overrides_by_name,
            inactive_prefixes=inactive_prefixes,
        )
        if parsed_entry is not None:
            parsed.append(parsed_entry)
            continue
        if failure_entry is not None:
            failed.append(failure_entry)

    return _dedupe_accounts_by_code(parsed), failed


def get_ggad_accounts_for_validation(*, refresh_cache: bool = False) -> list[dict]:
    del refresh_cache  # Reserved for parity with other cacheable helpers.
    raw_accounts = get_mcc_accounts()
    if not raw_accounts:
        return []
    return _parse_named_google_ads_accounts(raw_accounts)


def get_ggad_accounts(*, refresh_cache: bool = False) -> list[dict]:
    """
    Return normalized Google Ads accounts based on tenant naming config.
    """
    if not refresh_cache:
        cached, is_stale = get_google_ads_clients_cache_entry()
        if cached is not None and not is_stale:
            return cached

    parsed_accounts = get_ggad_accounts_for_validation(refresh_cache=True)
    if not parsed_accounts:
        set_google_ads_clients_cache([])
        return []

    results = [
        {
            "id": account.get("id"),
            "descriptiveName": account.get("descriptiveName"),
            "accountCode": standardize_account_code(account.get("accountCode")),
            "accountName": account.get("accountName"),
        }
        for account in parsed_accounts
        if not bool(account.get("inactiveByName"))
    ]

    set_google_ads_clients_cache(results)
    return results


def get_ggad_accounts_with_summary(*, refresh_cache: bool = False) -> dict:
    del refresh_cache  # Summary needs live evaluation to include failed accounts.
    raw_accounts = get_mcc_accounts()
    if not raw_accounts:
        set_google_ads_clients_cache([])
        return {
            "summary": {
                "total": 0,
                "valid": 0,
                "invalid": 0,
            },
            "validAccounts": [],
            "invalidAccounts": [],
        }

    parsed_accounts, failed_accounts = _parse_named_google_ads_accounts_with_failures(
        raw_accounts
    )
    clients = [
        {
            "id": account.get("id"),
            "descriptiveName": account.get("descriptiveName"),
            "accountCode": standardize_account_code(account.get("accountCode")),
            "accountName": account.get("accountName"),
        }
        for account in parsed_accounts
        if not bool(account.get("inactiveByName"))
    ]
    set_google_ads_clients_cache(clients)

    return {
        "summary": {
            "total": len(raw_accounts),
            "valid": len(clients),
            "invalid": len(failed_accounts),
        },
        "validAccounts": clients,
        "invalidAccounts": failed_accounts,
    }


# =====================
# BUDGETS (READ)
# =====================


def get_ggad_budget(customer_id: str) -> list[dict]:
    """
    Get all non-removed campaign budgets for a single Google Ads account.
    """
    client = get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          campaign_budget.id,
          campaign_budget.name,
          campaign_budget.explicitly_shared,
          campaign_budget.status,
          campaign_budget.amount_micros
        FROM campaign_budget
        WHERE campaign_budget.status != 'REMOVED'
    """

    results: list[dict] = []

    try:
        response = ga_service.search(customer_id=customer_id, query=query)

        for row in response:
            b = row.campaign_budget
            results.append(
                {
                    "budgetId": str(b.id),
                    "budgetName": b.name,
                    "explicitlyShared": b.explicitly_shared,
                    "status": b.status.name,
                    "amount": b.amount_micros / 1_000_000 if b.amount_micros else 0,
                }
            )

    except GoogleAdsException as ex:
        raise RuntimeError(f"Google Ads API error: {ex.failure}") from ex

    return results


def get_ggad_budgets(
    accounts: list[dict],
    *,
    refresh_cache: bool = False,
) -> list[dict]:
    """
    Get campaign budgets for multiple Google Ads accounts (parallelized).
    """

    def per_account_func(account: dict) -> list[dict]:
        budgets = get_ggad_budget(account["id"])
        return [
            {
                "customerId": account["id"],
                "accountCode": standardize_account_code(account.get("accountCode")),
                "accountName": account.get("accountName"),
                **b,
            }
            for b in budgets
        ]

    if not accounts:
        return []

    account_map: dict[str, dict] = {}
    account_codes: list[str] = []
    for account in accounts:
        code = standardize_account_code(account.get("accountCode"))
        if not code or code in account_map:
            continue
        account_map[code] = account
        account_codes.append(code)

    cached: dict[str, list[dict]] = {}
    missing: set[str] = set(account_codes)
    if not refresh_cache:
        cached, missing = get_google_ads_budgets_cache_entries(account_codes)

    results: list[dict] = []
    for code in account_codes:
        if code in cached:
            results.extend(cached[code])

    if missing:
        missing_accounts = [account_map[code] for code in account_codes if code in missing]
        tasks = [(per_account_func, (account,)) for account in missing_accounts]
        fetched_lists = run_parallel(tasks=tasks, api_name="google_ads")

        for account, fetched in zip(missing_accounts, fetched_lists):
            budgets = fetched if isinstance(fetched, list) else []
            results.extend(budgets)
            set_google_ads_budgets_cache(account.get("accountCode"), budgets)

    return results


# =====================
# CAMPAIGNS (READ)
# =====================


def get_ggad_campaign(customer_id: str) -> list[dict]:
    """
    Get campaigns for a single Google Ads account.
    """
    client = get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          campaign.advertising_channel_type,
          campaign.id,
          campaign.name,
          campaign_budget.id,
          campaign.status
        FROM campaign
        WHERE campaign.experiment_type = 'BASE'
        ORDER BY
          campaign.advertising_channel_type ASC,
          campaign.status ASC,
          campaign.name ASC
    """

    results: list[dict] = []

    try:
        response = ga_service.search(customer_id=customer_id, query=query)

        for row in response:
            results.append(
                {
                    "campaignId": str(row.campaign.id),
                    "campaignName": row.campaign.name,
                    "status": row.campaign.status.name,
                    "channelType": row.campaign.advertising_channel_type.name,
                    "budgetId": (
                        str(row.campaign_budget.id) if row.campaign_budget.id else None
                    ),
                }
            )

    except GoogleAdsException as ex:
        raise RuntimeError(f"Google Ads API error: {ex.failure}") from ex

    return results


def get_ggad_campaigns(
    accounts: list[dict],
    *,
    refresh_cache: bool = False,
) -> list[dict]:
    """
    Get Google Ads campaigns for multiple accounts based on tenant naming config.
    """
    adtypes = get_adtypes()
    allowed_adtypes = {str(code).strip().upper() for code in adtypes.keys() if code}
    allowed_adtypes.add(_UNKNOWN_ADTYPE_CODE)
    campaign_name_pattern = _compile_naming_pattern("campaign", adtypes)
    channel_to_adtype = _build_channel_type_to_adtype_map(adtypes)
    inactive_prefixes = get_google_ads_inactive_prefixes()

    def per_account_func(account: dict) -> list[dict]:
        campaigns = get_ggad_campaign(account["id"])
        account_code = standardize_account_code(account.get("accountCode")) or ""

        filtered: list[dict] = []

        for c in campaigns:
            name = str(c.get("campaignName", "")).strip()
            status = str(c.get("status", "")).strip().upper()
            match = campaign_name_pattern.match(name)

            if not match:
                continue

            if _is_zzz_name(
                name,
                inactive_prefixes=inactive_prefixes,
            ) and status != "ENABLED":
                continue

            groups = _normalize_named_groups(match)
            parsed_account_code = standardize_account_code(groups.get("accountCode")) or ""
            if parsed_account_code and parsed_account_code != account_code:
                continue

            ad_type = str(groups.get("adTypeCode", "")).strip().upper()
            if not ad_type:
                channel_type = str(c.get("channelType", "")).strip().upper()
                ad_type = channel_to_adtype.get(channel_type, _UNKNOWN_ADTYPE_CODE)

            if not ad_type or ad_type not in allowed_adtypes:
                continue

            filtered.append(
                {
                    "customerId": account["id"],
                    "accountCode": account_code,
                    "accountName": account.get("accountName"),
                    "adTypeCode": ad_type,
                    **c,
                }
            )

        return filtered

    if not accounts:
        return []

    account_map: dict[str, dict] = {}
    account_codes: list[str] = []
    for account in accounts:
        code = standardize_account_code(account.get("accountCode"))
        if not code or code in account_map:
            continue
        account_map[code] = account
        account_codes.append(code)

    cached: dict[str, list[dict]] = {}
    missing: set[str] = set(account_codes)
    if not refresh_cache:
        cached, missing = get_google_ads_campaigns_cache_entries(account_codes)

    results: list[dict] = []
    for code in account_codes:
        if code in cached:
            results.extend(cached[code])

    if missing:
        missing_accounts = [account_map[code] for code in account_codes if code in missing]
        tasks = [(per_account_func, (account,)) for account in missing_accounts]
        fetched_lists = run_parallel(tasks=tasks, api_name="google_ads")

        for account, fetched in zip(missing_accounts, fetched_lists):
            campaigns = fetched if isinstance(fetched, list) else []
            results.extend(campaigns)
            set_google_ads_campaigns_cache(account.get("accountCode"), campaigns)

    return results


def get_ggad_budget_adtype_candidates(
    accounts: list[dict],
) -> dict[tuple[str, str], str | None]:
    """
    Build first-seen adTypeCode candidates by (customerId, budgetId) from
    raw Google Ads campaigns.

    Unlike `get_ggad_campaigns`, this helper is intentionally permissive and is
    used only as fallback adType inference for budget rows. It does not exclude
    campaigns by inactive naming prefixes/status.
    """
    if not accounts:
        return {}

    adtypes = get_adtypes()
    allowed_adtypes = {str(code).strip().upper() for code in adtypes.keys() if code}
    allowed_adtypes.add(_UNKNOWN_ADTYPE_CODE)
    campaign_name_pattern = _compile_naming_pattern("campaign", adtypes)
    channel_to_adtype = _build_channel_type_to_adtype_map(adtypes)

    def per_account_func(account: dict) -> list[dict]:
        campaigns = get_ggad_campaign(account["id"])
        rows: list[dict] = []

        for c in campaigns:
            budget_id = str(c.get("budgetId", "")).strip()
            if not budget_id:
                continue

            ad_type: str | None = None
            name = str(c.get("campaignName", "")).strip()
            if name:
                match = campaign_name_pattern.match(name)
                if match:
                    groups = _normalize_named_groups(match)
                    parsed_ad_type = str(groups.get("adTypeCode", "")).strip().upper()
                    if parsed_ad_type in allowed_adtypes:
                        ad_type = parsed_ad_type

            if not ad_type:
                channel_type = str(c.get("channelType", "")).strip().upper()
                channel_ad_type = channel_to_adtype.get(
                    channel_type,
                    _UNKNOWN_ADTYPE_CODE,
                )
                if channel_ad_type in allowed_adtypes:
                    ad_type = channel_ad_type

            if not ad_type:
                continue

            rows.append(
                {
                    "customerId": str(account.get("id", "")).strip(),
                    "budgetId": budget_id,
                    "adTypeCode": ad_type,
                }
            )

        return rows

    tasks = [(per_account_func, (account,)) for account in accounts]
    candidate_rows = run_parallel_flatten(tasks=tasks, api_name="google_ads")

    result: dict[tuple[str, str], str | None] = {}
    for row in candidate_rows:
        if not isinstance(row, dict):
            continue
        customer_id = str(row.get("customerId", "")).strip()
        budget_id = str(row.get("budgetId", "")).strip()
        if not customer_id or not budget_id:
            continue
        key = (customer_id, budget_id)
        if key in result:
            continue
        ad_type = str(row.get("adTypeCode", "")).strip().upper()
        result[key] = ad_type or None

    return result


# =====================
# SPEND
# =====================


def _resolve_period(
    month: int | None,
    year: int | None,
) -> dict:
    if month is None or year is None:
        return get_current_period()
    start_date = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = date(year, month, last_day)
    return {
        "year": year,
        "month": month,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }


def get_ggad_spent(
    customer_id: str,
    month: int | None = None,
    year: int | None = None,
) -> list[dict]:
    """
    Get campaign spend for a single Google Ads account for the selected period.
    """
    period = _resolve_period(month, year)
    client = get_client()
    ga_service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
          segments.year,
          segments.month,
          campaign.id,
          metrics.cost_micros,
          campaign_budget.id
        FROM campaign
        WHERE campaign.status != 'REMOVED'
          AND segments.date >= '{period["start_date"]}'
          AND segments.date <= '{period["end_date"]}'
        ORDER BY
          segments.year DESC,
          segments.month DESC,
          campaign.advertising_channel_type ASC
    """

    results: list[dict] = []

    try:
        response = ga_service.search(customer_id=customer_id, query=query)

        for row in response:
            results.append(
                {
                    "year": row.segments.year,
                    "month": row.segments.month,
                    "campaignId": str(row.campaign.id),
                    "budgetId": (
                        str(row.campaign_budget.id) if row.campaign_budget.id else None
                    ),
                    "cost": row.metrics.cost_micros / 1_000_000,
                }
            )

    except GoogleAdsException as ex:
        raise RuntimeError(f"Google Ads API error: {ex.failure}") from ex

    return results


def get_ggad_spents(
    accounts: list[dict],
    month: int | None = None,
    year: int | None = None,
) -> list[dict]:
    """
    Get campaign spend for multiple Google Ads accounts (parallelized).
    """

    def per_account_func(account: dict) -> list[dict]:
        spents = get_ggad_spent(
            account["id"],
            month=month,
            year=year,
        )
        return [
            {
                "customerId": account["id"],
                "accountCode": standardize_account_code(account.get("accountCode")),
                "accountName": account.get("accountName"),
                **s,
            }
            for s in spents
        ]

    tasks = [(per_account_func, (account,)) for account in accounts]

    return run_parallel_flatten(tasks=tasks, api_name="google_ads")


# =====================
# VALIDATION
# =====================


def validate_updates(
    *,
    customer_id: str,
    updates: list[dict],
    mode: str,  # "campaign_status" | "budget"
) -> tuple[list[dict], list[dict]]:
    """
    Row-level validation and filtering.

    Returns:
        valid_updates, invalid_updates
    """
    if not updates:
        raise ValueError("No updates provided")

    valid: list[dict] = []
    invalid: list[dict] = []

    # =====================
    # CAMPAIGN STATUS
    # =====================
    if mode == "campaign_status":
        seen_ids = set()
        paused_count = 0
        inactive_prefixes = get_google_ads_inactive_prefixes()

        for r in updates:
            try:
                if _is_zzz_name(
                    r.get("campaignName"),
                    inactive_prefixes=inactive_prefixes,
                ):
                    continue
                if "campaignId" not in r:
                    raise ValueError("Missing campaignId")

                status_value = r.get("newStatus", r.get("status"))
                if not status_value:
                    raise ValueError("Missing newStatus or status")

                campaign_id = r["campaignId"]
                status = str(status_value).upper()

                if campaign_id in seen_ids:
                    raise ValueError("Duplicate campaignId")

                if status not in GGADS_ALLOWED_CAMPAIGN_STATUSES:
                    raise ValueError(f"Invalid status: {status}")

                if status == "PAUSED":
                    paused_count += 1
                    if paused_count > GGADS_MAX_PAUSED_CAMPAIGNS:
                        raise ValueError("Pause-all protection triggered")

                seen_ids.add(campaign_id)
                valid.append(r)

            except Exception as e:
                invalid.append({**r, "error": str(e)})

    # =====================
    # BUDGET
    # =====================
    elif mode == "budget":
        seen_ids = set()

        for r in updates:
            try:
                if not {"budgetId", "newAmount", "currentAmount"}.issubset(r):
                    raise ValueError("Missing budgetId, currentAmount, or newAmount")

                budget_id = r["budgetId"]
                new_amount = r["newAmount"]
                current_amount = r["currentAmount"]
                remaining_budget = r.get("remainingBudget")
                days_left = r.get("daysLeft")

                if budget_id in seen_ids:
                    raise ValueError("Duplicate budgetId")

                # ✅ BLOCK UPWARD spikes ONLY (allow within expected daily budget)
                expected_daily = None
                if remaining_budget is not None and days_left is not None:
                    try:
                        days_left_dec = Decimal(str(days_left))
                        if days_left_dec > 0:
                            expected_daily = (
                                Decimal(str(remaining_budget)) / days_left_dec
                            ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    except Exception:
                        expected_daily = None

                if new_amount > current_amount * GGADS_MAX_BUDGET_MULTIPLIER:
                    if expected_daily is not None:
                        new_amount_dec = Decimal(str(new_amount)).quantize(
                            Decimal("0.01"), rounding=ROUND_HALF_UP
                        )
                        if new_amount_dec <= expected_daily:
                            r["validationWarning"] = (
                                "Budget spike exceeds the allowed multiplier; "
                                "update allowed within expected daily budget."
                            )
                            r["expectedDaily"] = float(expected_daily)
                        else:
                            raise ValueError("Budget spike exceeds allowed multiplier")
                    else:
                        raise ValueError("Budget spike exceeds allowed multiplier")

                seen_ids.add(budget_id)
                valid.append(r)

            except Exception as e:
                invalid.append({**r, "error": str(e)})

    else:
        raise ValueError(f"Unknown validation mode: {mode}")

    return valid, invalid


# =====================
# UPDATE BUDGETS
# =====================


def update_budgets(
    *,
    customer_id: str,
    updates: list[dict],
) -> dict:
    """
    Params:
        customer_id: str
        updates: [
            { budgetId, currentAmount, newAmount }
        ]
    """
    valid, invalid = validate_updates(
        customer_id=customer_id,
        updates=updates,
        mode="budget",
    )
    account_code = next(
        (
            standardize_account_code(r.get("accountCode"))
            for r in updates
            if standardize_account_code(r.get("accountCode"))
        ),
        None,
    )

    if not valid:
        return {
            "customerId": customer_id,
            "accountCode": account_code,
            "operation": "update_budgets",
            "summary": {
                "total": len(updates),
                "succeeded": 0,
                "failed": len(invalid),
            },
            "successes": [],
            "failures": invalid,
        }

    client = get_client()
    service = client.get_service("CampaignBudgetService")

    successes: list[dict] = []
    failures = invalid.copy()
    warnings: list[dict] = []

    for chunk in _chunked(valid, GGADS_MAX_UPDATES_PER_REQUEST):
        operations = []

        for r in chunk:
            new_amount = r["newAmount"]
            if new_amount <= 0:
                new_amount = GGADS_MIN_BUDGET

            op = client.get_type("CampaignBudgetOperation")
            budget = op.update

            budget.resource_name = service.campaign_budget_path(
                customer_id,
                r["budgetId"],
            )
            # Quantize to cents to match Google Ads minimum money unit and avoid float drift.
            quantized_amount = Decimal(str(new_amount)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            budget.amount_micros = int(
                (quantized_amount * Decimal("1000000")).to_integral_value(
                    rounding=ROUND_HALF_UP
                )
            )

            op.update_mask.CopyFrom(FieldMask(paths=["amount_micros"]))

            operations.append(op)

        # -------- v22 request object --------
        request = client.get_type("MutateCampaignBudgetsRequest")
        request.customer_id = customer_id
        request.operations.extend(operations)
        request.partial_failure = True

        response = service.mutate_campaign_budgets(request=request)

        successful_indices = set(range(len(chunk)))

        # -------- parse partial failure (v22-safe) --------
        if response.partial_failure_error:
            failure_pb_cls = GoogleAdsFailure.pb()
            failure_pb = failure_pb_cls()

            for detail in response.partial_failure_error.details:
                if detail.Is(failure_pb_cls.DESCRIPTOR):
                    detail.Unpack(failure_pb)

            for err in failure_pb.errors:
                idx = err.location.field_path_elements[0].index
                successful_indices.discard(idx)
                failures.append(
                    {
                        "budgetId": chunk[idx]["budgetId"],
                        "oldAmount": chunk[idx].get("currentAmount"),
                        "newAmount": chunk[idx].get("newAmount"),
                        "error": err.message,
                    }
                )

        for i in successful_indices:
            warning = chunk[i].get("validationWarning")
            if warning:
                warnings.append(
                    {
                        "budgetId": chunk[i].get("budgetId"),
                        "accountCode": standardize_account_code(
                            chunk[i].get("accountCode")
                        ),
                        "campaignNames": chunk[i].get("campaignNames", []),
                        "currentAmount": chunk[i].get("currentAmount"),
                        "newAmount": chunk[i].get("newAmount"),
                        "expectedDaily": chunk[i].get("expectedDaily"),
                        "error": warning,
                    }
                )
            else:
                successes.append(
                    {
                        "budgetId": chunk[i]["budgetId"],
                        "campaignNames": chunk[i].get("campaignNames", []),
                        "oldAmount": chunk[i].get("currentAmount"),
                        "newAmount": max(chunk[i]["newAmount"], GGADS_MIN_BUDGET),
                    }
                )

    return {
        "customerId": customer_id,
        "accountCode": account_code,
        "operation": "update_budgets",
        "summary": {
            "total": len(updates),
            "succeeded": len(successes),
            "failed": len(failures),
            "warnings": len(warnings),
        },
        "successes": successes,
        "failures": failures,
        "warnings": warnings,
    }


# =====================
# UPDATE CAMPAIGN STATUSES
# =====================


def update_campaign_statuses(
    *,
    customer_id: str,
    updates: list[dict],
) -> dict:
    """
    Params:
        customer_id: str
        updates: [
            { campaignId, status }
        ]
    """
    inactive_prefixes = get_google_ads_inactive_prefixes()
    filtered_updates = [
        u
        for u in updates
        if not _is_zzz_name(
            u.get("campaignName"),
            inactive_prefixes=inactive_prefixes,
        )
    ]
    if not filtered_updates:
        account_code = next(
            (
                standardize_account_code(r.get("accountCode"))
                for r in updates
                if standardize_account_code(r.get("accountCode"))
            ),
            None,
        )
        return {
            "customerId": customer_id,
            "accountCode": account_code,
            "operation": "update_campaign_statuses",
            "summary": {
                "total": 0,
                "succeeded": 0,
                "failed": 0,
            },
            "successes": [],
            "failures": [],
        }

    valid, invalid = validate_updates(
        customer_id=customer_id,
        updates=filtered_updates,
        mode="campaign_status",
    )
    account_code = next(
        (
            standardize_account_code(r.get("accountCode"))
            for r in filtered_updates
            if standardize_account_code(r.get("accountCode"))
        ),
        None,
    )

    if not valid:
        return {
            "customerId": customer_id,
            "accountCode": account_code,
            "operation": "update_campaign_statuses",
            "summary": {
                "total": len(filtered_updates),
                "succeeded": 0,
                "failed": len(invalid),
            },
            "successes": [],
            "failures": invalid,
        }

    client = get_client()
    service = client.get_service("CampaignService")

    successes: list[dict] = []
    failures = invalid.copy()

    for chunk in _chunked(valid, GGADS_MAX_UPDATES_PER_REQUEST):
        operations = []

        for r in chunk:
            op = client.get_type("CampaignOperation")
            campaign = op.update

            campaign.resource_name = service.campaign_path(
                customer_id,
                r["campaignId"],
            )
            new_status_value = r.get("newStatus", r.get("status"))
            campaign.status = (
                client.enums.CampaignStatusEnum.ENABLED
                if str(new_status_value).upper() == "ENABLED"
                else client.enums.CampaignStatusEnum.PAUSED
            )

            op.update_mask.CopyFrom(FieldMask(paths=["status"]))

            operations.append(op)

        # -------- campaigns are atomic --------
        request = client.get_type("MutateCampaignsRequest")
        request.customer_id = customer_id
        request.operations.extend(operations)

        try:
            service.mutate_campaigns(request=request)

            successes.extend(
                {
                    "campaignId": r["campaignId"],
                    "oldStatus": r.get("oldStatus"),
                    "newStatus": r.get("newStatus", r.get("status")),
                }
                for r in chunk
            )

        except GoogleAdsException as ex:
            failures.extend(
                {
                    "campaignId": r["campaignId"],
                    "oldStatus": r.get("oldStatus"),
                    "newStatus": r.get("newStatus", r.get("status")),
                    "error": str(ex),
                }
                for r in chunk
            )

    return {
        "customerId": customer_id,
        "accountCode": account_code,
        "operation": "update_campaign_statuses",
        "summary": {
            "total": len(filtered_updates),
            "succeeded": len(successes),
            "failed": len(failures),
        },
        "successes": successes,
        "failures": failures,
    }
