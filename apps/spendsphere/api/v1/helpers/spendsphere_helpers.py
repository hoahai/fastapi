from fastapi import HTTPException

from apps.spendsphere.api.v1.helpers.db_queries import get_accounts


def validate_account_codes(account_codes: str | list[str] | None) -> list[dict]:
    """
    Validate accountCodes against DB.

    Rules:
    - None / ""     -> all accounts
    - "TAAA"        -> single account
    - ["TAAA","X"]  -> multiple accounts
    """

    accounts = get_accounts(account_codes)
    all_codes = {a["code"].upper() for a in accounts}

    if not account_codes:
        return accounts

    requested = [account_codes] if isinstance(account_codes, str) else account_codes

    requested_set = {c.strip().upper() for c in requested if c.strip()}
    missing = sorted(requested_set - all_codes)

    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Invalid accountCodes",
                "invalid_codes": missing,
                "valid_codes": sorted(all_codes),
            },
        )

    return accounts


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
