from __future__ import annotations

import re
from typing import TypedDict


CLIENT_BILLING_CODE_MAX_LENGTH = 20

_CLIENT_BILLING_CODE_PATTERN = re.compile(
    r"^(?P<year>\d{2})(?P<quarter>\d{2})-"
    r"(?P<accountCode>[A-Z0-9]+)-(?P<marketCode>[A-Z0-9]+)$"
)


class ClientBillingCodeParts(TypedDict):
    normalized: str
    year: int
    quarter: int
    accountCode: str
    marketCode: str


def parse_client_billing_code(
    value: object,
    *,
    field: str = "ClientBillingCode",
) -> ClientBillingCodeParts:
    text = str(value or "").strip().upper()
    if not text:
        raise ValueError(f"{field} is required")
    if len(text) > CLIENT_BILLING_CODE_MAX_LENGTH:
        raise ValueError(
            f"{field} must be <= {CLIENT_BILLING_CODE_MAX_LENGTH} characters"
        )

    match = _CLIENT_BILLING_CODE_PATTERN.fullmatch(text)
    if not match:
        raise ValueError(
            f"{field} must match YYQQ-ACCOUNTCODE-MARKETCODE "
            "(example: 2602-TAAA-AUS)"
        )

    quarter = int(match.group("quarter"))
    if quarter < 1 or quarter > 4:
        raise ValueError(f"{field} quarter must be 01-04")

    return {
        "normalized": text,
        "year": int(match.group("year")),
        "quarter": quarter,
        "accountCode": match.group("accountCode"),
        "marketCode": match.group("marketCode"),
    }

