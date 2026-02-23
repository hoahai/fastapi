from __future__ import annotations

from apps.spendsphere.api.v1.helpers.ggSheet import get_rollovers

_NUMERIC_KEYS = (
    "calculatedBudget",
    "budget",
    "netAmount",
    "amount",
    "rolloverAmount",
)


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def calculate_nucar_spreadsheet_budgets(
    account_codes: list[str],
    month: int,
    year: int,
) -> list[dict[str, object]]:
    """
    Parse NuCar spreadsheet rows and aggregate calculated budgets by account code.
    """
    rows = get_rollovers(
        account_codes=account_codes,
        month=month,
        year=year,
        include_unrollable=True,
    )

    aggregated: dict[str, dict[str, object]] = {}
    for row in rows:
        account_code = str(row.get("accountCode", "")).strip().upper()
        if not account_code:
            continue

        amount = None
        for key in _NUMERIC_KEYS:
            amount = _to_float(row.get(key))
            if amount is not None:
                break
        if amount is None:
            continue

        entry = aggregated.setdefault(
            account_code,
            {
                "accountCode": account_code,
                "calculatedBudget": 0.0,
                "source": "spreadsheet",
                "sourceRows": 0,
            },
        )
        entry["calculatedBudget"] = round(
            float(entry["calculatedBudget"]) + amount,
            2,
        )
        entry["sourceRows"] = int(entry["sourceRows"]) + 1

    return list(aggregated.values())
