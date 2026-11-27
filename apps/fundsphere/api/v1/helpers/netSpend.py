from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Iterable


NET_SPEND_HEADER = [
    "accountCode",
    "accountName",
    "serviceId",
    "serviceName",
    "subService",
    "departmentName",
    "budgetId",
    "grossAmount",
    "commission",
    "netAdjustment",
    "month",
    "year",
]

COMBINED_HEADER = NET_SPEND_HEADER + ["Value", "serviceGroup", "groupOrder"]


def _to_decimal(value: object, default: Decimal = Decimal("0")) -> Decimal:
    if value is None or value == "":
        return default
    if isinstance(value, Decimal):
        return value

    cleaned = str(value).strip().replace(",", "").replace("$", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return default


def _normalize_cell(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def _group_order(value: object) -> int:
    try:
        return int(Decimal(str(value or "0")))
    except (InvalidOperation, ValueError):
        return 999


def _service_group(value: object) -> str:
    return str(value or "").strip()


def parse_service_catalog(rows: Iterable[Iterable[object]]) -> list[dict[str, object]]:
    """Parse the configured A:D service catalog used by combined data."""
    services: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        values = list(row)
        if len(values) < 4:
            continue
        service_id = str(values[0] or "").strip()
        service_name = str(values[1] or "").strip()
        service_group = _service_group(values[2])
        if not service_id or not service_name or not service_group:
            continue
        if service_id.casefold() in seen:
            continue
        seen.add(service_id.casefold())
        services.append(
            {
                "serviceId": service_id,
                "serviceName": service_name,
                "serviceGroup": service_group,
                "groupOrder": _group_order(values[3]),
            }
        )
    return sorted(
        services,
        key=lambda item: (
            int(item["groupOrder"]),
            str(item["serviceName"]).casefold(),
        ),
    )


def parse_service_grouping_config(value: object) -> list[dict[str, object]]:
    """Parse tenant YAML service groups into the combined-data catalog shape."""
    groups = value
    if not isinstance(groups, list):
        return []

    services: list[dict[str, object]] = []
    fallback_order = 0
    for group_entry in groups:
        if not isinstance(group_entry, dict):
            continue
        group_name = str(group_entry.get("group") or "").strip()
        group_services = group_entry.get("services")
        if not group_name or not isinstance(group_services, list):
            continue

        for service_entry in group_services:
            if not isinstance(service_entry, dict):
                continue
            service_id = str(service_entry.get("id") or "").strip()
            service_name = str(service_entry.get("name") or "").strip()
            if not service_id or not service_name:
                continue
            fallback_order += 1
            services.append(
                {
                    "serviceId": service_id,
                    "serviceName": service_name,
                    "serviceGroup": group_name,
                    "groupOrder": _group_order(
                        service_entry.get("order") or fallback_order
                    ),
                }
            )

    return services


def build_net_spend_rows(
    rows: Iterable[dict[str, object]],
    *,
    service_catalog: list[dict[str, object]],
    month: int,
    year: int,
) -> list[dict[str, object]]:
    catalog_by_id = {
        str(item["serviceId"]).casefold(): item for item in service_catalog
    }
    result: list[dict[str, object]] = []
    for source in rows:
        row = dict(source)
        account_code = str(row.get("accountCode") or "").strip()
        service_id = str(row.get("serviceId") or "").strip()
        service = catalog_by_id.get(service_id.casefold())
        if not account_code or not service:
            continue

        gross = _to_decimal(row.get("grossAmount"))
        commission = _to_decimal(row.get("commission"))
        net_adjustment = _to_decimal(row.get("netAdjustment"))
        value = gross * (Decimal("1") - commission / Decimal("100")) + net_adjustment

        row.update(
            {
                "accountCode": account_code.upper(),
                "accountName": str(row.get("accountName") or "").strip(),
                "serviceId": service_id,
                "serviceName": str(service["serviceName"]),
                "subService": str(row.get("subService") or "").strip(),
                "departmentName": str(row.get("departmentName") or "").strip(),
                "budgetId": str(row.get("budgetId") or "").strip(),
                "grossAmount": gross,
                "commission": commission,
                "netAdjustment": net_adjustment,
                "month": month,
                "year": year,
                "Value": value,
                "serviceGroup": service["serviceGroup"],
                "groupOrder": service["groupOrder"],
            }
        )
        result.append(row)

    return sorted(
        result,
        key=lambda row: (
            str(row.get("accountName") or "").casefold(),
            int(row.get("groupOrder") or 999),
            str(row.get("serviceName") or "").casefold(),
            bool(str(row.get("subService") or "").strip()),
            str(row.get("subService") or "").casefold(),
        ),
    )


def _row_value(row: dict[str, object], *names: str) -> object:
    normalized = {
        str(key).strip().casefold(): value for key, value in row.items()
    }
    for name in names:
        if name.casefold() in normalized:
            return normalized[name.casefold()]
    return None


def build_rollover_rows(
    source_rows: Iterable[dict[str, object]],
    *,
    vendor_mappings: Iterable[dict[str, object]],
    month: int,
    year: int,
) -> list[dict[str, object]]:
    """Convert rollover-sheet rows into the same shape as budget rows."""
    mappings = {
        str(item.get("vendor") or "").strip().casefold(): item
        for item in vendor_mappings
        if str(item.get("vendor") or "").strip()
    }
    result: list[dict[str, object]] = []
    for source in source_rows:
        source_month = _to_decimal(_row_value(source, "month"))
        source_year = _to_decimal(_row_value(source, "year"))
        if int(source_month) != month or int(source_year) != year:
            continue

        rollable = str(_row_value(source, "rollable") or "1").strip().lower()
        if rollable in {"0", "false", "no", "n", "off"}:
            continue

        vendor = str(_row_value(source, "vendor") or "").strip()
        mapping = mappings.get(vendor.casefold())
        if not mapping:
            continue

        account_code = str(
            _row_value(source, "accountCode", "account_code") or ""
        ).strip()
        account_name = str(
            _row_value(source, "accountName", "account_name", "account name") or ""
        ).strip()
        if not account_code or not account_name:
            continue

        result.append(
            {
                "accountCode": account_code,
                "accountName": account_name,
                "serviceId": str(mapping.get("serviceid") or "").strip(),
                "serviceName": str(mapping.get("servicename") or "").strip(),
                "subService": str(_row_value(source, "note") or "").strip(),
                "departmentName": str(mapping.get("departmentname") or "").strip(),
                "budgetId": str(mapping.get("budgetid") or "").strip(),
                "grossAmount": _to_decimal(
                    _row_value(source, "roll amount to month", "amount", "grossAmount")
                ),
                "commission": _to_decimal(mapping.get("commission")),
                "netAdjustment": _to_decimal(mapping.get("netadjustment")),
                "month": month,
                "year": year,
            }
        )
    return result


def build_combined_values(rows: Iterable[dict[str, object]]) -> list[list[object]]:
    return [
        [_normalize_cell(row.get(key)) for key in COMBINED_HEADER]
        for row in rows
    ]
