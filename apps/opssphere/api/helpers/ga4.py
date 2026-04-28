from __future__ import annotations

from google.oauth2 import service_account
from googleapiclient.discovery import build

from shared.utils import resolve_secret_path

READONLY_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


def _get_credentials():
    cred_path = resolve_secret_path(
        "OPSSPHERE_GOOGLE_ACCOUNTS",
        "service-account.json",
        fallback_env_vars=("google_accounts",),
    )
    return service_account.Credentials.from_service_account_file(
        cred_path,
        scopes=READONLY_SCOPES,
    )


def _get_analytics_data_service():
    credentials = _get_credentials()
    return build(
        "analyticsdata",
        "v1beta",
        credentials=credentials,
        cache_discovery=False,
    )


def run_ga4_report(
    *,
    property_id: str,
    start_date: str,
    end_date: str,
    dimensions: list[str],
    metrics: list[str],
    limit: int | None = None,
    dimension_filter: dict[str, object] | None = None,
) -> dict[str, object]:
    if not metrics:
        raise ValueError("metrics must not be empty")

    request_body: dict[str, object] = {
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "metrics": [{"name": metric} for metric in metrics],
    }
    if dimensions:
        request_body["dimensions"] = [{"name": dimension} for dimension in dimensions]
    if limit is not None:
        request_body["limit"] = str(limit)
    if dimension_filter:
        request_body["dimensionFilter"] = dimension_filter

    service = _get_analytics_data_service()
    response = (
        service.properties()
        .runReport(
            property=f"properties/{property_id}",
            body=request_body,
        )
        .execute()
    )

    dimension_headers = [
        str(header.get("name", ""))
        for header in response.get("dimensionHeaders", [])
        if str(header.get("name", "")).strip()
    ]
    metric_headers = [
        str(header.get("name", ""))
        for header in response.get("metricHeaders", [])
        if str(header.get("name", "")).strip()
    ]

    rows: list[dict[str, object]] = []
    for row in response.get("rows", []):
        dimensions_payload = row.get("dimensionValues", [])
        metrics_payload = row.get("metricValues", [])

        row_dimensions: dict[str, object] = {}
        row_metrics: dict[str, object] = {}

        for idx, name in enumerate(dimension_headers):
            value = dimensions_payload[idx].get("value", "") if idx < len(dimensions_payload) else ""
            row_dimensions[name] = value

        for idx, name in enumerate(metric_headers):
            value = metrics_payload[idx].get("value", "") if idx < len(metrics_payload) else ""
            row_metrics[name] = value

        rows.append(
            {
                "dimensions": row_dimensions,
                "metrics": row_metrics,
            }
        )

    return {
        "propertyId": property_id,
        "dateRange": {"startDate": start_date, "endDate": end_date},
        "dimensionHeaders": dimension_headers,
        "metricHeaders": metric_headers,
        "rowCount": int(response.get("rowCount") or len(rows)),
        "rows": rows,
    }


def list_ga4_dimensions(
    *,
    property_id: str,
) -> list[dict[str, object]]:
    service = _get_analytics_data_service()
    metadata = (
        service.properties()
        .getMetadata(name=f"properties/{property_id}/metadata")
        .execute()
    )
    dimensions = metadata.get("dimensions", [])
    if not isinstance(dimensions, list):
        return []

    rows: list[dict[str, object]] = []
    for dim in dimensions:
        if not isinstance(dim, dict):
            continue
        api_name = str(dim.get("apiName") or "").strip()
        if not api_name:
            continue
        rows.append(
            {
                "apiName": api_name,
                "uiName": str(dim.get("uiName") or "").strip(),
                "description": str(dim.get("description") or "").strip(),
                "customDefinition": bool(dim.get("customDefinition") is True),
                "deprecatedApiNames": [
                    str(item).strip()
                    for item in (dim.get("deprecatedApiNames") or [])
                    if str(item).strip()
                ],
            }
        )

    rows.sort(key=lambda item: str(item.get("apiName") or ""))
    return rows
