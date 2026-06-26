from shared.auth.page_permissions_catalog import (
    list_page_catalog_for_app,
    normalize_page_keys,
    resolve_page_keys_for_api_path,
)


def test_list_page_catalog_for_app_includes_non_tradsphere_apps() -> None:
    assert [page["key"] for page in list_page_catalog_for_app(app_code="fundsphere")] == [
        "fundsphere_accounts",
    ]
    assert [page["key"] for page in list_page_catalog_for_app(app_code="leavesphere")] == [
        "leavesphere_home",
        "leavesphere_leave_management",
        "leavesphere_employees",
    ]


def test_resolve_page_keys_for_api_path_covers_app_specific_routes() -> None:
    assert resolve_page_keys_for_api_path(
        app_code="fundsphere",
        path="/api/fundsphere/v1/accounts/logo/upload",
    ) == {"fundsphere_accounts"}
    assert resolve_page_keys_for_api_path(
        app_code="shiftzy",
        path="/api/shiftzy/v1/employees",
    ) == {"shiftzy_employees"}
    assert resolve_page_keys_for_api_path(
        app_code="leavesphere",
        path="/api/leavesphere/v1/ui/my-pto/load",
    ) == {"leavesphere_home"}
    assert resolve_page_keys_for_api_path(
        app_code="leavesphere",
        path="/api/leavesphere/v1/admin/pto/setup",
    ) == {"leavesphere_leave_management"}
    assert resolve_page_keys_for_api_path(
        app_code="leavesphere",
        path="/api/leavesphere/v1/ui/employees/load",
    ) == {"leavesphere_employees"}


def test_normalize_page_keys_keeps_legacy_aliases() -> None:
    assert normalize_page_keys(
        app_code="leavesphere",
        page_keys=["leavesphere_admin_pto"],
    ) == ["leavesphere_leave_management"]
