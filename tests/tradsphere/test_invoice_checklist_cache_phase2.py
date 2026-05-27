import unittest
from decimal import Decimal
from unittest.mock import patch

from apps.tradsphere.api.v1.helpers import config as trad_config
from apps.tradsphere.api.v1.helpers import dbQueries as dq
from apps.tradsphere.api.v1.helpers import invoiceChecklists as inv
from shared import tenantDataCache


_TABLES = {
    "INVCHECKLISTS": "TradSphere_InvChecklist",
    "INVCHECKLISTSTATIONS": "TradSphere_InvChecklistStation",
    "INVCHECKLISTNOTES": "TradSphere_InvChecklistNote",
    "APPATTACHMENTS": "AppAttachment",
    "INVNOTEATTACHMENTS": "TradSphere_InvNoteAttachment",
    "ACCOUNTS": "TradSphere_Accounts",
    "MASTERACCOUNTS": "Accounts",
    "ESTNUMS": "TradSphere_EstNums",
    "DELIVERYMETHODS": "TradSphere_DeliveryMethods",
    "STATIONS": "TradSphere_Stations",
    "SCHEDULES": "TradSphere_Schedules",
    "SCHEDULESWEEKS": "TradSphere_ScheduleWeeks",
    "CONTACTS": "TradSphere_Contacts",
    "STATIONSCONTACTS": "TradSphere_StationsContacts",
}


class InvoiceChecklistCacheKeyTests(unittest.TestCase):
    def test_list_inv_checklists_builds_stable_key_with_all_inputs(self):
        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "get_db_read_cache_ttl_seconds", return_value=60
        ), patch.object(
            dq,
            "get_tenant_shared_cache_value",
            return_value=([{"id": "cid"}], True),
        ) as mock_get:
            rows = dq.list_inv_checklists(
                account_code="taaa",
                year=2026,
                month=4,
                status="OPEN",
            )

        self.assertEqual(rows, [{"id": "cid"}])
        key = mock_get.call_args.kwargs["cache_key"]
        self.assertIn("tradsphere_db_reads::inv_checklists::", key)
        self.assertIn("schema=v1", key)
        self.assertIn("account_code=TAAA", key)
        self.assertIn("year=2026", key)
        self.assertIn("month=4", key)
        self.assertIn("status=OPEN", key)

    def test_get_inv_checklist_row_builds_key_with_checklist_id(self):
        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "get_db_read_cache_ttl_seconds", return_value=60
        ), patch.object(
            dq,
            "get_tenant_shared_cache_value",
            return_value=([{"id": "abc"}], True),
        ) as mock_get:
            row = dq.get_inv_checklist_row(checklist_id="abc")

        self.assertEqual(row, {"id": "abc"})
        key = mock_get.call_args.kwargs["cache_key"]
        self.assertIn("tradsphere_db_reads::inv_checklist_row::", key)
        self.assertIn("schema=v1", key)
        self.assertIn("checklist_id=abc", key)

    def test_list_inv_checklist_stations_builds_key_with_dynamic_filters(self):
        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "get_db_read_cache_ttl_seconds", return_value=60
        ), patch.object(
            dq,
            "get_tenant_shared_cache_value",
            return_value=([{"id": 12}], True),
        ) as mock_get:
            rows = dq.list_inv_checklist_stations(
                checklist_id="cid",
                station_row_id=12,
                est_num=2042,
                station_code="kabc",
                status="MATCHED",
            )

        self.assertEqual(rows, [{"id": 12}])
        key = mock_get.call_args.kwargs["cache_key"]
        self.assertIn("tradsphere_db_reads::inv_checklist_stations::", key)
        self.assertIn("checklist_id=cid", key)
        self.assertIn("station_row_id=12", key)
        self.assertIn("est_num=2042", key)
        self.assertIn("station_code=KABC", key)
        self.assertIn("status=MATCHED", key)

    def test_station_rows_cache_uses_sorted_normalized_checklist_ids(self):
        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "get_db_read_cache_ttl_seconds", return_value=60
        ), patch.object(
            dq,
            "get_tenant_shared_cache_value",
            return_value=(None, False),
        ) as mock_get, patch.object(dq, "fetch_all", return_value=[]) as mock_fetch, patch.object(
            dq, "set_tenant_shared_cache_value", return_value=True
        ):
            rows = dq.list_inv_checklist_station_rows_for_checklists(
                checklist_ids=["b", "a", "b", "", " a "],
            )

        self.assertEqual(rows, [])
        key = mock_get.call_args.kwargs["cache_key"]
        self.assertIn("tradsphere_db_reads::inv_checklist_station_rows::", key)
        self.assertIn("checklist_ids=a,b", key)
        self.assertEqual(mock_fetch.call_args.args[1], ("a", "b"))

    def test_station_search_cache_uses_sorted_normalized_checklist_ids(self):
        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "get_db_read_cache_ttl_seconds", return_value=60
        ), patch.object(
            dq,
            "get_tenant_shared_cache_value",
            return_value=(None, False),
        ) as mock_get, patch.object(dq, "fetch_all", return_value=[]) as mock_fetch, patch.object(
            dq, "set_tenant_shared_cache_value", return_value=True
        ):
            rows = dq.list_inv_checklist_station_search_rows(
                checklist_ids=["z", "a", "z", "", " a "],
            )

        self.assertEqual(rows, [])
        key = mock_get.call_args.kwargs["cache_key"]
        self.assertIn("tradsphere_db_reads::inv_checklist_station_search::", key)
        self.assertIn("checklist_ids=a,z", key)
        self.assertEqual(mock_fetch.call_args.args[1], ("a", "z"))


class InvoiceChecklistDetailQueryTests(unittest.TestCase):
    def test_detail_query_skips_note_join_when_note_data_not_requested(self):
        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "fetch_all", return_value=[]
        ) as mock_fetch:
            rows = dq.get_inv_checklist_detail_rows(
                checklist_id="cid-1",
                include_notes=False,
            )

        self.assertEqual(rows, [])
        query = mock_fetch.call_args.args[0]
        self.assertNotIn("LEFT JOIN `TradSphere_InvChecklistNote` n", query)
        self.assertNotIn("noteId", query)
        self.assertNotIn("`TradSphere_InvNoteAttachment`", query)
        self.assertIn("ORDER BY s.id ASC", query)
        self.assertNotIn("n.id ASC", query)

    def test_detail_query_joins_notes_for_attachment_mode_without_legacy_attachment_join(self):
        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "fetch_all", return_value=[]
        ) as mock_fetch:
            rows = dq.get_inv_checklist_detail_rows(
                checklist_id="cid-1",
                include_notes=False,
                include_attachments=True,
            )

        self.assertEqual(rows, [])
        query = mock_fetch.call_args.args[0]
        self.assertIn("LEFT JOIN `TradSphere_InvChecklistNote` n", query)
        self.assertIn("n.id AS noteId", query)
        self.assertIn("ORDER BY s.id ASC, n.id ASC", query)
        self.assertNotIn("`TradSphere_InvNoteAttachment`", query)


class InvoiceChecklistTtlTests(unittest.TestCase):
    def test_db_read_ttl_keys_default_to_60_seconds(self):
        keys = [
            "db_inv_checklists_ttl_time",
            "db_inv_checklist_row_ttl_time",
            "db_inv_checklist_stations_ttl_time",
            "db_inv_checklist_station_rows_ttl_time",
            "db_inv_checklist_station_search_ttl_time",
        ]

        with patch.object(trad_config, "get_shared_cache_ttl_seconds") as mock_get:
            for key in keys:
                mock_get.return_value = 60
                value = trad_config.get_db_read_cache_ttl_seconds(key=key)
                self.assertEqual(value, 60)
                self.assertEqual(mock_get.call_args.kwargs["key"], key)
                self.assertEqual(mock_get.call_args.kwargs["default_seconds"], 60)

    def test_db_read_ttl_unknown_key_falls_back_to_default_300(self):
        with patch.object(trad_config, "get_shared_cache_ttl_seconds", return_value=300) as mock_get:
            value = trad_config.get_db_read_cache_ttl_seconds(key="db_unknown_ttl_key")

        self.assertEqual(value, 300)
        self.assertEqual(mock_get.call_args.kwargs["default_seconds"], 300)

    def test_shared_ttl_prefers_specific_key_then_ttl_time_then_default(self):
        with patch.object(tenantDataCache, "_get_shared_cache_config", return_value={"db_inv_checklists_ttl_time": 11, "ttl_time": 22}):
            self.assertEqual(
                tenantDataCache.get_shared_cache_ttl_seconds(
                    key="db_inv_checklists_ttl_time",
                    default_seconds=60,
                    app_name="TradSphere",
                ),
                11,
            )

        with patch.object(tenantDataCache, "_get_shared_cache_config", return_value={"ttl_time": 22}):
            self.assertEqual(
                tenantDataCache.get_shared_cache_ttl_seconds(
                    key="db_inv_checklists_ttl_time",
                    default_seconds=60,
                    app_name="TradSphere",
                ),
                22,
            )

        with patch.object(tenantDataCache, "_get_shared_cache_config", return_value={}):
            self.assertEqual(
                tenantDataCache.get_shared_cache_ttl_seconds(
                    key="db_inv_checklists_ttl_time",
                    default_seconds=60,
                    app_name="TradSphere",
                ),
                60,
            )


class InvoiceChecklistInvalidationTests(unittest.TestCase):
    def test_bulk_invalidation_helper_uses_invoice_checklist_prefix(self):
        with patch.object(
            dq,
            "delete_tenant_shared_cache_values_by_prefix",
            return_value=7,
        ) as mock_delete:
            removed = dq.invalidate_inv_checklist_related_cache_for_bulk_write()

        self.assertEqual(removed, 7)
        self.assertEqual(mock_delete.call_args.kwargs["bucket"], "db_reads")
        self.assertEqual(
            mock_delete.call_args.kwargs["cache_key_prefix"],
            "tradsphere_db_reads::inv_checklist",
        )

    def test_row_invalidation_helper_uses_targeted_row_prefix(self):
        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq,
            "delete_tenant_shared_cache_values_by_prefix",
            return_value=1,
        ) as mock_delete:
            removed = dq._invalidate_inv_checklist_row_cache(checklist_ids=["cid"])

        self.assertEqual(removed, 1)
        self.assertEqual(mock_delete.call_args.kwargs["bucket"], "db_reads")
        self.assertIn(
            "tradsphere_db_reads::inv_checklist_row::schema=v1::table=`TradSphere_InvChecklist`::checklist_id=cid",
            mock_delete.call_args.kwargs["cache_key_prefix"],
        )

    def test_checklist_mutations_invalidate_expected_scopes(self):
        with patch.object(dq, "execute_many", return_value=1), patch.object(
            dq, "_invalidate_inv_checklist_list_cache"
        ) as mock_list_invalidate, patch.object(
            dq, "_invalidate_inv_checklist_row_cache"
        ) as mock_row_invalidate:
            dq.insert_inv_checklist(
                {
                    "id": "cid",
                    "accountCode": "TAAA",
                    "year": 2026,
                    "month": 4,
                    "status": "OPEN",
                    "note": "n",
                }
            )

        mock_list_invalidate.assert_called_once()
        mock_row_invalidate.assert_called_once()

        with patch.object(dq, "run_transaction", return_value=1), patch.object(
            dq, "_invalidate_inv_checklist_list_cache"
        ) as mock_list_invalidate, patch.object(
            dq, "_invalidate_inv_checklist_row_cache"
        ) as mock_row_invalidate:
            dq.update_inv_checklist(checklist_id="cid", fields={"status": "DONE"})

        mock_list_invalidate.assert_called_once()
        mock_row_invalidate.assert_called_once()

        with patch.object(dq, "run_transaction", return_value=1), patch.object(
            dq, "_invalidate_inv_checklist_all_related_scopes"
        ) as mock_all_invalidate, patch.object(
            dq, "_invalidate_inv_checklist_note_detail_cache"
        ) as mock_note_detail_invalidate:
            dq.delete_inv_checklist(checklist_id="cid")

        mock_all_invalidate.assert_called_once()
        mock_note_detail_invalidate.assert_called_once()

    def test_station_mutations_invalidate_expected_scopes(self):
        with patch.object(dq, "run_transaction", return_value=10), patch.object(
            dq, "_invalidate_inv_checklist_station_scopes"
        ) as mock_station_invalidate:
            dq.insert_inv_checklist_station(
                {
                    "checklistId": "cid",
                    "estNum": 2042,
                    "stationCode": "KABC",
                    "status": "PENDING",
                }
            )

        mock_station_invalidate.assert_called_once_with(include_checklists_scope=True)

        with patch.object(dq, "run_transaction", return_value=1), patch.object(
            dq, "_invalidate_inv_checklist_station_scopes"
        ) as mock_station_invalidate, patch.object(
            dq, "_invalidate_inv_checklist_note_detail_cache"
        ) as mock_note_detail_invalidate:
            dq.update_inv_checklist_station(station_row_id=12, fields={"status": "MATCHED"})

        mock_station_invalidate.assert_called_once_with(include_checklists_scope=False)
        mock_note_detail_invalidate.assert_called_once()

        with patch.object(dq, "run_transaction", return_value=1), patch.object(
            dq, "_invalidate_inv_checklist_station_scopes"
        ) as mock_station_invalidate, patch.object(
            dq, "_invalidate_inv_checklist_note_detail_cache"
        ) as mock_note_detail_invalidate:
            dq.delete_inv_checklist_station(station_row_id=12)

        mock_station_invalidate.assert_called_once_with(include_checklists_scope=True)
        mock_note_detail_invalidate.assert_called_once()

    def test_note_and_attachment_mutations_invalidate_note_detail_cache(self):
        with patch.object(dq, "run_transaction", return_value=1), patch.object(
            dq, "_invalidate_inv_checklist_note_detail_cache"
        ) as mock_invalidate, patch.object(
            dq,
            "get_inv_note_attachment_row",
            return_value={"attachmentSource": "legacy_inv_note_attachment"},
        ):
            dq.insert_inv_checklist_note(
                {
                    "checklistStationId": 12,
                    "amount": 1.0,
                    "note": "x",
                }
            )
            dq.update_inv_checklist_note(note_id=5, fields={"note": "y"})
            dq.delete_inv_checklist_note(note_id=5)
            dq.insert_inv_note_attachment(
                {
                    "noteId": 5,
                    "url": "https://example.com/f.pdf",
                    "fileName": "f.pdf",
                    "fileType": "application/pdf",
                }
            )
            dq.update_inv_note_attachment(attachment_id=2, fields={"fileName": "g.pdf"})
            dq.delete_inv_note_attachment(attachment_id=2)

        self.assertEqual(mock_invalidate.call_count, 6)


class InvoiceChecklistAttachmentOwnershipTests(unittest.TestCase):
    def test_get_attachment_row_queries_app_attachment_with_tenant_filter(self):
        app_attachment_columns = [
            "id",
            "appCode",
            "ownerEntityType",
            "ownerEntityId",
            "storageProvider",
            "accessUrl",
            "originalFileName",
            "mimeType",
            "fileSize",
            "storageKey",
            "providerMetadata",
            "uploadedBy",
            "tenantSlug",
            "deletedAt",
            "dateCreated",
            "dateUpdated",
        ]
        legacy_columns = [
            "id",
            "noteId",
            "url",
            "fileName",
            "fileType",
            "dateCreated",
            "dateUpdated",
        ]

        def _columns_side_effect(*, table_name_quoted):
            if "AppAttachment" in table_name_quoted:
                return app_attachment_columns
            return legacy_columns

        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "_get_table_columns", side_effect=_columns_side_effect
        ), patch.object(dq, "fetch_all", return_value=[]) as mock_fetch:
            row = dq.get_inv_note_attachment_row(
                attachment_id=21,
                tenant_slug="demo-tenant",
            )

        self.assertIsNone(row)
        queries = [str(call.args[0]) for call in mock_fetch.call_args_list]
        params_list = [call.args[1] for call in mock_fetch.call_args_list]
        joined_queries = "\n".join(queries)
        app_call_index = next((idx for idx, query in enumerate(queries) if "FROM `AppAttachment` aa" in query), -1)
        self.assertGreaterEqual(app_call_index, 0)
        params = params_list[app_call_index]
        self.assertIn("FROM `AppAttachment` aa", joined_queries)
        self.assertIn("aa.appCode = %s", joined_queries)
        self.assertIn("aa.ownerEntityType = %s", joined_queries)
        self.assertIn("LOWER(aa.tenantSlug) = %s", joined_queries)
        self.assertIn("JOIN `TradSphere_InvChecklistNote` n ON n.id = CAST(aa.ownerEntityId AS UNSIGNED)", joined_queries)
        self.assertIn("JOIN `TradSphere_InvChecklistStation` s ON s.id = n.checklistStationId", joined_queries)
        self.assertIn("JOIN `TradSphere_InvChecklist` c ON c.id = s.checklistId", joined_queries)
        self.assertEqual(params, ("tradsphere", "invoice_checklist_note", 21, "demo-tenant"))

    def test_get_attachment_row_falls_back_to_legacy_when_app_table_missing(self):
        legacy_columns = [
            "id",
            "noteId",
            "url",
            "fileName",
            "fileType",
            "dateCreated",
            "dateUpdated",
        ]

        def _columns_side_effect(*, table_name_quoted):
            if "AppAttachment" in table_name_quoted:
                raise RuntimeError("table missing")
            return legacy_columns

        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "_get_table_columns", side_effect=_columns_side_effect
        ), patch.object(dq, "fetch_all", return_value=[]) as mock_fetch:
            row = dq.get_inv_note_attachment_row(
                attachment_id=8,
                tenant_slug="demo-tenant",
            )

        self.assertIsNone(row)
        query = mock_fetch.call_args.args[0]
        params = mock_fetch.call_args.args[1]
        self.assertNotIn("FROM `AppAttachment` aa", query)
        self.assertIn("JOIN `TradSphere_InvChecklistNote` n ON n.id = a.noteId", query)
        self.assertNotIn("tenantSlug", query)
        self.assertEqual(params, (8,))

    def test_delete_attachment_soft_deletes_app_attachment(self):
        app_columns = [
            "id",
            "deletedAt",
        ]
        captured: dict[str, object] = {}

        def _fake_run_transaction(work):
            class _Cursor:
                rowcount = 1

                def execute(self, query, params):
                    captured["query"] = query
                    captured["params"] = params

            return work(_Cursor())

        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "_get_table_columns", return_value=app_columns
        ), patch.object(dq, "run_transaction", side_effect=_fake_run_transaction), patch.object(
            dq,
            "get_inv_note_attachment_row",
            return_value={"attachmentSource": "app_attachment"},
        ):
            deleted = dq.delete_inv_note_attachment(
                attachment_id=34,
                tenant_slug="demo-tenant",
            )

        self.assertEqual(deleted, 1)
        query = str(captured["query"])
        params = captured["params"]
        self.assertIn("UPDATE `AppAttachment` SET deletedAt = CURRENT_TIMESTAMP WHERE id = %s", query)
        self.assertEqual(params, (34,))

    def test_soft_delete_app_attachments_for_invoice_note_uses_owner_scope(self):
        app_columns = [
            "id",
            "appCode",
            "ownerEntityType",
            "ownerEntityId",
            "tenantSlug",
            "deletedAt",
        ]
        captured: dict[str, object] = {}

        def _fake_run_transaction(work):
            class _Cursor:
                rowcount = 2

                def execute(self, query, params):
                    captured["query"] = query
                    captured["params"] = params

            return work(_Cursor())

        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "_get_table_columns", return_value=app_columns
        ), patch.object(dq, "run_transaction", side_effect=_fake_run_transaction):
            deleted = dq.soft_delete_app_attachments_for_invoice_note(
                note_id=133,
                tenant_slug="demo-tenant",
            )

        self.assertEqual(deleted, 2)
        query = str(captured["query"])
        params = captured["params"]
        self.assertIn("UPDATE `AppAttachment` SET deletedAt = CURRENT_TIMESTAMP", query)
        self.assertIn("appCode = %s", query)
        self.assertIn("ownerEntityType = %s", query)
        self.assertIn("ownerEntityId = %s", query)
        self.assertIn("LOWER(tenantSlug) = %s", query)
        self.assertIn("deletedAt IS NULL", query)
        self.assertEqual(params, ("tradsphere", "invoice_checklist_note", "133", "demo-tenant"))


class InvoiceChecklistSyncInvalidationTests(unittest.TestCase):
    def test_sync_write_mode_invalidates_after_bulk_creates(self):
        checklist_row = {
            "id": "cid",
            "accountCode": "TAAA",
            "year": 2026,
            "month": 4,
        }

        with patch.object(
            inv,
            "_build_expected_schedule_pairs_by_account",
            return_value={"TAAA": {(2042, "KABC")}},
        ), patch.object(
            inv.uuid,
            "uuid4",
            return_value="cid",
        ), patch.object(
            inv,
            "list_invoice_checklists_data",
            side_effect=[[], [checklist_row], [checklist_row]],
        ), patch.object(
            inv,
            "insert_inv_checklists_for_sync",
            return_value=1,
        ), patch.object(
            inv,
            "list_inv_checklist_station_rows_for_checklists",
            side_effect=[
                [],
                [
                    {
                        "id": 99,
                        "checklistId": "cid",
                        "estNum": 2042,
                        "stationCode": "KABC",
                        "status": None,
                    }
                ],
            ],
        ), patch.object(
            inv,
            "insert_inv_checklist_stations_for_sync",
            return_value=1,
        ), patch.object(
            inv,
            "_build_mismatch_rows_for_period",
            return_value=[],
        ), patch.object(
            inv,
            "invalidate_inv_checklist_related_cache_for_bulk_write",
        ) as mock_invalidate, patch.object(
            inv,
            "_safe_db_call",
            side_effect=lambda func, *args, **kwargs: func(*args, **kwargs),
        ):
            result = inv.sync_invoice_checklists_for_period_data(
                year=2026,
                month=4,
                preview_only=False,
            )

        self.assertEqual(result["createdChecklistsCount"], 1)
        self.assertEqual(result["createdStationsCount"], 1)
        mock_invalidate.assert_called_once()


class InvoiceChecklistNoteDeleteAttachmentCleanupTests(unittest.TestCase):
    def test_delete_note_cleans_linked_attachments_before_note_delete(self):
        attachment_rows = [
            {
                "attachmentId": 21,
                "attachmentStorageProvider": "cloudinary",
                "attachmentStorageKey": "tradsphere/tenants/taaa/invoice-checklist-note/5/a1",
                "attachmentProviderMetadata": {"resource_type": "image"},
            }
        ]
        with patch.object(inv, "get_tenant_id", return_value="taaa"), patch.object(
            inv, "_safe_db_call", side_effect=lambda func, *args, **kwargs: func(*args, **kwargs)
        ), patch.object(
            inv, "get_inv_checklist_note_row", return_value={"id": 5}
        ), patch.object(
            inv, "list_inv_note_attachments", return_value=attachment_rows
        ) as mock_list_attachments, patch.object(
            inv, "delete_file"
        ) as mock_delete_file, patch.object(
            inv, "delete_inv_note_attachment", return_value=1
        ) as mock_delete_attachment_row, patch.object(
            inv, "delete_inv_checklist_note", return_value=1
        ) as mock_delete_note:
            result = inv.delete_invoice_checklist_note_data(note_id=5)

        self.assertEqual(result, {"deleted": 1, "id": 5})
        mock_list_attachments.assert_called_once_with(note_id=5, tenant_slug="taaa")
        mock_delete_file.assert_called_once()
        mock_delete_attachment_row.assert_called_once_with(attachment_id=21, tenant_slug="taaa")
        mock_delete_note.assert_called_once_with(note_id=5)

    def test_delete_note_continues_when_storage_delete_fails(self):
        attachment_rows = [
            {
                "attachmentId": 21,
                "attachmentStorageProvider": "cloudinary",
                "attachmentStorageKey": "bad-key",
            }
        ]
        with patch.object(inv, "get_tenant_id", return_value="taaa"), patch.object(
            inv, "_safe_db_call", side_effect=lambda func, *args, **kwargs: func(*args, **kwargs)
        ), patch.object(
            inv, "get_inv_checklist_note_row", return_value={"id": 5}
        ), patch.object(
            inv, "list_inv_note_attachments", return_value=attachment_rows
        ), patch.object(
            inv, "delete_file", side_effect=RuntimeError("delete failed")
        ), patch.object(
            inv, "delete_inv_note_attachment", return_value=1
        ) as mock_delete_attachment_row, patch.object(
            inv, "delete_inv_checklist_note", return_value=1
        ) as mock_delete_note:
            result = inv.delete_invoice_checklist_note_data(note_id=5)

        self.assertEqual(result, {"deleted": 1, "id": 5})
        mock_delete_attachment_row.assert_called_once_with(attachment_id=21, tenant_slug="taaa")
        mock_delete_note.assert_called_once_with(note_id=5)

    def test_delete_note_runs_owner_scope_soft_delete_when_list_is_empty(self):
        with patch.object(inv, "get_tenant_id", return_value="taaa"), patch.object(
            inv, "_safe_db_call", side_effect=lambda func, *args, **kwargs: func(*args, **kwargs)
        ), patch.object(
            inv, "get_inv_checklist_note_row", return_value={"id": 5}
        ), patch.object(
            inv, "list_inv_note_attachments", return_value=[]
        ), patch.object(
            inv, "soft_delete_app_attachments_for_invoice_note", return_value=1
        ) as mock_soft_delete, patch.object(
            inv, "delete_inv_checklist_note", return_value=1
        ):
            result = inv.delete_invoice_checklist_note_data(note_id=5)

        self.assertEqual(result, {"deleted": 1, "id": 5})
        mock_soft_delete.assert_called_once_with(note_id=5, tenant_slug="taaa")


class InvoiceChecklistNoteInsertQueryTests(unittest.TestCase):
    def test_insert_note_omits_amount_column_when_not_provided(self):
        captured: dict[str, object] = {}

        def _fake_run_transaction(work):
            class _Cursor:
                lastrowid = 17

                def execute(self, query, params):
                    captured["query"] = query
                    captured["params"] = params

            return work(_Cursor())

        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "run_transaction", side_effect=_fake_run_transaction
        ), patch.object(dq, "_invalidate_inv_checklist_note_detail_cache", return_value=1):
            inserted = dq.insert_inv_checklist_note(
                {
                    "checklistStationId": 12,
                    "note": "Note only",
                }
            )

        self.assertEqual(inserted, 17)
        self.assertIn("(checklistStationId, note)", str(captured["query"]))
        self.assertEqual(captured["params"], (12, "Note only"))


class InvoiceChecklistNoteValidationTests(unittest.TestCase):
    def test_normalize_note_payload_accepts_note_without_amount(self):
        with patch.object(inv, "get_inv_checklist_station_row", return_value={"id": 12}):
            normalized = inv._normalize_note_payload(  # pylint: disable=protected-access
                {"checklistStationId": 12, "note": "Credit memo expected"}
            )

        self.assertEqual(normalized["checklistStationId"], 12)
        self.assertIsNone(normalized["amount"])
        self.assertEqual(normalized["note"], "Credit memo expected")

    def test_normalize_note_payload_accepts_amount_without_note(self):
        with patch.object(inv, "get_inv_checklist_station_row", return_value={"id": 12}):
            normalized = inv._normalize_note_payload(  # pylint: disable=protected-access
                {"checklistStationId": 12, "amount": "125.50"}
            )

        self.assertEqual(normalized["checklistStationId"], 12)
        self.assertEqual(normalized["amount"], Decimal("125.50"))
        self.assertIsNone(normalized["note"])

    def test_normalize_note_payload_rejects_when_amount_and_note_missing(self):
        with patch.object(inv, "get_inv_checklist_station_row", return_value={"id": 12}):
            with self.assertRaisesRegex(ValueError, "At least one of amount or note is required"):
                inv._normalize_note_payload({"checklistStationId": 12})  # pylint: disable=protected-access

    def test_update_note_allows_null_amount_when_existing_note_is_present(self):
        existing_row = {
            "id": 5,
            "checklistStationId": 12,
            "amount": Decimal("10.00"),
            "note": "Existing note",
        }
        updated_row = {
            "id": 5,
            "checklistStationId": 12,
            "amount": None,
            "note": "Existing note",
        }
        with patch.object(inv, "get_inv_checklist_note_row", side_effect=[existing_row, updated_row]), patch.object(
            inv, "update_inv_checklist_note", return_value=1
        ) as mock_update:
            result = inv.update_invoice_checklist_note_data(payload={"noteId": 5, "amount": None})

        self.assertEqual(result["id"], 5)
        self.assertIsNone(result["amount"])
        mock_update.assert_called_once_with(note_id=5, fields={"amount": None})

    def test_update_note_rejects_when_amount_and_note_become_empty(self):
        existing_row = {
            "id": 5,
            "checklistStationId": 12,
            "amount": None,
            "note": None,
        }
        with patch.object(inv, "get_inv_checklist_note_row", return_value=existing_row):
            with self.assertRaisesRegex(ValueError, "At least one of amount or note is required"):
                inv.update_invoice_checklist_note_data(payload={"noteId": 5, "note": "   "})


class InvoiceChecklistNoteDateSerializationTests(unittest.TestCase):
    def test_list_notes_accepts_cached_string_timestamps(self):
        rows = [
            {
                "noteId": 5,
                "checklistStationId": 12,
                "checklistId": "cid",
                "estNum": 26001,
                "stationCode": "KABC",
                "amount": 10.0,
                "note": "x",
                "dateCreated": "2026-05-20T10:00:00+07:00",
                "dateUpdated": "2026-05-20T10:01:00+07:00",
            }
        ]
        with patch.object(inv, "_safe_db_call", side_effect=lambda func, *args, **kwargs: rows):
            data = inv.list_invoice_checklist_notes_data(
                checklist_station_id=12,
                include_attachments=False,
            )

        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["dateCreated"], "2026-05-20T10:00:00+07:00")
        self.assertEqual(data[0]["dateUpdated"], "2026-05-20T10:01:00+07:00")

    def test_list_notes_with_attachments_uses_single_batched_attachment_lookup(self):
        note_rows = [
            {
                "noteId": 5,
                "checklistStationId": 12,
                "checklistId": "cid-1",
                "checklistYear": 2026,
                "checklistMonth": 5,
                "estNum": 26001,
                "stationCode": "KABC",
                "amount": 10.0,
                "note": "first",
                "dateCreated": "2026-05-20T10:00:00+07:00",
                "dateUpdated": "2026-05-20T10:01:00+07:00",
            },
            {
                "noteId": 6,
                "checklistStationId": 12,
                "checklistId": "cid-1",
                "checklistYear": 2026,
                "checklistMonth": 5,
                "estNum": 26001,
                "stationCode": "KABC",
                "amount": 12.5,
                "note": "second",
                "dateCreated": "2026-05-20T10:02:00+07:00",
                "dateUpdated": "2026-05-20T10:03:00+07:00",
            },
        ]
        attachment_rows = [
            {
                "attachmentId": 21,
                "attachmentNoteId": 5,
                "attachmentOriginalFileName": "invoice-21.pdf",
                "attachmentMimeType": "application/pdf",
                "attachmentSource": "app_attachment",
            },
            {
                "attachmentId": 22,
                "attachmentNoteId": 6,
                "attachmentFileName": "legacy-22.jpg",
                "attachmentFileType": "image/jpeg",
                "attachmentSource": "legacy_inv_note_attachment",
            },
            {
                "attachmentId": 23,
                "attachmentNoteId": 5,
                "attachmentOriginalFileName": "invoice-23.pdf",
                "attachmentMimeType": "application/pdf",
                "attachmentSource": "legacy_inv_note_attachment",
            },
        ]
        db_calls: list[object] = []

        def _safe_db_side_effect(func, *args, **kwargs):
            db_calls.append(func)
            if func is inv.list_inv_checklist_note_detail_rows:
                return note_rows
            if func is inv.list_inv_note_attachments:
                self.assertEqual(kwargs.get("note_ids"), [5, 6])
                self.assertEqual(kwargs.get("tenant_slug"), "demo-tenant")
                self.assertIsNone(kwargs.get("note_id"))
                return attachment_rows
            raise AssertionError(f"Unexpected db helper call: {getattr(func, '__name__', func)}")

        with patch.object(inv, "get_tenant_id", return_value="Demo-Tenant"), patch.object(
            inv, "_safe_db_call", side_effect=_safe_db_side_effect
        ):
            data = inv.list_invoice_checklist_notes_data(
                checklist_station_id=12,
                include_attachments=True,
            )

        self.assertEqual(len(data), 2)
        self.assertEqual([item["id"] for item in data], [5, 6])
        self.assertEqual([item["id"] for item in data[0]["attachments"]], [21, 23])
        self.assertEqual([item["source"] for item in data[0]["attachments"]], ["app_attachment", "legacy_inv_note_attachment"])
        self.assertEqual([item["id"] for item in data[1]["attachments"]], [22])
        self.assertIn("checklistStationId", data[0])
        self.assertIn("checklistId", data[0])
        self.assertEqual(db_calls.count(inv.list_inv_note_attachments), 1)

    def test_list_notes_with_attachments_returns_empty_arrays_when_none_exist(self):
        note_rows = [
            {
                "noteId": 5,
                "checklistStationId": 12,
                "checklistId": "cid-1",
                "checklistYear": 2026,
                "checklistMonth": 5,
                "estNum": 26001,
                "stationCode": "KABC",
                "amount": 10.0,
                "note": "first",
                "dateCreated": "2026-05-20T10:00:00+07:00",
                "dateUpdated": "2026-05-20T10:01:00+07:00",
            }
        ]

        def _safe_db_side_effect(func, *args, **kwargs):
            if func is inv.list_inv_checklist_note_detail_rows:
                return note_rows
            if func is inv.list_inv_note_attachments:
                self.assertEqual(kwargs.get("note_ids"), [5])
                return []
            raise AssertionError(f"Unexpected db helper call: {getattr(func, '__name__', func)}")

        with patch.object(inv, "get_tenant_id", return_value="demo-tenant"), patch.object(
            inv, "_safe_db_call", side_effect=_safe_db_side_effect
        ):
            data = inv.list_invoice_checklist_notes_data(
                checklist_station_id=12,
                include_attachments=True,
            )

        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["attachments"], [])


class InvoiceChecklistDetailAttachmentBatchingTests(unittest.TestCase):
    def test_get_checklist_detail_with_attachments_uses_single_batched_lookup(self):
        detail_rows = [
            {
                "checklistId": "cid-1",
                "checklistAccountCode": "TAAA",
                "checklistYear": 2026,
                "checklistMonth": 5,
                "checklistStatus": "OPEN",
                "checklistNote": "May",
                "checklistDateCreated": "2026-05-20T10:00:00+07:00",
                "checklistDateUpdated": "2026-05-20T10:01:00+07:00",
                "stationRowId": 12,
                "stationEstNum": 26001,
                "stationCode": "KABC",
                "stationStatus": "PENDING",
                "stationDateCreated": "2026-05-20T10:00:00+07:00",
                "stationDateUpdated": "2026-05-20T10:01:00+07:00",
                "noteId": 5,
                "noteAmount": 10.0,
                "noteText": "first",
                "noteDateCreated": "2026-05-20T10:00:00+07:00",
                "noteDateUpdated": "2026-05-20T10:01:00+07:00",
            },
            {
                "checklistId": "cid-1",
                "checklistAccountCode": "TAAA",
                "checklistYear": 2026,
                "checklistMonth": 5,
                "checklistStatus": "OPEN",
                "checklistNote": "May",
                "checklistDateCreated": "2026-05-20T10:00:00+07:00",
                "checklistDateUpdated": "2026-05-20T10:01:00+07:00",
                "stationRowId": 12,
                "stationEstNum": 26001,
                "stationCode": "KABC",
                "stationStatus": "PENDING",
                "stationDateCreated": "2026-05-20T10:00:00+07:00",
                "stationDateUpdated": "2026-05-20T10:01:00+07:00",
                "noteId": 6,
                "noteAmount": 12.5,
                "noteText": "second",
                "noteDateCreated": "2026-05-20T10:02:00+07:00",
                "noteDateUpdated": "2026-05-20T10:03:00+07:00",
            },
        ]
        attachment_rows = [
            {
                "attachmentId": 21,
                "attachmentNoteId": 5,
                "attachmentOriginalFileName": "invoice-21.pdf",
                "attachmentMimeType": "application/pdf",
                "attachmentSource": "app_attachment",
            },
            {
                "attachmentId": 22,
                "attachmentNoteId": 6,
                "attachmentFileName": "legacy-22.jpg",
                "attachmentFileType": "image/jpeg",
                "attachmentSource": "legacy_inv_note_attachment",
            },
            {
                "attachmentId": 23,
                "attachmentNoteId": 5,
                "attachmentOriginalFileName": "invoice-23.pdf",
                "attachmentMimeType": "application/pdf",
                "attachmentSource": "legacy_inv_note_attachment",
            },
        ]
        db_calls: list[object] = []

        def _safe_db_side_effect(func, *args, **kwargs):
            db_calls.append(func)
            if func is inv.get_inv_checklist_detail_rows:
                self.assertEqual(kwargs.get("checklist_id"), "cid-1")
                self.assertTrue(kwargs.get("include_notes"))
                self.assertTrue(kwargs.get("include_attachments"))
                return detail_rows
            if func is inv.list_inv_note_attachments:
                self.assertEqual(kwargs.get("note_ids"), [5, 6])
                self.assertEqual(kwargs.get("tenant_slug"), "demo-tenant")
                self.assertIsNone(kwargs.get("note_id"))
                return attachment_rows
            raise AssertionError(f"Unexpected db helper call: {getattr(func, '__name__', func)}")

        with patch.object(inv, "get_tenant_id", return_value="Demo-Tenant"), patch.object(
            inv, "_safe_db_call", side_effect=_safe_db_side_effect
        ):
            data = inv.get_invoice_checklists_data(
                checklist_id="cid-1",
                include_stations=True,
                include_notes=True,
                include_attachments=True,
            )

        stations = data["stations"]
        notes = stations[0]["notes"]
        self.assertEqual(data["id"], "cid-1")
        self.assertEqual([note["id"] for note in notes], [5, 6])
        self.assertEqual([item["id"] for item in notes[0]["attachments"]], [21, 23])
        self.assertEqual([item["source"] for item in notes[0]["attachments"]], ["app_attachment", "legacy_inv_note_attachment"])
        self.assertEqual([item["id"] for item in notes[1]["attachments"]], [22])
        self.assertEqual(db_calls.count(inv.list_inv_note_attachments), 1)


class InvoiceChecklistAttachmentStorageKeyTests(unittest.TestCase):
    def test_build_storage_key_uses_note_estnum_station_and_counter(self):
        key = inv._build_invoice_note_storage_key(
            note_id=15,
            est_num=2001,
            station_code="K-ABC",
            counter=3,
        )
        self.assertEqual(key, "15_2001_kabc_3")


class InvoiceChecklistStoredAttachmentFileNameTests(unittest.TestCase):
    def test_build_stored_attachment_file_name_uses_public_id_and_format(self):
        file_name = inv._build_stored_attachment_file_name(  # pylint: disable=protected-access
            provider_public_id="tradsphere/tenants/taaa/invoice-checklist-note/15_2001_kabc_3",
            provider_metadata={"format": "png"},
            mime_type="image/png",
            fallback_name="image.png",
        )
        self.assertEqual(file_name, "15_2001_kabc_3.png")

    def test_build_stored_attachment_file_name_keeps_existing_extension(self):
        file_name = inv._build_stored_attachment_file_name(  # pylint: disable=protected-access
            provider_public_id="tradsphere/tenants/taaa/invoice-checklist-note/15_2001_kabc_3.webp",
            provider_metadata={"format": "png"},
            mime_type="image/png",
            fallback_name="image.png",
        )
        self.assertEqual(file_name, "15_2001_kabc_3.webp")


class InvoiceChecklistBulkSaveFlowTests(unittest.TestCase):
    def test_bulk_save_returns_id_mappings_for_local_checklist_station_and_note(self):
        saved_kwargs: dict[str, object] = {}

        def _safe_db_side_effect(func, *args, **kwargs):
            if func is inv.save_inv_checklist_bulk_changes:
                saved_kwargs.update(kwargs)
                return {
                    "checklistIds": {"local-checklist-1": "server-checklist-1"},
                    "stationIds": {"-10": 910},
                    "noteIds": {"-20": 920},
                }
            raise AssertionError(f"Unexpected db helper call: {getattr(func, '__name__', func)}")

        payload = {
            "year": 2026,
            "month": 4,
            "selectedChecklistId": "local-checklist-1",
            "createChecklists": [
                {
                    "clientChecklistId": "local-checklist-1",
                    "accountCode": "taaa",
                    "year": 2026,
                    "month": 4,
                    "status": "OPEN",
                    "note": "April review",
                }
            ],
            "checklistUpdates": [],
            "deleteChecklistIds": [],
            "createStations": [
                {
                    "clientStationId": "-10",
                    "checklistClientId": "local-checklist-1",
                    "estNum": 2042,
                    "stationCode": "kabc",
                    "status": "PENDING",
                }
            ],
            "stationUpdates": [],
            "deleteStationIds": [],
            "createNotes": [
                {
                    "clientNoteId": "-20",
                    "checklistStationClientId": "-10",
                    "amount": "125.50",
                    "note": "Credit memo expected",
                }
            ],
            "noteUpdates": [],
            "deleteNoteIds": [],
        }

        with patch.object(inv.uuid, "uuid4", return_value="server-checklist-1"), patch.object(
            inv, "ensure_master_account_codes_exist", return_value=None
        ), patch.object(
            inv, "ensure_est_nums_exist", return_value=None
        ), patch.object(
            inv, "ensure_station_codes_exist", return_value=None
        ), patch.object(
            inv, "_safe_db_call", side_effect=_safe_db_side_effect
        ), patch.object(
            inv,
            "get_invoice_checklists_data",
            return_value={"id": "server-checklist-1", "stations": [], "status": "OPEN", "note": "April review"},
        ), patch.object(
            inv,
            "list_invoice_checklists_data",
            return_value=[{"id": "server-checklist-1", "accountCode": "TAAA", "year": 2026, "month": 4}],
        ):
            result = inv.bulk_save_invoice_checklists_data(payload=payload)

        self.assertEqual(result["selectedChecklistId"], "server-checklist-1")
        self.assertEqual(result["mappings"]["checklistIds"], {"local-checklist-1": "server-checklist-1"})
        self.assertEqual(result["mappings"]["stationIds"], {"-10": 910})
        self.assertEqual(result["mappings"]["noteIds"], {"-20": 920})
        self.assertEqual(result["summary"]["createdChecklistsCount"], 1)
        self.assertEqual(result["summary"]["createdStationsCount"], 1)
        self.assertEqual(result["summary"]["createdNotesCount"], 1)
        checklist_creates = saved_kwargs.get("checklist_creates") or []
        station_creates = saved_kwargs.get("station_creates") or []
        note_creates = saved_kwargs.get("note_creates") or []
        self.assertEqual(checklist_creates[0]["id"], "server-checklist-1")
        self.assertEqual(station_creates[0]["checklistId"], "server-checklist-1")
        self.assertEqual(note_creates[0]["checklistStationClientId"], "-10")

    def test_bulk_save_combines_create_update_delete_groups(self):
        save_call_kwargs: dict[str, object] = {}

        def _safe_db_side_effect(func, *args, **kwargs):
            if func is inv.list_inv_checklist_rows_by_ids:
                return [{"id": checklist_id} for checklist_id in (kwargs.get("checklist_ids") or [])]
            if func is inv.list_inv_checklist_station_rows_by_ids:
                return [{"id": station_id} for station_id in (kwargs.get("station_row_ids") or [])]
            if func is inv.list_inv_checklist_note_rows_by_ids:
                return [{"id": note_id, "note": "existing"} for note_id in (kwargs.get("note_ids") or [])]
            if func is inv.save_inv_checklist_bulk_changes:
                save_call_kwargs.update(kwargs)
                return {"checklistIds": {}, "stationIds": {}, "noteIds": {}}
            raise AssertionError(f"Unexpected db helper call: {getattr(func, '__name__', func)}")

        payload = {
            "year": 2026,
            "month": 4,
            "selectedChecklistId": "checklist-a",
            "createChecklists": [],
            "checklistUpdates": [{"checklistId": "checklist-a", "status": "DONE"}],
            "deleteChecklistIds": ["checklist-z", "checklist-z"],
            "createStations": [],
            "stationUpdates": [{"stationRowId": 10, "status": "MATCHED"}],
            "deleteStationIds": [9, 9],
            "createNotes": [],
            "noteUpdates": [{"noteId": 8, "note": "Updated"}],
            "deleteNoteIds": [7, 7],
        }

        with patch.object(inv, "ensure_master_account_codes_exist", return_value=None), patch.object(
            inv, "ensure_est_nums_exist", return_value=None
        ), patch.object(
            inv, "ensure_station_codes_exist", return_value=None
        ), patch.object(
            inv, "_safe_db_call", side_effect=_safe_db_side_effect
        ), patch.object(
            inv,
            "get_invoice_checklists_data",
            return_value={"id": "checklist-a", "stations": [], "status": "DONE", "note": ""},
        ), patch.object(inv, "list_invoice_checklists_data", return_value=[]):
            result = inv.bulk_save_invoice_checklists_data(payload=payload)

        self.assertEqual(result["deleted"]["checklistIds"], ["checklist-z"])
        self.assertEqual(result["deleted"]["stationIds"], [9])
        self.assertEqual(result["deleted"]["noteIds"], [7])
        self.assertEqual(result["summary"]["updatedChecklistsCount"], 1)
        self.assertEqual(result["summary"]["updatedStationsCount"], 1)
        self.assertEqual(result["summary"]["updatedNotesCount"], 1)
        self.assertEqual(save_call_kwargs["checklist_deletes"], ["checklist-z"])
        self.assertEqual(save_call_kwargs["station_deletes"], [9])
        self.assertEqual(save_call_kwargs["note_deletes"], [7])
        self.assertEqual(len(save_call_kwargs["checklist_updates"]), 1)
        self.assertEqual(len(save_call_kwargs["station_updates"]), 1)
        self.assertEqual(len(save_call_kwargs["note_updates"]), 1)

    def test_bulk_save_ignores_missing_checklist_delete_before_write(self):
        save_call_kwargs: dict = {}

        def _safe_db_side_effect(func, *args, **kwargs):
            if func is inv.list_inv_checklist_rows_by_ids:
                return []
            if func is inv.save_inv_checklist_bulk_changes:
                save_call_kwargs.update(kwargs)
                return {"checklistIds": {}, "stationIds": {}, "noteIds": {}}
            raise AssertionError(f"Unexpected db helper call: {getattr(func, '__name__', func)}")

        payload = {
            "year": 2026,
            "month": 4,
            "createChecklists": [],
            "checklistUpdates": [],
            "deleteChecklistIds": ["missing-checklist-id"],
            "createStations": [],
            "stationUpdates": [],
            "deleteStationIds": [],
            "createNotes": [],
            "noteUpdates": [],
            "deleteNoteIds": [],
        }

        with patch.object(inv, "_safe_db_call", side_effect=_safe_db_side_effect), patch.object(
            inv, "list_invoice_checklists_data", return_value=[]
        ):
            result = inv.bulk_save_invoice_checklists_data(payload=payload)

        self.assertEqual(result["deleted"]["checklistIds"], [])
        self.assertEqual(result["summary"]["deletedChecklistsCount"], 0)
        self.assertEqual(save_call_kwargs["checklist_deletes"], [])

    def test_bulk_save_rejects_missing_checklist_update_before_write(self):
        def _safe_db_side_effect(func, *args, **kwargs):
            if func is inv.list_inv_checklist_rows_by_ids:
                return []
            if func is inv.save_inv_checklist_bulk_changes:
                raise AssertionError("save_inv_checklist_bulk_changes must not run for invalid payload")
            raise AssertionError(f"Unexpected db helper call: {getattr(func, '__name__', func)}")

        payload = {
            "year": 2026,
            "month": 4,
            "createChecklists": [],
            "checklistUpdates": [{"checklistId": "missing-checklist-id", "status": "DONE"}],
            "deleteChecklistIds": [],
            "createStations": [],
            "stationUpdates": [],
            "deleteStationIds": [],
            "createNotes": [],
            "noteUpdates": [],
            "deleteNoteIds": [],
        }

        with patch.object(inv, "_safe_db_call", side_effect=_safe_db_side_effect):
            with self.assertRaisesRegex(inv.NotFoundError, "Checklist not found: missing-checklist-id"):
                inv.bulk_save_invoice_checklists_data(payload=payload)

    def test_bulk_save_rejects_missing_station_before_write(self):
        def _safe_db_side_effect(func, *args, **kwargs):
            if func is inv.list_inv_checklist_station_rows_by_ids:
                return []
            if func is inv.save_inv_checklist_bulk_changes:
                raise AssertionError("save_inv_checklist_bulk_changes must not run for invalid payload")
            raise AssertionError(f"Unexpected db helper call: {getattr(func, '__name__', func)}")

        payload = {
            "year": 2026,
            "month": 4,
            "createChecklists": [],
            "checklistUpdates": [],
            "deleteChecklistIds": [],
            "createStations": [],
            "stationUpdates": [],
            "deleteStationIds": [9991],
            "createNotes": [],
            "noteUpdates": [],
            "deleteNoteIds": [],
        }

        with patch.object(inv, "_safe_db_call", side_effect=_safe_db_side_effect):
            with self.assertRaisesRegex(inv.NotFoundError, "Checklist station not found: 9991"):
                inv.bulk_save_invoice_checklists_data(payload=payload)

    def test_bulk_save_rejects_missing_note_before_write(self):
        def _safe_db_side_effect(func, *args, **kwargs):
            if func is inv.list_inv_checklist_note_rows_by_ids:
                return []
            if func is inv.save_inv_checklist_bulk_changes:
                raise AssertionError("save_inv_checklist_bulk_changes must not run for invalid payload")
            raise AssertionError(f"Unexpected db helper call: {getattr(func, '__name__', func)}")

        payload = {
            "year": 2026,
            "month": 4,
            "createChecklists": [],
            "checklistUpdates": [],
            "deleteChecklistIds": [],
            "createStations": [],
            "stationUpdates": [],
            "deleteStationIds": [],
            "createNotes": [],
            "noteUpdates": [],
            "deleteNoteIds": [9992],
        }

        with patch.object(inv, "_safe_db_call", side_effect=_safe_db_side_effect):
            with self.assertRaisesRegex(inv.NotFoundError, "Checklist note not found: 9992"):
                inv.bulk_save_invoice_checklists_data(payload=payload)


class InvoiceChecklistUiLoadRecoveryTests(unittest.TestCase):
    def test_ui_load_recovers_from_stale_selected_checklist_id(self):
        stale_rows = [
            {
                "id": "stale-id",
                "accountCode": "TAAA",
                "year": 2026,
                "month": 5,
                "status": "OPEN",
                "note": "",
                "dateCreated": None,
                "dateUpdated": None,
                "stationCount": 0,
            }
        ]
        fresh_rows = [
            {
                "id": "fresh-id",
                "accountCode": "TAAA",
                "year": 2026,
                "month": 5,
                "status": "OPEN",
                "note": "",
                "dateCreated": None,
                "dateUpdated": None,
                "stationCount": 0,
            }
        ]
        stale_phase = {"enabled": True}

        def _list_side_effect(*, account_code=None, year=None, month=None, status=None):
            del account_code, status
            if year == 2026 and month == 5:
                return stale_rows if stale_phase["enabled"] else fresh_rows
            if year is None and month is None:
                return stale_rows if stale_phase["enabled"] else fresh_rows
            return []

        def _detail_side_effect(
            *,
            checklist_id=None,
            account_code=None,
            year=None,
            month=None,
            status=None,
            include_stations=False,
            include_notes=False,
            include_attachments=False,
        ):
            del account_code, year, month, status, include_stations, include_notes, include_attachments
            if checklist_id == "stale-id":
                raise inv.NotFoundError("Checklist not found: stale-id")
            if checklist_id == "fresh-id":
                return {
                    "id": "fresh-id",
                    "accountCode": "TAAA",
                    "year": 2026,
                    "month": 5,
                    "status": "OPEN",
                    "note": "",
                    "stations": [],
                }
            raise AssertionError(f"Unexpected checklist_id: {checklist_id}")

        def _invalidate_side_effect():
            stale_phase["enabled"] = False
            return 1

        with patch.object(inv, "list_invoice_checklists_data", side_effect=_list_side_effect), patch.object(
            inv, "get_invoice_checklists_data", side_effect=_detail_side_effect
        ), patch.object(
            inv, "_build_account_names_map", return_value={"TAAA": "Alpha Motors"}
        ), patch.object(
            inv, "_build_checklist_station_search_map", return_value={}
        ), patch.object(
            inv, "_build_expected_schedule_pairs_by_account", return_value={}
        ), patch.object(
            inv, "_build_mismatch_rows_for_period", return_value=[]
        ), patch.object(
            inv, "_build_station_metadata", return_value={}
        ), patch.object(
            inv, "invalidate_inv_checklist_related_cache_for_bulk_write", side_effect=_invalidate_side_effect
        ) as mock_invalidate:
            result = inv.get_invoice_checklists_ui_load_data(
                year=2026,
                month=5,
                checklist_id="stale-id",
                include_selected_detail=True,
            )

        self.assertEqual(mock_invalidate.call_count, 1)
        self.assertEqual(result["selectedChecklistId"], "fresh-id")
        self.assertEqual([row["id"] for row in result["checklists"]], ["fresh-id"])
        selected = result.get("selectedChecklist") or {}
        self.assertEqual(selected.get("id"), "fresh-id")


class InvoiceChecklistBulkSaveDbTests(unittest.TestCase):
    def test_bulk_save_db_write_failure_does_not_invalidate_caches(self):
        class _Cursor:
            def __init__(self):
                self.lastrowid = 100
                self.rowcount = 1

            def executemany(self, query, params):
                return None

            def execute(self, query, params):
                if "UPDATE `TradSphere_InvChecklist`" in str(query):
                    raise RuntimeError("forced db failure")
                return None

        def _fake_run_transaction(work):
            return work(_Cursor())

        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "_get_app_attachment_table_name", return_value="`AppAttachment`"
        ), patch.object(
            dq, "_get_app_attachment_columns", return_value=[]
        ), patch.object(
            dq, "_get_inv_note_attachment_columns", return_value=[]
        ), patch.object(
            dq, "run_transaction", side_effect=_fake_run_transaction
        ), patch.object(
            dq, "_invalidate_inv_checklist_all_related_scopes"
        ) as mock_invalidate_all, patch.object(
            dq, "_invalidate_inv_checklist_note_detail_cache"
        ) as mock_invalidate_note:
            with self.assertRaisesRegex(RuntimeError, "forced db failure"):
                dq.save_inv_checklist_bulk_changes(
                    checklist_creates=[
                        {
                            "clientChecklistId": "local-1",
                            "id": "server-1",
                            "accountCode": "TAAA",
                            "year": 2026,
                            "month": 4,
                            "status": "OPEN",
                            "note": "n",
                        }
                    ],
                    checklist_updates=[{"checklistId": "server-1", "status": "DONE"}],
                    checklist_deletes=[],
                    station_creates=[],
                    station_updates=[],
                    station_deletes=[],
                    note_creates=[],
                    note_updates=[],
                    note_deletes=[],
                    tenant_slug="demo-tenant",
                )

        mock_invalidate_all.assert_not_called()
        mock_invalidate_note.assert_not_called()

    def test_bulk_save_note_delete_scopes_app_attachment_update_by_tenant(self):
        executed: list[tuple[str, tuple[object, ...]]] = []

        class _Cursor:
            rowcount = 1
            lastrowid = 1

            def executemany(self, query, params):
                return None

            def execute(self, query, params):
                executed.append((str(query), tuple(params)))
                return None

        def _fake_run_transaction(work):
            return work(_Cursor())

        app_columns = ["appCode", "ownerEntityType", "ownerEntityId", "tenantSlug", "deletedAt"]

        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "_get_app_attachment_table_name", return_value="`AppAttachment`"
        ), patch.object(
            dq, "_get_app_attachment_columns", return_value=app_columns
        ), patch.object(
            dq, "_get_inv_note_attachment_columns", return_value=[]
        ), patch.object(
            dq, "run_transaction", side_effect=_fake_run_transaction
        ), patch.object(dq, "_invalidate_inv_checklist_all_related_scopes"), patch.object(
            dq, "_invalidate_inv_checklist_note_detail_cache"
        ):
            dq.save_inv_checklist_bulk_changes(
                checklist_creates=[],
                checklist_updates=[],
                checklist_deletes=[],
                station_creates=[],
                station_updates=[],
                station_deletes=[],
                note_creates=[],
                note_updates=[],
                note_deletes=[5],
                tenant_slug="demo-tenant",
            )

        scoped_queries = [item for item in executed if "AppAttachment" in item[0]]
        self.assertEqual(len(scoped_queries), 1)
        scoped_query, scoped_params = scoped_queries[0]
        self.assertIn("LOWER(tenantSlug) = %s", scoped_query)
        self.assertIn("ownerEntityId IN (%s)", scoped_query)
        self.assertEqual(scoped_params, ("tradsphere", "invoice_checklist_note", "5", "demo-tenant"))


if __name__ == "__main__":
    unittest.main()
