import unittest
from unittest.mock import patch

from apps.tradsphere.api.v1.helpers import config as trad_config
from apps.tradsphere.api.v1.helpers import dbQueries as dq
from apps.tradsphere.api.v1.helpers import invoiceChecklists as inv
from shared import tenantDataCache


_TABLES = {
    "INVCHECKLISTS": "TradSphere_InvChecklist",
    "INVCHECKLISTSTATIONS": "TradSphere_InvChecklistStation",
    "INVCHECKLISTNOTES": "TradSphere_InvChecklistNote",
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
            "tradsphere_db_reads::inv_checklist_",
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
        ) as mock_invalidate:
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
    def test_get_attachment_row_enforces_tenant_and_ownership_when_columns_exist(self):
        attachment_columns = [
            "id",
            "noteId",
            "url",
            "fileName",
            "fileType",
            "dateCreated",
            "dateUpdated",
            "tenantSlug",
            "ownerEntityType",
            "ownerEntityId",
            "deletedAt",
        ]
        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "_get_table_columns", return_value=attachment_columns
        ), patch.object(dq, "fetch_all", return_value=[]) as mock_fetch:
            row = dq.get_inv_note_attachment_row(
                attachment_id=21,
                tenant_slug="demo-tenant",
            )

        self.assertIsNone(row)
        query = mock_fetch.call_args.args[0]
        params = mock_fetch.call_args.args[1]
        self.assertIn("JOIN `TradSphere_InvChecklistNote` n ON n.id = a.noteId", query)
        self.assertIn("JOIN `TradSphere_InvChecklistStation` s ON s.id = n.checklistStationId", query)
        self.assertIn("JOIN `TradSphere_InvChecklist` c ON c.id = s.checklistId", query)
        self.assertIn("a.deletedAt IS NULL", query)
        self.assertIn("a.ownerEntityType = 'invoice_checklist_note'", query)
        self.assertIn("a.ownerEntityId = CAST(n.id AS CHAR)", query)
        self.assertIn("LOWER(a.tenantSlug) = %s", query)
        self.assertEqual(params, (21, "demo-tenant"))

    def test_get_attachment_row_keeps_join_guard_without_tenant_column(self):
        legacy_columns = [
            "id",
            "noteId",
            "url",
            "fileName",
            "fileType",
            "dateCreated",
            "dateUpdated",
        ]
        with patch.object(dq, "get_db_tables", return_value=dict(_TABLES)), patch.object(
            dq, "_get_table_columns", return_value=legacy_columns
        ), patch.object(dq, "fetch_all", return_value=[]) as mock_fetch:
            row = dq.get_inv_note_attachment_row(
                attachment_id=8,
                tenant_slug="demo-tenant",
            )

        self.assertIsNone(row)
        query = mock_fetch.call_args.args[0]
        params = mock_fetch.call_args.args[1]
        self.assertIn("JOIN `TradSphere_InvChecklistNote` n ON n.id = a.noteId", query)
        self.assertNotIn("tenantSlug", query)
        self.assertEqual(params, (8,))

    def test_delete_attachment_uses_join_and_tenant_predicate(self):
        attachment_columns = [
            "id",
            "noteId",
            "url",
            "fileName",
            "fileType",
            "dateCreated",
            "dateUpdated",
            "tenantSlug",
            "ownerEntityType",
            "ownerEntityId",
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
            dq, "_get_table_columns", return_value=attachment_columns
        ), patch.object(dq, "run_transaction", side_effect=_fake_run_transaction):
            deleted = dq.delete_inv_note_attachment(
                attachment_id=34,
                tenant_slug="demo-tenant",
            )

        self.assertEqual(deleted, 1)
        query = str(captured["query"])
        params = captured["params"]
        self.assertIn("UPDATE `TradSphere_InvNoteAttachment` a", query)
        self.assertIn("JOIN `TradSphere_InvChecklistNote` n ON n.id = a.noteId", query)
        self.assertIn("LOWER(a.tenantSlug) = %s", query)
        self.assertEqual(params, (34, "demo-tenant"))


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
            inv,
            "list_invoice_checklists_data",
            side_effect=[[], [checklist_row], [checklist_row]],
        ), patch.object(
            inv,
            "create_invoice_checklist_data",
            return_value=checklist_row,
        ), patch.object(
            inv,
            "list_inv_checklist_station_rows_for_checklists",
            return_value=[],
        ), patch.object(
            inv,
            "insert_inv_checklist_station",
            return_value=99,
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


if __name__ == "__main__":
    unittest.main()
