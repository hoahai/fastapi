import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import shared.google_oauth as google_oauth
from shared.auth.supabase_client import SupabaseClientError
from shared.google_calendar import (
    create_google_calendar_event,
    delete_google_calendar_event,
    get_google_calendar_event,
    list_google_calendar_events,
    patch_google_calendar_event,
    update_google_calendar_event,
)
from shared.google_oauth import GoogleOAuthConfig, revoke_google_oauth_connection


class GoogleCalendarHelperTests(unittest.TestCase):
    def setUp(self):
        self.config = GoogleOAuthConfig(
            client_id="client-id",
            client_secret="client-secret",
            scopes=("https://www.googleapis.com/auth/calendar.events",),
            state_secret="state-secret",
            state_bucket="state-bucket",
            connection_bucket="connection-bucket",
            connection_table="google_oauth_connections",
            redirect_uri="https://example.com/callback",
        )

    def _build_service(self):
        request = Mock()
        request.execute.return_value = {"id": "evt-1"}
        events = SimpleNamespace(
            get=Mock(return_value=request),
            list=Mock(return_value=request),
            insert=Mock(return_value=request),
            update=Mock(return_value=request),
            patch=Mock(return_value=request),
            delete=Mock(return_value=request),
        )
        service = SimpleNamespace(events=Mock(return_value=events))
        return service, events, request

    @patch("shared.google_calendar.build_google_service")
    def test_create_google_calendar_event(self, build_service):
        service, events, request = self._build_service()
        build_service.return_value = service

        result = create_google_calendar_event(
            tenant_id="nucar",
            config=self.config,
            calendar_id="primary",
            event={"summary": "PTO"},
        )

        self.assertEqual(result, {"id": "evt-1"})
        events.insert.assert_called_once()
        self.assertEqual(events.insert.call_args.kwargs["calendarId"], "primary")
        self.assertEqual(events.insert.call_args.kwargs["body"], {"summary": "PTO"})
        request.execute.assert_called_once()

    @patch("shared.google_calendar.build_google_service")
    def test_update_google_calendar_event(self, build_service):
        service, events, request = self._build_service()
        build_service.return_value = service

        result = update_google_calendar_event(
            tenant_id="nucar",
            config=self.config,
            calendar_id="primary",
            event_id="evt-1",
            event={"summary": "Updated PTO"},
        )

        self.assertEqual(result, {"id": "evt-1"})
        events.update.assert_called_once()
        self.assertEqual(events.update.call_args.kwargs["eventId"], "evt-1")
        self.assertEqual(events.update.call_args.kwargs["body"], {"summary": "Updated PTO"})
        request.execute.assert_called_once()

    @patch("shared.google_calendar.build_google_service")
    def test_patch_google_calendar_event(self, build_service):
        service, events, request = self._build_service()
        build_service.return_value = service

        result = patch_google_calendar_event(
            tenant_id="nucar",
            config=self.config,
            calendar_id="primary",
            event_id="evt-1",
            event={"summary": "Updated PTO"},
        )

        self.assertEqual(result, {"id": "evt-1"})
        events.patch.assert_called_once()
        request.execute.assert_called_once()

    @patch("shared.google_calendar.build_google_service")
    def test_delete_google_calendar_event(self, build_service):
        service, events, request = self._build_service()
        build_service.return_value = service

        result = delete_google_calendar_event(
            tenant_id="nucar",
            config=self.config,
            calendar_id="primary",
            event_id="evt-1",
        )

        self.assertEqual(result, {"id": "evt-1"})
        events.delete.assert_called_once()
        self.assertEqual(events.delete.call_args.kwargs["eventId"], "evt-1")
        request.execute.assert_called_once()

    @patch("shared.google_calendar.build_google_service")
    def test_get_and_list_google_calendar_events(self, build_service):
        service, events, request = self._build_service()
        build_service.return_value = service

        result = get_google_calendar_event(
            tenant_id="nucar",
            config=self.config,
            calendar_id="primary",
            event_id="evt-1",
        )
        self.assertEqual(result, {"id": "evt-1"})

        request.execute.reset_mock()
        list_result = list_google_calendar_events(
            tenant_id="nucar",
            config=self.config,
            calendar_id="primary",
            q="PTO",
        )
        self.assertEqual(list_result, {"id": "evt-1"})
        events.list.assert_called_once()
        self.assertEqual(events.list.call_args.kwargs["q"], "PTO")

    @patch("shared.google_oauth.clear_google_oauth_connection")
    @patch("shared.google_oauth.urlopen")
    @patch("shared.google_oauth._read_connection_record")
    def test_revoke_google_oauth_connection(self, read_record, urlopen_mock, clear_connection):
        read_record.return_value = {"refreshToken": "refresh-token"}
        urlopen_mock.return_value.__enter__.return_value = object()
        clear_connection.return_value = 1

        result = revoke_google_oauth_connection(
            tenant_id="nucar",
            config=self.config,
        )

        self.assertEqual(result["connected"], False)
        self.assertEqual(result["revokedRemote"], True)
        self.assertEqual(result["removed"], 1)
        self.assertIsNone(result["revocationError"])
        clear_connection.assert_called_once()

    @patch("shared.google_oauth.supabase_client.select_single_query")
    def test_get_google_oauth_connection_status_uses_supabase_storage(self, select_single_query):
        select_single_query.return_value = {
            "tenant_id": "nucar",
            "connection_key": "primary",
            "client_id": "client-id",
            "token_uri": "https://oauth2.googleapis.com/token",
            "refresh_token": "refresh-token",
            "scopes": ["https://www.googleapis.com/auth/calendar.events"],
            "connected_at": "2026-06-23T09:45:00+07:00",
            "last_authorized_at": "2026-06-23T09:46:00+07:00",
        }

        result = google_oauth.get_google_oauth_connection_status(tenant_id="nucar", config=self.config)

        self.assertEqual(result["connected"], True)
        self.assertEqual(result["connectedAt"], "2026-06-23T09:45:00+07:00")
        self.assertEqual(result["scopes"], ["https://www.googleapis.com/auth/calendar.events"])
        select_single_query.assert_called_once()

    @patch("shared.google_oauth.supabase_client.insert_row")
    @patch("shared.google_oauth.supabase_client.select_single_query")
    def test_write_connection_record_uses_supabase_storage(self, select_single_query, insert_row):
        select_single_query.return_value = None

        google_oauth._write_connection_record(
            tenant_id="nucar",
            config=self.config,
            record={
                "clientId": "client-id",
                "tokenUri": "https://oauth2.googleapis.com/token",
                "refreshToken": "refresh-token",
                "scopes": ["https://www.googleapis.com/auth/calendar.events"],
                "connectedAt": "2026-06-23T09:45:00+07:00",
                "lastAuthorizedAt": "2026-06-23T09:46:00+07:00",
            },
        )

        insert_row.assert_called_once()
        self.assertEqual(insert_row.call_args.kwargs["table"], "google_oauth_connections")
        self.assertEqual(insert_row.call_args.kwargs["row"]["tenant_id"], "nucar")
        self.assertEqual(insert_row.call_args.kwargs["row"]["connection_key"], "primary")
        self.assertEqual(insert_row.call_args.kwargs["row"]["refresh_token"], "refresh-token")

    @patch("shared.google_oauth.supabase_client.delete_rows")
    def test_clear_google_oauth_connection_uses_supabase_storage(self, delete_rows):
        delete_rows.return_value = [{"tenant_id": "nucar", "connection_key": "primary"}]

        removed = google_oauth.clear_google_oauth_connection(tenant_id="nucar", config=self.config)

        self.assertEqual(removed, 1)
        delete_rows.assert_called_once()

    @patch("shared.google_oauth.get_tenant_shared_cache_value")
    @patch("shared.google_oauth.supabase_client.select_single_query")
    def test_missing_supabase_table_falls_back_to_cache(self, select_single_query, get_cache_value):
        select_single_query.side_effect = SupabaseClientError(
            "Supabase request failed (404): {\"message\":\"Could not find the table 'public.google_oauth_connections' in the schema cache\"}"
        )
        get_cache_value.return_value = (
            {
                "tenantId": "nucar",
                "clientId": "client-id",
                "tokenUri": "https://oauth2.googleapis.com/token",
                "refreshToken": "refresh-token",
                "scopes": ["https://www.googleapis.com/auth/calendar.events"],
                "connectedAt": "2026-06-23T09:45:00+07:00",
                "lastAuthorizedAt": "2026-06-23T09:46:00+07:00",
            },
            True,
        )

        result = google_oauth.get_google_oauth_connection_status(tenant_id="nucar", config=self.config)

        self.assertEqual(result["connected"], True)
        self.assertEqual(result["connectedAt"], "2026-06-23T09:45:00+07:00")
        self.assertEqual(result["scopes"], ["https://www.googleapis.com/auth/calendar.events"])


if __name__ == "__main__":
    unittest.main()
