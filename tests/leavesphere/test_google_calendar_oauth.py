import json
import os
import tempfile
import unittest
from unittest.mock import patch

from shared.auth.signed_token import sign_json_token
from apps.leavesphere.api.v1.helpers.googleCalendarOAuth import (
    handle_google_calendar_oauth_callback,
    get_google_calendar_id,
    resolve_google_calendar_oauth_settings,
)


class LeaveSphereGoogleCalendarOAuthConfigTests(unittest.TestCase):
    def test_resolve_google_calendar_oauth_settings_uses_nested_tenant_config(self):
        payload = {
            "web": {
                "client_id": "client-id-from-file",
                "client_secret": "client-secret-from-file",
                "redirect_uris": [
                    "http://localhost:8000/api/leavesphere/v1/public/google-calendar/oauth/callback",
                ],
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "google-oauth-client.json")
            with open(file_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)

            with patch.dict(
                os.environ,
                {
                    "LEAVESPHERE_GOOGLE_CALENDAR": str(
                        {
                            "google_calendar": {
                                "json_key_file_path": file_path,
                                "ggCalendarId": "shared-calendar-id",
                                "scopes": [
                                    "https://www.googleapis.com/auth/calendar.events.owned"
                                ],
                            }
                        }
                    ),
                },
                clear=True,
            ):
                config = resolve_google_calendar_oauth_settings(request=None)
                shared_calendar_id = get_google_calendar_id()

        self.assertEqual(config.client_id, "client-id-from-file")
        self.assertEqual(config.client_secret, "client-secret-from-file")
        self.assertEqual(
            config.redirect_uri,
            "http://localhost:8000/api/leavesphere/v1/public/google-calendar/oauth/callback",
        )
        self.assertEqual(config.state_secret, "client-secret-from-file")
        self.assertEqual(shared_calendar_id, "shared-calendar-id")

    def test_resolve_google_calendar_oauth_settings_prefers_callback_request_over_file_redirect(self):
        payload = {
            "web": {
                "client_id": "client-id-from-file",
                "client_secret": "client-secret-from-file",
                "redirect_uris": [
                    "http://localhost:8000/api/leavesphere/v1/admin/google-calendar/oauth/status",
                ],
            }
        }

        class _Request:
            base_url = "http://localhost:8000/"

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "google-oauth-client.json")
            with open(file_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)

            with patch.dict(
                os.environ,
                {
                    "LEAVESPHERE_GOOGLE_CALENDAR": str(
                        {
                            "google_calendar": {
                                "json_key_file_path": file_path,
                                "ggCalendarId": "shared-calendar-id",
                                "scopes": [
                                    "https://www.googleapis.com/auth/calendar.events.owned"
                                ],
                            }
                        }
                    ),
                },
                clear=True,
            ):
                config = resolve_google_calendar_oauth_settings(request=_Request())

        self.assertEqual(
            config.redirect_uri,
            "http://localhost:8000/api/leavesphere/v1/public/google-calendar/oauth/callback",
        )

    def test_resolve_google_calendar_oauth_settings_uses_tenant_redirect_uri_override(self):
        payload = {
            "web": {
                "client_id": "client-id-from-file",
                "client_secret": "client-secret-from-file",
                "redirect_uris": [
                    "http://localhost:8000/api/leavesphere/v1/public/google-calendar/oauth/callback",
                ],
            }
        }

        class _Request:
            base_url = "http://127.0.0.1:8000/"

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "google-oauth-client.json")
            with open(file_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)

            with patch.dict(
                os.environ,
                {
                    "LEAVESPHERE_GOOGLE_CALENDAR": str(
                        {
                            "google_calendar": {
                                "json_key_file_path": file_path,
                                "ggCalendarId": "shared-calendar-id",
                                "redirect_uri": "http://localhost:8000/api/leavesphere/v1/public/google-calendar/oauth/callback",
                                "scopes": [
                                    "https://www.googleapis.com/auth/calendar.events.owned"
                                ],
                            }
                        }
                    ),
                },
                clear=True,
            ):
                config = resolve_google_calendar_oauth_settings(request=_Request())

        self.assertEqual(
            config.redirect_uri,
            "http://localhost:8000/api/leavesphere/v1/public/google-calendar/oauth/callback",
        )

    def test_handle_google_calendar_oauth_callback_bootstraps_tenant_context_from_state(self):
        payload = {
            "web": {
                "client_id": "client-id-from-file",
                "client_secret": "client-secret-from-file",
                "redirect_uris": [
                    "http://localhost:8000/api/leavesphere/v1/public/google-calendar/oauth/callback",
                ],
            }
        }

        class _Request:
            base_url = "http://localhost:8000/"

        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = os.path.join(tmpdir, "google-oauth-client.json")
            with open(file_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)

            state = sign_json_token(
                payload={
                    "tenantId": "taaa",
                    "purpose": "leavesphere.google_calendar.oauth",
                    "jti": "test-jti",
                    "iat": 1782230218,
                    "exp": 1782230818,
                },
                secret="client-secret-from-file",
            )

            captured: dict[str, object] = {}

            with patch.dict(
                os.environ,
                {
                    "LEAVESPHERE_GOOGLE_CALENDAR": str(
                        {
                            "google_calendar": {
                                "json_key_file_path": file_path,
                                "ggCalendarId": "shared-calendar-id",
                                "redirect_uri": "http://localhost:8000/api/leavesphere/v1/public/google-calendar/oauth/callback",
                                "scopes": [
                                    "https://www.googleapis.com/auth/calendar.events.owned"
                                ],
                            }
                        }
                    ),
                },
                clear=True,
            ), patch(
                "apps.leavesphere.api.v1.helpers.googleCalendarOAuth._handle_google_oauth_callback",
                side_effect=lambda **kwargs: captured.update(kwargs) or {
                    "connected": True,
                },
            ):
                result = handle_google_calendar_oauth_callback(
                    request=_Request(),
                    code="auth-code",
                    state=state,
                )

        self.assertEqual(result, {"connected": True})
        self.assertIn("config", captured)
        config = captured["config"]
        self.assertEqual(
            getattr(config, "client_id", ""),
            "426125637120-idnc13mtdpvma5pg1q0nddepsr1a2i4u.apps.googleusercontent.com",
        )
        self.assertEqual(
            getattr(config, "redirect_uri", ""),
            "http://localhost:8000/api/leavesphere/v1/public/google-calendar/oauth/callback",
        )


if __name__ == "__main__":
    unittest.main()
