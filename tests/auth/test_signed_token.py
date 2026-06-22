import unittest

from shared.auth.signed_token import sign_json_token, verify_json_token


class SignedTokenTests(unittest.TestCase):
    def test_sign_and_verify_json_token_round_trip(self) -> None:
        token = sign_json_token(
            payload={
                "tenant_id": "tenant-123",
                "tenant_slug": "acme",
                "request_id": "pto-456",
                "jti": "jti-789",
            },
            secret="super-secret",
        )

        claims = verify_json_token(token=token, secret="super-secret")

        self.assertEqual(claims["tenant_id"], "tenant-123")
        self.assertEqual(claims["tenant_slug"], "acme")
        self.assertEqual(claims["request_id"], "pto-456")
        self.assertEqual(claims["jti"], "jti-789")

    def test_verify_json_token_rejects_tampering(self) -> None:
        token = sign_json_token(payload={"foo": "bar"}, secret="super-secret")
        tampered = token[:-1] + ("A" if token[-1] != "A" else "B")

        with self.assertRaises(ValueError):
            verify_json_token(token=tampered, secret="super-secret")


if __name__ == "__main__":
    unittest.main()
