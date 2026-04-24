from __future__ import annotations

import re

_WHITESPACE_RE = re.compile(r"\s+")


def normalize_input_text(value: object) -> str:
    """Trim and collapse internal whitespace for free-text inputs."""
    text = str(value or "")
    text = text.replace("\u00A0", " ")
    return _WHITESPACE_RE.sub(" ", text).strip()


def normalize_optional_input_text(value: object) -> str | None:
    """Normalize text and return None when the final text is empty."""
    text = normalize_input_text(value)
    return text or None


def normalize_compact_token(value: object) -> str:
    """Normalize text and remove all spaces for token-like inputs."""
    return normalize_input_text(value).replace(" ", "")
