from __future__ import annotations

from urllib.parse import urlparse, urlunparse


def normalize_frontend_base_url(candidate: str) -> str | None:
    normalized = str(candidate or "").strip().rstrip("/")
    if not normalized:
        return None

    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None

    normalized_path = (parsed.path or "").rstrip("/")
    if normalized_path == "/":
        normalized_path = ""

    normalized_netloc = parsed.netloc
    if (parsed.hostname or "").strip().lower() == "localhost":
        host = "127.0.0.1"
        try:
            port_suffix = f":{parsed.port}" if parsed.port is not None else ""
        except ValueError:
            port_suffix = ""
        userinfo = ""
        if parsed.username:
            userinfo = parsed.username
            if parsed.password:
                userinfo = f"{userinfo}:{parsed.password}"
            userinfo = f"{userinfo}@"
        normalized_netloc = f"{userinfo}{host}{port_suffix}"

    return urlunparse((parsed.scheme, normalized_netloc, normalized_path, "", "", ""))


def resolve_frontend_base_url(*, request=None, configured_base_url: str | None = None) -> str | None:
    candidates: list[str] = []
    if configured_base_url:
        candidates.append(str(configured_base_url))

    if request is not None:
        origin_header = str(getattr(request, "headers", {}).get("origin") or "").strip()
        if origin_header:
            candidates.append(origin_header)

        request_url = getattr(request, "url", None)
        request_scheme = str(getattr(request_url, "scheme", "") or "").strip()
        request_netloc = str(getattr(request_url, "netloc", "") or "").strip()
        if request_scheme and request_netloc:
            candidates.append(f"{request_scheme}://{request_netloc}")

        request_base_url = str(getattr(request, "base_url", "") or "").strip()
        if request_base_url:
            candidates.append(request_base_url)

    for candidate in candidates:
        normalized = normalize_frontend_base_url(candidate)
        if normalized:
            return normalized

    return None
