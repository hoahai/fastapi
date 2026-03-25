from __future__ import annotations

from collections.abc import Iterable


def standardize_account_code(value: object | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    return cleaned.upper()


def standardize_account_codes(
    account_codes: str | Iterable[object] | None,
) -> list[str]:
    if account_codes is None:
        return []

    if isinstance(account_codes, str):
        candidates: list[object] = [account_codes]
    else:
        try:
            candidates = list(account_codes)
        except TypeError:
            return []

    normalized: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        chunks = [candidate]
        if isinstance(candidate, str):
            chunks = candidate.split(",")

        for chunk in chunks:
            code = standardize_account_code(chunk)
            if not code or code in seen:
                continue
            seen.add(code)
            normalized.append(code)
    return normalized


def standardize_account_code_set(
    account_codes: str | Iterable[object] | None,
) -> set[str]:
    return set(standardize_account_codes(account_codes))
