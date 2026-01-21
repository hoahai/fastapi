from __future__ import annotations

from dataclasses import dataclass
from contextvars import ContextVar
from pathlib import Path
import os
import re
import threading
from typing import Any, Iterable

from shared.constants import TIMEZONE as DEFAULT_TIMEZONE

try:
    import yaml
except ImportError:
    yaml = None


class TenantConfigError(RuntimeError):
    pass


def format_tenant_config_detail(
    app_name: str | None,
    *,
    missing: Iterable[str] | None = None,
    invalid: Iterable[str] | None = None,
) -> str:
    name = app_name or "Tenant"
    missing_list = [str(item) for item in (missing or [])]
    invalid_list = [str(item) for item in (invalid or [])]

    parts: list[str] = []
    if missing_list:
        parts.append(f"missing: {', '.join(missing_list)}")
    if invalid_list:
        parts.append(f"invalid: {', '.join(invalid_list)}")
    if not parts:
        parts.append("missing required values")

    return f"{name} tenant config " + "; ".join(parts)


def build_tenant_config_payload(
    app_name: str | None,
    *,
    missing: Iterable[str] | None = None,
    invalid: Iterable[str] | None = None,
) -> dict[str, object]:
    missing_list = [str(item) for item in (missing or [])]
    invalid_list = [str(item) for item in (invalid or [])]
    return {
        "app": app_name,
        "detail": format_tenant_config_detail(
            app_name,
            missing=missing_list,
            invalid=invalid_list,
        ),
        "missing": missing_list,
        "invalid": invalid_list,
    }


class TenantConfigValidationError(TenantConfigError):
    def __init__(
        self,
        *,
        app_name: str | None = None,
        missing: Iterable[str] | None = None,
        invalid: Iterable[str] | None = None,
    ) -> None:
        self.app_name = app_name
        self.missing = [str(item) for item in (missing or [])]
        self.invalid = [str(item) for item in (invalid or [])]
        message = format_tenant_config_detail(
            app_name,
            missing=self.missing,
            invalid=self.invalid,
        )
        super().__init__(message)


@dataclass(frozen=True)
class TenantContext:
    tenant_id: str
    env: dict[str, str]


_TENANT_CONTEXT: ContextVar[TenantContext | None] = ContextVar(
    "tenant_context",
    default=None,
)

LOCAL_ETC_DIR = Path(__file__).resolve().parents[1] / "etc"
LOCAL_SECRETS_DIR = LOCAL_ETC_DIR / "secrets"

_TENANT_ENV_CACHE: dict[str, tuple[float, dict[str, float], dict[str, str]]] = {}
_CACHE_LOCK = threading.Lock()


def normalize_tenant_id(raw: str) -> str:
    if raw is None:
        raise TenantConfigError("X-Tenant-Id header is missing")

    value = raw.strip()
    if not value:
        raise TenantConfigError("X-Tenant-Id header is empty")

    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise TenantConfigError("X-Tenant-Id header has invalid characters")

    return value.lower()


def _resolve_tenant_path(tenant_id: str) -> Path:
    filenames = (f"{tenant_id}.yaml", f"{tenant_id}.yml")
    for base in (Path("/etc/secrets"), LOCAL_SECRETS_DIR):
        for filename in filenames:
            candidate = base / filename
            if candidate.is_file():
                return candidate

    raise TenantConfigError(
        f"Tenant config not found for '{tenant_id}' in /etc/secrets or etc/secrets."
    )


def _stringify(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def _flatten_env(data: dict[str, Any]) -> dict[str, str]:
    env: dict[str, str] = {}
    for key, value in data.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                env[str(sub_key)] = _stringify(sub_value)
        else:
            env[str(key)] = _stringify(value)
    return env


def _load_yaml_file(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise TenantConfigError(f"Unable to read tenant config: {exc}") from exc

    data = yaml.safe_load(raw) if raw.strip() else None
    if not isinstance(data, dict):
        raise TenantConfigError(f"Config is empty or invalid: {path}")
    return data


def _resolve_include_path(value: str, base_dir: Path) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        if candidate.is_file():
            return candidate
        raise TenantConfigError(f"Include file not found: {candidate}")

    for base in (base_dir, Path("/etc/secrets"), LOCAL_SECRETS_DIR):
        path = base / value
        if path.is_file():
            return path

    raise TenantConfigError(
        f"Include file not found: {value} in {base_dir}, /etc/secrets, or etc/secrets."
    )


def _expand_includes(
    data: dict[str, Any],
    base_dir: Path,
    deps: set[Path],
) -> dict[str, Any]:
    if not isinstance(data, dict):
        return data

    include_value = data.get("include")
    if include_value is not None:
        merged: dict[str, Any] = {}
        include_list = (
            include_value if isinstance(include_value, list) else [include_value]
        )
        for item in include_list:
            if not isinstance(item, str):
                raise TenantConfigError("Include must be a string or list of strings")
            include_path = _resolve_include_path(item, base_dir)
            deps.add(include_path)
            include_data = _load_yaml_file(include_path)
            include_data = _expand_includes(
                include_data,
                include_path.parent,
                deps,
            )
            merged.update(include_data)

        overrides = {k: v for k, v in data.items() if k != "include"}
        merged.update(overrides)
        data = merged

    for key, value in list(data.items()):
        if isinstance(value, dict):
            data[key] = _expand_includes(value, base_dir, deps)

    return data


def load_tenant_env(tenant_id: str) -> dict[str, str]:
    if yaml is None:
        raise TenantConfigError("PyYAML is required to load tenant configs")

    tenant_id = normalize_tenant_id(tenant_id)
    path = _resolve_tenant_path(tenant_id)
    cache_key = str(path)

    try:
        mtime = path.stat().st_mtime
    except OSError as exc:
        raise TenantConfigError(f"Unable to read tenant config: {exc}") from exc

    with _CACHE_LOCK:
        cached = _TENANT_ENV_CACHE.get(cache_key)
        if cached and cached[0] == mtime:
            deps_mtime = cached[1]
            for dep_path, dep_mtime in deps_mtime.items():
                try:
                    if Path(dep_path).stat().st_mtime != dep_mtime:
                        break
                except OSError:
                    break
            else:
                return cached[2]

    data = _load_yaml_file(path)
    deps: set[Path] = set()
    data = _expand_includes(data, path.parent, deps)

    env = _flatten_env(data)

    with _CACHE_LOCK:
        deps_mtime = {}
        for dep in deps:
            try:
                deps_mtime[str(dep)] = dep.stat().st_mtime
            except OSError:
                continue
        _TENANT_ENV_CACHE[cache_key] = (mtime, deps_mtime, env)

    return env


def set_tenant_context(tenant_id: str) -> ContextVar.Token:
    tenant_id = normalize_tenant_id(tenant_id)
    env = load_tenant_env(tenant_id)
    return _TENANT_CONTEXT.set(TenantContext(tenant_id=tenant_id, env=env))


def reset_tenant_context(token: ContextVar.Token) -> None:
    _TENANT_CONTEXT.reset(token)


def get_tenant_id() -> str | None:
    ctx = _TENANT_CONTEXT.get()
    return ctx.tenant_id if ctx else None


def get_env(key: str, default: str | None = None) -> str | None:
    ctx = _TENANT_CONTEXT.get()
    if ctx and key in ctx.env:
        return ctx.env[key]
    return os.getenv(key, default)


def get_timezone(default: str | None = None) -> str:
    value = get_env("TIMEZONE", default or DEFAULT_TIMEZONE)
    if value is None or str(value).strip() == "":
        return default or DEFAULT_TIMEZONE
    return str(value).strip()
