from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class AuthPrincipal:
    user_id: str
    email: str | None = None
    raw_user: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class TenantAccessProfile:
    tenant_id: str
    tenant_slug: str
    app_id: str
    app_code: str
    role: str
    permissions: frozenset[str]


@dataclass(frozen=True)
class AuthorizationResult:
    principal: AuthPrincipal
    access: TenantAccessProfile
