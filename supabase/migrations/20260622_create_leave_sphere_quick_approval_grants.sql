create table if not exists leave_sphere_quick_approval_grants (
    id text primary key,
    tenant_id text not null,
    tenant_slug text not null,
    app_id text not null,
    request_id text not null,
    token text null unique,
    recipient_employee_id text null,
    recipient_email text not null,
    recipient_role text not null,
    jti text not null unique,
    request_snapshot_json jsonb not null default '{}'::jsonb,
    issued_at timestamptz not null,
    expires_at timestamptz not null,
    used_at timestamptz null,
    used_action text null,
    used_reason text null,
    revoked_at timestamptz null,
    created_by_user_id text null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint leave_sphere_quick_approval_grants_recipient_role_check
        check (recipient_role in ('manager', 'admin')),
    constraint leave_sphere_quick_approval_grants_used_action_check
        check (used_action is null or used_action in ('approved', 'rejected'))
);

alter table if exists leave_sphere_quick_approval_grants
    add column if not exists tenant_slug text;

update leave_sphere_quick_approval_grants
set tenant_slug = tenant_id
where tenant_slug is null or tenant_slug = '';

alter table if exists leave_sphere_quick_approval_grants
    alter column tenant_slug set not null;

create index if not exists leave_sphere_quick_approval_grants_tenant_idx
    on leave_sphere_quick_approval_grants (tenant_id);

create index if not exists leave_sphere_quick_approval_grants_tenant_slug_idx
    on leave_sphere_quick_approval_grants (tenant_slug);

create index if not exists leave_sphere_quick_approval_grants_request_idx
    on leave_sphere_quick_approval_grants (request_id);

create index if not exists leave_sphere_quick_approval_grants_email_idx
    on leave_sphere_quick_approval_grants (recipient_email);

create index if not exists leave_sphere_quick_approval_grants_expires_idx
    on leave_sphere_quick_approval_grants (expires_at);
