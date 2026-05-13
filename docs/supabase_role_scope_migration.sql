-- TheSphereWorks role scope migration
-- Purpose:
--   1) Canonicalize tenant/app roles to: admin, editor, viewer
--   2) Add global super_admin support via user_global_roles
--   3) Add multi-assignment invite support via invitation_assignments
--   4) Preserve legacy data and keep migration re-runnable

begin;

-- 1) Create global role table (idempotent)
create table if not exists user_global_roles (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  role text not null check (role in ('super_admin')),
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id, role)
);

-- 2) Create invitation assignment table (idempotent)
create table if not exists invitation_assignments (
  id uuid primary key default gen_random_uuid(),
  invitation_id uuid not null references invitations(id) on delete cascade,
  tenant_id uuid not null references tenants(id) on delete cascade,
  app_id uuid not null references apps(id) on delete cascade,
  role text not null check (role in ('viewer','editor','admin')),
  created_at timestamptz not null default now(),
  unique (invitation_id, tenant_id, app_id)
);

-- 3) Migrate legacy app-level super_admin into global roles.
--    Any user with workspace.super_admin in tenant_app_roles or role_permissions
--    is granted global super_admin.
insert into user_global_roles (user_id, role, active)
select distinct tar.user_id, 'super_admin', true
from tenant_app_roles tar
where tar.role in ('workspace.super_admin', 'super_admin')
on conflict (user_id, role) do update
set active = true,
    updated_at = now();

-- 4) Drop existing tenant_app_roles role check constraints before data rewrite.
--    This avoids violations when rewriting legacy role keys to canonical values.
do $$
declare
  role_constraint record;
begin
  for role_constraint in
    select c.conname
    from pg_constraint c
    join pg_class t on t.oid = c.conrelid
    where t.relname = 'tenant_app_roles'
      and c.contype = 'c'
      and pg_get_constraintdef(c.oid) ilike '%role%'
  loop
    execute format('alter table tenant_app_roles drop constraint %I', role_constraint.conname);
  end loop;
end $$;

-- 5) Canonicalize tenant_app_roles role values
update tenant_app_roles
set role = 'viewer'
where role in ('tradsphere.viewer', 'viewer', 'user');

update tenant_app_roles
set role = 'editor'
where role in ('tradsphere.editor', 'editor');

update tenant_app_roles
set role = 'admin'
where role in ('tradsphere.admin', 'admin');

-- Remove app-level super_admin rows so super_admin is global only.
delete from tenant_app_roles
where role in ('workspace.super_admin', 'super_admin');

-- 6) Canonicalize invitation role values (legacy + current)
update invitations
set role = 'viewer'
where role in ('tradsphere.viewer', 'viewer', 'user');

update invitations
set role = 'editor'
where role in ('tradsphere.editor', 'editor');

update invitations
set role = 'admin'
where role in ('tradsphere.admin', 'admin');

-- Any legacy super_admin invitation is downgraded to admin for safety.
update invitations
set role = 'admin'
where role in ('workspace.super_admin', 'super_admin');

-- 7) Canonicalize role_permissions role values
update role_permissions
set role = 'viewer'
where role in ('tradsphere.viewer', 'viewer', 'user');

update role_permissions
set role = 'editor'
where role in ('tradsphere.editor', 'editor');

update role_permissions
set role = 'admin'
where role in ('tradsphere.admin', 'admin');

update role_permissions
set role = 'super_admin'
where role in ('workspace.super_admin', 'super_admin');

-- Ensure super_admin contains global admin permission marker.
insert into role_permissions (role, permission)
values ('super_admin', 'workspace.super_admin')
on conflict (role, permission) do nothing;

-- 8) Backfill invitation_assignments from legacy invitations rows.
insert into invitation_assignments (invitation_id, tenant_id, app_id, role)
select i.id, i.tenant_id, i.app_id, i.role
from invitations i
left join invitation_assignments ia
  on ia.invitation_id = i.id
 and ia.tenant_id = i.tenant_id
 and ia.app_id = i.app_id
where ia.id is null
  and i.tenant_id is not null
  and i.app_id is not null
  and i.role in ('viewer','editor','admin')
on conflict (invitation_id, tenant_id, app_id) do update
set role = excluded.role;

-- 9) Tighten tenant_app_roles check constraint to canonical values.
do $$
begin
  alter table tenant_app_roles
    add constraint tenant_app_roles_role_check
    check (role in ('viewer','editor','admin'));
exception
  when duplicate_object then
    null;
end $$;

-- 10) Tighten invitation_assignments role check to canonical values (idempotent safety).
do $$
begin
  alter table invitation_assignments
    add constraint invitation_assignments_role_check
    check (role in ('viewer','editor','admin'));
exception
  when duplicate_object then
    null;
end $$;

commit;

-- Verification queries
-- A) canonical roles in tenant_app_roles
select role, count(*) as count
from tenant_app_roles
group by role
order by role;

-- B) global super admin assignments
select role, active, count(*) as count
from user_global_roles
group by role, active
order by role, active desc;

-- C) invitation role distribution + assignment rows
select role, count(*) as count
from invitations
group by role
order by role;

select count(*) as assignment_rows
from invitation_assignments;

-- D) users that still have forbidden legacy roles (should be zero rows)
select *
from tenant_app_roles
where role not in ('viewer','editor','admin');
