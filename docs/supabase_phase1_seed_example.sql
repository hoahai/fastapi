-- Phase 1 seed template (replace placeholders before running)
-- Preconditions:
--   1) docs/supabase_phase1_schema.sql has been applied
--   2) You have a real Supabase auth user in auth.users
--
-- Placeholder values to replace:
--   <PASTE_AUTH_USER_ID>      -> auth.users.id UUID from Supabase dashboard
--   <PASTE_TEST_USER_EMAIL>   -> auth.users.email for that user
--
-- Find auth user id in Supabase dashboard:
--   Authentication -> Users -> copy "UID"

begin;

-- 1) Base tenant + app records
insert into tenants (slug, name, active)
values ('taaa', 'TAAA', true)
on conflict (slug) do update
set name = excluded.name,
    active = excluded.active;

insert into apps (code, name, active)
values ('tradsphere', 'TradSphere', true)
on conflict (code) do update
set name = excluded.name,
    active = excluded.active;

-- 2) Role permission map (MVP app-level + feature-level)
insert into role_permissions (role, permission) values
  ('tradsphere.viewer', 'tradsphere.viewer'),
  ('tradsphere.viewer', 'tradsphere.contacts.viewer'),
  ('tradsphere.viewer', 'tradsphere.stations.viewer'),
  ('tradsphere.viewer', 'tradsphere.estnums.viewer'),
  ('tradsphere.viewer', 'tradsphere.schedules.viewer'),
  ('tradsphere.editor', 'tradsphere.viewer'),
  ('tradsphere.editor', 'tradsphere.editor'),
  ('tradsphere.editor', 'tradsphere.contacts.viewer'),
  ('tradsphere.editor', 'tradsphere.contacts.editor'),
  ('tradsphere.editor', 'tradsphere.stations.viewer'),
  ('tradsphere.editor', 'tradsphere.stations.editor'),
  ('tradsphere.editor', 'tradsphere.estnums.viewer'),
  ('tradsphere.editor', 'tradsphere.estnums.editor'),
  ('tradsphere.editor', 'tradsphere.schedules.viewer'),
  ('tradsphere.admin', 'tradsphere.viewer'),
  ('tradsphere.admin', 'tradsphere.editor'),
  ('tradsphere.admin', 'tradsphere.admin'),
  ('tradsphere.admin', 'tradsphere.contacts.viewer'),
  ('tradsphere.admin', 'tradsphere.contacts.editor'),
  ('tradsphere.admin', 'tradsphere.stations.viewer'),
  ('tradsphere.admin', 'tradsphere.stations.editor'),
  ('tradsphere.admin', 'tradsphere.estnums.viewer'),
  ('tradsphere.admin', 'tradsphere.estnums.editor'),
  ('tradsphere.admin', 'tradsphere.schedules.viewer'),
  ('tradsphere.admin', 'tradsphere.invites.admin')
on conflict (role, permission) do nothing;

-- 3) Profile mirror for user (app convenience table)
insert into profiles (user_id, email, full_name)
values ('762fec4b-1da3-4ba7-80e2-dd21622b6e0d', 'truonghoahai@gmail.com', null)
on conflict (user_id) do update
set email = excluded.email,
    full_name = excluded.full_name,
    updated_at = now();

-- 4) Tenant membership
insert into tenant_users (tenant_id, user_id, status)
select t.id, '<PASTE_AUTH_USER_ID>'::uuid, 'active'
from tenants t
where t.slug = 'taaa'
on conflict (tenant_id, user_id) do update
set status = excluded.status,
    updated_at = now();

-- 5) App role assignment (set initial role here)
-- Choose one role:
--   tradsphere.viewer | tradsphere.editor | tradsphere.admin
insert into tenant_app_roles (tenant_id, user_id, app_id, role)
select t.id, '<PASTE_AUTH_USER_ID>'::uuid, a.id, 'tradsphere.admin'
from tenants t
join apps a on a.code = 'tradsphere'
where t.slug = 'taaa'
on conflict (tenant_id, user_id, app_id) do update
set role = excluded.role,
    updated_at = now();

commit;

