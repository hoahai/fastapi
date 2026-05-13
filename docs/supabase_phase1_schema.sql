-- Supabase Phase 1 MVP Auth/Authorization schema

create table if not exists profiles (
  user_id uuid primary key references auth.users(id) on delete cascade,
  email text not null,
  full_name text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists tenants (
  id uuid primary key default gen_random_uuid(),
  slug text unique not null,
  name text not null,
  active boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists apps (
  id uuid primary key default gen_random_uuid(),
  code text unique not null,
  name text not null,
  active boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists tenant_users (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references tenants(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  status text not null check (status in ('active','pending','disabled')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, user_id)
);

create table if not exists tenant_app_roles (
  id uuid primary key default gen_random_uuid(),
  tenant_id uuid not null references tenants(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  app_id uuid not null references apps(id) on delete cascade,
  role text not null check (role in ('viewer','editor','admin')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, user_id, app_id)
);

create table if not exists user_global_roles (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  role text not null check (role in ('super_admin')),
  active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id, role)
);

create table if not exists role_permissions (
  id uuid primary key default gen_random_uuid(),
  role text not null,
  permission text not null,
  created_at timestamptz not null default now(),
  unique (role, permission)
);

create table if not exists invitations (
  id uuid primary key default gen_random_uuid(),
  token text unique not null,
  email text not null,
  tenant_id uuid not null references tenants(id) on delete cascade,
  app_id uuid not null references apps(id) on delete cascade,
  role text not null,
  status text not null check (status in ('pending','accepted','revoked','expired')),
  invited_by_user_id uuid references auth.users(id),
  expires_at timestamptz not null,
  accepted_at timestamptz,
  accepted_by_user_id uuid references auth.users(id),
  created_at timestamptz not null default now()
);

create table if not exists invitation_assignments (
  id uuid primary key default gen_random_uuid(),
  invitation_id uuid not null references invitations(id) on delete cascade,
  tenant_id uuid not null references tenants(id) on delete cascade,
  app_id uuid not null references apps(id) on delete cascade,
  role text not null check (role in ('viewer','editor','admin')),
  created_at timestamptz not null default now(),
  unique (invitation_id, tenant_id, app_id)
);
