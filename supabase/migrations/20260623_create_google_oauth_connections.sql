create table if not exists google_oauth_connections (
    tenant_id text not null,
    connection_key text not null,
    client_id text not null,
    token_uri text not null,
    refresh_token text not null,
    scopes jsonb not null default '[]'::jsonb,
    connected_at timestamptz not null default now(),
    last_authorized_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (tenant_id, connection_key)
);

create index if not exists google_oauth_connections_tenant_idx
    on google_oauth_connections (tenant_id);
