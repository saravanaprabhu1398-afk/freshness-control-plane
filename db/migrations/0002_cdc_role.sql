-- Dedicated, least-privilege role for Debezium (ADR-012).
-- REPLICATION lets it create and read the logical replication slot; SELECT on source.*
-- is needed for the initial snapshot. It cannot read telemetry, freshness, metrics or ops.
-- The password is set by the migrator from FCP_CDC_PASSWORD, never stored in this file.

do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'fcp_cdc') then
    create role fcp_cdc with login replication;
  end if;
end
$$;

do $$ begin execute format('grant connect on database %I to fcp_cdc', current_database()); end $$;
grant usage on schema source to fcp_cdc;
grant select on all tables in schema source to fcp_cdc;
alter default privileges in schema source grant select on tables to fcp_cdc;
