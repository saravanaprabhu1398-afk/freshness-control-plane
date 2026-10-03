-- Fare-drift ticks already applied. Each tick moves prices relative to the current price, so
-- re-running a tick would compound it; the simulator inserts here in the same transaction as
-- its price writes and skips a tick that is already present (idempotent ticks).
create table ops.drift_tick (
  tick         bigint primary key,
  applied_at   timestamptz not null default now(),
  fares_changed integer    not null
);
