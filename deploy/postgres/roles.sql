-- Applied by a database owner after migrations. Login roles and credentials are
-- provisioned externally; these NOLOGIN groups keep request/control/recovery access distinct.
BEGIN;
CREATE ROLE prudentia_scheduler NOLOGIN;
CREATE ROLE prudentia_controller NOLOGIN;
CREATE ROLE prudentia_backup NOLOGIN;
CREATE ROLE prudentia_recovery NOLOGIN;

GRANT CONNECT ON DATABASE prudentia TO prudentia_scheduler, prudentia_controller, prudentia_backup, prudentia_recovery;
GRANT USAGE ON SCHEMA public TO prudentia_scheduler, prudentia_controller;
GRANT SELECT, INSERT, UPDATE ON
  system_admission_state, request_records, admission_grants, reservations,
  instance_capacity, instance_projections, drain_intents, orphaned_capacity_debts,
  audit_events
TO prudentia_scheduler;
GRANT SELECT, INSERT, UPDATE, DELETE ON
  source_observations, instance_projections, instance_capacity, drain_intents,
  workload_operations, audit_events
TO prudentia_controller;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO prudentia_backup;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO prudentia_recovery;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO prudentia_scheduler, prudentia_controller, prudentia_recovery;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO prudentia_backup;
-- Deliberately no prudentia_gateway database role.
COMMIT;
