BEGIN;

DROP TRIGGER audit_events_append_only ON audit_events;
DROP TRIGGER audit_events_no_truncate ON audit_events;
DROP FUNCTION reject_audit_event_mutation();
DROP TABLE audit_events;
DROP TABLE cache_manifests;
DROP TABLE instance_projections;
DROP TABLE capability_manifests;
DROP TABLE drain_intents;
DROP TABLE workload_operations;
DROP TABLE source_observations;
DROP TABLE orphaned_capacity_debts;

ALTER TABLE admission_grants
    DROP CONSTRAINT admission_grants_reservation_shape,
    DROP CONSTRAINT admission_grants_authoritative_reservation_fkey,
    DROP CONSTRAINT admission_grants_request_record_fkey;

DROP TABLE reservations;
DROP TABLE instance_capacity;
DROP TABLE request_records;
DROP TABLE system_admission_state;

COMMIT;
