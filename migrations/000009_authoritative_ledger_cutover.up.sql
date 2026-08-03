BEGIN;

-- Retained reader versions are data, not process configuration. A writer may
-- advance only after every gateway sends candidates for all rows retained here.
CREATE TABLE system_lookup_read_versions (
    cluster_id text NOT NULL REFERENCES system_admission_state(cluster_id) ON DELETE CASCADE,
    version integer NOT NULL CHECK (version > 0),
    PRIMARY KEY (cluster_id, version)
);
CREATE TABLE system_digest_read_versions (
    cluster_id text NOT NULL REFERENCES system_admission_state(cluster_id) ON DELETE CASCADE,
    version integer NOT NULL CHECK (version > 0),
    PRIMARY KEY (cluster_id, version)
);
INSERT INTO system_lookup_read_versions SELECT cluster_id, lookup_write_version FROM system_admission_state;
INSERT INTO system_digest_read_versions SELECT cluster_id, digest_write_version FROM system_admission_state;
ALTER TABLE orphaned_capacity_debts ADD COLUMN provider_ack_sequence bigint
    CHECK (provider_ack_sequence IS NULL OR provider_ack_sequence > 0);

-- Give-up and terminal retries compare a stable result digest rather than
-- treating every retry after release as equivalent.
ALTER TABLE request_records ADD COLUMN terminal_result_hash bytea
    CHECK (terminal_result_hash IS NULL OR octet_length(terminal_result_hash)=32);
CREATE UNIQUE INDEX admission_grants_reservation_nonnull_idx
    ON admission_grants(reservation_id) WHERE reservation_id IS NOT NULL;

-- These relations remain only as rollback history. Any accidental old binary
-- now fails closed instead of creating a second authority after cutover.
CREATE FUNCTION reject_legacy_ledger_write() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'legacy transactional ledger is read-only after authoritative cutover'
        USING ERRCODE = '55000';
END
$$;
CREATE TRIGGER scheduler_reservations_cutover_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON scheduler_reservations
    FOR EACH STATEMENT EXECUTE FUNCTION reject_legacy_ledger_write();
CREATE TRIGGER capacity_debts_cutover_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON capacity_debts
    FOR EACH STATEMENT EXECUTE FUNCTION reject_legacy_ledger_write();
CREATE TRIGGER scheduler_crypto_versions_cutover_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON scheduler_crypto_versions
    FOR EACH STATEMENT EXECUTE FUNCTION reject_legacy_ledger_write();

COMMIT;
