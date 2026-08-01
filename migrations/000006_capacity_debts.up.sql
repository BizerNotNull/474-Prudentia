BEGIN;

CREATE TABLE capacity_debts (
    debt_id text PRIMARY KEY CHECK (octet_length(debt_id) BETWEEN 1 AND 256),
    reservation_id text NOT NULL UNIQUE REFERENCES scheduler_reservations (reservation_id),
    tenant_hash bytea NOT NULL REFERENCES tenant_counters (tenant_hash) CHECK (octet_length(tenant_hash) = 32),
    cluster text NOT NULL,
    namespace text NOT NULL,
    logical_engine text NOT NULL,
    pod_uid text NOT NULL,
    endpoint_epoch bigint NOT NULL,
    recovery_epoch bigint NOT NULL,
    slot_cost integer NOT NULL CHECK (slot_cost > 0),
    cause text NOT NULL CHECK (cause IN (
        'ambiguous_transport',
        'ambiguous_canceled',
        'ambiguous_protocol',
        'classification_timeout',
        'legacy_orphan'
    )),
    state text NOT NULL CHECK (state IN ('active', 'resolved_identity_gone')),
    resolution_evidence_hash bytea CHECK (
        resolution_evidence_hash IS NULL OR octet_length(resolution_evidence_hash) = 32
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at timestamptz,
    FOREIGN KEY (cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
        REFERENCES scheduler_backends (cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch),
    CHECK (
        (state = 'active' AND resolution_evidence_hash IS NULL AND resolved_at IS NULL)
        OR
        (state = 'resolved_identity_gone' AND resolution_evidence_hash IS NOT NULL AND resolved_at IS NOT NULL)
    )
);

CREATE INDEX capacity_debts_state_created_idx
    ON capacity_debts (state, created_at, debt_id);

INSERT INTO capacity_debts (
    debt_id,
    reservation_id,
    tenant_hash,
    cluster,
    namespace,
    logical_engine,
    pod_uid,
    endpoint_epoch,
    recovery_epoch,
    slot_cost,
    cause,
    state
)
SELECT
    'debt_' || reservation.reservation_id,
    reservation.reservation_id,
    reservation.tenant_hash,
    reservation.cluster,
    reservation.namespace,
    reservation.logical_engine,
    reservation.pod_uid,
    reservation.endpoint_epoch,
    reservation.recovery_epoch,
    reservation.slot_cost,
    'legacy_orphan',
    'active'
FROM scheduler_reservations AS reservation
JOIN admission_grants AS admission_grant
    ON admission_grant.reservation_id = reservation.reservation_id
    AND admission_grant.tenant_hash = reservation.tenant_hash
    AND admission_grant.slot_cost = reservation.slot_cost
    AND admission_grant.state = 'orphaned'
WHERE reservation.state = 'orphaned';

DO $$
BEGIN
    IF (SELECT count(*) FROM scheduler_reservations WHERE state = 'orphaned')
        <> (SELECT count(*) FROM capacity_debts)
        OR EXISTS (
            SELECT 1
            FROM (
                SELECT tenant_hash, sum(slot_cost) AS debt_slots
                FROM capacity_debts
                GROUP BY tenant_hash
            ) AS debt_totals
            JOIN tenant_counters USING (tenant_hash)
            WHERE tenant_counters.orphaned_grants < debt_totals.debt_slots
        )
        OR EXISTS (
            SELECT 1
            FROM (
                SELECT cluster, namespace, logical_engine, pod_uid, endpoint_epoch,
                    recovery_epoch, sum(slot_cost) AS debt_slots
                FROM capacity_debts
                GROUP BY cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch
            ) AS debt_totals
            JOIN scheduler_backends USING (
                cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch
            )
            WHERE scheduler_backends.orphaned_slots < debt_totals.debt_slots
        )
    THEN
        RAISE EXCEPTION 'capacity debt backfill found inconsistent orphan accounting';
    END IF;
END
$$;

COMMIT;
