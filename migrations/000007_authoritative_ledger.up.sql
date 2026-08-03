BEGIN;

-- The admission row is the first lock in every ledger transaction.  It also
-- makes recovery and all write-version changes database-coordinated.
CREATE TABLE system_admission_state (
    cluster_id text PRIMARY KEY CHECK (octet_length(cluster_id) BETWEEN 1 AND 256),
    recovery_epoch bigint NOT NULL CHECK (recovery_epoch > 0),
    admission_state text NOT NULL CHECK (admission_state IN ('open', 'fenced')),
    dispatch_state text NOT NULL CHECK (dispatch_state IN ('open', 'fenced')),
    schema_write_version integer NOT NULL CHECK (schema_write_version > 0),
    lookup_write_version integer NOT NULL CHECK (lookup_write_version > 0),
    digest_write_version integer NOT NULL CHECK (digest_write_version > 0),
    capability_kek_write_version integer NOT NULL CHECK (capability_kek_write_version > 0),
    capability_comparison_write_version integer NOT NULL CHECK (capability_comparison_write_version > 0),
    classification_policy_version integer NOT NULL CHECK (classification_policy_version > 0),
    cleanup_policy_version integer NOT NULL CHECK (cleanup_policy_version > 0),
    fenced_at timestamptz,
    fenced_by_hash bytea CHECK (fenced_by_hash IS NULL OR octet_length(fenced_by_hash) = 32),
    fence_reason text CHECK (fence_reason IS NULL OR octet_length(fence_reason) BETWEEN 1 AND 1024),
    reopened_at timestamptz,
    changed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    changed_by_hash bytea NOT NULL CHECK (octet_length(changed_by_hash) = 32),
    CHECK (
        (admission_state = 'open' AND dispatch_state = 'open' AND fenced_at IS NULL AND fenced_by_hash IS NULL)
        OR
        (fenced_at IS NOT NULL AND fenced_by_hash IS NOT NULL)
    )
);

INSERT INTO system_admission_state (
    cluster_id, recovery_epoch, admission_state, dispatch_state,
    schema_write_version, lookup_write_version, digest_write_version,
    capability_kek_write_version, capability_comparison_write_version,
    classification_policy_version, cleanup_policy_version, changed_by_hash
)
SELECT
    clusters.cluster_id,
    COALESCE((SELECT max(recovery_epoch) FROM scheduler_backends b WHERE b.cluster = clusters.cluster_id), 1),
    'open', 'open', 7,
    (SELECT lookup_write_version FROM scheduler_crypto_versions WHERE singleton),
    (SELECT digest_write_version FROM scheduler_crypto_versions WHERE singleton),
    1, 1, 1, 1,
    decode(md5('legacy-migration') || md5('legacy-migration/actor'), 'hex')
FROM (
    SELECT cluster AS cluster_id FROM scheduler_backends
    UNION
    SELECT cluster AS cluster_id FROM controller_writer_generations
    UNION
    SELECT 'default' WHERE NOT EXISTS (SELECT 1 FROM scheduler_backends)
        AND NOT EXISTS (SELECT 1 FROM controller_writer_generations)
) AS clusters;

CREATE TABLE request_records (
    request_id text PRIMARY KEY CHECK (octet_length(request_id) BETWEEN 1 AND 256),
    tenant_hash bytea NOT NULL CHECK (octet_length(tenant_hash) = 32),
    idempotency_lookup_version integer,
    idempotency_lookup_hmac bytea,
    digest_version integer NOT NULL CHECK (digest_version > 0),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    owner_attempt_hash bytea NOT NULL CHECK (octet_length(owner_attempt_hash) = 32),
    current_generation bigint NOT NULL CHECK (current_generation > 0),
    stage text NOT NULL CHECK (stage IN (
        'reserved', 'rerank_pending', 'dispatch_authorized', 'streaming', 'terminal'
    )),
    outcome text CHECK (outcome IN (
        'succeeded', 'failed', 'canceled', 'given_up', 'ambiguous'
    )),
    execution_deadline timestamptz NOT NULL,
    classification_after timestamptz NOT NULL,
    mutation_retry_deadline timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    terminal_at timestamptz,
    CHECK (
        (idempotency_lookup_version IS NULL AND idempotency_lookup_hmac IS NULL)
        OR
        (idempotency_lookup_version > 0 AND octet_length(idempotency_lookup_hmac) = 32)
    ),
    CHECK (classification_after >= execution_deadline),
    CHECK (mutation_retry_deadline >= execution_deadline),
    CHECK ((stage = 'terminal') = (outcome IS NOT NULL)),
    CHECK ((stage = 'terminal' AND terminal_at IS NOT NULL) OR (stage <> 'terminal' AND terminal_at IS NULL))
);

CREATE UNIQUE INDEX request_records_idempotency_idx
    ON request_records (tenant_hash, idempotency_lookup_version, idempotency_lookup_hmac)
    WHERE idempotency_lookup_version IS NOT NULL;
CREATE INDEX request_records_classification_idx
    ON request_records (classification_after, request_id)
    WHERE stage IN ('reserved', 'rerank_pending', 'dispatch_authorized', 'streaming');
CREATE INDEX request_records_mutation_retention_idx
    ON request_records (mutation_retry_deadline, request_id);
CREATE INDEX request_records_terminal_retention_idx
    ON request_records (terminal_at, request_id) WHERE stage = 'terminal';

INSERT INTO request_records (
    request_id, tenant_hash, idempotency_lookup_version, idempotency_lookup_hmac,
    digest_version, request_digest, owner_attempt_hash, current_generation,
    stage, outcome, execution_deadline, classification_after,
    mutation_retry_deadline, created_at, updated_at, terminal_at
)
SELECT
    r.request_id, r.tenant_hash, r.lookup_version, r.lookup_hmac,
    COALESCE(r.digest_version, 1), COALESCE(r.request_digest, r.command_hash),
    decode(md5(r.attempt_id) || md5('legacy-attempt/' || r.attempt_id), 'hex'),
    r.request_generation,
    CASE r.state
        WHEN 'abandoned_rerank' THEN 'rerank_pending'
        WHEN 'dispatch_authorized' THEN 'dispatch_authorized'
        WHEN 'orphaned' THEN 'terminal'
        WHEN 'released' THEN 'terminal'
        WHEN 'given_up' THEN 'terminal'
        ELSE 'reserved'
    END,
    CASE r.state
        WHEN 'orphaned' THEN 'ambiguous'
        WHEN 'released' THEN 'succeeded'
        WHEN 'given_up' THEN 'given_up'
        ELSE NULL
    END,
    r.execution_deadline,
    COALESCE(g.classification_after, r.execution_deadline),
    GREATEST(COALESCE(g.classification_after, r.execution_deadline), r.execution_deadline),
    r.created_at, r.updated_at,
    CASE WHEN r.state IN ('orphaned', 'released', 'given_up') THEN r.updated_at ELSE NULL END
FROM scheduler_reservations r
LEFT JOIN admission_grants g ON g.request_id = r.request_id;

CREATE TABLE instance_capacity (
    cluster_id text NOT NULL CHECK (octet_length(cluster_id) BETWEEN 1 AND 256),
    namespace text NOT NULL CHECK (octet_length(namespace) BETWEEN 1 AND 256),
    logical_engine text NOT NULL CHECK (octet_length(logical_engine) BETWEEN 1 AND 256),
    pod_uid text NOT NULL CHECK (octet_length(pod_uid) BETWEEN 1 AND 256),
    endpoint_epoch bigint NOT NULL CHECK (endpoint_epoch > 0),
    recovery_epoch bigint NOT NULL CHECK (recovery_epoch > 0),
    physical_slots integer NOT NULL CHECK (physical_slots > 0),
    admission_limit integer NOT NULL CHECK (admission_limit >= 0 AND admission_limit <= physical_slots),
    reserved_slots integer NOT NULL DEFAULT 0 CHECK (reserved_slots >= 0),
    orphaned_slots integer NOT NULL DEFAULT 0 CHECK (orphaned_slots >= 0),
    retired boolean NOT NULL DEFAULT false,
    projection_version bigint NOT NULL CHECK (projection_version > 0),
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch),
    FOREIGN KEY (cluster_id) REFERENCES system_admission_state (cluster_id),
    CHECK (reserved_slots + orphaned_slots <= physical_slots),
    CHECK (NOT retired OR admission_limit = 0)
);

INSERT INTO instance_capacity (
    cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch,
    physical_slots, admission_limit, reserved_slots, orphaned_slots, retired,
    projection_version
)
SELECT cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch,
    configured_slots, admission_limit, reserved_slots, orphaned_slots,
    admission_limit = 0 AND NOT healthy, 1
FROM scheduler_backends;

CREATE INDEX instance_capacity_admission_idx
    ON instance_capacity (cluster_id, retired, admission_limit)
    WHERE NOT retired;
CREATE INDEX instance_capacity_retention_idx
    ON instance_capacity (retired, updated_at)
    WHERE retired;

CREATE TABLE instance_projections (
    cluster_id text NOT NULL,
    namespace text NOT NULL,
    logical_engine text NOT NULL,
    pod_uid text NOT NULL,
    endpoint_epoch bigint NOT NULL,
    recovery_epoch bigint NOT NULL,
    normalized_proxy_endpoint text NOT NULL CHECK (octet_length(normalized_proxy_endpoint) BETWEEN 1 AND 2048),
    model_fingerprint bytea NOT NULL CHECK (octet_length(model_fingerprint) = 32),
    config_fingerprint bytea NOT NULL CHECK (octet_length(config_fingerprint) = 32),
    membership_fingerprint bytea NOT NULL CHECK (octet_length(membership_fingerprint) = 32),
    capability_manifest_id text NOT NULL CHECK (octet_length(capability_manifest_id) BETWEEN 1 AND 256),
    capability_manifest_version bigint NOT NULL CHECK (capability_manifest_version > 0),
    source_stamps jsonb NOT NULL CHECK (jsonb_typeof(source_stamps) = 'object'),
    health text NOT NULL CHECK (health IN ('healthy', 'unhealthy', 'incomplete', 'stale')),
    projection_version bigint NOT NULL CHECK (projection_version > 0),
    projected_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch),
    FOREIGN KEY (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
        REFERENCES instance_capacity (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
);

INSERT INTO instance_projections (
    cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch,
    normalized_proxy_endpoint, model_fingerprint, config_fingerprint,
    membership_fingerprint, capability_manifest_id, capability_manifest_version,
    source_stamps, health, projection_version
)
SELECT cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch,
    endpoint,
    decode(md5(model) || md5('legacy-model/' || model), 'hex'),
    decode(md5(cluster || '/' || namespace || '/' || logical_engine) ||
        md5('legacy-config/' || cluster || '/' || namespace || '/' || logical_engine), 'hex'),
    decode(md5(pod_uid) || md5('legacy-member/' || pod_uid), 'hex'),
    'legacy', 1,
    jsonb_build_object('legacy_eligible_until', eligible_until, 'source_namespace', source_namespace,
        'source_name', source_name),
    CASE WHEN healthy AND eligible_until > transaction_timestamp() THEN 'healthy'
        WHEN healthy THEN 'stale' ELSE 'unhealthy' END,
    1
FROM scheduler_backends;

CREATE TABLE reservations (
    reservation_id text PRIMARY KEY CHECK (octet_length(reservation_id) BETWEEN 1 AND 256),
    request_id text NOT NULL REFERENCES request_records (request_id),
    request_generation bigint NOT NULL CHECK (request_generation > 0),
    owner_attempt_hash bytea NOT NULL CHECK (octet_length(owner_attempt_hash) = 32),
    cluster_id text NOT NULL,
    namespace text NOT NULL,
    logical_engine text NOT NULL,
    pod_uid text NOT NULL,
    endpoint_epoch bigint NOT NULL,
    recovery_epoch bigint NOT NULL,
    slot_cost integer NOT NULL CHECK (slot_cost > 0),
    state text NOT NULL CHECK (state IN (
        'reserved', 'abandoned_rerank', 'dispatch_authorized', 'streaming', 'released', 'orphaned'
    )),
    is_current boolean NOT NULL DEFAULT true,
    capability_algorithm text NOT NULL CHECK (capability_algorithm IN ('legacy_nonce_prefixed_v1', 'aes_256_gcm_v1')),
    capability_kek_version integer NOT NULL CHECK (capability_kek_version > 0),
    capability_wrapped_data_key bytea,
    capability_nonce bytea,
    capability_ciphertext bytea NOT NULL CHECK (octet_length(capability_ciphertext) > 0),
    capability_comparison_version integer NOT NULL CHECK (capability_comparison_version > 0),
    capability_comparison_hash bytea NOT NULL CHECK (octet_length(capability_comparison_hash) = 32),
    execution_deadline timestamptz NOT NULL,
    classification_after timestamptz NOT NULL,
    capability_retry_deadline timestamptz NOT NULL,
    terminal_proof_kind text CHECK (terminal_proof_kind IN (
        'complete_response', 'provider_finish', 'not_sent', 'client_give_up',
        'classification', 'ambiguity', 'identity_gone', 'unsafe_override'
    )),
    terminal_proof_hash bytea CHECK (terminal_proof_hash IS NULL OR octet_length(terminal_proof_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    terminal_at timestamptz,
    UNIQUE (request_id, request_generation),
    FOREIGN KEY (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
        REFERENCES instance_capacity (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch),
    CHECK (classification_after >= execution_deadline),
    CHECK (capability_retry_deadline >= execution_deadline),
    CHECK (
        (capability_algorithm = 'legacy_nonce_prefixed_v1'
            AND capability_wrapped_data_key IS NULL AND capability_nonce IS NULL)
        OR
        (capability_algorithm = 'aes_256_gcm_v1'
            AND octet_length(capability_wrapped_data_key) > 0 AND octet_length(capability_nonce) = 12)
    ),
    CHECK ((state IN ('released', 'orphaned') AND terminal_at IS NOT NULL)
        OR (state NOT IN ('released', 'orphaned') AND terminal_at IS NULL))
);

CREATE UNIQUE INDEX reservations_current_request_idx ON reservations (request_id) WHERE is_current;
CREATE INDEX reservations_classification_idx
    ON reservations (classification_after, reservation_id)
    WHERE state IN ('reserved', 'dispatch_authorized', 'streaming');
CREATE INDEX reservations_capability_retention_idx
    ON reservations (capability_retry_deadline, reservation_id);
CREATE INDEX reservations_terminal_retention_idx
    ON reservations (terminal_at, reservation_id) WHERE terminal_at IS NOT NULL;

INSERT INTO reservations (
    reservation_id, request_id, request_generation, owner_attempt_hash,
    cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch,
    slot_cost, state, capability_algorithm, capability_kek_version,
    capability_ciphertext, capability_comparison_version, capability_comparison_hash,
    execution_deadline, classification_after, capability_retry_deadline,
    terminal_proof_kind, terminal_proof_hash, created_at, updated_at, terminal_at
)
SELECT
    r.reservation_id, r.request_id, r.request_generation, q.owner_attempt_hash,
    r.cluster, r.namespace, r.logical_engine, r.pod_uid, r.endpoint_epoch, r.recovery_epoch,
    r.slot_cost,
    CASE WHEN r.state = 'given_up' THEN 'released' ELSE r.state END,
    'legacy_nonce_prefixed_v1', 1, r.capability_ciphertext, 1, r.capability_hash,
    r.execution_deadline, q.classification_after, q.mutation_retry_deadline,
    CASE r.state
        WHEN 'given_up' THEN 'client_give_up'
        WHEN 'orphaned' THEN 'ambiguity'
        WHEN 'released' THEN 'provider_finish'
        ELSE NULL
    END,
    CASE WHEN r.state IN ('given_up', 'orphaned', 'released') THEN r.command_hash ELSE NULL END,
    r.created_at, r.updated_at,
    CASE WHEN r.state IN ('given_up', 'orphaned', 'released') THEN r.updated_at ELSE NULL END
FROM scheduler_reservations r
JOIN request_records q USING (request_id);

-- The old grant schema incorrectly made a retained rerank grant own one
-- reservation forever.  A grant belongs to the logical request instead.
ALTER TABLE admission_grants
    DROP CONSTRAINT admission_grants_reservation_id_key,
    DROP CONSTRAINT admission_grants_reservation_id_fkey;
ALTER TABLE admission_grants ALTER COLUMN reservation_id DROP NOT NULL;

UPDATE admission_grants g
SET state = 'retained_rerank', updated_at = transaction_timestamp()
FROM scheduler_reservations r
WHERE r.reservation_id = g.reservation_id
  AND r.state = 'abandoned_rerank'
  AND g.state = 'active_reserved';

INSERT INTO tenant_counters (tenant_hash, grant_limit, active_grants, orphaned_grants)
SELECT r.tenant_hash,
    sum(CASE WHEN r.state IN ('reserved', 'abandoned_rerank', 'dispatch_authorized') THEN r.slot_cost ELSE 0 END),
    0, 0
FROM scheduler_reservations r
WHERE NOT EXISTS (SELECT 1 FROM tenant_counters t WHERE t.tenant_hash = r.tenant_hash)
GROUP BY r.tenant_hash;

INSERT INTO admission_grants (
    grant_id, request_id, reservation_id, tenant_hash, slot_cost, state,
    execution_deadline, classification_after, created_at, updated_at
)
SELECT 'legacy-grant-' || r.reservation_id, r.request_id, r.reservation_id,
    r.tenant_hash, r.slot_cost,
    CASE r.state
        WHEN 'abandoned_rerank' THEN 'retained_rerank'
        WHEN 'orphaned' THEN 'orphaned'
        WHEN 'reserved' THEN 'active_reserved'
        WHEN 'dispatch_authorized' THEN 'active_reserved'
        ELSE 'released'
    END,
    r.execution_deadline, q.classification_after, r.created_at, r.updated_at
FROM scheduler_reservations r
JOIN request_records q USING (request_id)
WHERE NOT EXISTS (SELECT 1 FROM admission_grants g WHERE g.request_id = r.request_id);

WITH totals AS (
    SELECT tenant_hash,
        COALESCE(sum(active_contribution), 0)::integer AS active_total,
        COALESCE(sum(orphaned_contribution), 0)::integer AS orphaned_total
    FROM admission_grants GROUP BY tenant_hash
)
UPDATE tenant_counters t
SET active_grants = totals.active_total,
    orphaned_grants = totals.orphaned_total,
    grant_limit = GREATEST(t.grant_limit, totals.active_total + totals.orphaned_total),
    version = t.version + 1
FROM totals WHERE totals.tenant_hash = t.tenant_hash;

ALTER TABLE admission_grants
    ADD CONSTRAINT admission_grants_request_record_fkey
        FOREIGN KEY (request_id) REFERENCES request_records (request_id),
    ADD CONSTRAINT admission_grants_authoritative_reservation_fkey
        FOREIGN KEY (reservation_id) REFERENCES reservations (reservation_id),
    ADD CONSTRAINT admission_grants_reservation_shape CHECK (
        (state = 'retained_rerank' AND reservation_id IS NULL)
        OR (state <> 'retained_rerank' AND reservation_id IS NOT NULL)
    ) NOT VALID;
UPDATE admission_grants SET reservation_id = NULL WHERE state = 'retained_rerank';
ALTER TABLE admission_grants VALIDATE CONSTRAINT admission_grants_reservation_shape;

CREATE TABLE orphaned_capacity_debts (
    debt_id text PRIMARY KEY CHECK (octet_length(debt_id) BETWEEN 1 AND 256),
    reservation_id text NOT NULL UNIQUE REFERENCES reservations (reservation_id),
    request_id text NOT NULL REFERENCES request_records (request_id),
    tenant_hash bytea NOT NULL REFERENCES tenant_counters (tenant_hash),
    cluster_id text NOT NULL,
    namespace text NOT NULL,
    logical_engine text NOT NULL,
    pod_uid text NOT NULL,
    endpoint_epoch bigint NOT NULL,
    recovery_epoch bigint NOT NULL,
    slot_cost integer NOT NULL CHECK (slot_cost > 0),
    cause text NOT NULL CHECK (cause IN (
        'ambiguous_transport', 'ambiguous_canceled', 'ambiguous_protocol',
        'classification_timeout', 'legacy_orphan'
    )),
    state text NOT NULL CHECK (state IN (
        'active', 'resolved_provider_termination', 'resolved_identity_gone', 'unsafe_overridden'
    )),
    resolution_evidence_type text CHECK (resolution_evidence_type IN (
        'provider_termination', 'identity_gone', 'unsafe_override'
    )),
    resolution_evidence_hash bytea CHECK (
        resolution_evidence_hash IS NULL OR octet_length(resolution_evidence_hash) = 32
    ),
    resolution_actor_hash bytea CHECK (resolution_actor_hash IS NULL OR octet_length(resolution_actor_hash) = 32),
    override_ticket text CHECK (override_ticket IS NULL OR octet_length(override_ticket) BETWEEN 1 AND 256),
    override_reason text CHECK (override_reason IS NULL OR octet_length(override_reason) BETWEEN 1 AND 2048),
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    resolved_at timestamptz,
    FOREIGN KEY (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
        REFERENCES instance_capacity (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch),
    CHECK (
        (state = 'active' AND resolution_evidence_type IS NULL
            AND resolution_evidence_hash IS NULL AND resolution_actor_hash IS NULL
            AND override_ticket IS NULL AND override_reason IS NULL AND resolved_at IS NULL)
        OR
        (state = 'resolved_provider_termination' AND resolution_evidence_type = 'provider_termination'
            AND resolution_evidence_hash IS NOT NULL AND resolution_actor_hash IS NOT NULL
            AND override_ticket IS NULL AND override_reason IS NULL AND resolved_at IS NOT NULL)
        OR
        (state = 'resolved_identity_gone' AND resolution_evidence_type = 'identity_gone'
            AND resolution_evidence_hash IS NOT NULL AND resolution_actor_hash IS NOT NULL
            AND override_ticket IS NULL AND override_reason IS NULL AND resolved_at IS NOT NULL)
        OR
        (state = 'unsafe_overridden' AND resolution_evidence_type = 'unsafe_override'
            AND resolution_evidence_hash IS NOT NULL AND resolution_actor_hash IS NOT NULL
            AND override_ticket IS NOT NULL AND override_reason IS NOT NULL AND resolved_at IS NOT NULL)
    )
);

INSERT INTO orphaned_capacity_debts (
    debt_id, reservation_id, request_id, tenant_hash,
    cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch,
    slot_cost, cause, state, resolution_evidence_type, resolution_evidence_hash,
    resolution_actor_hash, created_at, updated_at, resolved_at
)
SELECT d.debt_id, d.reservation_id, r.request_id, d.tenant_hash,
    d.cluster, d.namespace, d.logical_engine, d.pod_uid, d.endpoint_epoch, d.recovery_epoch,
    d.slot_cost, d.cause, d.state,
    CASE WHEN d.state = 'resolved_identity_gone' THEN 'identity_gone' ELSE NULL END,
    d.resolution_evidence_hash,
    CASE WHEN d.state = 'resolved_identity_gone'
        THEN decode(md5('legacy-resolution') || md5('legacy-resolution/actor'), 'hex') ELSE NULL END,
    d.created_at, d.updated_at, d.resolved_at
FROM capacity_debts d
JOIN reservations r USING (reservation_id);

CREATE INDEX orphaned_capacity_debts_active_idx
    ON orphaned_capacity_debts (created_at, debt_id) WHERE state = 'active';
CREATE INDEX orphaned_capacity_debts_retention_idx
    ON orphaned_capacity_debts (resolved_at, debt_id) WHERE state <> 'active';

CREATE TABLE source_observations (
    cluster_id text NOT NULL,
    namespace text NOT NULL,
    logical_engine text NOT NULL,
    pod_uid text NOT NULL,
    endpoint_epoch bigint NOT NULL,
    recovery_epoch bigint NOT NULL,
    source_kind text NOT NULL CHECK (source_kind IN (
        'kubernetes_pod', 'kubernetes_workload', 'engine_health', 'engine_membership'
    )),
    writer_generation bigint NOT NULL CHECK (writer_generation > 0),
    source_sequence bigint NOT NULL CHECK (source_sequence > 0),
    accepted_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    expires_at timestamptz NOT NULL,
    ttl_policy_version integer NOT NULL CHECK (ttl_policy_version > 0),
    schema_version integer NOT NULL CHECK (schema_version > 0),
    normalized_payload jsonb NOT NULL CHECK (jsonb_typeof(normalized_payload) = 'object'),
    diagnostic_source_time timestamptz,
    PRIMARY KEY (
        cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch, source_kind
    ),
    FOREIGN KEY (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
        REFERENCES instance_capacity (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch),
    CHECK (expires_at > accepted_at)
);

INSERT INTO source_observations (
    cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch,
    source_kind, writer_generation, source_sequence, accepted_at, expires_at,
    ttl_policy_version, schema_version, normalized_payload, diagnostic_source_time
)
SELECT b.cluster, b.namespace, b.logical_engine, b.pod_uid, b.endpoint_epoch, b.recovery_epoch,
    'kubernetes_pod', COALESCE(g.current_generation, 1), 1,
    transaction_timestamp(),
    GREATEST(b.eligible_until, transaction_timestamp() + interval '1 microsecond'),
    1, 1,
    jsonb_build_object(
        'source_namespace', b.source_namespace,
        'source_name', b.source_name,
        'healthy', b.healthy,
        'eligible_until', b.eligible_until
    ),
    NULL
FROM scheduler_backends b
LEFT JOIN controller_writer_generations g ON g.cluster = b.cluster;
CREATE INDEX source_observations_expiry_idx ON source_observations (expires_at, source_kind);

CREATE TABLE workload_operations (
    cluster_id text NOT NULL REFERENCES system_admission_state (cluster_id),
    workload_uid text NOT NULL CHECK (octet_length(workload_uid) BETWEEN 1 AND 256),
    operation_generation bigint NOT NULL CHECK (operation_generation > 0),
    operation_token text NOT NULL UNIQUE CHECK (octet_length(operation_token) BETWEEN 1 AND 256),
    writer_generation bigint NOT NULL CHECK (writer_generation > 0),
    intent text NOT NULL CHECK (intent IN ('drain', 'scale', 'delete', 'evict', 'handoff_recovery')),
    desired_replicas integer CHECK (desired_replicas IS NULL OR desired_replicas >= 0),
    phase text NOT NULL CHECK (phase IN (
        'barrier_pending', 'barrier_observed', 'mutation_pending', 'observing_victims', 'completed', 'failed'
    )),
    prior_workload_resource_version text CHECK (
        prior_workload_resource_version IS NULL OR octet_length(prior_workload_resource_version) BETWEEN 1 AND 256
    ),
    current_workload_resource_version text CHECK (
        current_workload_resource_version IS NULL OR octet_length(current_workload_resource_version) BETWEEN 1 AND 256
    ),
    old_calls_quiescent_after timestamptz NOT NULL,
    barrier_observed_at timestamptz,
    pod_token_resource_versions jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(pod_token_resource_versions) = 'object'),
    before_victim_uids text[] NOT NULL DEFAULT '{}',
    actual_victim_uids text[] NOT NULL DEFAULT '{}',
    completion_proof_hash bytea CHECK (completion_proof_hash IS NULL OR octet_length(completion_proof_hash) = 32),
    is_current boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (cluster_id, workload_uid, operation_generation),
    CHECK (cardinality(before_victim_uids) <= 4096 AND cardinality(actual_victim_uids) <= 4096),
    CHECK ((phase = 'completed' AND barrier_observed_at IS NOT NULL
        AND completion_proof_hash IS NOT NULL AND completed_at IS NOT NULL)
        OR phase <> 'completed')
);
CREATE UNIQUE INDEX workload_operations_current_idx
    ON workload_operations (cluster_id, workload_uid) WHERE is_current;
CREATE INDEX workload_operations_incomplete_idx
    ON workload_operations (cluster_id, updated_at) WHERE phase NOT IN ('completed', 'failed');
CREATE INDEX workload_operations_retention_idx
    ON workload_operations (completed_at, cluster_id, workload_uid) WHERE completed_at IS NOT NULL;

CREATE TABLE drain_intents (
    drain_id text PRIMARY KEY CHECK (octet_length(drain_id) BETWEEN 1 AND 256),
    cluster_id text NOT NULL,
    namespace text,
    logical_engine text,
    pod_uid text,
    endpoint_epoch bigint,
    recovery_epoch bigint,
    scope_kind text NOT NULL CHECK (scope_kind IN ('workload', 'exact_identity')),
    workload_uid text CHECK (workload_uid IS NULL OR octet_length(workload_uid) BETWEEN 1 AND 256),
    state text NOT NULL CHECK (state IN ('active', 'barrier_pending', 'barrier_observed', 'cleared')),
    reason text NOT NULL CHECK (octet_length(reason) BETWEEN 1 AND 2048),
    requested_deadline timestamptz,
    hard_deadline timestamptz,
    writer_generation bigint NOT NULL CHECK (writer_generation > 0),
    operation_generation bigint,
    forced_actor_hash bytea CHECK (forced_actor_hash IS NULL OR octet_length(forced_actor_hash) = 32),
    forced_reason text CHECK (forced_reason IS NULL OR octet_length(forced_reason) BETWEEN 1 AND 2048),
    requested_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    cleared_at timestamptz,
    FOREIGN KEY (cluster_id) REFERENCES system_admission_state (cluster_id),
    FOREIGN KEY (cluster_id, workload_uid, operation_generation)
        REFERENCES workload_operations (cluster_id, workload_uid, operation_generation),
    FOREIGN KEY (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
        REFERENCES instance_capacity (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch),
    CHECK (requested_deadline IS NULL OR hard_deadline IS NULL OR hard_deadline >= requested_deadline),
    CHECK (
        (scope_kind = 'workload' AND workload_uid IS NOT NULL
            AND namespace IS NULL AND logical_engine IS NULL AND pod_uid IS NULL
            AND endpoint_epoch IS NULL AND recovery_epoch IS NULL)
        OR
        (scope_kind = 'exact_identity' AND workload_uid IS NULL
            AND namespace IS NOT NULL AND logical_engine IS NOT NULL AND pod_uid IS NOT NULL
            AND endpoint_epoch IS NOT NULL AND recovery_epoch IS NOT NULL)
    ),
    CHECK ((operation_generation IS NULL AND workload_uid IS NULL)
        OR (operation_generation IS NOT NULL AND workload_uid IS NOT NULL)),
    CHECK ((state = 'cleared' AND cleared_at IS NOT NULL) OR (state <> 'cleared' AND cleared_at IS NULL)),
    CHECK ((forced_actor_hash IS NULL AND forced_reason IS NULL)
        OR (forced_actor_hash IS NOT NULL AND forced_reason IS NOT NULL))
);
CREATE UNIQUE INDEX drain_intents_exact_active_idx
    ON drain_intents (cluster_id, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
    WHERE scope_kind = 'exact_identity' AND state <> 'cleared';
CREATE UNIQUE INDEX drain_intents_workload_active_idx
    ON drain_intents (cluster_id, workload_uid)
    WHERE scope_kind = 'workload' AND state <> 'cleared';
CREATE INDEX drain_intents_deadline_idx
    ON drain_intents (hard_deadline, drain_id) WHERE state <> 'cleared';
CREATE INDEX drain_intents_retention_idx
    ON drain_intents (cleared_at, drain_id) WHERE state = 'cleared';

CREATE TABLE capability_manifests (
    manifest_id text NOT NULL CHECK (octet_length(manifest_id) BETWEEN 1 AND 256),
    manifest_version bigint NOT NULL CHECK (manifest_version > 0),
    image_digest text NOT NULL CHECK (octet_length(image_digest) BETWEEN 1 AND 512),
    proxy_digest text NOT NULL CHECK (octet_length(proxy_digest) BETWEEN 1 AND 512),
    supported_routes jsonb NOT NULL CHECK (jsonb_typeof(supported_routes) = 'array'),
    supported_fields jsonb NOT NULL CHECK (jsonb_typeof(supported_fields) = 'object'),
    response_parsers jsonb NOT NULL CHECK (jsonb_typeof(response_parsers) = 'array'),
    identity_profile jsonb NOT NULL CHECK (jsonb_typeof(identity_profile) = 'object'),
    apc_isolation_mode text NOT NULL CHECK (apc_isolation_mode IN ('disabled', 'tenant_scoped', 'request_scoped')),
    termination_capabilities jsonb CHECK (termination_capabilities IS NULL OR jsonb_typeof(termination_capabilities) = 'object'),
    cache_capabilities jsonb CHECK (cache_capabilities IS NULL OR jsonb_typeof(cache_capabilities) = 'object'),
    mover_capabilities jsonb CHECK (mover_capabilities IS NULL OR jsonb_typeof(mover_capabilities) = 'object'),
    signature_algorithm text NOT NULL CHECK (octet_length(signature_algorithm) BETWEEN 1 AND 128),
    signature_key_version integer NOT NULL CHECK (signature_key_version > 0),
    signature bytea NOT NULL CHECK (octet_length(signature) > 0),
    valid_from timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (manifest_id, manifest_version),
    CHECK (valid_until > valid_from)
);
CREATE INDEX capability_manifests_validity_idx
    ON capability_manifests (valid_until, manifest_id, manifest_version);

INSERT INTO capability_manifests (
    manifest_id, manifest_version, image_digest, proxy_digest,
    supported_routes, supported_fields, response_parsers, identity_profile,
    apc_isolation_mode, signature_algorithm, signature_key_version, signature,
    valid_from, valid_until
)
SELECT 'legacy', 1, 'legacy:unknown', 'legacy:unknown',
    '[]'::jsonb, '{}'::jsonb, '[]'::jsonb,
    '{"status":"unverified_legacy"}'::jsonb, 'disabled',
    'legacy_unverified', 1, decode('00', 'hex'),
    '-infinity'::timestamptz, 'infinity'::timestamptz
WHERE EXISTS (SELECT 1 FROM instance_projections);

ALTER TABLE instance_projections
    ADD CONSTRAINT instance_projections_capability_manifest_fkey
    FOREIGN KEY (capability_manifest_id, capability_manifest_version)
    REFERENCES capability_manifests (manifest_id, manifest_version);

CREATE TABLE cache_manifests (
    cache_manifest_id text PRIMARY KEY CHECK (octet_length(cache_manifest_id) BETWEEN 1 AND 256),
    cache_identity_version integer,
    cache_identity_hmac bytea,
    tenant_scope_hash bytea NOT NULL CHECK (octet_length(tenant_scope_hash) = 32),
    model_fingerprint bytea NOT NULL CHECK (octet_length(model_fingerprint) = 32),
    image_digest text NOT NULL CHECK (octet_length(image_digest) BETWEEN 1 AND 512),
    adapter_fingerprint bytea CHECK (adapter_fingerprint IS NULL OR octet_length(adapter_fingerprint) = 32),
    tokenizer_fingerprint bytea NOT NULL CHECK (octet_length(tokenizer_fingerprint) = 32),
    dtype text NOT NULL CHECK (octet_length(dtype) BETWEEN 1 AND 128),
    parallelism_profile jsonb NOT NULL CHECK (jsonb_typeof(parallelism_profile) = 'object'),
    identity_profile_version integer NOT NULL CHECK (identity_profile_version > 0),
    opaque_adapter_location text NOT NULL CHECK (octet_length(opaque_adapter_location) BETWEEN 1 AND 2048),
    completeness text NOT NULL CHECK (completeness IN ('complete', 'partial')),
    state text NOT NULL CHECK (state IN ('available', 'loading', 'retired')),
    expires_at timestamptz NOT NULL,
    manifest jsonb NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CHECK (
        (cache_identity_version IS NULL AND cache_identity_hmac IS NULL)
        OR (cache_identity_version > 0 AND octet_length(cache_identity_hmac) = 32)
    )
);
CREATE UNIQUE INDEX cache_manifests_identity_idx
    ON cache_manifests (tenant_scope_hash, cache_identity_version, cache_identity_hmac)
    WHERE cache_identity_version IS NOT NULL;
CREATE INDEX cache_manifests_expiry_idx ON cache_manifests (expires_at, cache_manifest_id);

CREATE TABLE audit_events (
    event_id text PRIMARY KEY CHECK (octet_length(event_id) BETWEEN 1 AND 256),
    event_type text NOT NULL CHECK (event_type IN (
        'recovery_fenced', 'recovery_reopened', 'write_version_changed',
        'debt_resolved', 'debt_unsafe_overridden', 'drain_forced', 'operation_completed'
    )),
    actor_identity_hash bytea NOT NULL CHECK (octet_length(actor_identity_hash) = 32),
    service_identity_hash bytea NOT NULL CHECK (octet_length(service_identity_hash) = 32),
    target_type text NOT NULL CHECK (octet_length(target_type) BETWEEN 1 AND 128),
    target_hash bytea NOT NULL CHECK (octet_length(target_hash) = 32),
    debt_id text,
    reason text CHECK (reason IS NULL OR octet_length(reason) BETWEEN 1 AND 2048),
    ticket text CHECK (ticket IS NULL OR octet_length(ticket) BETWEEN 1 AND 256),
    event_metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(event_metadata) = 'object'),
    occurred_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    FOREIGN KEY (debt_id) REFERENCES orphaned_capacity_debts (debt_id),
    CHECK ((event_type = 'debt_unsafe_overridden' AND debt_id IS NOT NULL
        AND reason IS NOT NULL AND ticket IS NOT NULL) OR event_type <> 'debt_unsafe_overridden')
);
CREATE UNIQUE INDEX audit_events_unsafe_debt_idx
    ON audit_events (debt_id) WHERE event_type = 'debt_unsafe_overridden';
CREATE INDEX audit_events_retention_idx ON audit_events (occurred_at, event_id);
CREATE INDEX audit_events_target_idx ON audit_events (target_hash, occurred_at);

CREATE FUNCTION reject_audit_event_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'audit_events is append-only';
END
$$;

CREATE TRIGGER audit_events_append_only
BEFORE UPDATE OR DELETE ON audit_events
FOR EACH ROW EXECUTE FUNCTION reject_audit_event_mutation();

CREATE TRIGGER audit_events_no_truncate
BEFORE TRUNCATE ON audit_events
FOR EACH STATEMENT EXECUTE FUNCTION reject_audit_event_mutation();

COMMIT;
