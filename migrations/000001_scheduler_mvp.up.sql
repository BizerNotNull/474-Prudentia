CREATE TABLE scheduler_backends (
    cluster text NOT NULL,
    namespace text NOT NULL,
    logical_engine text NOT NULL,
    pod_uid text NOT NULL,
    endpoint_epoch bigint NOT NULL CHECK (endpoint_epoch > 0),
    recovery_epoch bigint NOT NULL CHECK (recovery_epoch > 0),
    model text NOT NULL,
    endpoint text NOT NULL,
    configured_slots integer NOT NULL CHECK (configured_slots > 0),
    admission_limit integer NOT NULL CHECK (admission_limit >= 0 AND admission_limit <= configured_slots),
    reserved_slots integer NOT NULL DEFAULT 0 CHECK (reserved_slots >= 0),
    orphaned_slots integer NOT NULL DEFAULT 0 CHECK (orphaned_slots >= 0),
    drain_active boolean NOT NULL DEFAULT false,
    healthy boolean NOT NULL DEFAULT false,
    eligible_until timestamptz NOT NULL,
    PRIMARY KEY (cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch),
    CHECK (reserved_slots + orphaned_slots <= configured_slots)
);

CREATE INDEX scheduler_backends_model_eligible_idx
    ON scheduler_backends (model, healthy, drain_active, eligible_until);

CREATE TABLE scheduler_reservations (
    reservation_id text PRIMARY KEY,
    request_id text NOT NULL UNIQUE,
    attempt_id text NOT NULL,
    command_hash bytea NOT NULL CHECK (octet_length(command_hash) = 32),
    tenant_hash bytea NOT NULL CHECK (octet_length(tenant_hash) = 32),
    model text NOT NULL,
    slot_cost integer NOT NULL CHECK (slot_cost > 0),
    request_generation bigint NOT NULL CHECK (request_generation > 0),
    cluster text NOT NULL,
    namespace text NOT NULL,
    logical_engine text NOT NULL,
    pod_uid text NOT NULL,
    endpoint_epoch bigint NOT NULL,
    recovery_epoch bigint NOT NULL,
    state text NOT NULL CHECK (state IN ('reserved', 'dispatch_authorized', 'released', 'given_up', 'orphaned')),
    capability_ciphertext bytea NOT NULL,
    capability_hash bytea NOT NULL CHECK (octet_length(capability_hash) = 32),
    execution_deadline timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
        REFERENCES scheduler_backends (cluster, namespace, logical_engine, pod_uid, endpoint_epoch, recovery_epoch)
);
