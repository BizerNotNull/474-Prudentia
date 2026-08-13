BEGIN;

CREATE TABLE recovery_fleet_members (
    recovery_epoch bigint NOT NULL CHECK (recovery_epoch > 0),
    pod_uid text NOT NULL CHECK (octet_length(pod_uid) BETWEEN 1 AND 256),
    captured_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (recovery_epoch, pod_uid)
);

CREATE INDEX recovery_fleet_members_retention_idx
    ON recovery_fleet_members (captured_at, recovery_epoch);

COMMIT;
