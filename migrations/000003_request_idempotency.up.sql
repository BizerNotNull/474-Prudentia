CREATE TABLE scheduler_crypto_versions (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    lookup_write_version integer NOT NULL CHECK (lookup_write_version > 0),
    digest_write_version integer NOT NULL CHECK (digest_write_version > 0)
);

INSERT INTO scheduler_crypto_versions (singleton, lookup_write_version, digest_write_version)
VALUES (true, 1, 1);

ALTER TABLE scheduler_reservations
    ADD COLUMN lookup_version integer,
    ADD COLUMN lookup_hmac bytea,
    ADD COLUMN digest_version integer,
    ADD COLUMN request_digest bytea,
    ADD CONSTRAINT scheduler_reservations_idempotency_shape CHECK (
        (lookup_version IS NULL AND lookup_hmac IS NULL AND digest_version IS NULL AND request_digest IS NULL)
        OR
        (lookup_version > 0 AND octet_length(lookup_hmac) = 32 AND digest_version > 0 AND octet_length(request_digest) = 32)
    );

CREATE UNIQUE INDEX scheduler_reservations_idempotency_key_idx
    ON scheduler_reservations (tenant_hash, lookup_version, lookup_hmac)
    WHERE lookup_version IS NOT NULL;
