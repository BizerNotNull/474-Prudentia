DROP INDEX scheduler_reservations_idempotency_key_idx;

ALTER TABLE scheduler_reservations
    DROP CONSTRAINT scheduler_reservations_idempotency_shape,
    DROP COLUMN request_digest,
    DROP COLUMN digest_version,
    DROP COLUMN lookup_hmac,
    DROP COLUMN lookup_version;

DROP TABLE scheduler_crypto_versions;
