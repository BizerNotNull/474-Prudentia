-- Cache metadata remains advisory, but every accepted row is tenant-private,
-- request-specific, complete, short-lived, and bound to exact signed manifests.
CREATE TABLE cache_identity_versions (
    singleton boolean NOT NULL DEFAULT true CHECK (singleton),
    current_write_version integer NOT NULL CHECK (current_write_version > 0),
    retained_read_versions integer[] NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (singleton),
    CHECK (cardinality(retained_read_versions) BETWEEN 1 AND 8),
    CHECK (current_write_version = ANY(retained_read_versions))
);

ALTER TABLE cache_manifests
    ADD COLUMN provider_manifest_id text,
    ADD COLUMN provider_manifest_version bigint,
    ADD COLUMN provider_manifest_digest bytea,
    ADD COLUMN proxy_digest text,
    ADD COLUMN connector_manifest_digest bytea,
    ADD COLUMN source_pod_uid text,
    ADD COLUMN source_endpoint_epoch bigint,
    ADD COLUMN source_recovery_epoch bigint,
    ADD COLUMN model_config_digest bytea,
    ADD COLUMN cache_format_version integer,
    ADD COLUMN cache_content_version integer,
    ADD COLUMN block_size integer,
    ADD COLUMN quantization text,
    ADD COLUMN attention_backend text,
    ADD COLUMN tensor_parallel integer,
    ADD COLUMN data_parallel integer,
    ADD COLUMN gpu_architecture text,
    ADD COLUMN driver_version text,
    ADD COLUMN cache_layout text;

-- Legacy/incomplete rows can never produce a locality hit.
UPDATE cache_manifests SET state = 'retired' WHERE completeness <> 'complete';

ALTER TABLE cache_manifests
    ADD CONSTRAINT cache_manifest_exact_compatibility CHECK (
        state = 'retired' OR (
            completeness = 'complete'
            AND cache_identity_version > 0
            AND octet_length(cache_identity_hmac) = 32
            AND octet_length(provider_manifest_id) BETWEEN 1 AND 256
            AND provider_manifest_version > 0
            AND octet_length(provider_manifest_digest) = 32
            AND octet_length(proxy_digest) BETWEEN 1 AND 512
            AND octet_length(connector_manifest_digest) = 32
            AND octet_length(source_pod_uid) BETWEEN 1 AND 256
            AND source_endpoint_epoch > 0
            AND source_recovery_epoch > 0
            AND octet_length(model_config_digest) = 32
            AND cache_format_version > 0 AND cache_content_version > 0
            AND block_size > 0
            AND octet_length(quantization) BETWEEN 1 AND 128
            AND octet_length(attention_backend) BETWEEN 1 AND 128
            AND tensor_parallel > 0 AND data_parallel > 0
            AND octet_length(gpu_architecture) BETWEEN 1 AND 128
            AND octet_length(driver_version) BETWEEN 1 AND 128
            AND octet_length(cache_layout) BETWEEN 1 AND 128
        )
    );

ALTER TABLE cache_manifests
    ADD CONSTRAINT cache_manifest_provider_revision_fkey
    FOREIGN KEY (provider_manifest_id, provider_manifest_version)
    REFERENCES capability_manifests (manifest_id, manifest_version);

CREATE OR REPLACE FUNCTION enforce_cache_manifest_ttl() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state <> 'retired' AND (
        NEW.expires_at <= transaction_timestamp()
        OR NEW.expires_at > transaction_timestamp() + interval '5 minutes'
    ) THEN
        RAISE EXCEPTION 'cache metadata TTL outside authoritative bound';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER cache_manifests_ttl_guard
BEFORE INSERT OR UPDATE ON cache_manifests
FOR EACH ROW EXECUTE FUNCTION enforce_cache_manifest_ttl();

CREATE INDEX cache_manifests_exact_lookup_idx ON cache_manifests (
    tenant_scope_hash, cache_identity_version, cache_identity_hmac,
    provider_manifest_digest, connector_manifest_digest, expires_at
) WHERE state = 'available' AND completeness = 'complete';
