DROP INDEX IF EXISTS cache_manifests_exact_lookup_idx;
DROP TRIGGER IF EXISTS cache_manifests_ttl_guard ON cache_manifests;
DROP FUNCTION IF EXISTS enforce_cache_manifest_ttl();
ALTER TABLE cache_manifests
    DROP CONSTRAINT IF EXISTS cache_manifest_provider_revision_fkey,
    DROP CONSTRAINT IF EXISTS cache_manifest_exact_compatibility,
    DROP COLUMN IF EXISTS provider_manifest_id,
    DROP COLUMN IF EXISTS provider_manifest_version,
    DROP COLUMN IF EXISTS provider_manifest_digest,
    DROP COLUMN IF EXISTS proxy_digest,
    DROP COLUMN IF EXISTS connector_manifest_digest,
    DROP COLUMN IF EXISTS source_pod_uid,
    DROP COLUMN IF EXISTS source_endpoint_epoch,
    DROP COLUMN IF EXISTS source_recovery_epoch,
    DROP COLUMN IF EXISTS model_config_digest,
    DROP COLUMN IF EXISTS cache_format_version,
    DROP COLUMN IF EXISTS cache_content_version,
    DROP COLUMN IF EXISTS block_size,
    DROP COLUMN IF EXISTS quantization,
    DROP COLUMN IF EXISTS attention_backend,
    DROP COLUMN IF EXISTS tensor_parallel,
    DROP COLUMN IF EXISTS data_parallel,
    DROP COLUMN IF EXISTS gpu_architecture,
    DROP COLUMN IF EXISTS driver_version,
    DROP COLUMN IF EXISTS cache_layout;
DROP TABLE IF EXISTS cache_identity_versions;
