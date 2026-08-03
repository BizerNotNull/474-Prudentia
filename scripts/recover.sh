#!/bin/sh
set -eu

usage() { echo "usage: recover.sh fence-restore|reopen" >&2; exit 64; }
[ "$#" -eq 1 ] || usage
stage=$1
: "${CLUSTER_ID:?CLUSTER_ID is required}"
: "${RECOVERY_EPOCH:?RECOVERY_EPOCH is required}"
: "${ACTOR_IDENTITY_SHA256:?ACTOR_IDENTITY_SHA256 is required}"
: "${SERVICE_IDENTITY_SHA256:?SERVICE_IDENTITY_SHA256 is required}"
: "${RECOVERY_TIMEOUT_SECONDS:=1800}"

case "$CLUSTER_ID" in *[!A-Za-z0-9._-]*|'') echo "invalid CLUSTER_ID" >&2; exit 64;; esac
case "$RECOVERY_EPOCH:$RECOVERY_TIMEOUT_SECONDS" in *[!0-9:]*|0:*|*:0) echo "invalid epoch or timeout" >&2; exit 64;; esac
case "$ACTOR_IDENTITY_SHA256:$SERVICE_IDENTITY_SHA256" in *[!0-9a-f:]*) echo "identity hashes must be lowercase hex" >&2; exit 64;; esac
[ "${#ACTOR_IDENTITY_SHA256}" -eq 64 ] && [ "${#SERVICE_IDENTITY_SHA256}" -eq 64 ] || { echo "identity hashes must be SHA-256" >&2; exit 64; }
CLUSTER_SHA256="$(printf %s "$CLUSTER_ID" | sha256sum | cut -d ' ' -f 1)"

fence_sql() {
  psql -v ON_ERROR_STOP=1 -v cluster="$CLUSTER_ID" -v epoch="$RECOVERY_EPOCH" -v cluster_hash="$CLUSTER_SHA256" \
    -v actor="$ACTOR_IDENTITY_SHA256" -v service="$SERVICE_IDENTITY_SHA256" <<'SQL'
SELECT set_config('prudentia.cluster', :'cluster', false);
SELECT set_config('prudentia.epoch', :'epoch', false);
SELECT set_config('prudentia.actor', :'actor', false);
SELECT set_config('prudentia.service', :'service', false);
SELECT set_config('prudentia.cluster_hash', :'cluster_hash', false);
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended(current_setting('prudentia.cluster'), 0));
DO $block$
DECLARE old_epoch bigint;
BEGIN
  SELECT recovery_epoch INTO STRICT old_epoch FROM system_admission_state WHERE cluster_id = current_setting('prudentia.cluster') FOR UPDATE;
  IF current_setting('prudentia.epoch')::bigint <= old_epoch THEN RAISE EXCEPTION 'recovery epoch must increase'; END IF;
  UPDATE system_admission_state
     SET recovery_epoch = current_setting('prudentia.epoch')::bigint, admission_state = 'fenced', dispatch_state = 'fenced',
         fenced_at = transaction_timestamp(), fenced_by_hash = decode(current_setting('prudentia.actor'), 'hex'),
         fence_reason = 'possible-data-loss recovery', reopened_at = NULL,
         changed_at = transaction_timestamp(), changed_by_hash = decode(current_setting('prudentia.actor'), 'hex')
   WHERE cluster_id = current_setting('prudentia.cluster');
  INSERT INTO audit_events(event_id,event_type,actor_identity_hash,service_identity_hash,target_type,target_hash,reason,event_metadata)
  VALUES ('recovery-fenced-' || current_setting('prudentia.epoch'), 'recovery_fenced',
          decode(current_setting('prudentia.actor'),'hex'), decode(current_setting('prudentia.service'),'hex'),
          'cluster', decode(current_setting('prudentia.cluster_hash'),'hex'), 'possible-data-loss recovery',
          jsonb_build_object('recovery_epoch', current_setting('prudentia.epoch')::bigint));
END $block$;
COMMIT;
SQL
}

case "$stage" in
fence-restore)
  : "${INFRASTRUCTURE_FENCE_PROOF:?INFRASTRUCTURE_FENCE_PROOF is required}"
  : "${BACKUP_FILE:?BACKUP_FILE is required}"
  [ -r "$INFRASTRUCTURE_FENCE_PROOF" ] && [ -r "$BACKUP_FILE" ] || { echo "fence proof and backup must be readable" >&2; exit 66; }
  grep -qx 'ingress=closed dispatch=closed' "$INFRASTRUCTURE_FENCE_PROOF" || { echo "infrastructure ingress/dispatch fence proof rejected" >&2; exit 65; }
  sha256sum -c "$BACKUP_FILE.sha256"
  # Fence the current ledger before destructive restore; the external proof is the
  # authority while the restored database image temporarily contains old state.
  psql -v ON_ERROR_STOP=1 -v cluster="$CLUSTER_ID" -v actor="$ACTOR_IDENTITY_SHA256" <<'SQL'
UPDATE system_admission_state SET admission_state='fenced', dispatch_state='fenced',
  fenced_at=transaction_timestamp(), fenced_by_hash=decode(:'actor','hex'),
  fence_reason='PITR restore in progress', changed_at=transaction_timestamp(), changed_by_hash=decode(:'actor','hex')
WHERE cluster_id=:'cluster';
SQL
  timeout -s TERM -k 30 "$RECOVERY_TIMEOUT_SECONDS" pg_restore --clean --if-exists --no-owner --no-privileges --exit-on-error --dbname="$PGDATABASE" "$BACKUP_FILE"
  fence_sql
  ;;
reopen)
  : "${FLEET_REBUILD_PROOF:?FLEET_REBUILD_PROOF is required}"
  [ -r "$FLEET_REBUILD_PROOF" ] || { echo "fleet proof must be readable" >&2; exit 66; }
  jq -e --argjson epoch "$RECOVERY_EPOCH" '
    .recoveryEpoch == $epoch and .oldIdentityCount == 0 and .registryRebuilt == true and
    .capacityRebuilt == true and .restoredRowsReconciled == true and .controllerGeneration > 0 and
    (.proofDigest | test("^sha256:[0-9a-f]{64}$"))' "$FLEET_REBUILD_PROOF" >/dev/null
  proof_digest="$(jq -r .proofDigest "$FLEET_REBUILD_PROOF")"
  psql -v ON_ERROR_STOP=1 -v cluster="$CLUSTER_ID" -v epoch="$RECOVERY_EPOCH" -v cluster_hash="$CLUSTER_SHA256" \
    -v actor="$ACTOR_IDENTITY_SHA256" -v service="$SERVICE_IDENTITY_SHA256" -v proof="$proof_digest" <<'SQL'
SELECT set_config('prudentia.cluster', :'cluster', false);
SELECT set_config('prudentia.epoch', :'epoch', false);
SELECT set_config('prudentia.actor', :'actor', false);
SELECT set_config('prudentia.service', :'service', false);
SELECT set_config('prudentia.proof', :'proof', false);
SELECT set_config('prudentia.cluster_hash', :'cluster_hash', false);
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended(current_setting('prudentia.cluster'), 0));
DO $block$
DECLARE stale_capacity bigint; stale_observations bigint;
BEGIN
  PERFORM 1 FROM system_admission_state
    WHERE cluster_id=current_setting('prudentia.cluster')
      AND recovery_epoch=current_setting('prudentia.epoch')::bigint
      AND admission_state='fenced' AND dispatch_state='fenced' FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'matching closed recovery fence not found'; END IF;
  SELECT count(*) INTO stale_capacity FROM instance_capacity
    WHERE cluster_id=current_setting('prudentia.cluster')
      AND recovery_epoch <> current_setting('prudentia.epoch')::bigint AND NOT retired;
  SELECT count(*) INTO stale_observations FROM source_observations
    WHERE cluster_id=current_setting('prudentia.cluster')
      AND recovery_epoch <> current_setting('prudentia.epoch')::bigint;
  IF stale_capacity <> 0 OR stale_observations <> 0 THEN RAISE EXCEPTION 'fleet rebuild incomplete'; END IF;
  UPDATE system_admission_state SET admission_state='open', dispatch_state='open', fenced_at=NULL, fenced_by_hash=NULL,
    fence_reason=NULL, reopened_at=transaction_timestamp(), changed_at=transaction_timestamp(),
    changed_by_hash=decode(current_setting('prudentia.actor'),'hex')
    WHERE cluster_id=current_setting('prudentia.cluster');
  INSERT INTO audit_events(event_id,event_type,actor_identity_hash,service_identity_hash,target_type,target_hash,reason,event_metadata)
  VALUES ('recovery-reopened-' || current_setting('prudentia.epoch'), 'recovery_reopened',
    decode(current_setting('prudentia.actor'),'hex'), decode(current_setting('prudentia.service'),'hex'),
    'cluster', decode(current_setting('prudentia.cluster_hash'),'hex'), 'fleet rebuild proof accepted',
    jsonb_build_object('recovery_epoch',current_setting('prudentia.epoch')::bigint,'proof_digest',current_setting('prudentia.proof')));
END $block$;
COMMIT;
SQL
  ;;
*) usage ;;
esac
