//go:build integration

package integration_test

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type coldVersionConfig struct {
	schemaWrite          int
	lookupWrite          int
	digestWrite          int
	capabilityKEK        int
	capabilityComparison int
	classificationPolicy int
	cleanupPolicy        int
	manifestSchema       uint16
	manifestCapability   uint16
	manifestRevision     int
	signatureKey         int
	manifestSignature    uint16
	observationTTLPolicy int
	observationSchema    int
	projection           int
}

var coldVersions = coldVersionConfig{
	schemaWrite:          9,
	lookupWrite:          1,
	digestWrite:          1,
	capabilityKEK:        1,
	capabilityComparison: 1,
	classificationPolicy: 1,
	cleanupPolicy:        1,
	manifestSchema:       domain.CurrentManifestSchemaVersion,
	manifestCapability:   domain.CurrentCapabilityVersion,
	manifestRevision:     1,
	signatureKey:         1,
	manifestSignature:    domain.CurrentSignatureVersion,
	observationTTLPolicy: 1,
	observationSchema:    1,
	projection:           1,
}

func coldIdentity(t *testing.T) domain.WorkloadIdentity {
	t.Helper()
	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{
		Cluster: coldCluster, Namespace: coldNamespace, LogicalEngine: coldEngine, PodUID: coldPodUID,
		EndpointEpoch: coldEndpointEpoch, RecoveryEpoch: coldRecoveryEpoch,
	})
	if err != nil {
		t.Fatalf("create cold-path identity: %v", err)
	}
	return identity
}

func seedColdBackend(t *testing.T, pool *pgxpool.Pool, proxyEndpoint string, manifest manifestMaterial) {
	t.Helper()
	actor := sha256.Sum256([]byte("cold-inference-integration"))
	tenant := sha256.Sum256([]byte(coldTenant))
	model := sha256.Sum256([]byte(coldModel))
	config := sha256.Sum256([]byte("cold-config"))
	membership := sha256.Sum256([]byte(coldPodUID))
	identity := coldIdentity(t)
	ctx := t.Context()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin cold inference seed: %v", err)
	}
	defer tx.Rollback(ctx)
	exec := func(name, statement string, arguments ...any) {
		t.Helper()
		if _, err := tx.Exec(ctx, statement, arguments...); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	exec("admission state", `INSERT INTO system_admission_state(
		cluster_id,recovery_epoch,admission_state,dispatch_state,schema_write_version,
		lookup_write_version,digest_write_version,capability_kek_write_version,
		capability_comparison_write_version,classification_policy_version,
		cleanup_policy_version,changed_by_hash)
		VALUES($1,$2,'open','open',$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT(cluster_id) DO UPDATE SET
			recovery_epoch=EXCLUDED.recovery_epoch,admission_state='open',dispatch_state='open',
			schema_write_version=EXCLUDED.schema_write_version,
			lookup_write_version=EXCLUDED.lookup_write_version,
			digest_write_version=EXCLUDED.digest_write_version,
			capability_kek_write_version=EXCLUDED.capability_kek_write_version,
			capability_comparison_write_version=EXCLUDED.capability_comparison_write_version,
			classification_policy_version=EXCLUDED.classification_policy_version,
			cleanup_policy_version=EXCLUDED.cleanup_policy_version,
			fenced_at=NULL,fenced_by_hash=NULL,fence_reason=NULL,reopened_at=transaction_timestamp(),
			changed_at=transaction_timestamp(),changed_by_hash=EXCLUDED.changed_by_hash`,
		identity.Cluster(), identity.RecoveryEpoch(), coldVersions.schemaWrite,
		coldVersions.lookupWrite, coldVersions.digestWrite, coldVersions.capabilityKEK,
		coldVersions.capabilityComparison, coldVersions.classificationPolicy,
		coldVersions.cleanupPolicy, actor[:])
	exec("lookup versions", `INSERT INTO system_lookup_read_versions(cluster_id,version) VALUES($1,$2) ON CONFLICT DO NOTHING`, identity.Cluster(), coldVersions.lookupWrite)
	exec("digest versions", `INSERT INTO system_digest_read_versions(cluster_id,version) VALUES($1,$2) ON CONFLICT DO NOTHING`, identity.Cluster(), coldVersions.digestWrite)
	exec("capability manifest", `INSERT INTO capability_manifests(
		manifest_id,manifest_version,image_digest,proxy_digest,supported_routes,supported_fields,
		response_parsers,identity_profile,apc_isolation_mode,termination_capabilities,
		signature_algorithm,signature_key_version,signature,valid_from,valid_until)
		VALUES($1,$2,$3,$4,'["/health","/v1/chat/completions"]','{"messages":true,"model":true,"stream":true}',
		'["openai-chat-sse-v1"]','{"mode":"exact_workload_mtls"}','disabled','{}',
		'ed25519',$5,decode('01','hex'),transaction_timestamp()-interval '1 hour',transaction_timestamp()+interval '12 hours')`,
		coldManifestID, coldVersions.manifestRevision, manifest.providerDigest, manifest.proxyDigest, coldVersions.signatureKey)
	exec("tenant counter", `INSERT INTO tenant_counters(tenant_hash,grant_limit) VALUES($1,4)`, tenant[:])
	exec("capacity", `INSERT INTO instance_capacity(
		cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,
		physical_slots,admission_limit,projection_version)
		VALUES($1,$2,$3,$4,$5,$6,2,2,1)`,
		identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(),
		identity.EndpointEpoch(), identity.RecoveryEpoch())
	exec("observations", `INSERT INTO source_observations(
		cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,
		source_kind,writer_generation,source_sequence,accepted_at,expires_at,
		ttl_policy_version,schema_version,normalized_payload)
		VALUES
		($1,$2,$3,$4,$5::bigint,$6::bigint,'structural',1,1,transaction_timestamp(),transaction_timestamp()+interval '1 hour',$9,$10,
		 jsonb_build_object('endpoint',$7::text,'model',$8::text,'workload_uid','cold-workload','endpoint_epoch',$5::bigint,'recovery_epoch',$6::bigint)),
		($1,$2,$3,$4,$5::bigint,$6::bigint,'runtime_health',1,1,transaction_timestamp(),transaction_timestamp()+interval '1 hour',$9,$10,
		 '{"state":2,"warm":true}')`,
		identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(),
		identity.EndpointEpoch(), identity.RecoveryEpoch(), proxyEndpoint, coldModel,
		coldVersions.observationTTLPolicy, coldVersions.observationSchema)
	exec("projection", `INSERT INTO instance_projections(
		cluster_id,namespace,logical_engine,pod_uid,endpoint_epoch,recovery_epoch,
		normalized_proxy_endpoint,model_fingerprint,config_fingerprint,membership_fingerprint,
		capability_manifest_id,capability_manifest_version,source_stamps,health,projection_version)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'{}','healthy',$13)`,
		identity.Cluster(), identity.Namespace(), identity.LogicalEngine(), identity.PodUID(),
		identity.EndpointEpoch(), identity.RecoveryEpoch(), proxyEndpoint, model[:], config[:],
		membership[:], coldManifestID, coldVersions.manifestRevision, coldVersions.projection)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit cold inference seed: %v", err)
	}
}

func requireNoProcessExit(t *testing.T, handles ...*processHandle) {
	t.Helper()
	for _, handle := range handles {
		select {
		case <-handle.done:
			t.Fatalf("%s exited unexpectedly: %v\n%s", handle.name, handle.waitError(), handle.logs.String())
		default:
		}
	}
}

func coldLedgerState(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	rows, err := pool.Query(t.Context(), `SELECT r.state,q.stage,coalesce(q.outcome,''),r.slot_cost
		FROM reservations r JOIN request_records q USING(request_id) ORDER BY r.created_at`)
	if err != nil {
		return "query error: " + err.Error()
	}
	defer rows.Close()
	var states []string
	for rows.Next() {
		var reservation, stage, outcome string
		var slotCost int
		if err := rows.Scan(&reservation, &stage, &outcome, &slotCost); err != nil {
			return "scan error: " + err.Error()
		}
		states = append(states, fmt.Sprintf("reservation=%s stage=%s outcome=%s cost=%d", reservation, stage, outcome, slotCost))
	}
	var active, orphaned, limit int
	tenant := sha256.Sum256([]byte(coldTenant))
	if err := pool.QueryRow(t.Context(), `SELECT active_grants,orphaned_grants,grant_limit FROM tenant_counters WHERE tenant_hash=$1`, tenant[:]).Scan(&active, &orphaned, &limit); err != nil {
		states = append(states, "tenant query error: "+err.Error())
	} else {
		states = append(states, fmt.Sprintf("tenant active=%d orphaned=%d limit=%d", active, orphaned, limit))
	}
	return strings.Join(states, "; ")
}

func assertColdLedgerReleased(t *testing.T, pool *pgxpool.Pool, wantTerminal int) {
	t.Helper()
	var released, activeDebt, reserved, orphaned, admission int
	var retired bool
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM reservations WHERE state='released'`).Scan(&released); err != nil {
		t.Fatalf("read released reservations: %v", err)
	}
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM orphaned_capacity_debts WHERE state='active'`).Scan(&activeDebt); err != nil {
		t.Fatalf("read active debt: %v", err)
	}
	if err := pool.QueryRow(t.Context(), `SELECT reserved_slots,orphaned_slots,admission_limit,retired FROM instance_capacity WHERE cluster_id=$1 AND pod_uid=$2`, coldCluster, coldPodUID).Scan(&reserved, &orphaned, &admission, &retired); err != nil {
		t.Fatalf("read cold-path capacity: %v", err)
	}
	if released != wantTerminal || activeDebt != 0 || reserved != 0 || orphaned != 0 || admission != 2 || retired {
		t.Fatalf("ledger not released: released=%d want=%d active_debt=%d reserved=%d orphaned=%d admission=%d retired=%t", released, wantTerminal, activeDebt, reserved, orphaned, admission, retired)
	}
}
