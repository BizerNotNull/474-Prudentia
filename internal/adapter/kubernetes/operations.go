package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	annotationOperationGeneration = "prudentia.io/operation-generation"
	annotationOperationToken      = "prudentia.io/operation-token"
	annotationAdmissionClosed     = "prudentia.io/admission-closed"
	annotationManaged             = "prudentia.io/managed"
	annotationReplicaAuthority    = "prudentia.io/replica-authority"
	annotationDesiredGeneration   = "prudentia.io/desired-generation"
)

var (
	ErrDisruptiveApply   = errors.New("desired apply contains a disruptive replica change")
	ErrOperationConflict = errors.New("Kubernetes operation fence no longer matches")
	ErrIdentityNotGone   = errors.New("exact workload identity is still usable")
)

// IdentityFence verifies that an exact workload identity can no longer authenticate or execute.
type IdentityFence interface {
	ExecutionRevoked(context.Context, domain.WorkloadIdentity) (domain.WriterGeneration, bool, []byte, error)
}

func (a *Adapter) mutationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	lifetime := a.config.MutationCallLifetime
	if lifetime <= 0 {
		lifetime = 10 * time.Second
	}
	return context.WithTimeout(ctx, lifetime)
}

func resourceRef(cluster, namespace, name string, uid types.UID, rv string) (domain.ResourceRef, error) {
	return domain.NewResourceRef(domain.ResourceRefParams{Cluster: cluster, Namespace: namespace, Name: name, UID: string(uid), ResourceVersion: rv})
}

func workloadRef(cluster string, object metav1.Object, kind domain.WorkloadKind, replicas int32) (domain.WorkloadRef, error) {
	r, err := resourceRef(cluster, object.GetNamespace(), object.GetName(), object.GetUID(), object.GetResourceVersion())
	if err != nil {
		return domain.WorkloadRef{}, err
	}
	return domain.NewWorkloadRef(kind, r, replicas)
}

func podRef(cluster string, pod *corev1.Pod, workloadUID string, op *domain.WorkloadOperationRef) (domain.PodRef, error) {
	r, err := resourceRef(cluster, pod.Namespace, pod.Name, pod.UID, pod.ResourceVersion)
	if err != nil {
		return domain.PodRef{}, err
	}
	p := domain.PodRefParams{Resource: r, WorkloadUID: workloadUID}
	if op != nil {
		p.OperationGeneration, p.OperationToken = op.Generation(), op.Token()
	}
	return domain.NewPodRef(p)
}

func barrierPatch(uid types.UID, rv string, annotations map[string]string, op domain.WorkloadOperationRef) ([]byte, error) {
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": string(uid)},
		{"op": "test", "path": "/metadata/resourceVersion", "value": rv},
	}
	if prior, ok := annotations[annotationOperationToken]; ok {
		patch = append(patch, map[string]any{"op": "test", "path": "/metadata/annotations/prudentia.io~1operation-token", "value": prior})
	}
	updated := make(map[string]string, len(annotations)+3)
	for key, value := range annotations {
		updated[key] = value
	}
	updated[annotationOperationGeneration] = strconv.FormatUint(op.Generation(), 10)
	updated[annotationOperationToken] = op.Token()
	updated[annotationAdmissionClosed] = "true"
	opName := "replace"
	if annotations == nil {
		opName = "add"
	}
	patch = append(patch, map[string]any{"op": opName, "path": "/metadata/annotations", "value": updated})
	return json.Marshal(patch)
}

func workloadBarrierPatch(uid types.UID, rv string, annotations, templateAnnotations map[string]string, op domain.WorkloadOperationRef) ([]byte, error) {
	raw, err := barrierPatch(uid, rv, annotations, op)
	if err != nil {
		return nil, err
	}
	var patch []map[string]any
	if err := json.Unmarshal(raw, &patch); err != nil {
		return nil, err
	}
	if prior, ok := templateAnnotations[annotationOperationToken]; ok {
		patch = append(patch, map[string]any{"op": "test", "path": "/spec/template/metadata/annotations/prudentia.io~1operation-token", "value": prior})
	}
	updated := make(map[string]string, len(templateAnnotations)+3)
	for key, value := range templateAnnotations {
		updated[key] = value
	}
	updated[annotationOperationGeneration] = strconv.FormatUint(op.Generation(), 10)
	updated[annotationOperationToken] = op.Token()
	updated[annotationAdmissionClosed] = "true"
	opName := "replace"
	if templateAnnotations == nil {
		opName = "add"
	}
	patch = append(patch, map[string]any{"op": opName, "path": "/spec/template/metadata/annotations", "value": updated})
	return json.Marshal(patch)
}

// InstallWorkloadOperationBarrier atomically advances the workload and every current owned Pod,
// then reads all objects back. A partial result is never returned as proof.
func (a *Adapter) InstallWorkloadOperationBarrier(ctx context.Context, operation domain.WorkloadOperation, ref domain.WorkloadRef, _ []domain.PodRef) (domain.WorkloadBarrierProof, error) {
	if operation.Ref().WorkloadUID() != ref.UID() || operation.Scope().UID() != ref.UID() {
		return domain.WorkloadBarrierProof{}, ErrOperationConflict
	}
	current, err := a.getWorkload(ctx, ref.Kind(), ref.Namespace(), ref.Name())
	if err != nil {
		return domain.WorkloadBarrierProof{}, err
	}
	if current.uid != ref.UID() {
		return domain.WorkloadBarrierProof{}, ErrOperationConflict
	}
	body, err := workloadBarrierPatch(types.UID(current.uid), current.rv, current.annotations, current.templateAnnotations, operation.Ref())
	if err != nil {
		return domain.WorkloadBarrierProof{}, err
	}
	mctx, cancel := a.mutationContext(ctx)
	defer cancel()
	if err := a.patchWorkload(mctx, ref.Kind(), ref.Namespace(), ref.Name(), body); err != nil {
		return domain.WorkloadBarrierProof{}, mapConflict(err)
	}
	pods, err := a.listOwnedPods(ctx, ref.UID(), ref.Namespace())
	if err != nil {
		return domain.WorkloadBarrierProof{}, err
	}
	for i := range pods {
		body, e := barrierPatch(pods[i].UID, pods[i].ResourceVersion, pods[i].Annotations, operation.Ref())
		if e != nil {
			return domain.WorkloadBarrierProof{}, e
		}
		pctx, pcancel := a.mutationContext(ctx)
		_, e = a.client.CoreV1().Pods(pods[i].Namespace).Patch(pctx, pods[i].Name, types.JSONPatchType, body, metav1.PatchOptions{})
		pcancel()
		if e != nil {
			return domain.WorkloadBarrierProof{}, mapConflict(e)
		}
	}
	observed, err := a.getWorkload(ctx, ref.Kind(), ref.Namespace(), ref.Name())
	if err != nil || observed.uid != ref.UID() || !hasBarrier(observed.annotations, operation.Ref()) || !hasBarrier(observed.templateAnnotations, operation.Ref()) {
		if err == nil {
			err = ErrOperationConflict
		}
		return domain.WorkloadBarrierProof{}, err
	}
	observedRef, err := workloadRef(a.config.Cluster, observed.object, ref.Kind(), observed.replicas)
	if err != nil {
		return domain.WorkloadBarrierProof{}, err
	}
	pods, err = a.listOwnedPods(ctx, ref.UID(), ref.Namespace())
	if err != nil {
		return domain.WorkloadBarrierProof{}, err
	}
	refs := make([]domain.PodRef, 0, len(pods))
	for i := range pods {
		if !hasBarrier(pods[i].Annotations, operation.Ref()) {
			return domain.WorkloadBarrierProof{}, ErrOperationConflict
		}
		p, e := podRef(a.config.Cluster, &pods[i], ref.UID(), ptr(operation.Ref()))
		if e != nil {
			return domain.WorkloadBarrierProof{}, e
		}
		refs = append(refs, p)
	}
	return domain.NewWorkloadBarrierProof(domain.WorkloadBarrierProofParams{Operation: operation.Ref(), Workload: observedRef, Pods: refs, ObservedAt: time.Now().UTC()})
}

func ptr(v domain.WorkloadOperationRef) *domain.WorkloadOperationRef { return &v }

func hasBarrier(a map[string]string, op domain.WorkloadOperationRef) bool {
	return a[annotationOperationGeneration] == strconv.FormatUint(op.Generation(), 10) && a[annotationOperationToken] == op.Token() && a[annotationAdmissionClosed] == "true"
}

func mapConflict(err error) error {
	if apierrors.IsConflict(err) || apierrors.IsInvalid(err) || apierrors.IsNotFound(err) {
		return fmt.Errorf("%w: %v", ErrOperationConflict, err)
	}
	return err
}

func (a *Adapter) RemovePodExact(ctx context.Context, op domain.WorkloadOperationRef, ref domain.PodRef, mode domain.RemovalMode) error {
	token, bound := ref.OperationToken()
	if !mode.Valid() || !bound || token != op.Token() || ref.OperationGeneration() != op.Generation() || ref.WorkloadUID() != op.WorkloadUID() {
		return ErrOperationConflict
	}
	uid, rv := types.UID(ref.UID()), ref.ResourceVersion()
	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}
	mctx, cancel := a.mutationContext(ctx)
	defer cancel()
	var err error
	if mode == domain.RemovalEvict {
		err = a.client.PolicyV1().Evictions(ref.Namespace()).Evict(mctx, &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: ref.Name(), Namespace: ref.Namespace()}, DeleteOptions: &options})
	} else {
		err = a.client.CoreV1().Pods(ref.Namespace()).Delete(mctx, ref.Name(), options)
	}
	if err != nil {
		return mapConflict(err)
	}
	return nil
}

func (a *Adapter) ScaleStatefulSetDown(ctx context.Context, op domain.WorkloadOperationRef, ref domain.StatefulSetRef, replicas int32) (domain.ScalePatchResult, error) {
	return a.scaleDown(ctx, op, ref.Workload(), replicas)
}

func (a *Adapter) ScaleDeploymentAfterWholeDrain(ctx context.Context, op domain.WorkloadOperationRef, ref domain.DeploymentRef, replicas int32) (domain.ScalePatchResult, error) {
	return a.scaleDown(ctx, op, ref.Workload(), replicas)
}

func (a *Adapter) scaleDown(ctx context.Context, op domain.WorkloadOperationRef, ref domain.WorkloadRef, replicas int32) (domain.ScalePatchResult, error) {
	if replicas < 0 || replicas >= ref.Replicas() || op.WorkloadUID() != ref.UID() {
		return domain.ScalePatchResult{}, ErrOperationConflict
	}
	current, err := a.getWorkload(ctx, ref.Kind(), ref.Namespace(), ref.Name())
	if err != nil {
		return domain.ScalePatchResult{}, err
	}
	if current.uid != ref.UID() || current.replicas != ref.Replicas() || !hasBarrier(current.annotations, op) {
		return domain.ScalePatchResult{}, ErrOperationConflict
	}
	patch, _ := json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": current.uid},
		{"op": "test", "path": "/metadata/resourceVersion", "value": current.rv},
		{"op": "test", "path": "/metadata/annotations/prudentia.io~1operation-generation", "value": strconv.FormatUint(op.Generation(), 10)},
		{"op": "test", "path": "/metadata/annotations/prudentia.io~1operation-token", "value": op.Token()},
		{"op": "test", "path": "/metadata/annotations/prudentia.io~1admission-closed", "value": "true"},
		{"op": "test", "path": "/spec/replicas", "value": ref.Replicas()},
		{"op": "replace", "path": "/spec/replicas", "value": replicas},
	})
	mctx, cancel := a.mutationContext(ctx)
	defer cancel()
	if err := a.patchWorkload(mctx, ref.Kind(), ref.Namespace(), ref.Name(), patch); err != nil {
		return domain.ScalePatchResult{}, mapConflict(err)
	}
	observed, err := a.getWorkload(ctx, ref.Kind(), ref.Namespace(), ref.Name())
	if err != nil || observed.uid != ref.UID() || observed.replicas != replicas {
		if err == nil {
			err = ErrOperationConflict
		}
		return domain.ScalePatchResult{}, err
	}
	resultRef, err := workloadRef(a.config.Cluster, observed.object, ref.Kind(), replicas)
	if err != nil {
		return domain.ScalePatchResult{}, err
	}
	return domain.NewScalePatchResult(op, resultRef, ref.Replicas(), replicas)
}

func (a *Adapter) ObserveWorkloadVictims(ctx context.Context, op domain.WorkloadOperationRef, before domain.PodUIDSet) (domain.WorkloadVictimObservation, error) {
	current, err := a.findWorkloadByUID(ctx, op.WorkloadUID())
	if err != nil {
		return domain.WorkloadVictimObservation{}, err
	}
	if !hasBarrier(current.annotations, op) {
		return domain.WorkloadVictimObservation{}, ErrOperationConflict
	}
	pods, err := a.listOwnedPods(ctx, op.WorkloadUID(), current.object.GetNamespace())
	if err != nil {
		return domain.WorkloadVictimObservation{}, err
	}
	present := make(map[string]corev1.Pod, len(pods))
	for _, pod := range pods {
		present[string(pod.UID)] = pod
	}
	var terminating, disappeared, surviving []string
	for _, uid := range before.Values() {
		pod, ok := present[uid]
		switch {
		case !ok:
			disappeared = append(disappeared, uid)
		case pod.DeletionTimestamp != nil:
			terminating = append(terminating, uid)
		default:
			surviving = append(surviving, uid)
		}
	}
	t, _ := domain.NewPodUIDSet(terminating)
	d, _ := domain.NewPodUIDSet(disappeared)
	s, _ := domain.NewPodUIDSet(surviving)
	ref, err := workloadRef(a.config.Cluster, current.object, current.kind, current.replicas)
	if err != nil {
		return domain.WorkloadVictimObservation{}, err
	}
	return domain.NewWorkloadVictimObservation(domain.WorkloadVictimObservationParams{Operation: op, Workload: ref, Before: before, Terminating: t, Disappeared: d, Surviving: s, ObservedAt: time.Now().UTC()})
}

// BuildWorkloadCompletionProof performs a fresh API-server level read. Reopen is
// impossible while a replacement/terminating Pod lacks the current operation barrier.
func (a *Adapter) BuildWorkloadCompletionProof(ctx context.Context, barrier domain.WorkloadBarrierProof, victims domain.WorkloadVictimObservation) (domain.WorkloadCompletionProof, error) {
	op := barrier.Operation()
	if victims.Operation() != op {
		return domain.WorkloadCompletionProof{}, ErrOperationConflict
	}
	current, err := a.findWorkloadByUID(ctx, op.WorkloadUID())
	if err != nil {
		return domain.WorkloadCompletionProof{}, err
	}
	if !hasBarrier(current.annotations, op) || !hasBarrier(current.templateAnnotations, op) {
		return domain.WorkloadCompletionProof{}, ErrOperationConflict
	}
	pods, err := a.listOwnedPods(ctx, op.WorkloadUID(), current.object.GetNamespace())
	if err != nil {
		return domain.WorkloadCompletionProof{}, err
	}
	currentPods := make([]domain.PodRef, 0, len(pods))
	for i := range pods {
		if pods[i].DeletionTimestamp != nil || !hasBarrier(pods[i].Annotations, op) {
			return domain.WorkloadCompletionProof{}, ErrOperationConflict
		}
		ref, err := podRef(a.config.Cluster, &pods[i], op.WorkloadUID(), &op)
		if err != nil {
			return domain.WorkloadCompletionProof{}, err
		}
		currentPods = append(currentPods, ref)
	}
	if int32(len(currentPods)) != current.replicas {
		return domain.WorkloadCompletionProof{}, ErrOperationConflict
	}
	workload, err := workloadRef(a.config.Cluster, current.object, current.kind, current.replicas)
	if err != nil {
		return domain.WorkloadCompletionProof{}, err
	}
	return domain.NewWorkloadCompletionProof(domain.WorkloadCompletionProofParams{
		Barrier: barrier, Victims: victims, Current: workload, CurrentPods: currentPods,
		DesiredReplicas: current.replicas, CompletedAt: time.Now().UTC(),
	})
}

func (a *Adapter) ObserveIdentityGone(ctx context.Context, id domain.WorkloadIdentity) (domain.IdentityGoneProof, error) {
	pods, err := a.client.CoreV1().Pods(id.Namespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.IdentityGoneProof{}, err
	}
	for _, pod := range pods.Items {
		if string(pod.UID) == id.PodUID() {
			return domain.IdentityGoneProof{}, ErrIdentityNotGone
		}
	}
	slices, err := a.client.DiscoveryV1().EndpointSlices(id.Namespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.IdentityGoneProof{}, err
	}
	for _, slice := range slices.Items {
		for _, endpoint := range slice.Endpoints {
			if endpoint.TargetRef != nil && string(endpoint.TargetRef.UID) == id.PodUID() {
				return domain.IdentityGoneProof{}, ErrIdentityNotGone
			}
		}
	}
	if a.config.IdentityFence == nil {
		return domain.IdentityGoneProof{}, ErrIdentityNotGone
	}
	generation, revoked, evidence, err := a.config.IdentityFence.ExecutionRevoked(ctx, id)
	if err != nil {
		return domain.IdentityGoneProof{}, err
	}
	if generation == 0 || !revoked || len(evidence) == 0 {
		return domain.IdentityGoneProof{}, ErrIdentityNotGone
	}
	podEvidence, err := json.Marshal(struct {
		Cluster, Namespace, PodUID   string
		EndpointEpoch, RecoveryEpoch uint64
		ListResourceVersion          string
		ObservedPodUIDs              []string
	}{
		Cluster: id.Cluster(), Namespace: id.Namespace(), PodUID: id.PodUID(),
		EndpointEpoch: id.EndpointEpoch(), RecoveryEpoch: id.RecoveryEpoch(),
		ListResourceVersion: pods.ResourceVersion, ObservedPodUIDs: sortedPodUIDs(pods.Items),
	})
	if err != nil {
		return domain.IdentityGoneProof{}, err
	}
	endpointEvidence, err := json.Marshal(struct {
		Cluster, Namespace, LogicalEngine, PodUID string
		EndpointEpoch, RecoveryEpoch              uint64
		ListResourceVersion                       string
		Slices                                    []endpointSliceEvidence
	}{
		Cluster: id.Cluster(), Namespace: id.Namespace(), LogicalEngine: id.LogicalEngine(), PodUID: id.PodUID(),
		EndpointEpoch: id.EndpointEpoch(), RecoveryEpoch: id.RecoveryEpoch(),
		ListResourceVersion: slices.ResourceVersion, Slices: exactEndpointSliceEvidence(slices.Items),
	})
	if err != nil {
		return domain.IdentityGoneProof{}, err
	}
	podHash := sha256.Sum256(podEvidence)
	endpointHash := sha256.Sum256(endpointEvidence)
	fenceBinding := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00", id.Cluster(), id.Namespace(), id.LogicalEngine(), id.PodUID(), id.EndpointEpoch(), id.RecoveryEpoch())
	fenceHash := sha256.Sum256(append([]byte(fenceBinding), evidence...))
	return domain.NewIdentityGoneProof(domain.IdentityGoneProofParams{WriterGeneration: generation, Identity: id, PodAbsenceEvidenceHash: podHash, EndpointWithdrawalEvidenceHash: endpointHash, ExecutionFenceEvidenceHash: fenceHash})
}

type endpointSliceEvidence struct {
	Name, ResourceVersion string
	Targets               []string
}

func sortedPodUIDs(pods []corev1.Pod) []string {
	values := make([]string, 0, len(pods))
	for i := range pods {
		values = append(values, string(pods[i].UID))
	}
	sort.Strings(values)
	return values
}

func exactEndpointSliceEvidence(slices []discoveryv1.EndpointSlice) []endpointSliceEvidence {
	values := make([]endpointSliceEvidence, 0, len(slices))
	for i := range slices {
		targets := make([]string, 0, len(slices[i].Endpoints))
		for _, endpoint := range slices[i].Endpoints {
			uid := ""
			if endpoint.TargetRef != nil {
				uid = string(endpoint.TargetRef.UID)
			}
			addresses := append([]string(nil), endpoint.Addresses...)
			sort.Strings(addresses)
			targets = append(targets, uid+"@"+strings.Join(addresses, ","))
		}
		sort.Strings(targets)
		values = append(values, endpointSliceEvidence{Name: slices[i].Name, ResourceVersion: slices[i].ResourceVersion, Targets: targets})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Name < values[j].Name })
	return values
}

type workloadLevel struct {
	object              metav1.Object
	kind                domain.WorkloadKind
	uid, rv             string
	replicas            int32
	annotations         map[string]string
	templateAnnotations map[string]string
}

func (a *Adapter) getWorkload(ctx context.Context, kind domain.WorkloadKind, namespace, name string) (workloadLevel, error) {
	if kind == domain.WorkloadDeployment {
		v, err := a.client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return workloadLevel{}, err
		}
		return deploymentLevel(v), nil
	}
	v, err := a.client.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return workloadLevel{}, err
	}
	return statefulSetLevel(v), nil
}

func deploymentLevel(v *appsv1.Deployment) workloadLevel {
	replicas := int32(1)
	if v.Spec.Replicas != nil {
		replicas = *v.Spec.Replicas
	}
	return workloadLevel{object: v, kind: domain.WorkloadDeployment, uid: string(v.UID), rv: v.ResourceVersion, replicas: replicas, annotations: v.Annotations, templateAnnotations: v.Spec.Template.Annotations}
}

func statefulSetLevel(v *appsv1.StatefulSet) workloadLevel {
	replicas := int32(1)
	if v.Spec.Replicas != nil {
		replicas = *v.Spec.Replicas
	}
	return workloadLevel{object: v, kind: domain.WorkloadStatefulSet, uid: string(v.UID), rv: v.ResourceVersion, replicas: replicas, annotations: v.Annotations, templateAnnotations: v.Spec.Template.Annotations}
}

func (a *Adapter) patchWorkload(ctx context.Context, kind domain.WorkloadKind, namespace, name string, patch []byte) error {
	if kind == domain.WorkloadDeployment {
		_, err := a.client.AppsV1().Deployments(namespace).Patch(ctx, name, types.JSONPatchType, patch, metav1.PatchOptions{})
		return err
	}
	_, err := a.client.AppsV1().StatefulSets(namespace).Patch(ctx, name, types.JSONPatchType, patch, metav1.PatchOptions{})
	return err
}

func (a *Adapter) findWorkloadByUID(ctx context.Context, uid string) (workloadLevel, error) {
	deployments, err := a.client.AppsV1().Deployments(a.config.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return workloadLevel{}, err
	}
	for i := range deployments.Items {
		if string(deployments.Items[i].UID) == uid {
			return deploymentLevel(&deployments.Items[i]), nil
		}
	}
	sets, err := a.client.AppsV1().StatefulSets(a.config.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return workloadLevel{}, err
	}
	for i := range sets.Items {
		if string(sets.Items[i].UID) == uid {
			return statefulSetLevel(&sets.Items[i]), nil
		}
	}
	return workloadLevel{}, apierrors.NewNotFound(appsv1.Resource("workload"), uid)
}

func (a *Adapter) listOwnedPods(ctx context.Context, workloadUID, namespace string) ([]corev1.Pod, error) {
	pods, err := a.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	replicaSets, err := a.client.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	rsOwner := map[string]string{}
	for _, rs := range replicaSets.Items {
		for _, owner := range rs.OwnerReferences {
			if owner.Controller != nil && *owner.Controller {
				rsOwner[string(rs.UID)] = string(owner.UID)
			}
		}
	}
	owned := make([]corev1.Pod, 0)
	for _, pod := range pods.Items {
		for _, owner := range pod.OwnerReferences {
			if owner.Controller == nil || !*owner.Controller {
				continue
			}
			ownerUID := string(owner.UID)
			if ownerUID == workloadUID || rsOwner[ownerUID] == workloadUID {
				owned = append(owned, pod)
				break
			}
		}
	}
	sort.Slice(owned, func(i, j int) bool { return string(owned[i].UID) < string(owned[j].UID) })
	return owned, nil
}
