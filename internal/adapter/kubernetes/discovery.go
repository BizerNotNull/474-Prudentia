package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strconv"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	annotationEnabled = "prudentia.io/enabled"
	proxyPortName     = "proxy"
)

// ReconcileDiscovery performs a level read rooted at an opted-in headless Service.
// Incomplete joins deliberately produce a tombstone rather than partial engine capacity.
func (a *Adapter) ReconcileDiscovery(ctx context.Context, key domain.ResourceKey, generation domain.WriterGeneration) ([]domain.Observation, error) {
	state, err := a.reconcileService(ctx, key)
	if err != nil {
		return nil, err
	}
	projections := state.Projections()
	observations := make([]domain.Observation, 0, len(projections))
	sequence := uint64(1)
	for _, projection := range projections {
		stamp, err := domain.NewSourceStamp(domain.SourceStructural, generation, domain.SourceSequence(sequence))
		if err != nil {
			return nil, err
		}
		// The legacy projection contains one Pod. Structural observations normalize it as a
		// complete one-member engine fact while the ResourceState API retains the full group.
		resource, err := domain.NewResourceRef(domain.ResourceRefParams{Cluster: a.config.Cluster, Namespace: key.Namespace(), Name: key.Name(), UID: projection.Identity().PodUID(), ResourceVersion: "observed"})
		if err != nil {
			return nil, err
		}
		workload, err := domain.NewWorkloadRef(domain.WorkloadDeployment, resource, int32(len(projections)))
		if err != nil {
			return nil, err
		}
		pod, err := domain.NewPodRef(domain.PodRefParams{Resource: resource, WorkloadUID: projection.Identity().PodUID()})
		if err != nil {
			return nil, err
		}
		fact, err := domain.NewStructuralFact(domain.StructuralFactParams{Endpoint: projection.Endpoint(), Model: projection.Model(), Workload: workload, Members: []domain.PodRef{pod}, EndpointEpoch: projection.Identity().EndpointEpoch(), RecoveryEpoch: projection.Identity().RecoveryEpoch()})
		if err != nil {
			return nil, err
		}
		observation, err := domain.NewObservation(domain.ObservationParams{Stamp: stamp, Identity: projection.Identity(), TTLClass: domain.TTLStructural, Structural: fact})
		if err != nil {
			return nil, err
		}
		observations = append(observations, observation)
		sequence++
	}
	return observations, nil
}

func (a *Adapter) reconcileService(ctx context.Context, key domain.ResourceKey) (domain.ResourceState, error) {
	service, err := a.client.CoreV1().Services(key.Namespace()).Get(ctx, key.Name(), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return domain.NewResourceState(a.config.Cluster, key, nil)
		}
		return domain.ResourceState{}, fmt.Errorf("read Service level state: %w", err)
	}
	if service.Annotations[annotationEnabled] != "true" || service.Spec.ClusterIP != corev1.ClusterIPNone {
		return domain.NewResourceState(a.config.Cluster, key, nil)
	}
	slices, err := a.client.DiscoveryV1().EndpointSlices(key.Namespace()).List(ctx, metav1.ListOptions{LabelSelector: discoveryv1.LabelServiceName + "=" + service.Name})
	if err != nil {
		return domain.ResourceState{}, fmt.Errorf("list EndpointSlices: %w", err)
	}
	if len(slices.Items) == 0 {
		return domain.NewResourceState(a.config.Cluster, key, nil)
	}
	pods, err := a.client.CoreV1().Pods(key.Namespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.ResourceState{}, fmt.Errorf("list target Pods: %w", err)
	}
	byUID := make(map[types.UID]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		byUID[pods.Items[i].UID] = &pods.Items[i]
	}
	replicaSets, err := a.client.AppsV1().ReplicaSets(key.Namespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.ResourceState{}, fmt.Errorf("list ReplicaSet ownership: %w", err)
	}
	deployments, err := a.client.AppsV1().Deployments(key.Namespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.ResourceState{}, fmt.Errorf("list Deployment ownership: %w", err)
	}
	statefulSets, err := a.client.AppsV1().StatefulSets(key.Namespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.ResourceState{}, fmt.Errorf("list StatefulSet ownership: %w", err)
	}
	workloads := make(map[types.UID]struct{}, len(deployments.Items)+len(statefulSets.Items))
	for _, deployment := range deployments.Items {
		workloads[deployment.UID] = struct{}{}
	}
	for _, statefulSet := range statefulSets.Items {
		workloads[statefulSet.UID] = struct{}{}
	}
	replicaSetOwners := make(map[types.UID]types.UID, len(replicaSets.Items))
	for _, replicaSet := range replicaSets.Items {
		if owner, ok := controllerOwner(replicaSet.OwnerReferences); ok {
			if _, managed := workloads[owner]; managed {
				replicaSetOwners[replicaSet.UID] = owner
			}
		}
	}
	var projections []domain.BackendProjection
	seen := map[types.UID]struct{}{}
	engine := ""
	var groupOwner types.UID
	for _, slice := range slices.Items {
		if !sliceHasProxyPort(slice, a.config.ProxyPort) {
			return domain.NewResourceState(a.config.Cluster, key, nil)
		}
		for _, endpoint := range slice.Endpoints {
			if endpoint.TargetRef == nil || endpoint.TargetRef.Kind != "Pod" || endpoint.TargetRef.UID == "" || endpoint.Conditions.Ready == nil || !*endpoint.Conditions.Ready || len(endpoint.Addresses) == 0 {
				return domain.NewResourceState(a.config.Cluster, key, nil)
			}
			pod := byUID[endpoint.TargetRef.UID]
			if pod == nil || !serviceSelects(service, pod) {
				return domain.NewResourceState(a.config.Cluster, key, nil)
			}
			owner, ok := controllerOwner(pod.OwnerReferences)
			if !ok {
				return domain.NewResourceState(a.config.Cluster, key, nil)
			}
			if parent, viaReplicaSet := replicaSetOwners[owner]; viaReplicaSet {
				owner = parent
			}
			if _, managed := workloads[owner]; !managed {
				return domain.NewResourceState(a.config.Cluster, key, nil)
			}
			if groupOwner == "" {
				groupOwner = owner
			} else if groupOwner != owner {
				return domain.NewResourceState(a.config.Cluster, key, nil)
			}
			if _, duplicate := seen[pod.UID]; duplicate {
				continue
			}
			seen[pod.UID] = struct{}{}
			copy := pod.DeepCopy()
			copy.Status.PodIP = endpoint.Addresses[0]
			projection, eligible, err := a.projection(copy)
			if err != nil || !eligible {
				return domain.NewResourceState(a.config.Cluster, key, nil)
			}
			if engine == "" {
				engine = projection.Identity().LogicalEngine()
			}
			if projection.Identity().LogicalEngine() != engine {
				return domain.NewResourceState(a.config.Cluster, key, nil)
			}
			projections = append(projections, projection)
		}
	}
	if len(projections) == 0 {
		return domain.NewResourceState(a.config.Cluster, key, nil)
	}
	sort.Slice(projections, func(i, j int) bool { return projections[i].Identity().PodUID() < projections[j].Identity().PodUID() })
	return domain.NewResourceState(a.config.Cluster, key, projections)
}

func sliceHasProxyPort(slice discoveryv1.EndpointSlice, port uint16) bool {
	for _, p := range slice.Ports {
		if p.Port != nil && uint16(*p.Port) == port && (p.Name == nil || *p.Name == proxyPortName) {
			return true
		}
	}
	return false
}

func serviceSelects(service *corev1.Service, pod *corev1.Pod) bool {
	if len(service.Spec.Selector) == 0 {
		return false
	}
	for key, value := range service.Spec.Selector {
		if pod.Labels[key] != value {
			return false
		}
	}
	return true
}

func controllerOwner(owners []metav1.OwnerReference) (types.UID, bool) {
	for _, owner := range owners {
		if owner.Controller != nil && *owner.Controller && owner.UID != "" {
			return owner.UID, true
		}
	}
	return "", false
}
func (a *Adapter) ReadDesired(ctx context.Context, key domain.ResourceKey) (domain.DesiredModel, error) {
	level, err := a.desiredWorkload(ctx, key)
	if err != nil {
		return domain.DesiredModel{}, err
	}
	authority := domain.ReplicaAuthorityExternal

	if level.annotations[annotationReplicaAuthority] == "prudentia" {
		authority = domain.ReplicaAuthorityPrudentia
	}
	generation, err := strconv.ParseUint(level.annotations[annotationDesiredGeneration], 10, 64)
	if err != nil || generation == 0 {
		return domain.DesiredModel{}, fmt.Errorf("invalid desired generation")
	}
	recovery, err := strconv.ParseUint(level.annotations[annotationRecoveryEpoch], 10, 64)
	if err != nil || recovery == 0 {
		return domain.DesiredModel{}, fmt.Errorf("invalid desired recovery epoch")
	}
	return domain.NewDesiredModel(domain.DesiredModelParams{Key: key, Model: level.annotations[annotationModel], Managed: level.annotations[annotationManaged] == "true", ReplicaAuthority: authority, Replicas: level.replicas, Generation: generation, RecoveryEpoch: recovery})
}

func (a *Adapter) ApplyDesired(ctx context.Context, desired domain.DesiredModel) (domain.ApplyResult, error) {
	level, err := a.desiredWorkload(ctx, desired.Key())
	if err != nil {
		return domain.ApplyResult{}, err
	}
	if desired.Replicas() != level.replicas {
		return domain.ApplyResult{}, ErrDisruptiveApply
	}
	annotations := map[string]string{annotationManaged: strconv.FormatBool(desired.Managed()), annotationReplicaAuthority: map[bool]string{true: "prudentia", false: "external"}[desired.ReplicaAuthority() == domain.ReplicaAuthorityPrudentia], annotationDesiredGeneration: strconv.FormatUint(desired.Generation(), 10), annotationRecoveryEpoch: strconv.FormatUint(desired.RecoveryEpoch(), 10), annotationModel: desired.Model()}
	unchanged := true
	for key, value := range annotations {
		if level.annotations[key] != value {
			unchanged = false
			break
		}
	}
	if !unchanged {
		kind := "Deployment"
		apiVersion := "apps/v1"
		if level.kind == domain.WorkloadStatefulSet {
			kind = "StatefulSet"
		}
		body, _ := json.Marshal(map[string]any{"apiVersion": apiVersion, "kind": kind, "metadata": map[string]any{"name": desired.Key().Name(), "namespace": desired.Key().Namespace(), "annotations": annotations}})
		mctx, cancel := a.mutationContext(ctx)
		defer cancel()
		options := metav1.PatchOptions{FieldManager: "prudentia-controller", Force: new(false)}
		if level.kind == domain.WorkloadDeployment {
			_, err = a.client.AppsV1().Deployments(desired.Key().Namespace()).Patch(mctx, desired.Key().Name(), types.ApplyPatchType, body, options)
		} else {
			_, err = a.client.AppsV1().StatefulSets(desired.Key().Namespace()).Patch(mctx, desired.Key().Name(), types.ApplyPatchType, body, options)
		}
		if err != nil {
			if apierrors.IsConflict(err) {
				ref, _ := workloadRef(a.config.Cluster, level.object, level.kind, level.replicas)
				return domain.NewApplyResult(domain.ApplyConflict, ref)
			}
			return domain.ApplyResult{}, err
		}
		level, err = a.desiredWorkload(ctx, desired.Key())
		if err != nil {
			return domain.ApplyResult{}, err
		}
	}
	ref, err := workloadRef(a.config.Cluster, level.object, level.kind, level.replicas)
	if err != nil {
		return domain.ApplyResult{}, err
	}
	mode := domain.ApplyUnchanged
	if !unchanged {
		mode = domain.ApplyUpdated
	}
	return domain.NewApplyResult(mode, ref)
}

func (a *Adapter) desiredWorkload(ctx context.Context, key domain.ResourceKey) (workloadLevel, error) {
	deployment, err := a.client.AppsV1().Deployments(key.Namespace()).Get(ctx, key.Name(), metav1.GetOptions{})
	if err == nil {
		return deploymentLevel(deployment), nil
	}
	if !apierrors.IsNotFound(err) {
		return workloadLevel{}, err
	}
	set, err := a.client.AppsV1().StatefulSets(key.Namespace()).Get(ctx, key.Name(), metav1.GetOptions{})
	if err != nil {
		return workloadLevel{}, err
	}
	return statefulSetLevel(set), nil
}

var _ = net.IP{}
var _ = appsv1.Deployment{}
