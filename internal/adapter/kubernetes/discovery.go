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

type discoveredEngine struct {
	state    domain.ResourceState
	workload domain.WorkloadRef
	members  []domain.PodRef
}

// ReconcileDiscovery performs a level read rooted at an opted-in headless Service.
// Every member receives the same exact, complete workload membership fact.
func (a *Adapter) ReconcileDiscovery(ctx context.Context, key domain.ResourceKey, generation domain.WriterGeneration) ([]domain.Observation, error) {
	group, err := a.discoverEngine(ctx, key)
	if err != nil {
		return nil, err
	}
	projections := group.state.Projections()
	observations := make([]domain.Observation, 0, len(projections))
	for _, projection := range projections {
		stamp, err := domain.NewSourceStamp(domain.SourceStructural, generation, a.nextSourceSequence())
		if err != nil {
			return nil, err
		}
		fact, err := domain.NewStructuralFact(domain.StructuralFactParams{
			Endpoint: projection.Endpoint(), Model: projection.Model(), Workload: group.workload,
			Members: group.members, EndpointEpoch: projection.Identity().EndpointEpoch(),
			RecoveryEpoch: projection.Identity().RecoveryEpoch(),
		})
		if err != nil {
			return nil, err
		}
		observation, err := domain.NewObservation(domain.ObservationParams{Stamp: stamp, Identity: projection.Identity(), TTLClass: domain.TTLStructural, Structural: fact})
		if err != nil {
			return nil, err
		}
		observations = append(observations, observation)
	}
	return observations, nil
}

func (a *Adapter) reconcileService(ctx context.Context, key domain.ResourceKey) (domain.ResourceState, error) {
	group, err := a.discoverEngine(ctx, key)
	return group.state, err
}

func (a *Adapter) discoverEngine(ctx context.Context, key domain.ResourceKey) (discoveredEngine, error) {
	empty := func() (discoveredEngine, error) {
		state, err := domain.NewResourceState(a.config.Cluster, key, nil)
		return discoveredEngine{state: state}, err
	}
	service, err := a.client.CoreV1().Services(key.Namespace()).Get(ctx, key.Name(), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return empty()
		}
		return discoveredEngine{}, fmt.Errorf("read Service level state: %w", err)
	}
	if service.Annotations[annotationEnabled] != "true" || service.Spec.ClusterIP != corev1.ClusterIPNone {
		return empty()
	}
	slices, err := a.client.DiscoveryV1().EndpointSlices(key.Namespace()).List(ctx, metav1.ListOptions{LabelSelector: discoveryv1.LabelServiceName + "=" + service.Name})
	if err != nil {
		return discoveredEngine{}, fmt.Errorf("list EndpointSlices: %w", err)
	}
	podList, err := a.client.CoreV1().Pods(key.Namespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return discoveredEngine{}, fmt.Errorf("list target Pods: %w", err)
	}
	selected := make(map[types.UID]*corev1.Pod)
	allPods := make(map[types.UID]*corev1.Pod, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		allPods[pod.UID] = pod
		if serviceSelects(service, pod) {
			selected[pod.UID] = pod
		}
	}
	if len(selected) == 0 || len(slices.Items) == 0 {
		return empty()
	}
	replicaSets, err := a.client.AppsV1().ReplicaSets(key.Namespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return discoveredEngine{}, fmt.Errorf("list ReplicaSet ownership: %w", err)
	}
	deployments, err := a.client.AppsV1().Deployments(key.Namespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return discoveredEngine{}, fmt.Errorf("list Deployment ownership: %w", err)
	}
	statefulSets, err := a.client.AppsV1().StatefulSets(key.Namespace()).List(ctx, metav1.ListOptions{})
	if err != nil {
		return discoveredEngine{}, fmt.Errorf("list StatefulSet ownership: %w", err)
	}
	workloads := make(map[types.UID]workloadLevel, len(deployments.Items)+len(statefulSets.Items))
	for i := range deployments.Items {
		level := deploymentLevel(&deployments.Items[i])
		workloads[deployments.Items[i].UID] = level
	}
	for i := range statefulSets.Items {
		level := statefulSetLevel(&statefulSets.Items[i])
		workloads[statefulSets.Items[i].UID] = level
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
	seen := make(map[types.UID]struct{}, len(selected))
	model, engine := "", ""
	var groupOwner types.UID
	for _, slice := range slices.Items {
		if !sliceHasProxyPort(slice, a.config.ProxyPort) {
			return empty()
		}
		for _, endpoint := range slice.Endpoints {
			if endpoint.TargetRef == nil || endpoint.TargetRef.Kind != "Pod" || endpoint.TargetRef.UID == "" || endpoint.Conditions.Ready == nil || !*endpoint.Conditions.Ready || len(endpoint.Addresses) != 1 {
				return empty()
			}
			pod := allPods[endpoint.TargetRef.UID]
			if pod == nil || selected[pod.UID] == nil || endpoint.Addresses[0] != pod.Status.PodIP {
				return empty()
			}
			owner, ok := controllerOwner(pod.OwnerReferences)
			if parent, viaReplicaSet := replicaSetOwners[owner]; viaReplicaSet {
				owner = parent
			}
			if !ok || workloads[owner].object == nil || (groupOwner != "" && groupOwner != owner) {
				return empty()
			}
			groupOwner = owner
			if _, duplicate := seen[pod.UID]; duplicate {
				continue
			}
			projection, eligible, err := a.projection(ctx, pod)
			if err != nil {
				return discoveredEngine{}, err
			}
			if !eligible || (model != "" && projection.Model() != model) || (engine != "" && projection.Identity().LogicalEngine() != engine) {
				return empty()
			}
			model = projection.Model()
			engine = projection.Identity().LogicalEngine()
			seen[pod.UID] = struct{}{}
			projections = append(projections, projection)
		}
	}
	if len(seen) != len(selected) {
		return empty()
	}
	sort.Slice(projections, func(i, j int) bool { return projections[i].Identity().PodUID() < projections[j].Identity().PodUID() })
	level := workloads[groupOwner]
	workload, err := workloadRef(a.config.Cluster, level.object, level.kind, level.replicas)
	if err != nil {
		return discoveredEngine{}, err
	}
	members := make([]domain.PodRef, 0, len(projections))
	for _, projection := range projections {
		pod := selected[types.UID(projection.Identity().PodUID())]
		member, err := podRef(a.config.Cluster, pod, string(groupOwner), nil)
		if err != nil {
			return discoveredEngine{}, err
		}
		members = append(members, member)
	}
	state, err := domain.NewResourceState(a.config.Cluster, key, projections)
	if err != nil {
		return discoveredEngine{}, err
	}
	return discoveredEngine{state: state, workload: workload, members: members}, nil
}

func sliceHasProxyPort(slice discoveryv1.EndpointSlice, port uint16) bool {
	for _, p := range slice.Ports {
		if p.Port != nil && p.Name != nil && *p.Name == proxyPortName && uint16(*p.Port) == port {
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
