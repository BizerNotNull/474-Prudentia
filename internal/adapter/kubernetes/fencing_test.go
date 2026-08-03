package kubernetes

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestJoinedDiscoveryDeduplicatesHintsAndTracksReplacementUID(t *testing.T) {
	ready := true
	port := int32(8443)
	portName := proxyPortName
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "engine", Annotations: map[string]string{annotationEnabled: "true"}}, Spec: corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone, Selector: map[string]string{"app": "engine"}}}
	pod := eligiblePod("new-uid", "10.0.0.2")
	controlled := true
	workload := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "engine", UID: "workload-uid", ResourceVersion: "workload-rv"}}
	pod.OwnerReferences = []metav1.OwnerReference{{UID: workload.UID, Controller: &controlled}}
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "engine-a", Labels: map[string]string{discoveryv1.LabelServiceName: "engine"}}, Ports: []discoveryv1.EndpointPort{{Name: &portName, Port: &port}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.2"}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}, TargetRef: &corev1.ObjectReference{Kind: "Pod", UID: pod.UID}}, {Addresses: []string{"10.0.0.2"}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}, TargetRef: &corev1.ObjectReference{Kind: "Pod", UID: pod.UID}}}}
	client := fake.NewClientset(service, workload, pod, slice)
	a := &Adapter{client: client, config: Config{Cluster: "cluster", Namespace: "models", ProxyPort: 8443, ObservationTTL: time.Minute, IdentityRegistry: fakeIdentityRegistry{ready: true}}}
	key, _ := domain.NewResourceKey("models", "engine")
	first, err := a.reconcileService(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	second, err := a.reconcileService(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Projections()) != 1 || len(second.Projections()) != 1 || first.Projections()[0].Identity().PodUID() != "new-uid" || second.Projections()[0].Identity().PodUID() != "new-uid" {
		t.Fatal("duplicate hints did not converge to one replacement identity")
	}
}

func TestDiscoveryRejectsIncompleteAndMixedRevisionGroups(t *testing.T) {
	ready := true
	port, portName := int32(8443), proxyPortName
	replicas := int32(2)
	controlled := true
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "engine", Annotations: map[string]string{annotationEnabled: "true"}}, Spec: corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone, Selector: map[string]string{"app": "engine"}}}
	workload := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "engine", UID: "workload-uid", ResourceVersion: "workload-rv"}, Spec: appsv1.StatefulSetSpec{Replicas: &replicas}}
	first, second := eligiblePod("uid-1", "10.0.0.1"), eligiblePod("uid-2", "10.0.0.2")
	first.OwnerReferences = []metav1.OwnerReference{{UID: workload.UID, Controller: &controlled}}
	second.OwnerReferences = first.OwnerReferences
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "engine-a", Labels: map[string]string{discoveryv1.LabelServiceName: "engine"}}, Ports: []discoveryv1.EndpointPort{{Name: &portName, Port: &port}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{first.Status.PodIP}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}, TargetRef: &corev1.ObjectReference{Kind: "Pod", UID: first.UID}}}}
	client := fake.NewClientset(service, workload, first, second, slice)
	adapter := &Adapter{client: client, config: Config{Cluster: "cluster", Namespace: "models", ProxyPort: 8443, ObservationTTL: time.Minute, IdentityRegistry: fakeIdentityRegistry{ready: true}}}
	key, _ := domain.NewResourceKey("models", "engine")
	state, err := adapter.reconcileService(context.Background(), key)
	if err != nil || len(state.Projections()) != 0 {
		t.Fatalf("incomplete engine group became eligible: projections=%d err=%v", len(state.Projections()), err)
	}
	slice.Endpoints = append(slice.Endpoints, discoveryv1.Endpoint{Addresses: []string{second.Status.PodIP}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}, TargetRef: &corev1.ObjectReference{Kind: "Pod", UID: second.UID}})
	if _, err := client.DiscoveryV1().EndpointSlices("models").Update(context.Background(), slice, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	second.Annotations[annotationModel] = "different-revision"
	if _, err := client.CoreV1().Pods("models").Update(context.Background(), second, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	state, err = adapter.reconcileService(context.Background(), key)
	if err != nil || len(state.Projections()) != 0 {
		t.Fatalf("mixed-revision engine group became eligible: projections=%d err=%v", len(state.Projections()), err)
	}
}

func TestDiscoveryEnqueuesConsumingServiceAndUsesMonotonicCompleteFacts(t *testing.T) {
	ready := true
	port, portName := int32(8443), proxyPortName
	controlled := true
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "engine", Annotations: map[string]string{annotationEnabled: "true"}}, Spec: corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone, Selector: map[string]string{"app": "engine"}}}
	workload := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "engine", UID: "workload-uid", ResourceVersion: "workload-rv"}}
	pod := eligiblePod("uid-1", "10.0.0.1")
	pod.OwnerReferences = []metav1.OwnerReference{{UID: workload.UID, Controller: &controlled}}
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "engine-a", Labels: map[string]string{discoveryv1.LabelServiceName: "engine"}}, Ports: []discoveryv1.EndpointPort{{Name: &portName, Port: &port}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{pod.Status.PodIP}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}, TargetRef: &corev1.ObjectReference{Kind: "Pod", UID: pod.UID}}}}
	adapter := &Adapter{client: fake.NewClientset(service, workload, pod, slice), config: Config{Cluster: "cluster", Namespace: "models", ProxyPort: 8443, ObservationTTL: time.Minute, IdentityRegistry: fakeIdentityRegistry{ready: true}}}
	var keys []domain.ResourceKey
	adapter.enqueueObject(pod, func(key domain.ResourceKey) { keys = append(keys, key) })
	if len(keys) != 1 || keys[0].Name() != "engine" {
		t.Fatalf("Pod hint enqueued %v, want consuming Service engine", keys)
	}
	key := keys[0]
	first, err := adapter.ReconcileDiscovery(context.Background(), key, 3)
	if err != nil {
		t.Fatal(err)
	}
	second, err := adapter.ReconcileDiscovery(context.Background(), key, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || len(second) != 1 || second[0].Stamp().Sequence() <= first[0].Stamp().Sequence() {
		t.Fatalf("source sequence did not advance across writer generations: first=%v second=%v", first, second)
	}
	fact, ok := second[0].Structural()
	if !ok || fact.Workload().UID() != "workload-uid" || len(fact.Members()) != 1 || fact.Members()[0].UID() != "uid-1" {
		t.Fatal("structural observation did not bind exact complete workload membership")
	}
}

func TestBarrierPatchTestsUIDResourceVersionAndPreviousToken(t *testing.T) {
	replicas := int32(1)
	controlled := true
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "engine", UID: "workload-uid", ResourceVersion: "7", Annotations: map[string]string{annotationOperationToken: "old-token"}}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}}
	pod := eligiblePod("pod-uid", "10.0.0.3")
	pod.ResourceVersion = "9"
	pod.OwnerReferences = []metav1.OwnerReference{{UID: deployment.UID, Controller: &controlled}}
	client := fake.NewClientset(deployment, pod)
	var patches [][]byte
	client.PrependReactor("patch", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
		patches = append(patches, append([]byte(nil), action.(ktesting.PatchAction).GetPatch()...))
		return false, nil, nil
	})
	a := &Adapter{client: client, config: Config{Cluster: "cluster", Namespace: "models", MutationCallLifetime: time.Second}}
	resource, _ := domain.NewResourceRef(domain.ResourceRefParams{Cluster: "cluster", Namespace: "models", Name: "engine", UID: "workload-uid", ResourceVersion: "7"})
	workload, _ := domain.NewWorkloadRef(domain.WorkloadDeployment, resource, 1)
	op, _ := domain.NewWorkloadOperation(domain.WorkloadOperationParams{Scope: workload, Intent: domain.OperationHandoff, Phase: domain.OperationBarrierPending, Generation: 2, Token: "new-token", OldCallsQuiescentAfter: time.Now().Add(time.Second)})
	if _, err := a.InstallWorkloadOperationBarrier(context.Background(), op, workload, nil); err != nil {
		t.Fatal(err)
	}
	if len(patches) != 2 {
		t.Fatalf("patch count=%d, want workload and Pod", len(patches))
	}
	for _, raw := range patches {
		var operations []map[string]any
		if err := json.Unmarshal(raw, &operations); err != nil {
			t.Fatal(err)
		}
		paths := map[string]bool{}
		for _, operation := range operations {
			if operation["op"] == "test" {
				paths[operation["path"].(string)] = true
			}
		}
		if !paths["/metadata/uid"] || !paths["/metadata/resourceVersion"] {
			t.Fatalf("barrier lacks atomic identity tests: %s", raw)
		}
	}
	var workloadOps []map[string]any
	_ = json.Unmarshal(patches[0], &workloadOps)
	foundPrior, foundTemplate := false, false
	for _, operation := range workloadOps {
		foundPrior = foundPrior || operation["path"] == "/metadata/annotations/prudentia.io~1operation-token" && operation["value"] == "old-token"
		foundTemplate = foundTemplate || operation["path"] == "/spec/template/metadata/annotations"
	}
	if !foundPrior || !foundTemplate {
		t.Fatal("workload barrier did not test previous token and close the replacement Pod template")
	}
}

func TestExactRemovalCarriesUIDAndTokenBoundResourceVersion(t *testing.T) {
	client := fake.NewClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "pod", UID: "pod-uid", ResourceVersion: "token-rv"}})
	a := &Adapter{client: client, config: Config{MutationCallLifetime: time.Second}}
	resource, _ := domain.NewResourceRef(domain.ResourceRefParams{Cluster: "cluster", Namespace: "models", Name: "pod", UID: "pod-uid", ResourceVersion: "token-rv"})
	pod, _ := domain.NewPodRef(domain.PodRefParams{Resource: resource, WorkloadUID: "workload-uid", OperationGeneration: 4, OperationToken: "token"})
	op, _ := domain.NewWorkloadOperationRef("workload-uid", 4, "token")
	if err := a.RemovePodExact(context.Background(), op, pod, domain.RemovalDelete); err != nil {
		t.Fatal(err)
	}
	action := client.Actions()[0].(ktesting.DeleteAction)
	preconditions := action.GetDeleteOptions().Preconditions
	if preconditions == nil || preconditions.UID == nil || *preconditions.UID != types.UID("pod-uid") || preconditions.ResourceVersion == nil || *preconditions.ResourceVersion != "token-rv" {
		t.Fatal("exact delete omitted UID or token-bound resourceVersion")
	}
}

func eligiblePod(uid, ip string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "pod-" + uid, UID: types.UID(uid), ResourceVersion: "pod-rv-" + uid, Labels: map[string]string{"app": "engine"}, Annotations: map[string]string{annotationModel: "engine", annotationEngine: "engine", annotationEndpointEpoch: "1", annotationRecoveryEpoch: "1", annotationSlots: "2"}}, Status: corev1.PodStatus{PodIP: ip, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
}
