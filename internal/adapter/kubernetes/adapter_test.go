package kubernetes

import (
	"context"
	"testing"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type fakeIdentityRegistry struct {
	ready bool
}

func (f fakeIdentityRegistry) RegistrationReady(context.Context, domain.WorkloadIdentity, string) (bool, error) {
	return f.ready, nil
}

func TestProjectionRequiresReadyExactIdentity(t *testing.T) {
	adapter := &Adapter{config: Config{Cluster: "cluster-a", ProxyPort: 8443, ObservationTTL: 30 * time.Second, IdentityRegistry: fakeIdentityRegistry{ready: true}}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "models", Name: "engine-0", UID: types.UID("pod-uid-1"),
			Annotations: map[string]string{
				annotationModel: "model-a", annotationEngine: "engine-a",
				annotationEndpointEpoch: "3", annotationRecoveryEpoch: "2",
				annotationSlots: "4",
			},
		},
		Status: corev1.PodStatus{
			PodIP:      "10.0.0.7",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	projection, eligible, err := adapter.projection(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	if !eligible {
		t.Fatal("ready exact-identity Pod was not eligible")
	}
	if projection.Endpoint() != "https://10.0.0.7:8443" || projection.ConfiguredSlots() != 4 || projection.Identity().PodUID() != "pod-uid-1" {
		t.Fatalf("unexpected projection: endpoint=%q slots=%d uid=%q", projection.Endpoint(), projection.ConfiguredSlots(), projection.Identity().PodUID())
	}

	adapter.config.IdentityRegistry = fakeIdentityRegistry{}
	if _, eligible, err := adapter.projection(context.Background(), pod); err != nil || eligible {
		t.Fatalf("Pod without attested proxy registration became eligible: eligible=%v err=%v", eligible, err)
	}
}
