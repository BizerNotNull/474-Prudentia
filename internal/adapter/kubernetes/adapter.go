package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	annotationModel         = "prudentia.io/model"
	annotationEngine        = "prudentia.io/logical-engine"
	annotationEndpointEpoch = "prudentia.io/endpoint-epoch"
	annotationRecoveryEpoch = "prudentia.io/recovery-epoch"
	annotationSlots         = "prudentia.io/configured-slots"
	annotationProxyReady    = "prudentia.io/proxy-ready"
)

type Config struct {
	Cluster              string
	Namespace            string
	LabelSelector        string
	ProxyPort            uint16
	ObservationTTL       time.Duration
	ResyncPeriod         time.Duration
	LeaseNamespace       string
	LeaseName            string
	Holder               string
	LeaseDuration        time.Duration
	RenewDeadline        time.Duration
	RetryPeriod          time.Duration
	MutationCallLifetime time.Duration
	IdentityFence        IdentityFence
}

type Adapter struct {
	config       Config
	client       kubernetes.Interface
	coordination coordinationv1.CoordinationV1Interface
	informer     cache.SharedIndexInformer
	startOnce    sync.Once
	started      chan struct{}
}

func NewInCluster(config Config) (*Adapter, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster Kubernetes configuration: %w", err)
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	return New(client, config)
}

func New(client kubernetes.Interface, config Config) (*Adapter, error) {
	if client == nil || config.Cluster == "" || config.Namespace == "" || config.ProxyPort == 0 || config.ObservationTTL <= 0 || config.ObservationTTL > 10*time.Minute || config.ResyncPeriod < time.Second || config.LeaseNamespace == "" || config.LeaseName == "" || config.Holder == "" || config.LeaseDuration <= 0 || config.RenewDeadline <= 0 || config.RetryPeriod <= 0 || config.RenewDeadline >= config.LeaseDuration || config.RetryPeriod >= config.RenewDeadline {
		return nil, errors.New("invalid Kubernetes adapter configuration")
	}
	factory := informers.NewSharedInformerFactoryWithOptions(client, config.ResyncPeriod,
		informers.WithNamespace(config.Namespace),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = config.LabelSelector
			options.FieldSelector = fields.Everything().String()
		}))
	return &Adapter{
		config: config, client: client, coordination: client.CoordinationV1(),
		informer: factory.Core().V1().Pods().Informer(), started: make(chan struct{}),
	}, nil
}

func (a *Adapter) RunDiscovery(ctx context.Context, sink func(domain.ResourceKey)) error {
	return a.Run(ctx, sink)
}

func (a *Adapter) Run(ctx context.Context, sink func(domain.ResourceKey)) error {
	if sink == nil {
		return errors.New("nil reconcile sink")
	}
	_, err := a.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(object any) { a.enqueueObject(object, sink) },
		UpdateFunc: func(_, current any) { a.enqueueObject(current, sink) },
		DeleteFunc: func(object any) { a.enqueueDeleted(object, sink) },
	})
	if err != nil {
		return fmt.Errorf("register Pod event handler: %w", err)
	}
	a.startOnce.Do(func() { close(a.started) })
	go a.informer.Run(ctx.Done())
	ticker := time.NewTicker(a.config.ResyncPeriod)
	defer ticker.Stop()
	for {
		if err := a.enqueueServices(ctx, sink); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (a *Adapter) WaitForSync(ctx context.Context) error {
	select {
	case <-a.started:
	case <-ctx.Done():
		return ctx.Err()
	}
	if !cache.WaitForCacheSync(ctx.Done(), a.informer.HasSynced) {
		return errors.New("Pod informer cache did not synchronize")
	}
	return nil
}

func (a *Adapter) ListKeys(ctx context.Context) ([]domain.ResourceKey, error) {
	services, err := a.client.CoreV1().Services(a.config.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list opted-in Services: %w", err)
	}
	keys := make([]domain.ResourceKey, 0, len(services.Items))
	for _, service := range services.Items {
		if service.Annotations[annotationEnabled] != "true" || service.Spec.ClusterIP != corev1.ClusterIPNone {
			continue
		}
		key, err := domain.NewResourceKey(service.Namespace, service.Name)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (a *Adapter) enqueueServices(ctx context.Context, sink func(domain.ResourceKey)) error {
	keys, err := a.ListKeys(ctx)
	if err != nil {
		return err
	}
	for _, key := range keys {
		sink(key)
	}
	return nil
}

func (a *Adapter) Reconcile(ctx context.Context, key domain.ResourceKey) (domain.ResourceState, error) {
	if service, err := a.client.CoreV1().Services(key.Namespace()).Get(ctx, key.Name(), metav1.GetOptions{}); err == nil {
		if service.Annotations[annotationEnabled] == "true" {
			return a.reconcileService(ctx, key)
		}
	} else if !apierrors.IsNotFound(err) {
		return domain.ResourceState{}, fmt.Errorf("read Service level state: %w", err)
	}
	pod, err := a.client.CoreV1().Pods(key.Namespace()).Get(ctx, key.Name(), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return domain.NewResourceState(a.config.Cluster, key, nil)
		}
		return domain.ResourceState{}, fmt.Errorf("read Pod level state: %w", err)
	}
	projection, eligible, err := a.projection(pod)
	if err != nil {
		return domain.ResourceState{}, err
	}
	if !eligible {
		return domain.NewResourceState(a.config.Cluster, key, nil)
	}
	return domain.NewResourceState(a.config.Cluster, key, []domain.BackendProjection{projection})
}

func (a *Adapter) Elect(ctx context.Context, callback func(context.Context) error) error {
	if callback == nil {
		return errors.New("nil leader callback")
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Namespace: a.config.LeaseNamespace, Name: a.config.LeaseName},
		Client:     a.coordination,
		LockConfig: resourcelock.ResourceLockConfig{Identity: a.config.Holder},
	}
	callbackErr := make(chan error, 1)
	electionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: lock, LeaseDuration: a.config.LeaseDuration, RenewDeadline: a.config.RenewDeadline, RetryPeriod: a.config.RetryPeriod,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				callbackErr <- callback(leaderCtx)
				cancel()
			},
			OnStoppedLeading: func() {},
		},
	})
	if err != nil {
		return fmt.Errorf("configure leader election: %w", err)
	}
	elector.Run(electionCtx)
	select {
	case err := <-callbackErr:
		return err
	default:
		return nil
	}
}

func (a *Adapter) enqueueObject(object any, sink func(domain.ResourceKey)) {
	key, err := cache.MetaNamespaceKeyFunc(object)
	if err == nil {
		a.enqueueKey(key, sink)
	}
}

func (a *Adapter) enqueueDeleted(object any, sink func(domain.ResourceKey)) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(object)
	if err == nil {
		a.enqueueKey(key, sink)
	}
}

func (a *Adapter) enqueueKey(value string, sink func(domain.ResourceKey)) {
	namespace, name, err := cache.SplitMetaNamespaceKey(value)
	if err != nil {
		return
	}
	key, err := domain.NewResourceKey(namespace, name)
	if err == nil {
		sink(key)
	}
}

func (a *Adapter) projection(pod *corev1.Pod) (domain.BackendProjection, bool, error) {
	if pod.DeletionTimestamp != nil || pod.Status.PodIP == "" || pod.Annotations[annotationProxyReady] != "true" || !podReady(pod) {
		return domain.BackendProjection{}, false, nil
	}
	model := pod.Annotations[annotationModel]
	engine := pod.Annotations[annotationEngine]
	endpointEpoch, err := strconv.ParseUint(pod.Annotations[annotationEndpointEpoch], 10, 64)
	if err != nil || endpointEpoch == 0 {
		return domain.BackendProjection{}, false, nil
	}
	recoveryEpoch, err := strconv.ParseUint(pod.Annotations[annotationRecoveryEpoch], 10, 64)
	if err != nil || recoveryEpoch == 0 {
		return domain.BackendProjection{}, false, nil
	}
	slots, err := strconv.ParseUint(pod.Annotations[annotationSlots], 10, 32)
	if err != nil || slots == 0 || slots > 1024 {
		return domain.BackendProjection{}, false, nil
	}
	identity, err := domain.NewWorkloadIdentity(domain.WorkloadIdentityParams{
		Cluster: a.config.Cluster, Namespace: pod.Namespace, LogicalEngine: engine,
		PodUID: string(pod.UID), EndpointEpoch: endpointEpoch, RecoveryEpoch: recoveryEpoch,
	})
	if err != nil {
		return domain.BackendProjection{}, false, nil
	}
	endpoint := "https://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(a.config.ProxyPort)))
	projection, err := domain.NewBackendProjection(domain.BackendProjectionParams{
		Identity: identity, Model: model, Endpoint: endpoint,
		ConfiguredSlots: uint32(slots), FreshFor: a.config.ObservationTTL,
	})
	if err != nil {
		return domain.BackendProjection{}, false, nil
	}
	return projection, true, nil
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
