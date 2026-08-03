package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/BizerNotNull/474-Prudentia/internal/domain"
)

// OperationCatalog is the inward ledger port used by the leader protocol. Every method
// is generation-fenced by its implementation; Kubernetes cancellation is not a fence.
type OperationCatalog interface {
	ListIncompleteWorkloadOperations(context.Context, domain.WriterGeneration) ([]domain.WorkloadOperation, error)
	AdvanceWorkloadOperationFence(context.Context, domain.WriterGeneration, domain.WorkloadRef, domain.WorkloadOperationIntent) (domain.WorkloadOperation, error)
	RecordWorkloadBarrier(context.Context, domain.WriterGeneration, domain.WorkloadBarrierProof) error
	WaitForOldCallsQuiescent(context.Context, domain.WriterGeneration, domain.WorkloadOperationRef) error
	RecordWorkloadVictims(context.Context, domain.WriterGeneration, domain.WorkloadVictimObservation) error
	CompleteWorkloadOperationAndReopen(context.Context, domain.WriterGeneration, domain.WorkloadOperationRef) error
}

// WorkloadControl is the Kubernetes-facing inward port. Implementations must use the
// exact UID/resourceVersion/token preconditions represented by these domain values.
type WorkloadControl interface {
	InstallWorkloadOperationBarrier(context.Context, domain.WorkloadOperation, domain.WorkloadRef, []domain.PodRef) (domain.WorkloadBarrierProof, error)
	ObserveWorkloadVictims(context.Context, domain.WorkloadOperationRef, domain.PodUIDSet) (domain.WorkloadVictimObservation, error)
}

// DrainCatalog extends the handoff ledger protocol with durable drain preparation and
// exact accounting. PrepareDrainOperation closes admission before returning an operation.
type DrainCatalog interface {
	OperationCatalog
	PrepareDrainOperation(context.Context, domain.WriterGeneration, domain.DrainScope) (domain.WorkloadOperation, error)
	WaitForDrainQuiescence(context.Context, domain.WriterGeneration, domain.WorkloadOperationRef) error
}

type ExactPodRemoval struct {
	Pod  domain.PodRef
	Mode domain.RemovalMode
}

type DrainMutation struct {
	TargetReplicas int32
	Scale          bool
	Removals       []ExactPodRemoval
}

type DrainMutationCatalog interface {
	DrainMutation(context.Context, domain.WriterGeneration, domain.WorkloadOperationRef) (DrainMutation, error)
}

type DrainMutationControl interface {
	RemovePodExact(context.Context, domain.WorkloadOperationRef, domain.PodRef, domain.RemovalMode) error
	ScaleStatefulSetDown(context.Context, domain.WorkloadOperationRef, domain.StatefulSetRef, int32) (domain.ScalePatchResult, error)
	ScaleDeploymentAfterWholeDrain(context.Context, domain.WorkloadOperationRef, domain.DeploymentRef, int32) (domain.ScalePatchResult, error)
}

// RecoveryCatalog and RecoveryControl keep PITR policy inward: the controller orders the
// closed fence, workload roll, observation, and transactional reopen without Kubernetes DTOs.
type RecoveryCatalog interface {
	BeginFleetRecovery(context.Context, domain.WriterGeneration, domain.RecoveryEpoch) error
	CompleteFleetRecovery(context.Context, domain.WriterGeneration, domain.FleetRebuildProof) error
}

type RecoveryControl interface {
	RollManagedFleet(context.Context, domain.RecoveryEpoch) error
	ObserveFleetRebuild(context.Context, domain.WriterGeneration, domain.RecoveryEpoch) (domain.FleetRebuildProof, error)
}

func (c *Controller) operationPorts() (OperationCatalog, WorkloadControl, bool) {
	ledger, ledgerOK := c.catalog.(OperationCatalog)
	control, controlOK := c.source.(WorkloadControl)
	return ledger, control, ledgerOK && controlOK
}

// FenceWorkloadHandoff advances a durable token, observes its Kubernetes barrier, waits
// out bounded old calls, and accounts for their actual effects before reopening.
func (c *Controller) FenceWorkloadHandoff(ctx context.Context, gen domain.WriterGeneration, scope domain.WorkloadRef) (domain.WorkloadBarrierProof, error) {
	ledger, control, ok := c.operationPorts()
	if !ok {
		return domain.WorkloadBarrierProof{}, errors.New("workload handoff ports are unavailable")
	}
	op, err := ledger.AdvanceWorkloadOperationFence(ctx, gen, scope, domain.OperationHandoff)
	if err != nil {
		return domain.WorkloadBarrierProof{}, fmt.Errorf("advance workload handoff fence: %w", err)
	}
	proof, err := control.InstallWorkloadOperationBarrier(ctx, op, scope, nil)
	if err != nil {
		return domain.WorkloadBarrierProof{}, fmt.Errorf("install workload handoff barrier: %w", err)
	}
	if err := ledger.RecordWorkloadBarrier(ctx, gen, proof); err != nil {
		return domain.WorkloadBarrierProof{}, fmt.Errorf("record workload handoff barrier: %w", err)
	}
	if err := ledger.WaitForOldCallsQuiescent(ctx, gen, op.Ref()); err != nil {
		return domain.WorkloadBarrierProof{}, fmt.Errorf("wait for old Kubernetes calls: %w", err)
	}
	beforeValues := make([]string, 0, len(proof.Pods()))
	for _, pod := range proof.Pods() {
		beforeValues = append(beforeValues, pod.UID())
	}
	before, err := domain.NewPodUIDSet(beforeValues)
	if err != nil {
		return domain.WorkloadBarrierProof{}, err
	}
	victims, err := control.ObserveWorkloadVictims(ctx, op.Ref(), before)
	if err != nil {
		return domain.WorkloadBarrierProof{}, fmt.Errorf("observe workload handoff victims: %w", err)
	}
	if err := ledger.RecordWorkloadVictims(ctx, gen, victims); err != nil {
		return domain.WorkloadBarrierProof{}, fmt.Errorf("record workload handoff victims: %w", err)
	}
	if err := ledger.CompleteWorkloadOperationAndReopen(ctx, gen, op.Ref()); err != nil {
		return domain.WorkloadBarrierProof{}, fmt.Errorf("complete workload handoff: %w", err)
	}
	return proof, nil
}

// ReconcileDrain keeps admission closed from durable preparation through barrier,
// quiescence, and actual-victim accounting. Exact mutation planning remains ledger-owned.
func (c *Controller) ReconcileDrain(ctx context.Context, gen domain.WriterGeneration, scope domain.DrainScope) error {
	ledger, ok := c.catalog.(DrainCatalog)
	control, controlOK := c.source.(WorkloadControl)
	if !ok || !controlOK {
		return errors.New("drain ports are unavailable")
	}
	op, err := ledger.PrepareDrainOperation(ctx, gen, scope)
	if err != nil {
		return fmt.Errorf("prepare durable drain: %w", err)
	}
	proof, err := control.InstallWorkloadOperationBarrier(ctx, op, op.Scope(), nil)
	if err != nil {
		return fmt.Errorf("install drain barrier: %w", err)
	}
	if err := ledger.RecordWorkloadBarrier(ctx, gen, proof); err != nil {
		return fmt.Errorf("record drain barrier: %w", err)
	}
	if err := ledger.WaitForDrainQuiescence(ctx, gen, op.Ref()); err != nil {
		return fmt.Errorf("wait for drain quiescence: %w", err)
	}
	beforeValues := make([]string, 0, len(proof.Pods()))
	for _, pod := range proof.Pods() {
		beforeValues = append(beforeValues, pod.UID())
	}
	before, err := domain.NewPodUIDSet(beforeValues)
	if err != nil {
		return err
	}
	if planner, ok := c.catalog.(DrainMutationCatalog); ok {
		mutator, ok := c.source.(DrainMutationControl)
		if !ok {
			return errors.New("drain mutation is planned but conditional Kubernetes mutation is unavailable")
		}
		mutation, err := planner.DrainMutation(ctx, gen, op.Ref())
		if err != nil {
			return fmt.Errorf("read current drain mutation: %w", err)
		}
		for _, removal := range mutation.Removals {
			if err := mutator.RemovePodExact(ctx, op.Ref(), removal.Pod, removal.Mode); err != nil {
				return fmt.Errorf("remove exact drained Pod: %w", err)
			}
		}
		if mutation.Scale {
			if op.Scope().Kind() == domain.WorkloadDeployment {
				ref, err := domain.NewDeploymentRef(proof.Workload())
				if err != nil {
					return err
				}
				if _, err := mutator.ScaleDeploymentAfterWholeDrain(ctx, op.Ref(), ref, mutation.TargetReplicas); err != nil {
					return fmt.Errorf("conditionally scale Deployment: %w", err)
				}
			} else {
				ref, err := domain.NewStatefulSetRef(proof.Workload())
				if err != nil {
					return err
				}
				if _, err := mutator.ScaleStatefulSetDown(ctx, op.Ref(), ref, mutation.TargetReplicas); err != nil {
					return fmt.Errorf("conditionally scale StatefulSet: %w", err)
				}
			}
		}
	}
	victims, err := control.ObserveWorkloadVictims(ctx, op.Ref(), before)
	if err != nil {
		return fmt.Errorf("observe drain victims: %w", err)
	}
	if err := ledger.RecordWorkloadVictims(ctx, gen, victims); err != nil {
		return fmt.Errorf("record drain victims: %w", err)
	}
	if err := ledger.CompleteWorkloadOperationAndReopen(ctx, gen, op.Ref()); err != nil {
		return fmt.Errorf("complete drain: %w", err)
	}
	return nil
}

func (c *Controller) RecoverAfterLedgerRestore(ctx context.Context, gen domain.WriterGeneration, epoch domain.RecoveryEpoch) error {
	ledger, ok := c.catalog.(RecoveryCatalog)
	control, controlOK := c.source.(RecoveryControl)
	if !ok || !controlOK {
		return errors.New("fleet recovery ports are unavailable")
	}
	if err := ledger.BeginFleetRecovery(ctx, gen, epoch); err != nil {
		return fmt.Errorf("begin fleet recovery fence: %w", err)
	}
	if err := control.RollManagedFleet(ctx, epoch); err != nil {
		return fmt.Errorf("roll managed fleet recovery epoch: %w", err)
	}
	proof, err := control.ObserveFleetRebuild(ctx, gen, epoch)
	if err != nil {
		return fmt.Errorf("observe fleet rebuild: %w", err)
	}
	if err := ledger.CompleteFleetRecovery(ctx, gen, proof); err != nil {
		return fmt.Errorf("complete fleet recovery: %w", err)
	}
	return nil
}
