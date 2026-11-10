package networkruntime

import (
	"testing"
	"time"

	bitcoin "github.com/cylewitruk-stacks/stacks-k8s/apis/network/bitcoin/v1alpha2"
	common "github.com/cylewitruk-stacks/stacks-k8s/apis/network/common/v1alpha2"
	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
	"github.com/cylewitruk-stacks/stacks-k8s/operators/network/internal/foundation"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// restoreFixture pins a snapshot in the gate fixture's genesis and returns a preparation record
// whose first ceiling was observed long enough ago to expire a gate-by-gate deadline.
func restoreFixture(restored bool) (
	*api.StacksNetwork,
	*api.StacksGenesis,
	*api.StacksChainstateSnapshot,
	*bitcoin.BitcoinInitialization,
) {
	root, g := gateFixture()
	snapshot := &api.StacksChainstateSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "seed", Namespace: root.Namespace, UID: "snapshot"},
		Spec:       api.StacksChainstateSnapshotSpec{Image: "snapshot:test"},
	}
	if restored {
		g.Spec.Source.Dependencies = append(g.Spec.Source.Dependencies, common.Binding{
			Kind:        api.KindStacksChainstateSnapshot,
			Name:        snapshot.Name,
			UID:         snapshot.UID,
			Fingerprint: foundation.Digest(snapshot.Spec),
		})
		root.Status.GenesisRef.Fingerprint = foundation.Digest(g.Spec)
	}
	first := metav1.NewTime(time.Now().Add(-3 * time.Minute).Truncate(time.Second))
	record := &bitcoin.BitcoinInitialization{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "initial",
			Namespace:       root.Namespace,
			UID:             "initial",
			OwnerReferences: g.OwnerReferences,
		},
		Spec:   bitcoin.BitcoinInitializationSpec{NetworkUID: root.UID},
		Status: bitcoin.BitcoinInitializationStatus{FirstCeilingObservedAt: &first},
	}
	return root, g, snapshot, record
}

func TestRestoredGatesWaitForPreparationWithoutLatching(t *testing.T) {
	for _, restored := range []bool{false, true} {
		root, g, snapshot, record := restoreFixture(restored)
		r := fixtureReconciler(t, g, snapshot, record)
		failed, err := r.reconcileGates(t.Context(), root, record, nil)
		if err != nil {
			t.Fatal(err)
		}
		state := root.Status.Initialization
		if !restored {
			// Control: the same record on a network initialized from genesis expires its deadline.
			if !failed || !meta.IsStatusConditionTrue(root.Status.Conditions, "Failed") {
				t.Fatalf("control did not latch the first-gate deadline: %+v", root.Status)
			}
			continue
		}
		if failed || meta.IsStatusConditionTrue(root.Status.Conditions, "Failed") || state.Completed ||
			state.GateIndex != 0 || state.RestoredFrom != nil {
			t.Fatalf("restored network latched or completed before preparation: %+v", root.Status)
		}
	}
}

func TestRestoredPreparationCompletesEveryGate(t *testing.T) {
	root, g, snapshot, record := restoreFixture(true)
	prepared := metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
	record.Status.PreparedAt = &prepared
	r := fixtureReconciler(t, g, snapshot, record)
	failed, err := r.reconcileGates(t.Context(), root, record, nil)
	if err != nil || failed {
		t.Fatalf("failed=%v err=%v", failed, err)
	}
	state := root.Status.Initialization
	if !state.Completed || int(state.GateIndex) != len(g.Spec.Bootstrap.Gates) || state.AuthorizedCeiling != 251 {
		t.Fatalf("restore did not complete the frozen gates: %+v", state)
	}
	for _, gate := range state.Gates {
		if gate.CompletedAt == nil || !gate.CompletedAt.Equal(&prepared) {
			t.Fatalf("gate %s completion %v, want preparation time", gate.Name, gate.CompletedAt)
		}
	}
	if ref := state.RestoredFrom; ref == nil || ref.Kind != api.KindStacksChainstateSnapshot ||
		ref.Name != "seed" || ref.UID != "snapshot" || ref.Fingerprint != foundation.Digest(snapshot.Spec) {
		t.Fatalf("completion does not pin its snapshot: %+v", state.RestoredFrom)
	}
	// Completion is retained: a later pass neither re-reads the snapshot nor changes the record.
	if err := r.Client.Delete(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	before := state.DeepCopy()
	if _, err := r.reconcileGates(t.Context(), root, record, nil); err != nil {
		t.Fatal(err)
	}
	if !before.Completed || root.Status.Initialization.RestoredFrom == nil ||
		!root.Status.Initialization.Completed {
		t.Fatalf("completed restore was not retained: %+v", root.Status.Initialization)
	}
}

func TestRestoredGatesNeedTheirSnapshot(t *testing.T) {
	root, g, _, record := restoreFixture(true)
	r := fixtureReconciler(t, g, record)
	if _, err := r.reconcileGates(t.Context(), root, record, nil); err == nil {
		t.Fatal("a pinned snapshot that no longer exists must not be guessed around")
	}
	if root.Status.Initialization != nil && root.Status.Initialization.Completed {
		t.Fatal("completed without the pinned snapshot")
	}
}
