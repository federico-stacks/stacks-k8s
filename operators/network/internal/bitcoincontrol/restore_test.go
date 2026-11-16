package bitcoincontrol

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	bitcoin "github.com/cylewitruk-stacks/stacks-k8s/apis/network/bitcoin/v1alpha2"
	common "github.com/cylewitruk-stacks/stacks-k8s/apis/network/common/v1alpha2"
	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
	"github.com/cylewitruk-stacks/stacks-k8s/operators/network/internal/foundation"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// restoreFrom pins a snapshot captured at height in the fixture's frozen genesis and moves the
// modeled node to that restored chain, past the first gate's ceiling.
func (f *testFixture) restoreFrom(t *testing.T, height int64) *api.StacksChainstateSnapshot {
	t.Helper()
	ctx := context.Background()
	snapshot := &api.StacksChainstateSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "seed", Namespace: "test"},
		Spec: api.StacksChainstateSnapshotSpec{
			Image:   "snapshot:test",
			Bitcoin: api.SnapshotBitcoinTip{Height: height, BlockHash: fmt.Sprintf("%064x", height)},
		},
	}
	if e := f.c.Create(ctx, snapshot); e != nil {
		t.Fatal(e)
	}
	genesis := &api.StacksGenesis{}
	if e := f.c.Get(ctx, client.ObjectKey{Namespace: "test", Name: "genesis"}, genesis); e != nil {
		t.Fatal(e)
	}
	genesis.Spec.Source.Dependencies = append(genesis.Spec.Source.Dependencies, common.Binding{
		Kind:        api.KindStacksChainstateSnapshot,
		Name:        snapshot.Name,
		UID:         snapshot.UID,
		Fingerprint: foundation.Digest(snapshot.Spec),
	})
	if e := f.c.Update(ctx, genesis); e != nil {
		t.Fatal(e)
	}
	ref := binding("StacksGenesis", genesis)
	ref.Fingerprint = foundation.Digest(genesis.Spec)
	root := &api.StacksNetwork{}
	if e := f.c.Get(ctx, client.ObjectKeyFromObject(f.root), root); e != nil {
		t.Fatal(e)
	}
	root.Status.GenesisRef = &ref
	if e := f.c.Status().Update(ctx, root); e != nil {
		t.Fatal(e)
	}
	initial := f.readInitial(t)
	initial.Spec.Genesis = ref
	if e := f.c.Update(ctx, initial); e != nil {
		t.Fatal(e)
	}
	f.rpc.height = height
	return snapshot
}

func (f *testFixture) generated() int {
	f.rpc.mu.Lock()
	defer f.rpc.mu.Unlock()
	n := 0
	for _, call := range f.rpc.calls {
		if strings.HasPrefix(call, "Generate") {
			n++
		}
	}
	return n
}

func TestRestoreRefreshesTheTipOnceThenPrepares(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.worker.Now = func() time.Time { return f.now }
	f.restoreFrom(t, 217)
	s := f.schedulerFor()
	if e := f.worker.Step(ctx); e != nil {
		t.Fatal(e)
	}
	f.reconcile(t, s)
	f.reconcile(t, s)
	offer := f.readRecord(t).Spec.Offer
	if offer == nil || offer.Mode != bitcoin.OfferRestore || offer.ExpectedHeight != 217 ||
		offer.ExpectedTip != fmt.Sprintf("%064x", 217) || offer.Ceiling != 218 || offerBlocks(offer) != 1 ||
		offer.Address != testAddress {
		t.Fatalf("restore must offer exactly one block on the snapshot tip: %+v", offer)
	}
	if initial := f.readInitial(t); initial.Status.PreparedAt != nil {
		t.Fatal("preparation recorded before the refresh block")
	}
	if e := f.worker.Step(ctx); e != nil {
		t.Fatal(e)
	}
	eventually(t, func() bool { return f.readRecord(t).Status.CompletedOffer == offer.Number })
	if e := f.worker.Step(ctx); e != nil {
		t.Fatal(e)
	}
	f.now = f.now.Add(time.Second)
	f.reconcile(t, s)
	initial := f.readInitial(t)
	if initial.Status.PreparedAt == nil || initial.Status.Reason != reasonRestorePrepared ||
		initial.Status.LastAccountedOffer != offer.Number || len(initial.Status.Funded) != 0 ||
		f.rpc.height != 218 || f.generated() != 1 {
		t.Fatalf("restore did not prepare on the refreshed tip: %+v height %d", initial.Status, f.rpc.height)
	}
	for range 3 {
		f.now = f.now.Add(time.Second)
		f.reconcile(t, s)
		_ = f.worker.Step(ctx)
	}
	if f.generated() != 1 {
		t.Fatal("restore generated more than the single refresh block")
	}
}

// A restored miner has usually spent part of its bootstrap funding on block commits, so the restore
// needs only the mature output the miner start gate requires, not MatureOutputsPerMiner.
func TestRestoreRequiresOneMatureOutputPerMiner(t *testing.T) {
	// The fixture's wallet has one mature coinbase output from height 101.
	for name, height := range map[string]int64{
		"bootstrap funding partly spent": 217,
		"no mature output":               100,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			f.worker.Now = func() time.Time { return f.now }
			f.restoreFrom(t, height)
			initial := f.readInitial(t)
			initial.Spec.MatureOutputsPerMiner = 2
			if e := f.c.Update(ctx, initial); e != nil {
				t.Fatal(e)
			}
			if e := f.worker.Step(ctx); e != nil {
				t.Fatal(e)
			}
			s := f.schedulerFor()
			f.reconcile(t, s)
			f.reconcile(t, s)
			initial = f.readInitial(t)
			if height >= 101 {
				if offer := initial.Status.Offer; offer == nil || offer.Mode != bitcoin.OfferRestore {
					t.Fatalf("restore must offer the refresh with one mature output: %+v", initial.Status)
				}
				return
			}
			if initial.Status.Offer != nil || initial.Status.Phase != bitcoin.InitializationBlocked ||
				initial.Status.Reason != reasonRestoredWalletsImmature || f.generated() != 0 {
				t.Fatalf("restore must block without a mature miner output: %+v", initial.Status)
			}
		})
	}
}

// The worker reads the pinned snapshot to authorize the refresh, so its Role must grant exactly
// that snapshot, and none for a network initialized from genesis.
func TestWorkerRoleGrantsOnlyThePinnedSnapshot(t *testing.T) {
	f := baselineFixture(t)
	genesis := &api.StacksGenesis{ObjectMeta: metav1.ObjectMeta{
		Name: f.initial.Spec.Genesis.Name, Namespace: "test", UID: f.initial.Spec.Genesis.UID,
	}}
	granted := func() []string {
		t.Helper()
		objects, err := workerResources(f.node, f.record, f.initial, "worker:test", 1, nil, genesis)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, rule := range objects[1].(*rbacv1.Role).Rules {
			if rule.Resources[0] == api.ResourceStacksChainstateSnapshot {
				if len(rule.ResourceNames) == 0 || len(rule.Verbs) != 1 || rule.Verbs[0] != "get" {
					t.Fatalf("snapshot grant is not a named get: %+v", rule)
				}
				names = append(names, rule.ResourceNames...)
			}
		}
		return names
	}
	if names := granted(); len(names) != 0 {
		t.Fatalf("a network from genesis must not read snapshots: %v", names)
	}
	genesis.Spec.Source.Dependencies = []common.Binding{
		{Kind: api.KindStacksEpochSchedule, Name: "epochs"},
		{Kind: api.KindStacksChainstateSnapshot, Name: "seed", UID: "seed-uid"},
	}
	if names := granted(); len(names) != 1 || names[0] != "seed" {
		t.Fatalf("restored worker must read exactly the pinned snapshot: %v", names)
	}
}

func TestRestoreRefusesAnUnexpectedChain(t *testing.T) {
	for name, mutate := range map[string]func(*testFixture){
		"other tip":    func(f *testFixture) { f.rpc.tip = strings.Repeat("f", 64) },
		"lower height": func(f *testFixture) { f.rpc.height = 216 },
		"snapshot deleted": func(f *testFixture) {
			_ = f.c.Delete(context.Background(), &api.StacksChainstateSnapshot{
				ObjectMeta: metav1.ObjectMeta{Name: "seed", Namespace: "test"},
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			f.worker.Now = func() time.Time { return f.now }
			f.restoreFrom(t, 217)
			mutate(f)
			if e := f.worker.Step(ctx); e != nil {
				t.Fatal(e)
			}
			s := f.schedulerFor()
			f.reconcile(t, s)
			f.reconcile(t, s)
			initial := f.readInitial(t)
			want := reasonRestoredChainDiffers
			if name == "snapshot deleted" {
				want = reasonRestoreSnapshotUnavailable
			}
			if initial.Status.Offer != nil || initial.Status.PreparedAt != nil ||
				initial.Status.Phase != bitcoin.InitializationBlocked || initial.Status.Reason != want {
				t.Fatalf("unexpected restore status %+v", initial.Status)
			}
			if f.generated() != 0 {
				t.Fatal("generated on an unproven chain")
			}
		})
	}
}

func TestWorkerRefusesRestoreOffersBeyondTheSnapshot(t *testing.T) {
	for name, mutate := range map[string]func(*bitcoin.BitcoinBlockOffer){
		// The control case proves this path generates when the offer is exactly the refresh.
		"valid":        func(*bitcoin.BitcoinBlockOffer) {},
		"two blocks":   func(o *bitcoin.BitcoinBlockOffer) { o.Count, o.Ceiling = 2, 219 },
		"other height": func(o *bitcoin.BitcoinBlockOffer) { o.ExpectedHeight, o.Ceiling = 216, 217 },
		"other wallet": func(o *bitcoin.BitcoinBlockOffer) { o.Address = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			f.worker.Now = func() time.Time { return f.now }
			f.restoreFrom(t, 217)
			f.offer(t, 217)
			initial := f.readInitial(t)
			offer := initial.Status.Offer
			offer.Mode, offer.Ceiling = bitcoin.OfferRestore, 218
			mutate(offer)
			if e := f.c.Status().Update(ctx, initial); e != nil {
				t.Fatal(e)
			}
			record := f.readRecord(t)
			record.Spec.Offer = offer.DeepCopy()
			if e := f.c.Update(ctx, record); e != nil {
				t.Fatal(e)
			}
			f.rpc.height = offer.ExpectedHeight
			_ = f.worker.Step(ctx)
			_ = f.worker.Step(ctx)
			want := 0
			if name == "valid" {
				want = 1
			}
			if got := f.generated(); got != want {
				t.Fatalf("worker generated %d blocks, want %d", got, want)
			}
		})
	}
}
