//go:build integration

package foundationintegration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	bitcoin "github.com/cylewitruk-stacks/stacks-k8s/apis/network/bitcoin/v1alpha2"
	stacks "github.com/cylewitruk-stacks/stacks-k8s/apis/network/stacks/v1alpha2"
	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
	"github.com/cylewitruk-stacks/stacks-k8s/operators/network/internal/foundation"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// restoreHarness drives one namespace's foundation resolution without a manager.
type restoreHarness struct {
	c       client.Client
	root    *foundation.Reconciler
	account *foundation.IdentityReconciler
	wallet  *foundation.IdentityReconciler
	jobs    map[types.UID]bool
}

func (h *restoreHarness) step(t *testing.T, ctx context.Context, ns string) *api.StacksNetwork {
	t.Helper()
	network := &api.StacksNetwork{}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "network"}}
	driveRoot(t, ctx, h.c, h.root, request, network)
	var accounts stacks.StacksAccountList
	if err := h.c.List(ctx, &accounts, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	for _, a := range accounts.Items {
		if _, err := h.account.Reconcile(
			ctx,
			ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&a)},
		); err != nil {
			t.Fatal(err)
		}
	}
	var wallets bitcoin.BitcoinWalletList
	if err := h.c.List(ctx, &wallets, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	for _, w := range wallets.Items {
		if _, err := h.wallet.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&w)}); err != nil {
			t.Fatal(err)
		}
	}
	// envtest has no kubelet/Job controller. Execute each Job to completion once.
	var jobs batchv1.JobList
	if err := h.c.List(ctx, &jobs, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs.Items {
		if h.jobs[job.UID] {
			continue
		}
		var input foundation.KeyJobInput
		for _, arg := range job.Spec.Template.Spec.Containers[0].Args {
			if strings.HasPrefix(arg, "--input=") {
				if err := json.Unmarshal([]byte(strings.TrimPrefix(arg, "--input=")), &input); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := foundation.RunKeyJob(ctx, h.c, input); err != nil {
			t.Fatalf("resolver %s: %v", job.Name, err)
		}
		h.jobs[job.UID] = true
	}
	if err := h.c.Get(ctx, request.NamespacedName, network); err != nil {
		t.Fatal(err)
	}
	return network
}

func (h *restoreHarness) drive(
	t *testing.T,
	ctx context.Context,
	ns string,
	done func(*api.StacksNetwork) bool,
) *api.StacksNetwork {
	t.Helper()
	var network *api.StacksNetwork
	for i := 0; i < 60; i++ {
		network = h.step(t, ctx, ns)
		if done(network) {
			return network
		}
	}
	t.Fatalf("%s did not converge: %+v", ns, network.Status)
	return nil
}

func resolvedReason(reason, contains string) func(*api.StacksNetwork) bool {
	return func(n *api.StacksNetwork) bool {
		c := meta.FindStatusCondition(n.Status.Conditions, "Resolved")
		return c != nil && c.Reason == reason && strings.Contains(c.Message, contains)
	}
}

// snapshotOf projects a frozen network the way a capture tool must, independently of the operator.
func snapshotOf(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	source *api.StacksNetwork,
	ns, name string,
) *api.StacksChainstateSnapshot {
	t.Helper()
	var genesis api.StacksGenesis
	if err := c.Get(
		ctx,
		types.NamespacedName{Namespace: source.Namespace, Name: source.Status.GenesisRef.Name},
		&genesis,
	); err != nil {
		t.Fatal(err)
	}
	zeros := func(n int) string { return strings.Repeat("0", n) }
	snapshot := &api.StacksChainstateSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: api.StacksChainstateSnapshotSpec{
			Image:         "snapshot:test",
			GenesisDigest: source.Status.GenesisDigest,
			Bitcoin:       api.SnapshotBitcoinTip{Height: 217, BlockHash: zeros(64)},
			Stacks:        api.SnapshotStacksTip{Height: 126, BlockHash: zeros(64), IndexBlockID: zeros(64)},
		},
	}
	for _, req := range genesis.Spec.Bootstrap.Requirements {
		var p api.StacksNetworkParticipant
		if err := c.Get(
			ctx,
			types.NamespacedName{Namespace: source.Namespace, Name: req.Participant.Name},
			&p,
		); err != nil {
			t.Fatal(err)
		}
		member := api.SnapshotParticipant{
			Name:           p.Spec.ParticipantName,
			Kind:           req.Kind,
			MiningEnabled:  req.MiningEnabled,
			AmountMicroSTX: req.AmountMicroSTX,
			LockCycles:     req.LockCycles,
		}
		seen := map[string]bool{}
		for _, a := range req.Accounts {
			if !seen[a.Identity.Address] {
				seen[a.Identity.Address] = true
				member.Accounts = append(member.Accounts, a.Identity)
			}
		}
		sort.Slice(
			member.Accounts,
			func(i, j int) bool { return member.Accounts[i].Address < member.Accounts[j].Address },
		)
		var image *string
		switch cfg := p.Status.Admission.Configuration; {
		case cfg.BitcoinNode != nil:
			image = cfg.BitcoinNode.Image
		case cfg.StacksNode != nil:
			image = cfg.StacksNode.Image
		case cfg.StacksSigner != nil:
			image = cfg.StacksSigner.Image
		}
		if image != nil {
			member.Archive = &api.SnapshotArchive{Path: member.Name + ".tar.gz", SHA256: zeros(64), Image: *image}
		}
		snapshot.Spec.Participants = append(snapshot.Spec.Participants, member)
	}
	return snapshot
}

// admitRestoreCopy admits minimal.yaml into another namespace as a restoring network. With
// importKeys, every account imports the source network's key, so public identities match. A
// non-empty policy sets the restore's stacksImagePolicy.
func admitRestoreCopy(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	source, target, snapshot string,
	importKeys bool,
	policy api.StacksImagePolicy,
) {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "docs", "design", "public-api", "examples", "minimal.yaml")
	// #nosec G304 -- Path is derived from repository fixtures, not a remote request.
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }() // Read/cleanup completion cannot change the operation's result.
	decoder := yaml.NewYAMLOrJSONDecoder(file, 4096)
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); errors.Is(err, io.EOF) {
			return
		} else if err != nil {
			t.Fatal(err)
		}
		obj := &unstructured.Unstructured{}
		if len(raw) == 0 || json.Unmarshal(raw, &obj.Object) != nil || obj.GetKind() == "" {
			continue
		}
		switch obj.GetKind() {
		case "Namespace":
			obj.SetName(target)
		case stacks.KindStacksAccount:
			obj.SetNamespace(target)
			if importKeys {
				importAccountKey(t, ctx, c, source, obj)
			}
		case api.KindStacksNetwork:
			obj.SetNamespace(target)
			restore := map[string]any{"snapshotRef": map[string]any{"name": snapshot}}
			if policy != "" {
				restore["stacksImagePolicy"] = string(policy)
			}
			if err := unstructured.SetNestedField(obj.Object, restore, "spec", "genesis", "restore"); err != nil {
				t.Fatal(err)
			}
			if err := unstructured.SetNestedField(obj.Object, "Running", "spec", "operation"); err != nil {
				t.Fatal(err)
			}
		default:
			obj.SetNamespace(target)
		}
		if err := c.Create(ctx, obj, client.FieldValidation("Strict")); err != nil {
			t.Fatalf("create %s %s/%s: %v", target, obj.GetKind(), obj.GetName(), err)
		}
	}
}

// importAccountKey copies the source account's generated key into an immutable Secret in the
// target namespace and points the account at it instead of generating a new key.
func importAccountKey(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	source string,
	obj *unstructured.Unstructured,
) {
	t.Helper()
	var account stacks.StacksAccount
	if err := c.Get(ctx, types.NamespacedName{Namespace: source, Name: obj.GetName()}, &account); err != nil {
		t.Fatal(err)
	}
	ref := account.Status.CredentialsRef
	if ref == nil {
		t.Fatalf("source account %s has no credentials", obj.GetName())
	}
	var key corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: source, Name: ref.Name}, &key); err != nil {
		t.Fatal(err)
	}
	imported := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "imported-" + obj.GetName(), Namespace: obj.GetNamespace()},
		Immutable:  ptr.To(true),
		Data:       map[string][]byte{ref.Key: key.Data[ref.Key]},
	}
	if err := c.Create(ctx, imported); err != nil {
		t.Fatal(err)
	}
	secretRef := map[string]any{"secretRef": map[string]any{"name": imported.Name, "key": ref.Key}}
	if err := unstructured.SetNestedField(obj.Object, secretRef, "spec", "key"); err != nil {
		t.Fatal(err)
	}
}

// verifyChainstateRestore freezes minimal.yaml as a snapshot source, then checks that a restoring
// network is rejected before freezing unless it reproduces the source chain and cohort.
func verifyChainstateRestore(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	root *foundation.Reconciler,
	account, wallet *foundation.IdentityReconciler,
) {
	t.Helper()
	h := &restoreHarness{c: c, root: root, account: account, wallet: wallet, jobs: map[types.UID]bool{}}
	const sourceNS = "lab-minimal"
	var source api.StacksNetwork
	if err := c.Get(ctx, types.NamespacedName{Namespace: sourceNS, Name: "network"}, &source); err != nil {
		t.Fatal(err)
	}
	source.Spec.Operation = "Running"
	updateObject(t, ctx, c, &source)
	frozen := h.drive(t, ctx, sourceNS, func(n *api.StacksNetwork) bool { return n.Status.GenesisRef != nil })

	// Generated keys: an absent snapshot waits, then the cohort mismatch blocks the freeze.
	admitRestoreCopy(t, ctx, c, sourceNS, "restore-generated", "seed", false, "")
	h.drive(t, ctx, "restore-generated", resolvedReason("GenesisInputsUnavailable", "chainstate snapshot unavailable"))
	if err := c.Create(ctx, snapshotOf(t, ctx, c, frozen, "restore-generated", "seed")); err != nil {
		t.Fatal(err)
	}
	mismatched := h.drive(
		t,
		ctx,
		"restore-generated",
		resolvedReason("SnapshotMismatch", "snapshot seed does not match"),
	)
	message := meta.FindStatusCondition(mismatched.Status.Conditions, "Resolved").Message
	if !strings.Contains(message, "genesis digest") || !strings.Contains(message, "public identities differ") ||
		mismatched.Status.GenesisRef != nil || mismatched.Status.Phase != api.NetworkPhaseResolutionError {
		t.Fatalf("mismatch not reported before freeze: %s %+v", message, mismatched.Status)
	}
	var artifacts api.StacksGenesisList
	if err := c.List(
		ctx,
		&artifacts,
		client.InNamespace("restore-generated"),
	); err != nil ||
		len(artifacts.Items) != 0 {
		t.Fatalf("mismatched restore froze: %v %d", err, len(artifacts.Items))
	}

	// Imported keys reproduce the source chain and cohort, so the restore freezes and pins its snapshot.
	admitRestoreCopy(t, ctx, c, sourceNS, "restore-imported", "seed", true, "")
	snapshot := snapshotOf(t, ctx, c, frozen, "restore-imported", "seed")
	if err := c.Create(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	restored := h.drive(
		t,
		ctx,
		"restore-imported",
		func(n *api.StacksNetwork) bool { return n.Status.GenesisRef != nil },
	)
	if restored.Status.GenesisDigest != frozen.Status.GenesisDigest {
		t.Fatalf(
			"same keys froze a different chain: %s, source %s",
			restored.Status.GenesisDigest,
			frozen.Status.GenesisDigest,
		)
	}
	var genesis api.StacksGenesis
	if err := c.Get(
		ctx,
		types.NamespacedName{Namespace: "restore-imported", Name: restored.Status.GenesisRef.Name},
		&genesis,
	); err != nil {
		t.Fatal(err)
	}
	pinned := false
	for _, b := range genesis.Spec.Source.Dependencies {
		pinned = pinned || b.Kind == api.KindStacksChainstateSnapshot && b.Name == "seed" && b.UID == snapshot.UID &&
			b.Fingerprint != ""
	}
	if !pinned {
		t.Fatalf("frozen genesis does not pin its snapshot: %+v", genesis.Spec.Source.Dependencies)
	}
	if policy := restored.Spec.Genesis.Restore.StacksImagePolicy; policy != api.StacksImagePolicyExact ||
		genesis.Spec.Source.RestoredImageChanges != nil {
		t.Fatalf("default policy %q recorded image changes %+v", policy, genesis.Spec.Source.RestoredImageChanges)
	}

	// A snapshot written by another Stacks build restores only when the network allows it, and the
	// frozen genesis records the change.
	verifyStacksImagePolicy(t, ctx, h, frozen, sourceNS)
}

func verifyStacksImagePolicy(
	t *testing.T,
	ctx context.Context,
	h *restoreHarness,
	frozen *api.StacksNetwork,
	sourceNS string,
) {
	t.Helper()
	older := func(ns string) (*api.StacksChainstateSnapshot, api.RestoredImageChange) {
		snapshot := snapshotOf(t, ctx, h.c, frozen, ns, "seed")
		for i, p := range snapshot.Spec.Participants {
			if p.Kind == api.ParticipantStacksNode {
				change := api.RestoredImageChange{
					Participant:   p.Name,
					SnapshotImage: "stacks-node:older",
					Image:         p.Archive.Image,
				}
				snapshot.Spec.Participants[i].Archive.Image = change.SnapshotImage
				return snapshot, change
			}
		}
		t.Fatal("fixture has no Stacks node")
		return nil, api.RestoredImageChange{}
	}

	admitRestoreCopy(t, ctx, h.c, sourceNS, "restore-exact-build", "seed", true, api.StacksImagePolicyExact)
	snapshot, change := older("restore-exact-build")
	if err := h.c.Create(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	h.drive(t, ctx, "restore-exact-build", resolvedReason("SnapshotMismatch", change.Participant+" image "))

	admitRestoreCopy(t, ctx, h.c, sourceNS, "restore-new-build", "seed", true, api.StacksImagePolicyAllowDifferent)
	snapshot, change = older("restore-new-build")
	if err := h.c.Create(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	restored := h.drive(
		t,
		ctx,
		"restore-new-build",
		func(n *api.StacksNetwork) bool { return n.Status.GenesisRef != nil },
	)
	var genesis api.StacksGenesis
	if err := h.c.Get(
		ctx,
		types.NamespacedName{Namespace: "restore-new-build", Name: restored.Status.GenesisRef.Name},
		&genesis,
	); err != nil {
		t.Fatal(err)
	}
	if got := genesis.Spec.Source.RestoredImageChanges; len(got) != 1 || got[0] != change {
		t.Fatalf("recorded image changes %+v, want %+v", got, change)
	}
	if restored.Status.GenesisDigest != frozen.Status.GenesisDigest {
		t.Fatal("an image change altered the chain configuration")
	}
}
