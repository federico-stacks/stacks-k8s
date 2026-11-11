package foundation

import (
	"errors"
	"strings"
	"testing"

	bitcoin "github.com/cylewitruk-stacks/stacks-k8s/apis/network/bitcoin/v1alpha2"
	common "github.com/cylewitruk-stacks/stacks-k8s/apis/network/common/v1alpha2"
	stacks "github.com/cylewitruk-stacks/stacks-k8s/apis/network/stacks/v1alpha2"
	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// restoreFixture returns a cohort and the snapshot its own projection would produce.
func restoreFixture() (map[string]restoreMember, *api.StacksChainstateSnapshot) {
	identity := func(address string) api.PublicAccount {
		return api.PublicAccount{
			Binding:  common.Binding{Kind: stacks.KindStacksAccount, Name: "generated-" + address, UID: "per-network"},
			Identity: common.PublicIdentity{Address: address, PublicKey: "02" + strings.Repeat("a", 64)},
		}
	}
	image := func(name string) common.ActorFields { return common.ActorFields{Image: ptr.To(name)} }
	cohort := map[string]restoreMember{
		"btc-01": {
			requirement:   api.BootstrapRequirement{Kind: api.ParticipantBitcoinNode},
			configuration: api.Configuration{BitcoinNode: &bitcoin.BitcoinNodeSpec{ActorFields: image("bitcoin:1")}},
		},
		"node-01": {
			requirement: api.BootstrapRequirement{
				Kind:          api.ParticipantStacksNode,
				MiningEnabled: ptr.To(true),
				// Duplicate and unordered accounts project onto one sorted identity list.
				Accounts: []api.PublicAccount{identity("ST2B"), identity("ST1A"), identity("ST2B")},
			},
			configuration: api.Configuration{StacksNode: &stacks.StacksNodeSpec{ActorFields: image("stacks:1")}},
		},
		"stacker-01": {
			requirement: api.BootstrapRequirement{
				Kind:           api.ParticipantStacksStacker,
				AmountMicroSTX: ptr.To(common.Amount("1000")),
				LockCycles:     ptr.To(int32(12)),
				Accounts:       []api.PublicAccount{identity("ST3C")},
			},
			configuration: api.Configuration{StacksStacker: &stacks.StacksStackerSpec{}},
		},
	}
	snapshot := &api.StacksChainstateSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "seed"},
		Spec:       api.StacksChainstateSnapshotSpec{GenesisDigest: "sha256:chain"},
	}
	for _, name := range []string{"btc-01", "node-01", "stacker-01"} {
		p := snapshotParticipant(name, cohort[name].requirement)
		if image := actorImage(cohort[name].configuration); image != "" {
			p.Archive = &api.SnapshotArchive{Path: name + ".tar.gz", Image: image}
		}
		snapshot.Spec.Participants = append(snapshot.Spec.Participants, p)
	}
	return cohort, snapshot
}

func TestSnapshotParticipantProjectsPublicValuesOnly(t *testing.T) {
	cohort, _ := restoreFixture()
	p := snapshotParticipant("node-01", cohort["node-01"].requirement)
	if len(p.Accounts) != 2 || p.Accounts[0].Address != "ST1A" || p.Accounts[1].Address != "ST2B" ||
		!ptr.Equal(p.MiningEnabled, ptr.To(true)) || p.AmountMicroSTX != nil || p.Archive != nil {
		t.Fatalf("projection: %+v", p)
	}
}

func TestValidateRestoreAcceptsMatchingCohort(t *testing.T) {
	cohort, snapshot := restoreFixture()
	if _, err := validateRestore(snapshot, "sha256:chain", cohort, ""); err != nil {
		t.Fatal(err)
	}
	// Per-network bindings, account order and worker policy are outside the comparison.
	member := cohort["node-01"]
	member.requirement.Accounts = append(
		[]api.PublicAccount{},
		member.requirement.Accounts[1],
		member.requirement.Accounts[0],
	)
	member.requirement.Accounts[0].Binding.UID = "other-network"
	member.requirement.PolicyDigest = "sha256:other-resources"
	cohort["node-01"] = member
	if _, err := validateRestore(snapshot, "sha256:chain", cohort, ""); err != nil {
		t.Fatalf("non-consensus differences rejected: %v", err)
	}
}

func TestValidateRestoreReportsEveryDifference(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(map[string]restoreMember, *api.StacksChainstateSnapshot)
		want   string
	}{
		"genesis": {
			func(_ map[string]restoreMember, s *api.StacksChainstateSnapshot) {
				s.Spec.GenesisDigest = "sha256:other"
			},
			"genesis digest sha256:chain differs from sha256:other",
		},
		"missing": {
			func(c map[string]restoreMember, _ *api.StacksChainstateSnapshot) { delete(c, "stacker-01") },
			"StacksStacker stacker-01 is missing from the initial cohort",
		},
		"extra": {
			func(c map[string]restoreMember, _ *api.StacksChainstateSnapshot) {
				c["node-02"] = restoreMember{requirement: api.BootstrapRequirement{Kind: api.ParticipantStacksNode}}
			},
			"StacksNode node-02 is not in the snapshot; add it after initialization",
		},
		"kind": {
			func(c map[string]restoreMember, _ *api.StacksChainstateSnapshot) {
				m := c["btc-01"]
				m.requirement.Kind = api.ParticipantStacksNode
				c["btc-01"] = m
			},
			"btc-01 is StacksNode, not BitcoinNode",
		},
		"identity": {
			func(c map[string]restoreMember, _ *api.StacksChainstateSnapshot) {
				m := c["stacker-01"]
				m.requirement.Accounts = []api.PublicAccount{{Identity: common.PublicIdentity{Address: "ST9Z"}}}
				c["stacker-01"] = m
			},
			"stacker-01 public identities differ",
		},
		"mining": {
			func(c map[string]restoreMember, _ *api.StacksChainstateSnapshot) {
				m := c["node-01"]
				m.requirement.MiningEnabled = ptr.To(false)
				c["node-01"] = m
			},
			"node-01 mining role differs",
		},
		"stake": {
			func(c map[string]restoreMember, _ *api.StacksChainstateSnapshot) {
				m := c["stacker-01"]
				m.requirement.LockCycles = ptr.To(int32(6))
				c["stacker-01"] = m
			},
			"stacker-01 stake differs",
		},
		"image": {
			func(c map[string]restoreMember, _ *api.StacksChainstateSnapshot) {
				m := c["node-01"]
				m.configuration = api.Configuration{
					StacksNode: &stacks.StacksNodeSpec{ActorFields: common.ActorFields{Image: ptr.To("stacks:2")}},
				}
				c["node-01"] = m
			},
			"node-01 image stacks:2 differs from stacks:1, which wrote the data " +
				"(stacksImagePolicy AllowDifferent accepts another Stacks image)",
		},
	} {
		t.Run(name, func(t *testing.T) {
			cohort, snapshot := restoreFixture()
			tc.mutate(cohort, snapshot)
			_, err := validateRestore(snapshot, "sha256:chain", cohort, api.StacksImagePolicyExact)
			var mismatch *restoreMismatchError
			if !errors.As(err, &mismatch) || !strings.Contains(err.Error(), tc.want) ||
				!strings.HasPrefix(err.Error(), "snapshot seed does not match: ") {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestValidateRestoreStacksImagePolicy(t *testing.T) {
	withImages := func(stacksImage, bitcoinImage string) map[string]restoreMember {
		cohort, _ := restoreFixture()
		node := cohort["node-01"]
		node.configuration = api.Configuration{
			StacksNode: &stacks.StacksNodeSpec{ActorFields: common.ActorFields{Image: ptr.To(stacksImage)}},
		}
		cohort["node-01"] = node
		btc := cohort["btc-01"]
		btc.configuration = api.Configuration{
			BitcoinNode: &bitcoin.BitcoinNodeSpec{ActorFields: common.ActorFields{Image: ptr.To(bitcoinImage)}},
		}
		cohort["btc-01"] = btc
		return cohort
	}
	_, snapshot := restoreFixture()

	changes, err := validateRestore(snapshot, "sha256:chain", withImages("stacks:2", "bitcoin:1"),
		api.StacksImagePolicyAllowDifferent)
	want := api.RestoredImageChange{Participant: "node-01", SnapshotImage: "stacks:1", Image: "stacks:2"}
	if err != nil || len(changes) != 1 || changes[0] != want {
		t.Fatalf("AllowDifferent: %v %+v", err, changes)
	}

	changes, err = validateRestore(snapshot, "sha256:chain", withImages("stacks:1", "bitcoin:1"),
		api.StacksImagePolicyAllowDifferent)
	if err != nil || changes != nil {
		t.Fatalf("unchanged images recorded: %v %+v", err, changes)
	}

	// Bitcoin node images must match under every policy.
	_, err = validateRestore(snapshot, "sha256:chain", withImages("stacks:2", "bitcoin:2"),
		api.StacksImagePolicyAllowDifferent)
	// The Bitcoin problem carries no AllowDifferent hint, and the allowed Stacks change is no problem.
	const bitcoinProblem = "btc-01 image bitcoin:2 differs from bitcoin:1, which wrote the data"
	if err == nil || !strings.HasSuffix(err.Error(), ": "+bitcoinProblem) || strings.Contains(err.Error(), "node-01") {
		t.Fatalf("Bitcoin image change: %v", err)
	}

	for _, policy := range []api.StacksImagePolicy{"", api.StacksImagePolicyExact} {
		if _, err = validateRestore(snapshot, "sha256:chain", withImages("stacks:2", "bitcoin:1"), policy); err == nil {
			t.Fatalf("policy %q accepted a Stacks image change", policy)
		}
	}
}

func TestRestoreMismatchMessageIsBounded(t *testing.T) {
	err := &restoreMismatchError{snapshot: "seed", problems: strings.Split("a b c d e f g h i j", " ")}
	if got := err.Error(); got != "snapshot seed does not match: a; b; c; d; e; f; g; h; and 2 more" {
		t.Fatalf("message: %s", got)
	}
}
