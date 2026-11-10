package foundation

import (
	"context"
	"fmt"
	"sort"
	"strings"

	common "github.com/cylewitruk-stacks/stacks-k8s/apis/network/common/v1alpha2"
	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RestoredSnapshot returns the chainstate snapshot a frozen genesis pins, or nil for a network
// initialized from genesis. The snapshot must still exist with its pinned identity and content.
func RestoredSnapshot(
	ctx context.Context,
	reader client.Reader,
	genesis *api.StacksGenesis,
) (*api.StacksChainstateSnapshot, error) {
	for _, b := range genesis.Spec.Source.Dependencies {
		if b.Kind != api.KindStacksChainstateSnapshot {
			continue
		}
		var snapshot api.StacksChainstateSnapshot
		if err := reader.Get(ctx, client.ObjectKey{Namespace: genesis.Namespace, Name: b.Name}, &snapshot); err != nil {
			return nil, fmt.Errorf("chainstate snapshot %s unavailable", b.Name)
		}
		if snapshot.UID != b.UID || snapshot.DeletionTimestamp != nil || Digest(snapshot.Spec) != b.Fingerprint {
			return nil, fmt.Errorf("chainstate snapshot %s identity changed", b.Name)
		}
		return &snapshot, nil
	}
	return nil, nil
}

// restoreMember pairs one initial cohort requirement with its compiled policy.
type restoreMember struct {
	requirement   api.BootstrapRequirement
	configuration api.Configuration
}

// restoreMismatchError lists every difference between a restoring network and its snapshot.
type restoreMismatchError struct {
	snapshot string
	problems []string
}

func (e *restoreMismatchError) Error() string {
	const shown = 8
	problems, more := e.problems, ""
	if len(problems) > shown {
		problems, more = problems[:shown], fmt.Sprintf("; and %d more", len(problems)-shown)
	}
	return fmt.Sprintf("snapshot %s does not match: %s%s", e.snapshot, strings.Join(problems, "; "), more)
}

// snapshotParticipant projects a cohort requirement onto the values restored data depends on.
// Allocation identities (UIDs) and whole-policy digests are excluded, so resources, overrides
// and placement may differ between the source and the restoring network.
func snapshotParticipant(name string, req api.BootstrapRequirement) api.SnapshotParticipant {
	p := api.SnapshotParticipant{
		Name:           name,
		Kind:           req.Kind,
		MiningEnabled:  req.MiningEnabled,
		AmountMicroSTX: req.AmountMicroSTX,
		LockCycles:     req.LockCycles,
	}
	seen := map[string]bool{}
	for _, a := range req.Accounts {
		if !seen[a.Identity.Address] {
			seen[a.Identity.Address] = true
			p.Accounts = append(p.Accounts, a.Identity)
		}
	}
	sortIdentities(p.Accounts)
	return p
}

func sortIdentities(ids []common.PublicIdentity) {
	sort.Slice(ids, func(i, j int) bool { return ids[i].Address < ids[j].Address })
}

func samePublicIdentities(a, b []common.PublicIdentity) bool {
	if len(a) != len(b) {
		return false
	}
	a, b = append([]common.PublicIdentity(nil), a...), append([]common.PublicIdentity(nil), b...)
	sortIdentities(a)
	sortIdentities(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// actorImage returns an actor's compiled image, or empty for worker participants.
func actorImage(c api.Configuration) string {
	switch {
	case c.BitcoinNode != nil:
		return ptr.Deref(c.BitcoinNode.Image, "")
	case c.StacksNode != nil:
		return ptr.Deref(c.StacksNode.Image, "")
	case c.StacksSigner != nil:
		return ptr.Deref(c.StacksSigner.Image, "")
	}
	return ""
}

// validateRestore checks that a restoring network reproduces the snapshot's chain configuration,
// its exact initial cohort and the actor images that wrote the captured data.
func validateRestore(
	snapshot *api.StacksChainstateSnapshot,
	genesisDigest string,
	cohort map[string]restoreMember,
) error {
	var problems []string
	if genesisDigest != snapshot.Spec.GenesisDigest {
		problems = append(
			problems,
			fmt.Sprintf("genesis digest %s differs from %s", genesisDigest, snapshot.Spec.GenesisDigest),
		)
	}
	recorded := map[string]bool{}
	for _, want := range snapshot.Spec.Participants {
		recorded[want.Name] = true
		member, ok := cohort[want.Name]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s %s is missing from the initial cohort", want.Kind, want.Name))
			continue
		}
		got := snapshotParticipant(want.Name, member.requirement)
		if got.Kind != want.Kind {
			problems = append(problems, fmt.Sprintf("%s is %s, not %s", want.Name, got.Kind, want.Kind))
			continue
		}
		if !samePublicIdentities(got.Accounts, want.Accounts) {
			problems = append(problems, fmt.Sprintf("%s public identities differ", want.Name))
		}
		if !ptr.Equal(got.MiningEnabled, want.MiningEnabled) {
			problems = append(problems, fmt.Sprintf("%s mining role differs", want.Name))
		}
		if !ptr.Equal(got.AmountMicroSTX, want.AmountMicroSTX) || !ptr.Equal(got.LockCycles, want.LockCycles) {
			problems = append(problems, fmt.Sprintf("%s stake differs", want.Name))
		}
		if want.Archive != nil {
			if image := actorImage(member.configuration); image != want.Archive.Image {
				problems = append(
					problems,
					fmt.Sprintf(
						"%s image %s differs from %s, which wrote the data",
						want.Name,
						image,
						want.Archive.Image,
					),
				)
			}
		}
	}
	extra := []string{}
	for name := range cohort {
		if !recorded[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(extra)
	for _, name := range extra {
		problems = append(
			problems,
			fmt.Sprintf(
				"%s %s is not in the snapshot; add it after initialization",
				cohort[name].requirement.Kind,
				name,
			),
		)
	}
	if len(problems) == 0 {
		return nil
	}
	return &restoreMismatchError{snapshot: snapshot.Name, problems: problems}
}
