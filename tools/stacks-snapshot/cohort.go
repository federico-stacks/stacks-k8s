package main

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	common "github.com/cylewitruk-stacks/stacks-k8s/apis/network/common/v1alpha2"
	stacks "github.com/cylewitruk-stacks/stacks-k8s/apis/network/stacks/v1alpha2"
	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
)

// member is one frozen initial cohort requirement with its participant and accounts.
type member struct {
	requirement api.BootstrapRequirement
	participant api.StacksNetworkParticipant
	accounts    []stacks.StacksAccount
}

// archived reports whether a participant kind owns chain data on a volume.
func archived(kind api.ParticipantKind) bool {
	return kind == api.ParticipantBitcoinNode || kind == api.ParticipantStacksNode ||
		kind == api.ParticipantStacksSigner
}

// project records what a restoring network must reproduce, the same projection restore
// validation compares: names, kinds, public identities, mining role, stake and actor images.
// Checksums are filled in after export.
func project(members []member) ([]api.SnapshotParticipant, error) {
	var generated, problems []string
	out := make([]api.SnapshotParticipant, 0, len(members))
	for _, m := range members {
		for _, a := range m.accounts {
			// Omission on an account means generation.
			if a.Spec.Key == nil || a.Spec.Key.Generate != nil {
				generated = append(generated, a.Name)
			}
		}
		p := api.SnapshotParticipant{
			Name:           m.participant.Spec.ParticipantName,
			Kind:           m.requirement.Kind,
			MiningEnabled:  m.requirement.MiningEnabled,
			AmountMicroSTX: m.requirement.AmountMicroSTX,
			LockCycles:     m.requirement.LockCycles,
		}
		seen := map[string]bool{}
		for _, a := range m.requirement.Accounts {
			if !seen[a.Identity.Address] {
				seen[a.Identity.Address] = true
				p.Accounts = append(p.Accounts, a.Identity)
			}
		}
		sort.Slice(p.Accounts, func(i, j int) bool { return p.Accounts[i].Address < p.Accounts[j].Address })
		if archived(p.Kind) {
			image := actorImage(&m.participant)
			if image == "" {
				problems = append(problems, fmt.Sprintf("%s has no admitted actor image", p.Name))
			}
			p.Archive = &api.SnapshotArchive{Path: p.Name + ".tar.gz", Image: image}
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if len(generated) > 0 {
		sort.Strings(generated)
		return nil, fmt.Errorf(
			"accounts %s use generated keys, which a restoring network cannot reproduce; "+
				"import their keys (key.secretRef) in the source network",
			strings.Join(unique(generated), ", "),
		)
	}
	if len(problems) > 0 {
		return nil, errors.New(strings.Join(problems, "; "))
	}
	return out, nil
}

// actorImage returns the admitted image that writes an actor's data, or empty.
func actorImage(p *api.StacksNetworkParticipant) string {
	if p.Status.Admission == nil {
		return ""
	}
	var image *string
	switch c := p.Status.Admission.Configuration; {
	case c.BitcoinNode != nil:
		image = c.BitcoinNode.Image
	case c.StacksNode != nil:
		image = c.StacksNode.Image
	case c.StacksSigner != nil:
		image = c.StacksSigner.Image
	}
	if image == nil {
		return ""
	}
	return *image
}

func unique(sorted []string) []string {
	out := sorted[:0]
	for i, s := range sorted {
		if i == 0 || s != sorted[i-1] {
			out = append(out, s)
		}
	}
	return out
}

// accountBindings returns the distinct StacksAccounts a requirement binds.
func accountBindings(req api.BootstrapRequirement) []common.Binding {
	var out []common.Binding
	seen := map[string]bool{}
	for _, a := range req.Accounts {
		if a.Binding.Kind == stacks.KindStacksAccount && !seen[a.Binding.Name] {
			seen[a.Binding.Name] = true
			out = append(out, a.Binding)
		}
	}
	return out
}
