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
// validation compares: names, kinds, public identities by role, links between participants,
// mining role, stake, registry threshold and actor images. Every bound account must use an
// imported key, including worker accounts that are not compared: genesis funding puts them in
// the genesis digest. Checksums are filled in after export.
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
		if m.participant.Status.Admission == nil {
			problems = append(problems, fmt.Sprintf("%s has no admitted configuration",
				m.participant.Spec.ParticipantName))
			continue
		}
		p := projectParticipant(
			m.participant.Spec.ParticipantName,
			m.requirement,
			m.participant.Status.Admission.Configuration,
		)
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

// projectParticipant projects one cohort member exactly as the operator's restore validation does
// (operators/network/internal/foundation/restore.go, snapshotParticipant): accounts by the role
// that selects them, public keys in lower case, sorted by role and address.
func projectParticipant(name string, req api.BootstrapRequirement, c api.Configuration) api.SnapshotParticipant {
	p := api.SnapshotParticipant{
		Name:           name,
		Kind:           req.Kind,
		MiningEnabled:  req.MiningEnabled,
		AmountMicroSTX: req.AmountMicroSTX,
		LockCycles:     req.LockCycles,
	}
	identities := map[string]common.PublicIdentity{}
	for _, a := range req.Accounts {
		identities[a.Binding.Name] = a.Identity
	}
	add := func(role api.SnapshotAccountRole, ref *common.NameRef) {
		if ref == nil {
			return
		}
		if identity, ok := identities[ref.Name]; ok {
			identity.PublicKey = strings.ToLower(identity.PublicKey)
			p.Accounts = append(p.Accounts, api.SnapshotAccount{Role: role, PublicIdentity: identity})
		}
	}
	link := func(ref *common.NameRef) string {
		if ref == nil {
			return ""
		}
		return ref.Name
	}
	switch {
	case c.StacksNode != nil:
		add(api.SnapshotAccountRoleIdentity, c.StacksNode.IdentityAccountRef)
		p.BitcoinNode = link(c.StacksNode.BitcoinNodeRef)
	case c.StacksSigner != nil:
		add(api.SnapshotAccountRoleSigner, c.StacksSigner.AccountRef)
		p.Node = link(c.StacksSigner.NodeRef)
	case c.StacksStacker != nil:
		add(api.SnapshotAccountRoleHolder, c.StacksStacker.HolderAccountRef)
		add(api.SnapshotAccountRoleAdministrator, c.StacksStacker.AdministratorAccountRef)
		p.Signer = link(c.StacksStacker.SignerRef)
	case c.StacksContractSet != nil:
		add(api.SnapshotAccountRoleDeployer, c.StacksContractSet.DeployerAccountRef)
		if registry := c.StacksContractSet.Initialization; registry != nil {
			for i := range registry.SignerAccountRefs {
				add(api.SnapshotAccountRoleRegistrySigner, &registry.SignerAccountRefs[i])
			}
			add(api.SnapshotAccountRoleAggregateKey, &registry.AggregateKeyAccountRef)
			threshold := registry.Threshold
			p.RegistryThreshold = &threshold
		}
	}
	sort.Slice(p.Accounts, func(i, j int) bool {
		if p.Accounts[i].Role != p.Accounts[j].Role {
			return p.Accounts[i].Role < p.Accounts[j].Role
		}
		return p.Accounts[i].Address < p.Accounts[j].Address
	})
	unique := p.Accounts[:0]
	for i, a := range p.Accounts {
		if i == 0 || a != p.Accounts[i-1] {
			unique = append(unique, a)
		}
	}
	p.Accounts = unique
	return p
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
