package main

import (
	"strings"
	"testing"

	bitcoin "github.com/cylewitruk-stacks/stacks-k8s/apis/network/bitcoin/v1alpha2"
	common "github.com/cylewitruk-stacks/stacks-k8s/apis/network/common/v1alpha2"
	stacks "github.com/cylewitruk-stacks/stacks-k8s/apis/network/stacks/v1alpha2"
	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

func identity(n byte) common.PublicIdentity {
	return common.PublicIdentity{
		Address:   "ST" + strings.Repeat(string('A'+n), 30),
		PublicKey: "02" + strings.Repeat("0", 62) + string('0'+n) + "0",
	}
}

func account(name string, key *common.KeySource) stacks.StacksAccount {
	return stacks.StacksAccount{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid")},
		Spec:       stacks.StacksAccountSpec{Key: key},
	}
}

var imported = &common.KeySource{SecretRef: &common.SecretKeyRef{Name: "keys", Key: "k"}}

func participant(name string, kind api.ParticipantKind, config api.Configuration) api.StacksNetworkParticipant {
	return api.StacksNetworkParticipant{
		ObjectMeta: metav1.ObjectMeta{Name: "p-" + name, UID: types.UID(name + "-uid")},
		Spec:       api.StacksNetworkParticipantSpec{NetworkUID: networkUID, ParticipantName: name, Kind: kind},
		Status:     api.ParticipantStatus{Admission: &api.Admission{Configuration: config}},
	}
}

func cohortFixture() []member {
	node := participant("node-01", api.ParticipantStacksNode, api.Configuration{
		StacksNode: &stacks.StacksNodeSpec{ActorFields: common.ActorFields{Image: ptr.To("stacks-node:3.1")}},
	})
	signer := participant("signer-01", api.ParticipantStacksSigner, api.Configuration{
		StacksSigner: &stacks.StacksSignerSpec{ActorFields: common.ActorFields{Image: ptr.To("stacks-signer:3.1")}},
	})
	btc := participant("btc-01", api.ParticipantBitcoinNode, api.Configuration{
		BitcoinNode: &bitcoin.BitcoinNodeSpec{ActorFields: common.ActorFields{Image: ptr.To("bitcoind:28")}},
	})
	stacker := participant("stacker-01", api.ParticipantStacksStacker, api.Configuration{})
	return []member{
		{
			requirement: api.BootstrapRequirement{
				Kind:          api.ParticipantStacksNode,
				MiningEnabled: ptr.To(true),
				Accounts: []api.PublicAccount{
					{Binding: common.Binding{Kind: stacks.KindStacksAccount, Name: "miner"}, Identity: identity(2)},
					{Binding: common.Binding{Kind: stacks.KindStacksAccount, Name: "miner"}, Identity: identity(2)},
					{Binding: common.Binding{Kind: stacks.KindStacksAccount, Name: "other"}, Identity: identity(1)},
				},
			},
			participant: node,
			accounts:    []stacks.StacksAccount{account("miner", imported), account("other", imported)},
		},
		{requirement: api.BootstrapRequirement{Kind: api.ParticipantStacksSigner}, participant: signer},
		{requirement: api.BootstrapRequirement{Kind: api.ParticipantBitcoinNode}, participant: btc},
		{
			requirement: api.BootstrapRequirement{
				Kind:           api.ParticipantStacksStacker,
				AmountMicroSTX: ptr.To(common.Amount("1000")),
				LockCycles:     ptr.To[int32](12),
			},
			participant: stacker,
		},
	}
}

func TestProjectRecordsWhatRestoreValidates(t *testing.T) {
	got, err := project(cohortFixture())
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, p := range got {
		names = append(names, p.Name)
	}
	if strings.Join(names, ",") != "btc-01,node-01,signer-01,stacker-01" {
		t.Fatalf("order %v", names)
	}
	node := got[1]
	if node.Kind != api.ParticipantStacksNode || !ptr.Deref(node.MiningEnabled, false) ||
		len(node.Accounts) != 2 || node.Accounts[0] != identity(1) || node.Accounts[1] != identity(2) {
		t.Fatalf("node projection %+v", node)
	}
	if node.Archive == nil || node.Archive.Path != "node-01.tar.gz" || node.Archive.Image != "stacks-node:3.1" {
		t.Fatalf("node archive %+v", node.Archive)
	}
	if got[0].Archive.Image != "bitcoind:28" || got[2].Archive.Image != "stacks-signer:3.1" {
		t.Fatalf("actor images %+v %+v", got[0].Archive, got[2].Archive)
	}
	stacker := got[3]
	if stacker.Archive != nil || *stacker.AmountMicroSTX != "1000" || *stacker.LockCycles != 12 {
		t.Fatalf("stacker projection %+v", stacker)
	}
}

func TestProjectRefusesGeneratedKeys(t *testing.T) {
	for name, key := range map[string]*common.KeySource{
		"omitted":   nil,
		"generated": {Generate: ptr.To(true)},
	} {
		t.Run(name, func(t *testing.T) {
			members := cohortFixture()
			members[0].accounts[1] = account("other", key)
			members[1].accounts = []stacks.StacksAccount{account("other", key), account("signer", key)}
			_, err := project(members)
			if err == nil || !strings.Contains(err.Error(), "accounts other, signer use generated keys") {
				t.Fatalf("got %v", err)
			}
		})
	}
	members := cohortFixture()
	members[0].accounts[1] = account("other", &common.KeySource{PublicIdentity: ptr.To(identity(1))})
	if _, err := project(members); err != nil {
		t.Fatalf("public identity refused: %v", err)
	}
}

func TestProjectRequiresActorImages(t *testing.T) {
	members := cohortFixture()
	members[2].participant.Status.Admission = nil
	if _, err := project(members); err == nil || !strings.Contains(err.Error(), "btc-01 has no admitted actor image") {
		t.Fatalf("got %v", err)
	}
}

func TestAccountBindingsSelectsDistinctStacksAccounts(t *testing.T) {
	req := api.BootstrapRequirement{Accounts: []api.PublicAccount{
		{Binding: common.Binding{Kind: stacks.KindStacksAccount, Name: "a"}},
		{Binding: common.Binding{Kind: "Other", Name: "b"}},
		{Binding: common.Binding{Kind: stacks.KindStacksAccount, Name: "a"}},
	}}
	if got := accountBindings(req); len(got) != 1 || got[0].Name != "a" {
		t.Fatalf("got %v", got)
	}
}
