package v1alpha2

import (
	common "github.com/cylewitruk-stacks/stacks-k8s/apis/network/common/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StacksChainstateSnapshot describes one captured chainstate artifact that fresh networks may restore.
// It records public facts only; private keys stay in the restoring network's own Secrets.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="self.spec == oldSelf.spec",message="chainstate snapshots are immutable"
type StacksChainstateSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// Spec identifies the artifact and the public inputs a restoring network must reproduce.
	Spec StacksChainstateSnapshotSpec `json:"spec"`
	// Status reports structural resolution.
	Status common.ResolutionStatus `json:"status,omitempty"`
}

// StacksChainstateSnapshotSpec identifies an artifact and its source network's initial cohort.
// +kubebuilder:validation:XValidation:rule="self.participants.exists(p, has(p.archive))",message="a snapshot carries at least one actor archive"
type StacksChainstateSnapshotSpec struct {
	// Image carries the actor archives under /snapshot. Restore verifies every archive checksum,
	// so a tag is accepted; a digest reference additionally pins delivery.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`
	// GenesisDigest identifies the source network's public chain configuration.
	// +kubebuilder:validation:MaxLength=71
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	GenesisDigest string `json:"genesisDigest"`
	// Participants projects the source initial cohort onto the values a restore depends on.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1000
	// +listType=map
	// +listMapKey=name
	Participants []SnapshotParticipant `json:"participants"`
	// Bitcoin records the captured burnchain tip.
	Bitcoin SnapshotBitcoinTip `json:"bitcoin"`
	// Stacks records the captured Stacks tip.
	Stacks SnapshotStacksTip `json:"stacks"`
}

// SnapshotParticipant is the restore-relevant projection of one initial cohort member.
// Per-network identities (UIDs) and whole-policy digests are deliberately absent: resources,
// overrides and placement may differ in a restoring network.
// +kubebuilder:validation:XValidation:rule="has(self.archive) == (self.kind in ['BitcoinNode','StacksNode','StacksSigner'])",message="exactly the actor kinds carry an archive"
// +kubebuilder:validation:XValidation:rule="!has(self.miningEnabled) || self.kind == 'StacksNode'",message="miningEnabled applies to StacksNode only"
// +kubebuilder:validation:XValidation:rule="(!has(self.amountMicroSTX) && !has(self.lockCycles)) || self.kind == 'StacksStacker'",message="stake applies to StacksStacker only"
type SnapshotParticipant struct {
	// Name is the source network-relative instance name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`
	// Kind is the participant domain.
	Kind ParticipantKind `json:"kind"`
	// Accounts lists the public identities the participant used, ordered by address.
	// +kubebuilder:validation:MaxItems=1000
	// +listType=atomic
	Accounts []common.PublicIdentity `json:"accounts,omitempty"`
	// MiningEnabled preserves a Stacks node's mining role.
	MiningEnabled *bool `json:"miningEnabled,omitempty"`
	// AmountMicroSTX preserves a stacker's stake.
	AmountMicroSTX *common.Amount `json:"amountMicroSTX,omitempty"`
	// LockCycles preserves a stacker's lock coverage.
	// +kubebuilder:validation:Minimum=2
	// +kubebuilder:validation:Maximum=12
	LockCycles *int32 `json:"lockCycles,omitempty"`
	// Archive locates an actor's captured data.
	Archive *SnapshotArchive `json:"archive,omitempty"`
}

// SnapshotArchive locates one actor's captured data in the snapshot image.
type SnapshotArchive struct {
	// Path names a gzip tar archive directly under /snapshot.
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[a-z0-9][-a-z0-9.]*\.tar\.gz$`
	Path string `json:"path"`
	// SHA256 is the archive's hex checksum, verified before extraction.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	SHA256 string `json:"sha256"`
	// Image is the actor image that last wrote the data.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`
}

// SnapshotBitcoinTip identifies the captured best Bitcoin block.
type SnapshotBitcoinTip struct {
	// Height is the captured block height.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=4294967295
	Height int64 `json:"height"`
	// BlockHash is the captured block hash as Bitcoin RPC reports it.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	BlockHash string `json:"blockHash"`
}

// SnapshotStacksTip identifies the captured canonical Stacks tip.
type SnapshotStacksTip struct {
	// Height is the captured Stacks block height.
	// +kubebuilder:validation:Minimum=0
	Height int64 `json:"height"`
	// BlockHash is the captured tip block hash.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	BlockHash string `json:"blockHash"`
	// ConsensusHash is the captured tip's consensus hash.
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{40}$`
	ConsensusHash string `json:"consensusHash"`
	// RewardCycle is the PoX reward cycle at capture.
	// +kubebuilder:validation:Minimum=0
	RewardCycle int64 `json:"rewardCycle"`
}

// StacksChainstateSnapshotList contains API objects.
// +kubebuilder:object:root=true
type StacksChainstateSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	// Items contains objects.
	Items []StacksChainstateSnapshot `json:"items"`
}

// GetResolutionStatus exposes independent snapshot validation.
func (v *StacksChainstateSnapshot) GetResolutionStatus() *common.ResolutionStatus { return &v.Status }
