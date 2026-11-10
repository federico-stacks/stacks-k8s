package participantworkload

import (
	"context"
	"fmt"

	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// restoreContainer names the init container that fills a restored actor's data volume.
const restoreContainer = "restore-data"

// restoreScript extracts one snapshot archive into /data once. The marker is written only after a
// complete extraction, so an interrupted attempt is discarded and repeated on the next start.
// Peer databases and mempools are dropped: they hold the source network's peers and transactions.
const restoreScript = `marker=/data/.chainstate-restore
[ -f "$marker" ] && exit 0
archive="/snapshot/$1"
[ "$(sha256sum "$archive" | cut -d ' ' -f 1)" = "$2" ] || { echo "archive checksum mismatch" >&2; exit 1; }
find /data -mindepth 1 -delete
tar -xzf "$archive" -C /data
find /data \( -name peer.sqlite -o -name 'mempool.sqlite*' \) -delete
echo "$2" >"$marker"`

// chainstateRestore selects one actor archive of a chainstate snapshot.
type chainstateRestore struct {
	image   string
	archive api.SnapshotArchive
}

// chainstateRestoreFor returns the participant's archive when its network was restored from a
// snapshot. Networks initialized from genesis, and participants added after a restore, start empty.
// The snapshot must remain available while the network runs: every actor start uses its image.
func (r *Reconciler) chainstateRestoreFor(
	ctx context.Context,
	root *api.StacksNetwork,
	p *api.StacksNetworkParticipant,
) (*chainstateRestore, error) {
	var genesis api.StacksGenesis
	if err := r.Reader.Get(
		ctx,
		client.ObjectKey{Namespace: root.Namespace, Name: root.Status.GenesisRef.Name},
		&genesis,
	); err != nil {
		return nil, fmt.Errorf("genesis unavailable")
	}
	for _, b := range genesis.Spec.Source.Dependencies {
		if b.Kind != api.KindStacksChainstateSnapshot {
			continue
		}
		var snapshot api.StacksChainstateSnapshot
		if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: root.Namespace, Name: b.Name}, &snapshot); err != nil {
			return nil, fmt.Errorf("chainstate snapshot %s unavailable", b.Name)
		}
		if snapshot.UID != b.UID || snapshot.DeletionTimestamp != nil || digest(snapshot.Spec) != b.Fingerprint {
			return nil, fmt.Errorf("chainstate snapshot %s identity changed", b.Name)
		}
		for _, member := range snapshot.Spec.Participants {
			if member.Name == p.Spec.ParticipantName && member.Kind == p.Spec.Kind && member.Archive != nil {
				return &chainstateRestore{image: snapshot.Spec.Image, archive: *member.Archive}, nil
			}
		}
		return nil, nil
	}
	return nil, nil
}

// restoreInit runs before every other actor container, with the actor's own non-root identity.
func restoreInit(restore *chainstateRestore) corev1.Container {
	return corev1.Container{
		Name:            restoreContainer,
		Image:           restore.image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"sh", "-ec", restoreScript, "--", restore.archive.Path, restore.archive.SHA256},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			ReadOnlyRootFilesystem:   ptr.To(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
	}
}

// initContainerFailed reports a terminated or crash-looping init container with a non-zero exit.
func initContainerFailed(pod *corev1.Pod, name string) bool {
	for _, status := range pod.Status.InitContainerStatuses {
		if status.Name == name &&
			((status.State.Terminated != nil && status.State.Terminated.ExitCode != 0) ||
				(status.State.Waiting != nil &&
					status.LastTerminationState.Terminated != nil &&
					status.LastTerminationState.Terminated.ExitCode != 0)) {
			return true
		}
	}
	return false
}
