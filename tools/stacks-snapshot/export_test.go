package main

import (
	"context"
	"strings"
	"testing"

	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestExportPodReadsTheVolumeWithoutPrivileges(t *testing.T) {
	pod := exportPod(networkUID, "node-01", "data-node-01-0", "busybox:1.36", 1800)
	if pod.GenerateName != "snapshot-export-node-01-" || pod.Labels[exportLabel] != networkUID {
		t.Fatalf("identity %+v", pod.ObjectMeta)
	}
	spec := pod.Spec
	if *spec.AutomountServiceAccountToken || *spec.ActiveDeadlineSeconds != 1800 ||
		spec.RestartPolicy != corev1.RestartPolicyNever || spec.NodeName != "" {
		t.Fatalf("pod spec %+v", spec)
	}
	sc := spec.SecurityContext
	if *sc.RunAsUser != actorUser || *sc.RunAsGroup != actorUser || !*sc.RunAsNonRoot ||
		sc.FSGroup != nil || sc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("pod security %+v", sc)
	}
	if len(spec.Containers) != 1 || len(spec.Volumes) != 1 {
		t.Fatal("unexpected containers or volumes")
	}
	ctr := spec.Containers[0]
	if ctr.Name != exportContainer || ctr.Image != "busybox:1.36" ||
		strings.Join(ctr.Command, " ") != "sleep 1800" ||
		*ctr.SecurityContext.AllowPrivilegeEscalation || !*ctr.SecurityContext.ReadOnlyRootFilesystem ||
		ctr.SecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Fatalf("container %+v", ctr)
	}
	if m := ctr.VolumeMounts[0]; m.MountPath != dataMount || !m.ReadOnly {
		t.Fatalf("mount %+v", m)
	}
	if v := spec.Volumes[0].PersistentVolumeClaim; v.ClaimName != "data-node-01-0" || !v.ReadOnly {
		t.Fatalf("volume %+v", v)
	}
}

func claim(name, participantUID string, phase corev1.PersistentVolumeClaimPhase) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "lab", Labels: map[string]string{
			api.LabelNetworkUID:     networkUID,
			api.LabelParticipantUID: participantUID,
			api.LabelRole:           api.RoleActor,
		}},
		Status: corev1.PersistentVolumeClaimStatus{Phase: phase},
	}
}

func TestActorClaimSelectsTheOneBoundVolume(t *testing.T) {
	p := participant("node-01", api.ParticipantStacksNode, api.Configuration{})
	p.Spec.NetworkUID = networkUID
	ctx := context.Background()

	c := &cluster{core: fake.NewClientset(
		claim("data-node-01-0", "node-01-uid", corev1.ClaimBound),
		claim("data-signer-01-0", "signer-01-uid", corev1.ClaimBound),
	), namespace: "lab"}
	if name, err := c.actorClaim(ctx, &p); err != nil || name != "data-node-01-0" {
		t.Fatalf("got %s %v", name, err)
	}
	c = &cluster{core: fake.NewClientset(claim("data-node-01-0", "node-01-uid", corev1.ClaimPending)), namespace: "lab"}
	if _, err := c.actorClaim(ctx, &p); err == nil {
		t.Fatal("unbound volume accepted")
	}
	c = &cluster{core: fake.NewClientset(), namespace: "lab"}
	if _, err := c.actorClaim(ctx, &p); err == nil {
		t.Fatal("missing volume accepted")
	}
}

func TestRemoveExportPodsDeletesOnlyThisNetworksLeftovers(t *testing.T) {
	pod := func(name, uid string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "lab", Labels: map[string]string{exportLabel: uid},
		}}
	}
	other := "87654321-1234-1234-1234-123456789abc"
	core := fake.NewClientset(pod("left", networkUID), pod("other", other), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "actor", Namespace: "lab"},
	})
	c := &cluster{core: core, namespace: "lab"}
	if err := c.removeExportPods(context.Background(), networkUID); err != nil {
		t.Fatal(err)
	}
	pods, err := core.CoreV1().Pods("lab").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, p := range pods.Items {
		names = append(names, p.Name)
	}
	if strings.Join(names, ",") != "actor,other" {
		t.Fatalf("remaining %v", names)
	}
}

func TestLimitedBufferKeepsTheStart(t *testing.T) {
	var b limitedBuffer
	n, err := b.Write([]byte(strings.Repeat("x", 5000)))
	if err != nil || n != 5000 || b.Len() != 4096 {
		t.Fatalf("n=%d len=%d err=%v", n, b.Len(), err)
	}
	if n, _ = b.Write([]byte("more")); n != 4 || b.Len() != 4096 {
		t.Fatal("buffer grew past its bound")
	}
}
