package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/utils/ptr"
)

const (
	// exportLabel marks export Pods with the source network UID, for cleanup after a crash.
	exportLabel     = "snapshot.stacks.org/network-uid"
	exportContainer = "export"
	dataMount       = "/data"
	// actorUser is the user and group actors run as, so the export Pod can read their files.
	actorUser = 1000
)

// tarCommand streams the data root as an uncompressed tar; compression and filtering happen
// locally, so the export image needs only tar.
var tarCommand = []string{"tar", "-C", dataMount, "-cf", "-", "."}

// removeExportPods deletes export Pods a previous capture of this network left behind.
func (c *cluster) removeExportPods(ctx context.Context, networkUID string) error {
	pods, err := c.core.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{exportLabel: networkUID}).String(),
	})
	if err != nil {
		return fmt.Errorf("list export pods: %w", err)
	}
	for _, p := range pods.Items {
		if err = c.deletePod(ctx, p.Name); err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) deletePod(ctx context.Context, name string) error {
	err := c.core.CoreV1().Pods(c.namespace).Delete(ctx, name, metav1.DeleteOptions{
		GracePeriodSeconds: ptr.To[int64](0),
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete export pod %s: %w", name, err)
	}
	return nil
}

// actorClaim finds the one bound data volume of an actor participant.
func (c *cluster) actorClaim(ctx context.Context, p *api.StacksNetworkParticipant) (string, error) {
	claims, err := c.core.CoreV1().PersistentVolumeClaims(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			api.LabelNetworkUID:     string(p.Spec.NetworkUID),
			api.LabelParticipantUID: string(p.UID),
			api.LabelRole:           api.RoleActor,
		}).String(),
	})
	if err != nil {
		return "", fmt.Errorf("list volumes: %w", err)
	}
	if len(claims.Items) != 1 {
		return "", fmt.Errorf("found %d data volumes, expected 1", len(claims.Items))
	}
	claim := claims.Items[0]
	if claim.Status.Phase != corev1.ClaimBound || claim.DeletionTimestamp != nil {
		return "", fmt.Errorf("data volume %s is not bound", claim.Name)
	}
	return claim.Name, nil
}

// export streams one actor's data volume into a local archive and returns its sha256 and size.
func (c *cluster) export(
	ctx context.Context,
	r request,
	p *api.StacksNetworkParticipant,
	file string,
) (string, int64, error) {
	claim, err := c.actorClaim(ctx, p)
	if err != nil {
		return "", 0, err
	}
	deadline := int64(r.TimeoutSeconds)
	pod, err := c.core.CoreV1().Pods(c.namespace).Create(
		ctx,
		exportPod(r.NetworkUID, p.Spec.ParticipantName, claim, r.ExportImage, deadline),
		metav1.CreateOptions{},
	)
	if err != nil {
		return "", 0, fmt.Errorf("create export pod: %w", err)
	}
	defer func() {
		// Delete even when the capture deadline has passed.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = c.deletePod(cleanup, pod.Name)
	}()
	err = poll(ctx, "export pod "+pod.Name, func() (bool, error) {
		current, err := c.core.CoreV1().Pods(c.namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if current.UID != pod.UID {
			return false, fatal{errors.New("export pod identity changed")}
		}
		switch current.Status.Phase {
		case corev1.PodRunning:
			return true, nil
		case corev1.PodFailed, corev1.PodSucceeded:
			return false, fatal{fmt.Errorf("export pod ended in phase %s", current.Status.Phase)}
		case corev1.PodPending, corev1.PodUnknown:
		}
		return false, nil
	})
	if err != nil {
		return "", 0, err
	}
	// #nosec G304 -- file is the archive path inside the requested, freshly emptied output directory.
	f, err := os.OpenFile(file, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", 0, fmt.Errorf("create archive: %w", err)
	}
	sum, size, err := c.archive(ctx, pod.Name, f)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return sum, size, err
}

// archive runs tar in the export Pod and rewrites its output into dst.
func (c *cluster) archive(ctx context.Context, pod string, dst io.Writer) (string, int64, error) {
	pr, pw := io.Pipe()
	streamed := make(chan error, 1)
	go func() {
		err := c.stream(ctx, pod, pw)
		_ = pw.CloseWithError(err)
		streamed <- err
	}()
	sum, size, err := rewrite(pr, dst)
	if err != nil {
		_ = pr.CloseWithError(err)
		<-streamed
		return "", 0, err
	}
	// Drain the tar padding after the end-of-archive marker so the stream can complete.
	_, _ = io.Copy(io.Discard, pr)
	if err = <-streamed; err != nil {
		return "", 0, err
	}
	return sum, size, nil
}

// limitedBuffer keeps the first bytes of a stream for error reports.
type limitedBuffer struct{ bytes.Buffer }

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if room := 4096 - b.Len(); room > 0 {
		b.Buffer.Write(p[:min(room, len(p))])
	}
	return len(p), nil
}

// stream executes tarCommand in the export container, writing its stdout to w.
func (c *cluster) stream(ctx context.Context, pod string, w io.Writer) error {
	req := c.core.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(c.namespace).
		Name(pod).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: exportContainer,
			Command:   tarCommand,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	websocket, err := remotecommand.NewWebSocketExecutor(c.exec, "GET", req.URL().String())
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	spdy, err := remotecommand.NewSPDYExecutor(c.exec, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	executor, err := remotecommand.NewFallbackExecutor(websocket, spdy, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	var stderr limitedBuffer
	if err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: w, Stderr: &stderr}); err != nil {
		if detail := strings.TrimSpace(stderr.String()); detail != "" {
			return fmt.Errorf("tar: %w: %s", err, detail)
		}
		return fmt.Errorf("tar: %w", err)
	}
	return nil
}

// exportPod mounts one actor volume read-only in a Pod that only waits to be exec'd. The
// scheduler places it on the volume's node. It runs as the actor user without privileges and
// expires on its own if the capture dies.
func exportPod(networkUID, participant, claim, image string, deadline int64) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "snapshot-export-" + participant + "-",
			Labels:       map[string]string{exportLabel: networkUID},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			AutomountServiceAccountToken:  ptr.To(false),
			EnableServiceLinks:            ptr.To(false),
			ActiveDeadlineSeconds:         ptr.To(deadline),
			TerminationGracePeriodSeconds: ptr.To[int64](0),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser:      ptr.To[int64](actorUser),
				RunAsGroup:     ptr.To[int64](actorUser),
				RunAsNonRoot:   ptr.To(true),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Containers: []corev1.Container{{
				Name:            exportContainer,
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{"sleep", strconv.FormatInt(deadline, 10)},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr.To(false),
					ReadOnlyRootFilesystem:   ptr.To(true),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
				VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: dataMount, ReadOnly: true}},
			}},
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim, ReadOnly: true},
				},
			}},
		},
	}
}
