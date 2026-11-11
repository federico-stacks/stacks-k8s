package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
	"sigs.k8s.io/yaml"
)

// stillRounds is how many consecutive refreshes of every observation must repeat the same tips.
const stillRounds = 2

// manifestFile is the ready-to-apply snapshot resource written next to the archives.
const manifestFile = "snapshot.yaml"

// report is the capture result; it holds public chain facts and checksums only.
type report struct {
	NetworkUID string                 `json:"networkUID"`
	Snapshot   string                 `json:"snapshot"`
	Manifest   string                 `json:"manifest"`
	Bitcoin    api.SnapshotBitcoinTip `json:"bitcoin"`
	Stacks     api.SnapshotStacksTip  `json:"stacks"`
	Archives   []archiveReport        `json:"archives"`
	Steps      []step                 `json:"steps"`
}

type archiveReport struct {
	Participant string `json:"participant"`
	Path        string `json:"path"`
	SHA256      string `json:"sha256"`
	Bytes       int64  `json:"bytes"`
}

type step struct {
	Name string    `json:"name"`
	At   time.Time `json:"at"`
}

// capture checks the network, pauses it until its chain is still, records the tips, stops it,
// exports every actor volume and writes the snapshot manifest.
func capture(ctx context.Context, c *cluster, r request, progress func(string)) (report, error) {
	out := report{NetworkUID: r.NetworkUID, Snapshot: r.Snapshot}
	mark := func(name, detail string) {
		out.Steps = append(out.Steps, step{Name: name, At: time.Now().UTC()})
		progress(detail)
	}
	root, err := c.network(ctx, r.NetworkUID, r.Network)
	if err != nil {
		return out, err
	}
	if err = checkCapturable(root); err != nil {
		return out, err
	}
	members, err := c.cohort(ctx, root)
	if err != nil {
		return out, err
	}
	participants, err := project(members)
	if err != nil {
		return out, err
	}
	mark("checked", fmt.Sprintf("network %s is Initialized; %d cohort participants", r.Network, len(members)))

	if r.BitcoinHeight > 0 {
		err = poll(ctx, fmt.Sprintf("Bitcoin height %d", r.BitcoinHeight), func() (bool, error) {
			tip, _, err := c.bitcoinView(ctx, root)
			return err == nil && tip.Height >= r.BitcoinHeight, err
		})
		if err != nil {
			return out, err
		}
		mark("reached-height", fmt.Sprintf("Bitcoin reached height %d", r.BitcoinHeight))
	}

	if root.Spec.Operation != api.NetworkOperationPaused {
		if err = c.patchOperation(ctx, root, api.NetworkOperationPaused); err != nil {
			return out, err
		}
	}
	if err = c.waitPhase(ctx, r.NetworkUID, r.Network, api.NetworkPhasePaused); err != nil {
		return out, err
	}
	mark("paused", "network Paused; waiting for the chain to be still")

	var s settler
	err = poll(ctx, "a still chain", func() (bool, error) {
		btc, bo, err := c.bitcoinView(ctx, root)
		if err != nil {
			return false, err
		}
		stx, so, err := c.stacksView(ctx, members)
		if err != nil {
			return false, err
		}
		out.Bitcoin, out.Stacks = btc, stx
		still, why := s.step(append(bo, so...))
		if why != "" {
			return false, errors.New(why)
		}
		return still, nil
	})
	if err != nil {
		return out, err
	}
	mark("settled", fmt.Sprintf("chain still at Bitcoin %d, Stacks %d", out.Bitcoin.Height, out.Stacks.Height))

	if err = c.patchOperation(ctx, root, api.NetworkOperationStopped); err != nil {
		return out, err
	}
	if err = c.waitPhase(ctx, r.NetworkUID, r.Network, api.NetworkPhaseStopped); err != nil {
		return out, err
	}
	mark("stopped", "network Stopped; exporting actor volumes")

	if err = c.removeExportPods(ctx, r.NetworkUID); err != nil {
		return out, err
	}
	byName := map[string]*api.StacksNetworkParticipant{}
	for i := range members {
		byName[members[i].participant.Spec.ParticipantName] = &members[i].participant
	}
	for i := range participants {
		p := &participants[i]
		if p.Archive == nil {
			continue
		}
		file := filepath.Join(r.Output, p.Archive.Path)
		sum, size, err := c.export(ctx, r, byName[p.Name], file)
		if err != nil {
			return out, fmt.Errorf("export %s: %w", p.Name, err)
		}
		p.Archive.SHA256 = sum
		out.Archives = append(out.Archives, archiveReport{p.Name, p.Archive.Path, sum, size})
		progress(fmt.Sprintf("exported %s (%d bytes)", p.Archive.Path, size))
	}
	mark("exported", fmt.Sprintf("%d archives exported", len(out.Archives)))

	data, err := manifest(r.Snapshot, r.Image, root.Status.GenesisDigest, participants, out.Bitcoin, out.Stacks)
	if err != nil {
		return out, err
	}
	out.Manifest = filepath.Join(r.Output, manifestFile)
	if err = os.WriteFile(out.Manifest, data, 0o600); err != nil {
		return out, fmt.Errorf("write manifest: %w", err)
	}
	mark("written", "wrote "+out.Manifest)
	return out, nil
}

// settler decides when the chain is still: every observation refreshed stillRounds times in a row
// with the same tips, and all Bitcoin nodes and all Stacks nodes agreeing. It compares operator
// timestamps only with each other, never with the local clock.
type settler struct {
	last   map[string]time.Time
	view   string
	rounds int
}

// step consumes one read of every observation. It returns whether the chain is still, or why
// the read cannot count.
func (s *settler) step(seen []observation) (bool, string) {
	tips := map[string]string{}
	for _, o := range seen {
		domain := strings.SplitN(o.source, "/", 2)[0]
		if t, ok := tips[domain]; ok && t != o.tip {
			s.last, s.rounds = nil, 0
			return false, domain + " nodes disagree on the tip"
		}
		tips[domain] = o.tip
	}
	if s.last != nil {
		for _, o := range seen {
			if !o.observedAt.After(s.last[o.source]) {
				return false, ""
			}
		}
	}
	parts := make([]string, 0, len(tips))
	for domain, tip := range tips {
		parts = append(parts, domain+"="+tip)
	}
	sort.Strings(parts)
	view := strings.Join(parts, ",")
	if s.last != nil && view == s.view {
		s.rounds++
	} else {
		s.rounds = 0
	}
	s.view = view
	s.last = map[string]time.Time{}
	for _, o := range seen {
		s.last[o.source] = o.observedAt
	}
	return s.rounds >= stillRounds, ""
}

// snapshotManifest is the applied form of the snapshot, without server-owned fields.
type snapshotManifest struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec api.StacksChainstateSnapshotSpec `json:"spec"`
}

// manifest renders a namespace-free StacksChainstateSnapshot to apply wherever it is restored.
func manifest(
	name, image, genesisDigest string,
	participants []api.SnapshotParticipant,
	btc api.SnapshotBitcoinTip,
	stx api.SnapshotStacksTip,
) ([]byte, error) {
	m := snapshotManifest{APIVersion: api.GroupVersion.String(), Kind: api.KindStacksChainstateSnapshot}
	m.Metadata.Name = name
	m.Spec = api.StacksChainstateSnapshotSpec{
		Image:         image,
		GenesisDigest: genesisDigest,
		Participants:  participants,
		Bitcoin:       btc,
		Stacks:        stx,
	}
	return yaml.Marshal(m)
}
