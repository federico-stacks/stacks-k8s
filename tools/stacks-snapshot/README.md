# stacks-snapshot

Capture an initialized network's chain data as a restorable chainstate snapshot. The
result is one gzip tar archive per Bitcoin node, Stacks node and signer, plus a
ready-to-apply `StacksChainstateSnapshot` manifest. A network that sets
`spec.genesis.restore.snapshotRef` to that resource starts from the captured chain
instead of initializing from genesis.

Capture **stops the source network**. `Stopped` is terminal, so capture a network you
no longer need running.

```bash
GOWORK=off go -C tools/stacks-snapshot run . capture < capture.json > report.json
```

```json
{
  "kubeconfig": "/path/to/kubeconfig",
  "context": "kind-stacks-k8s",
  "namespace": "lab",
  "network": "network",
  "networkUID": "12345678-1234-1234-1234-123456789abc",
  "snapshot": "epoch40-minimal",
  "image": "localhost/stacks-snapshots/epoch40-minimal:v1",
  "output": "/abs/path/epoch40-minimal",
  "bitcoinHeight": 260,
  "timeoutSeconds": 1800
}
```

| Field | Meaning |
|---|---|
| `kubeconfig`, `context` | Cluster selection. Static certificate or token credentials only; exec and auth-provider plugins are rejected. |
| `namespace`, `network`, `networkUID` | The source network. The UID guards against acting on a recreated network with the same name. |
| `snapshot` | Name of the `StacksChainstateSnapshot` the manifest declares. |
| `image` | Reference of the snapshot image that will hold the archives under `/snapshot`; recorded as `spec.image`. Capture does not build or push it. |
| `exportImage` | Optional. Image with `sleep` and `tar` for the export Pod; defaults to BusyBox pinned by digest. |
| `output` | Absolute path of a new or empty directory for the archives and `snapshot.yaml`. |
| `bitcoinHeight` | Optional. Pause once every Bitcoin node reports at least this height. |
| `timeoutSeconds` | Bound for the whole capture, 60 to 86400. |

## What capture does

1. Requires an `Initialized` network that is not stopped, and reads its frozen initial
   cohort. It refuses accounts with generated keys: a restoring network must import the
   same keys to reproduce the cohort's public identities.
2. Optionally waits for `bitcoinHeight`, then sets `spec.operation: Paused` and waits
   for phase `Paused`. Bitcoin production stops; nodes keep running.
3. Waits until the chain is still: every Bitcoin execution and Stacks node observation
   is refreshed twice in a row with the same tips, and the nodes of each chain agree.
   It records the Bitcoin and Stacks tips from these status observations.
4. Sets `spec.operation: Stopped` and waits for phase `Stopped`. Actor volumes stay.
5. For each actor volume, creates a Pod that mounts it read-only and streams `tar` out
   of it over `exec`. Locally, it drops the root entry and the peer and mempool
   databases, gzips the stream and computes its sha256.
6. Writes `snapshot.yaml` and prints a report with the tips, archive checksums and step
   times. Neither contains keys.

Only the initial cohort is captured. Participants that joined later are not part of the
snapshot; add them to the restored network after it initializes.

The export Pod runs as the actor user (1000) without privileges, a service account
token or a writable root filesystem. It expires on its own after `timeoutSeconds`, and
a later capture of the same network deletes leftovers.

## Permissions

Capture uses the caller's credentials; the operator gains no permissions. Outside a
cluster-admin context, bind this Role in the source namespace:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: stacks-snapshot-capture
rules:
  - apiGroups: [network.stacks.org]
    resources: [stacksnetworks]
    verbs: [get, patch]
  - apiGroups: [network.stacks.org]
    resources: [stacksnetworkparticipants, stacksgeneses]
    verbs: [get]
  - apiGroups: [stacks.stacks.org]
    resources: [stacksaccounts]
    verbs: [get]
  - apiGroups: [bitcoin.stacks.org]
    resources: [bitcoinexecutions]
    verbs: [get]
  - apiGroups: [""]
    resources: [persistentvolumeclaims]
    verbs: [list]
  - apiGroups: [""]
    resources: [pods]
    verbs: [get, list, create, delete]
  - apiGroups: [""]
    resources: [pods/exec]
    verbs: [get, create]
```

No Secret, Node or PersistentVolume access is needed: the scheduler places the export
Pod on its volume's node.
