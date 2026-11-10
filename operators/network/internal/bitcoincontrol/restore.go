package bitcoincontrol

import (
	"context"
	"errors"
	"math"
	"time"

	bitcoin "github.com/cylewitruk-stacks/stacks-k8s/apis/network/bitcoin/v1alpha2"
	api "github.com/cylewitruk-stacks/stacks-k8s/apis/network/v1alpha2"
	"github.com/cylewitruk-stacks/stacks-k8s/operators/network/internal/foundation"
	"github.com/cylewitruk-stacks/stacks-k8s/operators/network/internal/objectref"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// restoredSnapshot returns the chainstate snapshot pinned by the network's exact frozen genesis.
// It returns nil when the network was initialized from genesis, and also when the frozen genesis
// cannot be established: the bootstrap path then reports that failure through its own gate checks.
func restoredSnapshot(
	ctx context.Context,
	reader client.Reader,
	root *api.StacksNetwork,
	initial *bitcoin.BitcoinInitialization,
) (*api.StacksChainstateSnapshot, error) {
	ref := root.Status.GenesisRef
	if ref == nil || *ref != initial.Spec.Genesis {
		return nil, nil
	}
	var genesis api.StacksGenesis
	if err := reader.Get(ctx, client.ObjectKey{Namespace: root.Namespace, Name: ref.Name}, &genesis); err != nil ||
		genesis.UID != ref.UID || genesis.DeletionTimestamp != nil || !metav1.IsControlledBy(&genesis, root) ||
		foundation.Digest(genesis.Spec) != ref.Fingerprint {
		return nil, nil //nolint:nilerr // An unestablished genesis is reported by the gate checks, not here.
	}
	return foundation.RestoredSnapshot(ctx, reader, &genesis)
}

// reconcileRestore prepares Bitcoin for a network restored from a chainstate snapshot. Instead of
// generating up to the first gate, it proves the restored chain (the snapshot's exact height and
// tip, ready wallets and mature miner outputs), then generates exactly one block: stacks-node
// refuses a burnchain tip older than two hours, and a snapshot is usually older. Only then does it
// record preparation, which lets the Stacks actors start on a fresh tip.
func (s *Scheduler) reconcileRestore(
	ctx context.Context,
	root *api.StacksNetwork,
	record *bitcoin.BitcoinInitialization,
	snapshot *api.StacksChainstateSnapshot,
	report func(bitcoin.InitializationPhase, string) (ctrl.Result, error),
) (ctrl.Result, error) {
	record.Status.NextOpportunityAt = nil
	if record.Status.PreparedAt != nil {
		return report(bitcoin.InitializationHeld, reasonRestorePrepared)
	}
	production, e := s.currentProduction(ctx, root, record)
	if e != nil {
		return report(bitcoin.InitializationBlocked, reasonProductionIdentityUnavailable)
	}
	current := objectref.Participant(production)
	record.Status.Production = &current
	if root.Spec.Operation == api.NetworkOperationPaused ||
		production.Spec.Control != nil && ptr.Deref(production.Spec.Control.Paused, false) {
		return report(bitcoin.InitializationPaused, api.ReasonDesiredPause)
	}
	allRecords, reason := s.executions(ctx, root)
	if reason != "" {
		return report(bitcoin.InitializationBlocked, reason)
	}
	if target := allRecords[string(record.Spec.Target.UID)]; target != nil {
		accountRestoreReceipt(root, record, target)
	}
	view, phase, reason := s.converge(ctx, root, record, allRecords)
	if reason != "" {
		return report(phase, reason)
	}
	if !minersMature(record, view.mature) {
		return report(bitcoin.InitializationBlocked, reasonRestoredWalletsImmature)
	}
	restored := snapshot.Spec.Bitcoin
	offer := record.Status.Offer
	if offer != nil && offer.Mode == bitcoin.OfferRestore && offer.Number <= record.Status.LastAccountedOffer {
		if view.height == restored.Height && view.tip == restored.BlockHash {
			return report(bitcoin.InitializationWaiting, reasonAwaitingPostReceiptObservation)
		}
		if view.height != restored.Height+1 {
			return report(bitcoin.InitializationBlocked, reasonRestoredChainDiffers)
		}
		now := metav1.NewTime(s.Now().UTC())
		record.Status.PreparedAt = &now
		if record.Status.FirstCeilingObservedAt == nil {
			record.Status.FirstCeilingObservedAt = &now
		}
		return report(bitcoin.InitializationHeld, reasonRestorePrepared)
	}
	if view.height != restored.Height || view.tip != restored.BlockHash {
		return report(bitcoin.InitializationBlocked, reasonRestoredChainDiffers)
	}
	if offer != nil && offer.Mode == bitcoin.OfferRestore && offer.Number > record.Status.LastAccountedOffer &&
		s.Now().Before(offer.ExpiresAt.Time) && offer.Production == current &&
		offer.PolicyDigest == production.Status.Admission.PolicyDigest {
		if !equality.Semantic.DeepEqual(view.target.Spec.Offer, offer) {
			view.target.Spec.Offer = offer.DeepCopy()
			if e := s.Client.Update(ctx, view.target); e != nil {
				return ctrl.Result{}, e
			}
		}
		return report(bitcoin.InitializationPreparing, reasonOpportunityOutstanding)
	}
	if e := foundation.ValidateAdmissionEligibility(ctx, s.Reader, production); e != nil {
		record.Status.Offer = nil
		return report(bitcoin.InitializationBlocked, reasonProductionSourceUnavailable)
	}
	number := max(record.Status.LastAccountedOffer, view.target.Status.CompletedOffer)
	if offer != nil {
		number = max(number, offer.Number)
	}
	if view.target.Spec.Offer != nil {
		number = max(number, view.target.Spec.Offer.Number)
	}
	if number == math.MaxInt64 {
		return report(bitcoin.InitializationBlocked, reasonOpportunityCounterExhausted)
	}
	payout := record.Spec.PayoutWallet
	record.Status.Offer = &bitcoin.BitcoinBlockOffer{
		Mode:           bitcoin.OfferRestore,
		Initialization: objectref.BitcoinInitialization(record),
		Production:     current,
		PolicyDigest:   production.Status.Admission.PolicyDigest,
		Number:         number + 1,
		Address:        payout.Address,
		Wallet:         payout.Wallet,
		ExpectedHeight: restored.Height,
		ExpectedTip:    restored.BlockHash,
		Ceiling:        restored.Height + 1,
		ExpiresAt:      metav1.NewTime(s.Now().Add(bootstrapAdmissionWindow).UTC()),
	}
	result, e := report(bitcoin.InitializationPreparing, reasonOpportunitySelected)
	result.RequeueAfter = time.Millisecond
	return result, e
}

// accountRestoreReceipt advances the scheduler cursor once the worker reports the refresh block.
// The refresh pays the payout wallet, so no miner funding is counted.
func accountRestoreReceipt(
	root *api.StacksNetwork,
	initial *bitcoin.BitcoinInitialization,
	execution *bitcoin.BitcoinExecution,
) bool {
	if execution.Spec.NetworkUID != root.UID || !metav1.IsControlledBy(execution, root) ||
		execution.Spec.Participant != initial.Spec.Target {
		return false
	}
	receipt := execution.Status.LastReceipt
	if receipt == nil || receipt.Request.Method != bitcoin.RPCGenerate || receipt.Request.Action != nil ||
		receipt.Request.Offer == nil ||
		receipt.Request.Reservation != objectref.BitcoinInitialization(initial) ||
		receipt.Request.Target.Participant != initial.Spec.Target ||
		!hashValid(receipt.BlockHash) {
		return false
	}
	offer := receipt.Request.Offer
	if offer.Mode != bitcoin.OfferRestore || offer.Initialization != objectref.BitcoinInitialization(initial) ||
		offer.Number <= initial.Status.LastAccountedOffer ||
		offer.Number != execution.Status.CompletedOffer ||
		offerBlocks(offer) != 1 ||
		offer.Ceiling != offer.ExpectedHeight+1 ||
		offer.Wallet != initial.Spec.PayoutWallet.Wallet ||
		offer.Address != initial.Spec.PayoutWallet.Address {
		return false
	}
	initial.Status.LastAccountedOffer = offer.Number
	return true
}

// authorizeRestoreOffer permits only the single refresh block, to the payout wallet, on top of the
// exact restored tip, and only before preparation is recorded.
func (w *Worker) authorizeRestoreOffer(ctx context.Context, a admitted, offer *bitcoin.BitcoinBlockOffer) error {
	initial := a.initialization
	if initial.Spec.Target.UID != a.participant.UID || initial.Status.PreparedAt != nil ||
		offerBlocks(offer) != 1 ||
		offer.Ceiling != offer.ExpectedHeight+1 ||
		offer.Wallet != initial.Spec.PayoutWallet.Wallet ||
		offer.Address != initial.Spec.PayoutWallet.Address {
		return errors.New("restore refresh is not authorized")
	}
	snapshot, err := restoredSnapshot(ctx, w.Reader, a.root, initial)
	if err != nil || snapshot == nil || offer.ExpectedHeight != snapshot.Spec.Bitcoin.Height ||
		offer.ExpectedTip != snapshot.Spec.Bitcoin.BlockHash {
		return errors.New("restore refresh does not extend the snapshot tip")
	}
	return nil
}
