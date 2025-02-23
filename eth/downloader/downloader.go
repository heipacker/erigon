// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package downloader contains the manual full chain synchronisation.
package downloader

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
)

var (
	MaxHashFetch    = 512 // Amount of hashes to be fetched per retrieval request
	MaxBlockFetch   = 128 // Amount of blocks to be fetched per retrieval request
	MaxHeaderFetch  = 192 // Amount of block headers to be fetched per retrieval request
	MaxSkeletonSize = 128 // Number of header fetches to need for a skeleton assembly
	MaxReceiptFetch = 256 // Amount of transaction receipts to allow fetching per request
	MaxStateFetch   = 384 // Amount of node state values to allow fetching per request

	MaxForkAncestry  = 1000 * params.EpochDuration // Maximum chain reorganisation
	rttMinEstimate   = 2 * time.Second             // Minimum round-trip time to target for download requests
	rttMaxEstimate   = 20 * time.Second            // Maximum round-trip time to target for download requests
	rttMinConfidence = 0.1                         // Worse confidence factor in our estimated RTT value
	ttlScaling       = 3                           // Constant scaling factor for RTT -> TTL conversion
	ttlLimit         = time.Minute                 // Maximum TTL allowance to prevent reaching crazy timeouts

	qosTuningPeers   = 5    // Number of peers to tune based on (best peers)
	qosConfidenceCap = 10   // Number of peers above which not to modify RTT confidence
	qosTuningImpact  = 0.25 // Impact that a new tuning target has on the previous value

	maxHeadersProcess          = 16536                            // Number of header download results to import at once into the chain
	maxResultsProcess          = 16536                            // Number of content download results to import at once into the chain
	fullMaxForkAncestry uint64 = params.FullImmutabilityThreshold // Maximum chain reorganisation (locally redeclared so tests can reduce it)

	reorgProtThreshold   = 48 // Threshold number of recent blocks to disable mini reorg protection
	reorgProtHeaderDelay = 2  // Number of headers to delay delivering to cover mini reorgs

	fsHeaderCheckFrequency = 100             // Verification frequency of the downloaded headers during fast sync
	fsHeaderSafetyNet      = 2048            // Number of headers to discard in case a chain violation is detected
	fsHeaderForceVerify    = 24              // Number of headers to verify before and after the pivot to accept it
	fsHeaderContCheck      = 3 * time.Second // Time interval to check for header continuations during state download
	fsMinFullBlocks        = 64              // Number of blocks to retrieve fully even in fast sync
)

var (
	errBusy                    = errors.New("busy")
	errUnknownPeer             = errors.New("peer is unknown or unhealthy")
	errBadPeer                 = errors.New("action from bad peer ignored")
	errStallingPeer            = errors.New("peer is stalling")
	errUnsyncedPeer            = errors.New("unsynced peer")
	errNoPeers                 = errors.New("no peers to keep download active")
	errTimeout                 = errors.New("timeout")
	errEmptyHeaderSet          = errors.New("empty header set by peer")
	errPeersUnavailable        = errors.New("no peers available or all tried for download")
	errInvalidAncestor         = errors.New("retrieved ancestor is invalid")
	errInvalidChain            = errors.New("retrieved hash chain is invalid")
	errInvalidBody             = errors.New("retrieved block body is invalid")
	errInvalidReceipt          = errors.New("retrieved receipt is invalid")
	errCancelContentProcessing = errors.New("content processing canceled (requested)")
	errCanceled                = errors.New("syncing canceled (requested)")
	errNoSyncActive            = errors.New("no sync active")
	errTooOld                  = errors.New("peer's protocol version too old")
	errNoAncestorFound         = errors.New("no common ancestor found")
)

type Downloader struct {
	// WARNING: The `rttEstimate` and `rttConfidence` fields are accessed atomically.
	// On 32 bit platforms, only 64-bit aligned fields can be atomic. The struct is
	// guaranteed to be so aligned, so take advantage of that. For more information,
	// see https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
	rttEstimate   uint64 // Round trip time to target for download requests
	rttConfidence uint64 // Confidence in the estimated RTT (unit: millionths to allow atomic ops)

	queue *queue   // Scheduler for selecting the hashes to download
	peers *peerSet // Set of active peers from which download can proceed

	stateDB ethdb.Database // Database to state sync into (and deduplicate via)
	//stateBloom *trie.SyncBloom       // Bloom filter for fast trie node existence checks

	// Statistics
	syncStatsChainOrigin uint64       // Origin block number where syncing started at
	syncStatsChainHeight uint64       // Highest block number known when syncing started
	syncStatsLock        sync.RWMutex // Lock protecting the sync stats fields

	engine      consensus.Engine
	vmConfig    *vm.Config
	chainConfig *params.ChainConfig

	// Callbacks
	dropPeer peerDropFn // Drops a peer for misbehaving

	// Status
	synchroniseMock func(id string, hash common.Hash) error // Replacement for synchronise during testing
	synchronising   int32
	notified        int32
	committed       int32
	ancientLimit    uint64 // The maximum block number which can be regarded as ancient data.

	// Channels
	headerCh      chan dataPack        // Channel receiving inbound block headers
	bodyCh        chan dataPack        // Channel receiving inbound block bodies
	receiptCh     chan dataPack        // Channel receiving inbound receipts
	bodyWakeCh    chan bool            // Channel to signal the block body fetcher of new tasks
	receiptWakeCh chan bool            // Channel to signal the receipt fetcher of new tasks
	headerProcCh  chan []*types.Header // Channel to feed the header processor new tasks

	// State sync
	pivotHeader *types.Header // Pivot block header to dynamically push the syncing state root
	pivotLock   sync.RWMutex  // Lock protecting pivot header reads from updates

	// Cancellation and termination
	cancelPeer string         // Identifier of the peer currently being used as the master (cancel on drop)
	cancelCh   chan struct{}  // Channel to cancel mid-flight syncs
	cancelLock sync.RWMutex   // Lock to protect the cancel channel and peer in delivers
	cancelWg   sync.WaitGroup // Make sure all fetcher goroutines have exited.

	quitCh   chan struct{} // Quit channel to signal termination
	quitLock sync.Mutex    // Lock to prevent double closes

	// Testing hooks
	syncInitHook  func(uint64, uint64)  // Method to call upon initiating a new sync run
	bodyFetchHook func([]*types.Header) // Method to call upon starting a block body fetch

	storageMode ethdb.StorageMode
	tmpdir      string
	batchSize   datasize.ByteSize

	headersState    *stagedsync.StageState
	headersUnwinder stagedsync.Unwinder

	bodiesState    *stagedsync.StageState
	bodiesUnwinder stagedsync.Unwinder

	stagedSyncState *stagedsync.State
	stagedSync      *stagedsync.StagedSync
}

// New creates a new downloader to fetch hashes and blocks from remote peers.
func New(stateDB ethdb.Database, chainConfig *params.ChainConfig, engine consensus.Engine, vmConfig *vm.Config, dropPeer peerDropFn, sm ethdb.StorageMode) *Downloader {
	dl := &Downloader{
		stateDB:       stateDB,
		queue:         newQueue(blockCacheMaxItems, blockCacheInitialItems),
		peers:         newPeerSet(),
		rttEstimate:   uint64(rttMaxEstimate),
		rttConfidence: uint64(1000000),
		chainConfig:   chainConfig,
		engine:        engine,
		vmConfig:      vmConfig,
		dropPeer:      dropPeer,
		headerCh:      make(chan dataPack, 1),
		bodyCh:        make(chan dataPack, 1),
		receiptCh:     make(chan dataPack, 1),
		bodyWakeCh:    make(chan bool, 1),
		receiptWakeCh: make(chan bool, 1),
		headerProcCh:  make(chan []*types.Header, 1),
		quitCh:        make(chan struct{}),
		storageMode:   sm,
	}
	go dl.qosTuner()
	return dl
}

// SetStagedSync sets the staged sync instance (by protocol manager)
func (d *Downloader) SetStagedSync(stagedSync *stagedsync.StagedSync) {
	d.stagedSync = stagedSync
}

// DataDir sets the directory where download is allowed to create temporary files
func (d *Downloader) SetTmpDir(tmpdir string) {
	d.tmpdir = tmpdir
}

func (d *Downloader) SetBatchSize(batchSize datasize.ByteSize) {
	d.batchSize = batchSize
}

func (d *Downloader) SetChainConfig(chainConfig *params.ChainConfig) {
	d.chainConfig = chainConfig
}

// Progress retrieves the synchronisation boundaries, specifically the origin
// block where synchronisation started at (may have failed/suspended); the block
// or header sync is currently at; and the latest known block which the sync targets.
//
// In addition, during the state download phase of fast synchronisation the number
// of processed and the total number of known states are also returned. Otherwise
// these are zero.
func (d *Downloader) Progress() ethereum.SyncProgress {
	// Lock the current stats and return the progress
	d.syncStatsLock.RLock()
	defer d.syncStatsLock.RUnlock()

	current, err := stages.GetStageProgress(d.stateDB, stages.Finish)
	if err != nil {
		log.Error("Could not get current progress", "error", err)
	}
	return ethereum.SyncProgress{
		StartingBlock: d.syncStatsChainOrigin,
		CurrentBlock:  current,
		HighestBlock:  d.syncStatsChainHeight,
	}
}

// Synchronising returns whether the downloader is currently retrieving blocks.
func (d *Downloader) Synchronising() bool {
	return atomic.LoadInt32(&d.synchronising) > 0
}

// RegisterPeer injects a new download peer into the set of block source to be
// used for fetching hashes and blocks from.
func (d *Downloader) RegisterPeer(id string, version uint, peer Peer) error {
	var logger log.Logger
	if len(id) < 16 {
		// Tests use short IDs, don't choke on them
		logger = log.New("peer", id)
	} else {
		logger = log.New("peer", id[:8])
	}
	logger.Trace("Registering sync peer")
	if err := d.peers.Register(newPeerConnection(id, version, peer, logger)); err != nil {
		logger.Error("Failed to register sync peer", "err", err)
		return err
	}
	d.qosReduceConfidence()

	return nil
}

// UnregisterPeer remove a peer from the known list, preventing any action from
// the specified peer. An effort is also made to return any pending fetches into
// the queue.
func (d *Downloader) UnregisterPeer(id string) error {
	// Unregister the peer from the active peer set and revoke any fetch tasks
	var logger log.Logger
	if len(id) < 16 {
		// Tests use short IDs, don't choke on them
		logger = log.New("peer", id)
	} else {
		logger = log.New("peer", id[:8])
	}
	logger.Trace("Unregistering sync peer")
	if err := d.peers.Unregister(id); err != nil {
		logger.Error("Failed to unregister sync peer", "err", err)
		return err
	}
	d.queue.Revoke(id)

	return nil
}

// Synchronise tries to sync up our local block chain with a remote peer, both
// adding various sanity checks as well as wrapping it with various log entries.
func (d *Downloader) Synchronise(id string, head common.Hash, blockNumber uint64, txPool *core.TxPool) error {
	err := d.synchronise(id, head, blockNumber, txPool)

	switch err {
	case nil, errBusy, errCanceled:
		return err
	}

	if errors.Is(err, errInvalidChain) || errors.Is(err, errBadPeer) || errors.Is(err, errTimeout) ||
		errors.Is(err, errStallingPeer) || errors.Is(err, errUnsyncedPeer) || errors.Is(err, errEmptyHeaderSet) ||
		errors.Is(err, errPeersUnavailable) || errors.Is(err, errTooOld) || errors.Is(err, errInvalidAncestor) {
		log.Warn("Synchronisation failed, dropping peer", "peer", id, "err", err)
		if d.dropPeer == nil {
			// The dropPeer method is nil when `--copydb` is used for a local copy.
			// Timeouts can occur if e.g. compaction hits at the wrong time, and can be ignored
			log.Warn("Downloader wants to drop peer, but peerdrop-function is not set", "peer", id)
		} else {
			d.dropPeer(id)
		}
		return err
	}
	log.Warn("Synchronisation failed, retrying", "err", err)
	return err
}

// synchronise will select the peer and use it for synchronising. If an empty string is given
// it will use the best peer possible and synchronize if its TD is higher than our own. If any of the
// checks fail an error will be returned. This method is synchronous
func (d *Downloader) synchronise(id string, hash common.Hash, blockNumber uint64, txPool *core.TxPool) error {
	// Mock out the synchronisation if testing
	if d.synchroniseMock != nil {
		return d.synchroniseMock(id, hash)
	}
	// Make sure only one goroutine is ever allowed past this point at once
	if !atomic.CompareAndSwapInt32(&d.synchronising, 0, 1) {
		return errBusy
	}
	defer atomic.StoreInt32(&d.synchronising, 0)

	// Post a user notification of the sync (only once per session)
	if atomic.CompareAndSwapInt32(&d.notified, 0, 1) {
		log.Info("Block synchronisation started")
	}

	// Reset the queue, peer set and wake channels to clean any internal leftover state
	d.queue.Reset(blockCacheMaxItems, blockCacheInitialItems)
	d.peers.Reset()

	for _, ch := range []chan bool{d.bodyWakeCh, d.receiptWakeCh} {
		select {
		case <-ch:
		default:
		}
	}
	for _, ch := range []chan dataPack{d.headerCh, d.bodyCh, d.receiptCh} {
		for empty := false; !empty; {
			select {
			case <-ch:
			default:
				empty = true
			}
		}
	}
	for empty := false; !empty; {
		select {
		case <-d.headerProcCh:
		default:
			empty = true
		}
	}
	// Create cancel channel for aborting mid-flight and mark the master peer
	d.cancelLock.Lock()
	d.cancelCh = make(chan struct{})
	d.cancelPeer = id
	d.cancelLock.Unlock()

	defer d.Cancel() // No matter what, we can't leave the cancel channel open

	// Retrieve the origin peer and initiate the downloading process
	p := d.peers.Peer(id)
	if p == nil {
		return errUnknownPeer
	}
	return d.syncWithPeer(p, hash, blockNumber, txPool)
}

// syncWithPeer starts a block synchronization based on the hash chain from the
// specified peer and head hash.s
func (d *Downloader) syncWithPeer(p *peerConnection, hash common.Hash, blockNumber uint64, txPool *core.TxPool) (err error) {
	if p.version < 64 {
		return fmt.Errorf("%w: advertized %d < required %d", errTooOld, p.version, 64)
	}

	log.Debug("Synchronising with the network", "peer", p.id, "eth", p.version, "head", hash, "blockNumber", blockNumber)
	defer func(start time.Time) {
		log.Debug("Synchronisation terminated", "elapsed", common.PrettyDuration(time.Since(start)))
	}(time.Now())

	// Look up the sync boundaries: the common ancestor and the target block
	height, err := d.fetchHeight(p)
	if err != nil {
		return err
	}

	origin, err := d.findAncestor(p, height)
	if err != nil {
		return err
	}

	syncStatsChainHeight := d.GetSyncStatsChainHeight()
	d.syncStatsLock.Lock()
	if syncStatsChainHeight <= origin || d.syncStatsChainOrigin > origin {
		d.syncStatsChainOrigin = origin
	}
	d.syncStatsLock.Unlock()
	d.SetSyncStatsChainHeight(height)

	// Ensure our origin point is below any fast sync pivot point
	pivot := uint64(0)

	d.committed = 1
	// Initiate the sync using a concurrent header and content retrieval algorithm
	d.queue.Prepare(origin + 1)
	if d.syncInitHook != nil {
		d.syncInitHook(origin, height)
	}

	fetchers := []func() error{
		func() error { return d.fetchHeaders(p, origin+1) }, // Headers are always retrieved
		func() error { return d.processHeaders(origin+1, pivot, blockNumber) },
	}

	hashStateStageProgress, err := stages.GetStageProgress(d.stateDB, stages.HashState) // because later stages can be disabled
	if err != nil {
		return err
	}
	finishAtBefore, err := stages.GetStageProgress(d.stateDB, stages.Finish)
	if err != nil {
		return err
	}

	canRunCycleInOneTransaction := height-origin < 1024 && height-hashStateStageProgress < 1024
	//syncCycleStart := time.Now()

	d.stagedSyncState, err = d.stagedSync.Prepare(
		d,
		d.chainConfig,
		d.engine,
		d.vmConfig,
		d.stateDB,
		nil,
		p.id,
		d.storageMode,
		d.tmpdir,
		d.batchSize,
		d.quitCh,
		fetchers,
		txPool,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// begin tx at stage right after head/body download Or at first unwind stage
	// it's temporary solution
	d.stagedSyncState.BeforeStageRun(stages.Senders, func(tx ethdb.RwTx) (ethdb.RwTx, error) {
		if !canRunCycleInOneTransaction {
			return tx, nil
		}

		log.Debug("Begin tx")
		return d.stateDB.RwKV().BeginRw(context.Background())
	})
	d.stagedSyncState.BeforeStageRun(stages.Finish, func(tx ethdb.RwTx) (ethdb.RwTx, error) {
		if !canRunCycleInOneTransaction {
			return tx, nil
		}

		commitStart := time.Now()
		if errTx := tx.Commit(); errTx != nil {
			return tx, errTx
		}
		log.Info("Commit cycle", "in", time.Since(commitStart))
		return nil, nil
	})
	d.stagedSyncState.OnBeforeUnwind(func(id stages.SyncStage, tx ethdb.RwTx) (ethdb.RwTx, error) {
		if !canRunCycleInOneTransaction {
			return tx, nil
		}
		if d.stagedSyncState.IsBefore(id, stages.Bodies) || d.stagedSyncState.IsAfter(id, stages.TxPool) {
			return tx, nil
		}
		if tx != nil {
			return tx, nil
		}
		log.Debug("Begin tx")
		return d.stateDB.RwKV().BeginRw(context.Background())
	})
	d.stagedSyncState.BeforeStageUnwind(stages.Bodies, func(tx ethdb.RwTx) (ethdb.RwTx, error) {
		if !canRunCycleInOneTransaction {
			return tx, nil
		}
		if tx == nil {
			return nil, nil
		}
		commitStart := time.Now()
		if errTx := tx.Commit(); errTx != nil {
			return nil, errTx
		}
		log.Info("Commit unwind cycle", "in", time.Since(commitStart))
		return nil, nil
	})

	v, err := d.stateDB.GetOne(dbutils.SyncStageUnwind, []byte(stages.Finish))
	if err != nil {
		return err
	}
	var unwindTo uint64
	if len(v) > 0 {
		unwindTo = binary.BigEndian.Uint64(v)
	}

	err = d.stagedSyncState.Run(d.stateDB, nil)
	if err != nil {
		return err
	}

	err = stagedsync.NotifyNewHeaders(context.Background(), finishAtBefore, unwindTo, d.stagedSync.Notifier, d.stateDB.RwKV())
	if err != nil {
		return err
	}

	return nil
}

// spawnSync runs d.process and all given fetcher functions to completion in
// separate goroutines, returning the first error that appears.
func (d *Downloader) spawnSync(fetchers []func() error) error {
	errc := make(chan error, len(fetchers))

	d.cancelWg.Add(len(fetchers))
	for _, fn := range fetchers {
		fn := fn
		go func() { defer d.cancelWg.Done(); errc <- fn() }()
	}

	// Wait for the first error, then terminate the others.
	var err error
	for i := 0; i < len(fetchers); i++ {
		if i == len(fetchers)-1 {
			// Close the queue when all fetchers have exited.
			// This will cause the block processor to end when
			// it has processed the queue.
			d.queue.Close()
		}
		if err = <-errc; err != nil && err != errCanceled {
			break
		}
	}

	d.queue.Close()
	d.Cancel()

	return err
}

// cancel aborts all of the operations and resets the queue. However, cancel does
// not wait for the running download goroutines to finish. This method should be
// used when cancelling the downloads from inside the downloader.
func (d *Downloader) cancel() {
	// Close the current cancel channel
	d.cancelLock.Lock()
	defer d.cancelLock.Unlock()
	common.SafeClose(d.cancelCh)
}

// Cancel aborts all of the operations and waits for all download goroutines to
// finish before returning.
func (d *Downloader) Cancel() {
	d.cancel()
	d.cancelWg.Wait()

	atomic.StoreUint64(&d.ancientLimit, 0)
	log.Debug("Reset ancient limit to zero")
}

// Terminate interrupts the downloader, canceling all pending operations.
// The downloader cannot be reused after calling Terminate.
func (d *Downloader) Terminate() {
	// Close the termination channel (make sure double close is allowed)
	d.quitLock.Lock()
	common.SafeClose(d.quitCh)

	d.quitLock.Unlock()

	// Cancel any pending download requests
	d.Cancel()
}

// fetchHeight retrieves the head header of the remote peer to aid in estimating
// the total time a pending synchronisation would take.
func (d *Downloader) fetchHeight(p *peerConnection) (uint64, error) {
	p.log.Debug("Retrieving remote chain height")

	_, headNumber := p.peer.Head()
	return headNumber, nil
}

// calculateRequestSpan calculates what headers to request from a peer when trying to determine the
// common ancestor.
// It returns parameters to be used for peer.RequestHeadersByNumber:
//  from - starting block number
//  count - number of headers to request
//  skip - number of headers to skip
// and also returns 'max', the last block which is expected to be returned by the remote peers,
// given the (from,count,skip)
func calculateRequestSpan(remoteHeight, localHeight uint64) (int64, int, int, uint64) {
	var (
		from     int
		count    int
		MaxCount = MaxHeaderFetch / 16
	)
	// requestHead is the highest block that we will ask for. If requestHead is not offset,
	// the highest block that we will get is 16 blocks back from head, which means we
	// will fetch 14 or 15 blocks unnecessarily in the case the height difference
	// between us and the peer is 1-2 blocks, which is most common
	requestHead := int(remoteHeight) - 1
	if requestHead < 0 {
		requestHead = 0
	}
	// requestBottom is the lowest block we want included in the query
	// Ideally, we want to include the one just below our own head
	requestBottom := int(localHeight - 1)
	if requestBottom < 0 {
		requestBottom = 0
	}
	totalSpan := requestHead - requestBottom
	span := 1 + totalSpan/MaxCount
	if span < 2 {
		span = 2
	}
	if span > 16 {
		span = 16
	}

	count = 1 + totalSpan/span
	if count > MaxCount {
		count = MaxCount
	}
	if count < 2 {
		count = 2
	}
	from = requestHead - (count-1)*span
	if from < 0 {
		from = 0
	}
	max := from + (count-1)*span
	return int64(from), count, span - 1, uint64(max)
}

// findAncestor tries to locate the common ancestor link of the local chain and
// a remote peers blockchain. In the general case when our node was in sync and
// on the correct chain, checking the top N links should already get us a match.
// In the rare scenario when we ended up on a long reorganisation (i.e. none of
// the head links match), we do a binary search to find the common ancestor.
func (d *Downloader) findAncestor(p *peerConnection, remoteHeight uint64) (uint64, error) {
	// Figure out the valid ancestor range to prevent rewrite attacks
	floor := int64(-1)

	localHeight := *rawdb.ReadHeaderNumber(d.stateDB, rawdb.ReadHeadHeaderHash(d.stateDB))

	p.log.Debug("Looking for common ancestor", "local", localHeight, "remote", remoteHeight)

	ancestor, err := d.findAncestorSpanSearch(p, remoteHeight, localHeight, floor)
	if err == nil {
		return ancestor, nil
	}
	// The returned error was not nil.
	// If the error returned does not reflect that a common ancestor was not found, return it.
	// If the error reflects that a common ancestor was not found, continue to binary search,
	// where the error value will be reassigned.
	if !errors.Is(err, errNoAncestorFound) {
		return 0, err
	}

	ancestor, err = d.findAncestorBinarySearch(p, remoteHeight, floor)
	if err != nil {
		return 0, err
	}
	return ancestor, nil
}

func (d *Downloader) findAncestorSpanSearch(p *peerConnection, remoteHeight, localHeight uint64, floor int64) (commonAncestor uint64, err error) {
	from, count, skip, max := calculateRequestSpan(remoteHeight, localHeight)

	p.log.Trace("Span searching for common ancestor", "count", count, "from", from, "skip", skip)
	go func() { _ = p.peer.RequestHeadersByNumber(uint64(from), count, skip, false) }()

	// Wait for the remote response to the head fetch
	number, hash := uint64(0), common.Hash{}

	ttl := d.requestTTL()
	timeout := time.After(ttl)

	for finished := false; !finished; {
		select {
		case <-d.cancelCh:
			return 0, errCanceled

		case packet := <-d.headerCh:
			// Discard anything not from the origin peer
			if packet.PeerId() != p.id {
				log.Debug("Received headers from incorrect peer", "peer", packet.PeerId())
				break
			}
			// Make sure the peer actually gave something valid
			headers := packet.(*headerPack).headers
			if len(headers) == 0 {
				p.log.Warn("Empty head header set")
				return 0, errEmptyHeaderSet
			}
			// Make sure the peer's reply conforms to the request
			for i, header := range headers {
				expectNumber := from + int64(i)*int64(skip+1)
				if number := header.Number.Int64(); number != expectNumber {
					p.log.Warn("Head headers broke chain ordering", "index", i, "requested", expectNumber, "received", number)
					return 0, fmt.Errorf("%w: %v", errInvalidChain, errors.New("head headers broke chain ordering"))
				}
			}
			// Check if a common ancestor was found
			finished = true
			for i := len(headers) - 1; i >= 0; i-- {
				// Skip any headers that underflow/overflow our requested set
				if headers[i].Number.Int64() < from || headers[i].Number.Uint64() > max {
					continue
				}
				// Otherwise check if we already know the header or not
				h := headers[i].Hash()
				n := headers[i].Number.Uint64()

				if rawdb.HasHeader(d.stateDB, h, n) {
					number, hash = n, h
					break
				}
			}

		case <-timeout:
			p.log.Debug("Waiting for head header timed out", "elapsed", ttl)
			return 0, errTimeout

		case <-d.bodyCh:
		case <-d.receiptCh:
			// Out of bounds delivery, ignore
		}
	}
	// If the head fetch already found an ancestor, return
	if hash != (common.Hash{}) {
		if int64(number) <= floor {
			p.log.Warn("Ancestor below allowance", "number", number, "hash", hash, "allowance", floor)
			return 0, errInvalidAncestor
		}
		p.log.Debug("Found common ancestor", "number", number, "hash", hash)
		return number, nil
	}
	return 0, errNoAncestorFound
}

func (d *Downloader) findAncestorBinarySearch(p *peerConnection, remoteHeight uint64, floor int64) (commonAncestor uint64, err error) {
	hash := common.Hash{}

	// Ancestor not found, we need to binary search over our chain
	start, end := uint64(0), remoteHeight
	if floor > 0 {
		start = uint64(floor)
	}
	p.log.Trace("Binary searching for common ancestor", "start", start, "end", end)

	for start+1 < end {
		// Split our chain interval in two, and request the hash to cross check
		check := (start + end) / 2

		ttl := d.requestTTL()
		timeout := time.After(ttl)

		go func() { _ = p.peer.RequestHeadersByNumber(check, 1, 0, false) }()

		// Wait until a reply arrives to this request
		for arrived := false; !arrived; {
			select {
			case <-d.cancelCh:
				return 0, errCanceled

			case packet := <-d.headerCh:
				// Discard anything not from the origin peer
				if packet.PeerId() != p.id {
					log.Debug("Received headers from incorrect peer", "peer", packet.PeerId())
					break
				}
				// Make sure the peer actually gave something valid
				headers := packet.(*headerPack).headers
				if len(headers) != 1 {
					p.log.Warn("Multiple headers for single request", "headers", len(headers))
					return 0, fmt.Errorf("%w: multiple headers (%d) for single request", errBadPeer, len(headers))
				}
				arrived = true

				// Modify the search interval based on the response
				h := headers[0].Hash()
				n := headers[0].Number.Uint64()

				if !rawdb.HasHeader(d.stateDB, h, n) {
					end = check
					break
				}
				// Independent of sync mode, header surely exists
				header := rawdb.ReadHeader(d.stateDB, h, n)
				if header.Number.Uint64() != check {
					p.log.Warn("Received non requested header", "number", header.Number, "hash", header.Hash(), "request", check)
					return 0, fmt.Errorf("%w: non-requested header (%d)", errBadPeer, header.Number)
				}
				start = check
				hash = h

			case <-timeout:
				p.log.Debug("Waiting for search header timed out", "elapsed", ttl)
				return 0, errTimeout

			case <-d.bodyCh:
			case <-d.receiptCh:
				// Out of bounds delivery, ignore
			}
		}
	}
	// Ensure valid ancestry and return
	if int64(start) <= floor {
		p.log.Warn("Ancestor below allowance", "number", start, "hash", hash, "allowance", floor)
		return 0, errInvalidAncestor
	}
	p.log.Debug("Found common ancestor", "number", start, "hash", hash)
	return start, nil
}

// fetchHeaders keeps retrieving headers concurrently from the number
// requested, until no more are returned, potentially throttling on the way. To
// facilitate concurrency but still protect against malicious nodes sending bad
// headers, we construct a header chain skeleton using the "origin" peer we are
// syncing with, and fill in the missing headers using anyone else. Headers from
// other peers are only accepted if they map cleanly to the skeleton. If no one
// can fill in the skeleton - not even the origin peer - it's assumed invalid and
// the origin is dropped.
func (d *Downloader) fetchHeaders(p *peerConnection, from uint64) error {
	p.log.Debug("Directing header downloads", "origin", from)
	defer p.log.Debug("Header download terminated")

	// Create a timeout timer, and the associated header fetcher
	skeleton := true            // Skeleton assembly phase or finishing up
	pivoting := false           // Whether the next request is pivot verification
	request := time.Now()       // time of the last skeleton fetch request
	timeout := time.NewTimer(0) // timer to dump a non-responsive active peer
	<-timeout.C                 // timeout channel should be initially empty
	defer timeout.Stop()

	var ttl time.Duration
	getHeaders := func(from uint64) {
		request = time.Now()

		ttl = d.requestTTL()
		timeout.Reset(ttl)

		if skeleton {
			p.log.Trace("Fetching skeleton headers", "count", MaxHeaderFetch, "from", from)
			go func() {
				_ = p.peer.RequestHeadersByNumber(from+uint64(MaxHeaderFetch)-1, MaxSkeletonSize, MaxHeaderFetch-1, false)
			}()
		} else {
			p.log.Trace("Fetching full headers", "count", MaxHeaderFetch, "from", from)
			go func() { _ = p.peer.RequestHeadersByNumber(from, MaxHeaderFetch, 0, false) }()
		}
	}
	getNextPivot := func() {
		pivoting = true
		request = time.Now()

		ttl = d.requestTTL()
		timeout.Reset(ttl)

		d.pivotLock.RLock()
		pivot := d.pivotHeader.Number.Uint64()
		d.pivotLock.RUnlock()

		p.log.Trace("Fetching next pivot header", "number", pivot+uint64(fsMinFullBlocks))
		//move +64 when it's 2x64-8 deep
		go p.peer.RequestHeadersByNumber(pivot+uint64(fsMinFullBlocks), 2, fsMinFullBlocks-9, false) //nolint:errcheck
	}
	// Start pulling the header chain skeleton until all is done
	ancestor := from
	getHeaders(from)

	for {
		select {
		case <-d.cancelCh:
			return errCanceled

		case packet := <-d.headerCh:
			// Make sure the active peer is giving us the skeleton headers
			if packet.PeerId() != p.id {
				log.Debug("Received skeleton from incorrect peer", "peer", packet.PeerId())
				break
			}
			headerReqTimer.UpdateSince(request)
			timeout.Stop()

			// If the pivot is being checked, move if it became stale and run the real retrieval
			var pivot uint64

			d.pivotLock.RLock()
			if d.pivotHeader != nil {
				pivot = d.pivotHeader.Number.Uint64()
			}
			d.pivotLock.RUnlock()

			if pivoting {
				if packet.Items() == 2 {
					// Retrieve the headers and do some sanity checks, just in case
					headers := packet.(*headerPack).headers

					if have, want := headers[0].Number.Uint64(), pivot+uint64(fsMinFullBlocks); have != want {
						log.Warn("Peer sent invalid next pivot", "have", have, "want", want)
						return fmt.Errorf("%w: next pivot number %d != requested %d", errInvalidChain, have, want)
					}
					if have, want := headers[1].Number.Uint64(), pivot+2*uint64(fsMinFullBlocks)-8; have != want {
						log.Warn("Peer sent invalid pivot confirmer", "have", have, "want", want)
						return fmt.Errorf("%w: next pivot confirmer number %d != requested %d", errInvalidChain, have, want)
					}
					log.Warn("Pivot seemingly stale, moving", "old", pivot, "new", headers[0].Number)

					d.pivotLock.Lock()
					d.pivotHeader = headers[0]
					d.pivotLock.Unlock()

					// turbo-geth: this code is commented out because turbo-geth does not support
					// fast sync
					//
					// Write out the pivot into the database so a rollback beyond
					// it will reenable fast sync and update the state root that
					// the state syncer will be downloading.
					// rawdb.WriteLastPivotNumber(d.stateDB, pivot)
				}
				pivoting = false
				getHeaders(from)
				continue
			}
			// If the skeleton's finished, pull any remaining head headers directly from the origin
			if skeleton && packet.Items() == 0 {
				skeleton = false
				getHeaders(from)
				continue
			}
			// If no more headers are inbound, notify the content fetchers and return
			if packet.Items() == 0 {
				// Don't abort header fetches while the pivot is downloading
				if atomic.LoadInt32(&d.committed) == 0 && pivot <= from {
					p.log.Debug("No headers, waiting for pivot commit")
					select {
					case <-time.After(fsHeaderContCheck):
						getHeaders(from)
						continue
					case <-d.cancelCh:
						return errCanceled
					}
				}
				// Pivot done (or not in fast sync) and no more headers, terminate the process
				p.log.Debug("No more headers available")
				select {
				case d.headerProcCh <- nil:
					return nil
				case <-d.cancelCh:
					return errCanceled
				}
			}
			headers := packet.(*headerPack).headers

			// If we received a skeleton batch, resolve internals concurrently
			if skeleton {
				filled, proced, err := d.fillHeaderSkeleton(from, headers)
				if err != nil {
					p.log.Debug("Skeleton chain invalid", "err", err)
					return fmt.Errorf("fillHeaderSkeleton failed %w: %v", errInvalidChain, err)
				}
				headers = filled[proced:]
				from += uint64(proced)
			} else {
				// If we're closing in on the chain head, but haven't yet reached it, delay
				// the last few headers so mini reorgs on the head don't cause invalid hash
				// chain errors.
				if n := len(headers); n > 0 {
					// Retrieve the current head we're at

					headHash := rawdb.ReadHeadHeaderHash(d.stateDB)
					head := *rawdb.ReadHeaderNumber(d.stateDB, headHash)

					// If the head is below the common ancestor, we're actually deduplicating
					// already existing chain segments, so use the ancestor as the fake head.
					// Otherwise we might end up delaying header deliveries pointlessly.
					if head < ancestor {
						head = ancestor
					}
					// If the head is way older than this batch, delay the last few headers
					if head+uint64(reorgProtThreshold) < headers[n-1].Number.Uint64() {
						delay := reorgProtHeaderDelay
						if delay > n {
							delay = n
						}
						headers = headers[:n-delay]
					}
				}
			}
			// Insert all the new headers and fetch the next batch
			if len(headers) > 0 {
				p.log.Trace("Scheduling new headers", "count", len(headers), "from", from)
				select {
				case d.headerProcCh <- headers:
				case <-d.cancelCh:
					return errCanceled
				}
				from += uint64(len(headers))

				// If we're still skeleton filling fast sync, check pivot staleness
				// before continuing to the next skeleton filling
				if skeleton && pivot > 0 {
					getNextPivot()
				} else {
					getHeaders(from)
				}
			} else {
				// No headers delivered, or all of them being delayed, sleep a bit and retry
				p.log.Trace("All headers delayed, waiting")
				select {
				case <-time.After(fsHeaderContCheck):
					getHeaders(from)
					continue
				case <-d.cancelCh:
					return errCanceled
				}
			}

		case <-timeout.C:
			if d.dropPeer == nil {
				// The dropPeer method is nil when `--copydb` is used for a local copy.
				// Timeouts can occur if e.g. compaction hits at the wrong time, and can be ignored
				p.log.Warn("Downloader wants to drop peer, but peerdrop-function is not set", "peer", p.id)
				break
			}
			// Header retrieval timed out, consider the peer bad and drop
			p.log.Debug("Header request timed out", "elapsed", ttl)
			headerTimeoutMeter.Mark(1)
			d.dropPeer(p.id)

			select {
			case d.headerProcCh <- nil:
			case <-d.cancelCh:
			}
			return fmt.Errorf("%w: header request timed out", errBadPeer)
		}
	}
}

// fillHeaderSkeleton concurrently retrieves headers from all our available peers
// and maps them to the provided skeleton header chain.
//
// Any partial results from the beginning of the skeleton is (if possible) forwarded
// immediately to the header processor to keep the rest of the pipeline full even
// in the case of header stalls.
//
// The method returns the entire filled skeleton and also the number of headers
// already forwarded for processing.
func (d *Downloader) fillHeaderSkeleton(from uint64, skeleton []*types.Header) ([]*types.Header, int, error) {
	log.Debug("Filling up skeleton", "from", from)
	d.queue.ScheduleSkeleton(from, skeleton)

	var (
		deliver = func(packet dataPack) (int, error) {
			pack := packet.(*headerPack)
			return d.queue.DeliverHeaders(pack.peerID, pack.headers, d.headerProcCh)
		}
		expire  = func() map[string]int { return d.queue.ExpireHeaders(d.requestTTL()) }
		reserve = func(p *peerConnection, count int) (*fetchRequest, bool, bool) {
			return d.queue.ReserveHeaders(p, count), false, false
		}
		fetch    = func(p *peerConnection, req *fetchRequest) error { return p.FetchHeaders(req.From, MaxHeaderFetch) }
		capacity = func(p *peerConnection) int { return p.HeaderCapacity(d.requestRTT()) }
		setIdle  = func(p *peerConnection, accepted int, deliveryTime time.Time) {
			p.SetHeadersIdle(accepted, deliveryTime)
		}
	)
	err := d.fetchParts(d.headerCh, deliver, d.queue.headerContCh, expire,
		d.queue.PendingHeaders, d.queue.InFlightHeaders, reserve,
		nil, fetch, d.queue.CancelHeaders, capacity, d.peers.HeaderIdlePeers, setIdle, "headers")

	log.Debug("Skeleton fill terminated", "err", err)

	filled, proced := d.queue.RetrieveHeaders()
	return filled, proced, err
}

// fetchBodies iteratively downloads the scheduled block bodies, taking any
// available peers, reserving a chunk of blocks for each, waiting for delivery
// and also periodically checking for timeouts.
func (d *Downloader) fetchBodies(from uint64) error {
	log.Debug("Downloading block bodies", "origin", from)

	var (
		deliver = func(packet dataPack) (int, error) {
			pack := packet.(*bodyPack)
			return d.queue.DeliverBodies(pack.peerID, pack.transactions, pack.uncles)
		}
		expire   = func() map[string]int { return d.queue.ExpireBodies(d.requestTTL()) }
		fetch    = func(p *peerConnection, req *fetchRequest) error { return p.FetchBodies(req) }
		capacity = func(p *peerConnection) int { return p.BlockCapacity(d.requestRTT()) }
		setIdle  = func(p *peerConnection, accepted int, deliveryTime time.Time) { p.SetBodiesIdle(accepted, deliveryTime) }
	)
	err := d.fetchParts(d.bodyCh, deliver, d.bodyWakeCh, expire,
		d.queue.PendingBlocks, d.queue.InFlightBlocks, d.queue.ReserveBodies,
		d.bodyFetchHook, fetch, d.queue.CancelBodies, capacity, d.peers.BodyIdlePeers, setIdle, "bodies")

	log.Debug("Block body download terminated", "err", err)
	return err
}

// fetchParts iteratively downloads scheduled block parts, taking any available
// peers, reserving a chunk of fetch requests for each, waiting for delivery and
// also periodically checking for timeouts.
//
// As the scheduling/timeout logic mostly is the same for all downloaded data
// types, this method is used by each for data gathering and is instrumented with
// various callbacks to handle the slight differences between processing them.
//
// The instrumentation parameters:
//  - errCancel:   error type to return if the fetch operation is cancelled (mostly makes logging nicer)
//  - deliveryCh:  channel from which to retrieve downloaded data packets (merged from all concurrent peers)
//  - deliver:     processing callback to deliver data packets into type specific download queues (usually within `queue`)
//  - wakeCh:      notification channel for waking the fetcher when new tasks are available (or sync completed)
//  - expire:      task callback method to abort requests that took too long and return the faulty peers (traffic shaping)
//  - pending:     task callback for the number of requests still needing download (detect completion/non-completability)
//  - inFlight:    task callback for the number of in-progress requests (wait for all active downloads to finish)
//  - throttle:    task callback to check if the processing queue is full and activate throttling (bound memory use)
//  - reserve:     task callback to reserve new download tasks to a particular peer (also signals partial completions)
//  - fetchHook:   tester callback to notify of new tasks being initiated (allows testing the scheduling logic)
//  - fetch:       network callback to actually send a particular download request to a physical remote peer
//  - cancel:      task callback to abort an in-flight download request and allow rescheduling it (in case of lost peer)
//  - capacity:    network callback to retrieve the estimated type-specific bandwidth capacity of a peer (traffic shaping)
//  - idle:        network callback to retrieve the currently (type specific) idle peers that can be assigned tasks
//  - setIdle:     network callback to set a peer back to idle and update its estimated capacity (traffic shaping)
//  - kind:        textual label of the type being downloaded to display in log messages
func (d *Downloader) fetchParts(deliveryCh chan dataPack, deliver func(dataPack) (int, error), wakeCh chan bool,
	expire func() map[string]int, pending func() int, inFlight func() bool, reserve func(*peerConnection, int) (*fetchRequest, bool, bool),
	fetchHook func([]*types.Header), fetch func(*peerConnection, *fetchRequest) error, cancel func(*fetchRequest), capacity func(*peerConnection) int,
	idle func() ([]*peerConnection, int), setIdle func(*peerConnection, int, time.Time), kind string) error {

	// Create a ticker to detect expired retrieval tasks
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	update := make(chan struct{}, 1)

	// Prepare the queue and fetch block parts until the block header fetcher's done
	finished := false
	for {
		select {
		case <-d.cancelCh:
			return errCanceled

		case packet := <-deliveryCh:
			deliveryTime := time.Now()
			// If the peer was previously banned and failed to deliver its pack
			// in a reasonable time frame, ignore its message.
			if peer := d.peers.Peer(packet.PeerId()); peer != nil {
				// Deliver the received chunk of data and check chain validity
				accepted, err := deliver(packet)
				if errors.Is(err, errInvalidChain) {
					return err
				}
				// Unless a peer delivered something completely else than requested (usually
				// caused by a timed out request which came through in the end), set it to
				// idle. If the delivery's stale, the peer should have already been idled.
				if !errors.Is(err, errStaleDelivery) {
					setIdle(peer, accepted, deliveryTime)
				}
				// Issue a log to the user to see what's going on
				switch {
				case err == nil && packet.Items() == 0:
					peer.log.Trace("Requested data not delivered", "type", kind)
				case err == nil:
					peer.log.Trace("Delivered new batch of data", "type", kind, "count", packet.Stats())
				default:
					peer.log.Debug("Failed to deliver retrieved data", "type", kind, "err", err)
				}
			}
			// Blocks assembled, try to update the progress
			select {
			case update <- struct{}{}:
			default:
			}

		case cont := <-wakeCh:
			// The header fetcher sent a continuation flag, check if it's done
			if !cont {
				finished = true
			}
			// Headers arrive, try to update the progress
			select {
			case update <- struct{}{}:
			default:
			}

		case <-ticker.C:
			// Sanity check update the progress
			select {
			case update <- struct{}{}:
			default:
			}

		case <-update:
			// Short circuit if we lost all our peers
			if d.peers.Len() == 0 {
				return errNoPeers
			}
			// Check for fetch request timeouts and demote the responsible peers
			for pid, fails := range expire() {
				if peer := d.peers.Peer(pid); peer != nil {
					// If a lot of retrieval elements expired, we might have overestimated the remote peer or perhaps
					// ourselves. Only reset to minimal throughput but don't drop just yet. If even the minimal times
					// out that sync wise we need to get rid of the peer.
					//
					// The reason the minimum threshold is 2 is because the downloader tries to estimate the bandwidth
					// and latency of a peer separately, which requires pushing the measures capacity a bit and seeing
					// how response times reacts, to it always requests one more than the minimum (i.e. min 2).
					if fails > 2 {
						peer.log.Trace("Data delivery timed out", "type", kind)
						setIdle(peer, 0, time.Now())
					} else {
						peer.log.Debug("Stalling delivery, dropping", "type", kind)

						if d.dropPeer == nil {
							// The dropPeer method is nil when `--copydb` is used for a local copy.
							// Timeouts can occur if e.g. compaction hits at the wrong time, and can be ignored
							peer.log.Warn("Downloader wants to drop peer, but peerdrop-function is not set", "peer", pid)
						} else {
							d.dropPeer(pid)

							// If this peer was the master peer, abort sync immediately
							d.cancelLock.RLock()
							master := pid == d.cancelPeer
							d.cancelLock.RUnlock()

							if master {
								d.cancel()
								return errTimeout
							}
						}
					}
				}
			}
			// If there's nothing more to fetch, wait or terminate
			if pending() == 0 {
				if !inFlight() && finished {
					log.Debug("Data fetching completed", "type", kind)
					return nil
				}
				break
			}
			// Send a download request to all idle peers, until throttled
			progressed, throttled, running := false, false, inFlight()
			idles, total := idle()
			pendCount := pending()
			for _, peer := range idles {
				// Short circuit if throttling activated
				if throttled {
					break
				}
				// Short circuit if there is no more available task.
				if pendCount = pending(); pendCount == 0 {
					break
				}
				// Reserve a chunk of fetches for a peer. A nil can mean either that
				// no more headers are available, or that the peer is known not to
				// have them.
				request, progress, throttle := reserve(peer, capacity(peer))
				if progress {
					progressed = true
				}
				if throttle {
					throttled = true
					throttleCounter.Inc(1)
				}
				if request == nil {
					continue
				}
				if request.From > 0 {
					peer.log.Trace("Requesting new batch of data", "type", kind, "from", request.From)
				} else {
					peer.log.Trace("Requesting new batch of data", "type", kind, "count", len(request.Headers), "from", request.Headers[0].Number)
				}
				// Fetch the chunk and make sure any errors return the hashes to the queue
				if fetchHook != nil {
					fetchHook(request.Headers)
				}
				if err := fetch(peer, request); err != nil {
					// Although we could try and make an attempt to fix this, this error really
					// means that we've double allocated a fetch task to a peer. If that is the
					// case, the internal state of the downloader and the queue is very wrong so
					// better hard crash and note the error instead of silently accumulating into
					// a much bigger issue.
					panic(fmt.Sprintf("%v: %s fetch assignment failed", peer, kind))
				}
				running = true
			}
			// Make sure that we have peers available for fetching. If all peers have been tried
			// and all failed throw an error
			if !progressed && !throttled && !running && len(idles) == total && pendCount > 0 {
				return errPeersUnavailable
			}
		}
	}
}

// processHeaders takes batches of retrieved headers from an input channel and
// keeps processing and scheduling them into the header chain and downloader's
// queue until the stream ends or a failure occurs.
func (d *Downloader) processHeaders(origin uint64, pivot uint64, blockNumber uint64) error {
	log.Debug("processHeaders", "origin", origin, "bn", blockNumber)
	// Keep a count of uncertain headers to roll back
	var (
		rollback uint64 // Zero means no rollback (fine as you can't unroll the genesis)
	)

	for {
		select {
		case <-d.cancelCh:
			return errCanceled

		case headers := <-d.headerProcCh:
			// Terminate header processing if we synced up
			if len(headers) == 0 {
				return nil
			}
			for len(headers) > 0 {
				// Terminate if something failed in between processing chunks
				if err := common.Stopped(d.quitCh); err != nil {
					return err
				}
				// Select the next chunk of headers to import
				limit := maxHeadersProcess
				if limit > len(headers) {
					limit = len(headers)
				}
				chunk := headers[:limit]

				// If we're importing pure headers, verify based on their recentness
				var pivot uint64

				d.pivotLock.RLock()
				if d.pivotHeader != nil {
					pivot = d.pivotHeader.Number.Uint64()
				}
				d.pivotLock.RUnlock()

				frequency := fsHeaderCheckFrequency
				if chunk[len(chunk)-1].Number.Uint64()+uint64(fsHeaderForceVerify) > pivot {
					frequency = 1
				}
				var n int
				var err error
				var newCanonical bool

				verifyStart := time.Now()
				if err = stagedsync.VerifyHeaders(d.stateDB, chunk, d.chainConfig, d.engine, frequency); err != nil {
					log.Warn("Invalid header encountered", "number", chunk[n].Number, "hash", chunk[n].Hash(), "parent", chunk[n].ParentHash, "err", err)
					return fmt.Errorf("%w: %v", errInvalidChain, err)
				}
				verifyDuration := time.Since(verifyStart)
				var reorg bool
				var forkBlockNumber uint64
				logPrefix := d.stagedSyncState.LogPrefix()
				newCanonical, reorg, forkBlockNumber, err = stagedsync.InsertHeaderChain(logPrefix, d.stateDB, chunk, verifyDuration)
				if reorg && d.headersUnwinder != nil {
					// Need to unwind further stages
					if err1 := d.headersUnwinder.UnwindTo(forkBlockNumber, d.stateDB); err1 != nil {
						return fmt.Errorf("%s: unwinding all stages to %d: %v", logPrefix, forkBlockNumber, err1)
					}
				}

				if err == nil && newCanonical && d.headersState != nil {
					if err1 := d.headersState.Update(d.stateDB, chunk[len(chunk)-1].Number.Uint64()); err1 != nil {
						return fmt.Errorf("saving SyncStage Headers progress: %v", err1)
					}
				}
				if err != nil {
					log.Warn("Invalid header encountered", "number", chunk[n].Number, "hash", chunk[n].Hash(), "parent", chunk[n].ParentHash, "err", err)
					return fmt.Errorf("%w: %v", errInvalidChain, err)
				}

				head := chunk[len(chunk)-1].Number.Uint64()
				if head-rollback > uint64(fsHeaderSafetyNet) {
					rollback = head - uint64(fsHeaderSafetyNet)
				} else {
					rollback = 1
				}

				headers = headers[limit:]
				origin += uint64(limit)
			}
			// Update the highest block number we know if a higher one is found.
			d.setGreaterSyncStatsChainHeight(origin-1, origin)
		}
	}
}

func (d *Downloader) importBlockResults(logPrefix string, results []*fetchResult) (uint64, error) {
	// Check for any early termination requests
	if len(results) == 0 {
		return 0, nil
	}
	if err := common.Stopped(d.quitCh); err != nil {
		return 0, errCancelContentProcessing
	}
	// Retrieve the a batch of results to import
	first, last := results[0].Header, results[len(results)-1].Header
	log.Debug("Inserting downloaded chain", "items", len(results),
		"firstnum", first.Number, "firsthash", first.Hash(),
		"lastnum", last.Number, "lasthash", last.Hash(),
	)
	blocks := make([]*types.Block, len(results))
	for i, result := range results {
		blocks[i] = types.NewBlockWithHeader(result.Header).WithBody(result.Transactions, result.Uncles)
	}
	tx, err2 := d.stateDB.Begin(context.Background(), ethdb.RW)
	if err2 != nil {
		return 0, err2
	}
	defer tx.Rollback()

	var index int
	var stopped bool
	var err error
	stopped, err = core.InsertBodyChain(logPrefix, context.Background(), tx, blocks, true /* newCanonical */)
	if stopped {
		index = 0
	} else {
		index = len(results)
	}
	if err == nil {
		if err1 := tx.Commit(); err1 != nil {
			return 0, err1
		}
	} else {
		tx.Rollback()
	}
	if err != nil {
		if index < len(results) {
			log.Debug("Downloaded item processing failed", "number", results[index].Header.Number, "hash", results[index].Header.Hash(), "err", err)
		} else {
			// The InsertChain method in blockchain.go will sometimes return an out-of-bounds index,
			// when it needs to preprocess blocks to import a sidechain.
			// The importer will put together a new list of blocks to import, which is a superset
			// of the blocks delivered from the downloader, and the indexing will be off.
			log.Debug("Downloaded item processing failed on sidechain import", "index", index, "err", err)
		}
		return 0, fmt.Errorf("importBlockResults failed %w: %v", errInvalidChain, err)
	}
	if index > 0 && d.bodiesState != nil {
		if err1 := d.bodiesState.Update(d.stateDB, blocks[index-1].NumberU64()); err1 != nil {
			return 0, fmt.Errorf("saving SyncStage Bodies progress: %v", err1)
		}

		return blocks[index-1].NumberU64() + 1, nil
	}
	return 0, nil
}

// DeliverHeaders injects a new batch of block headers received from a remote
// node into the download schedule.
func (d *Downloader) DeliverHeaders(id string, headers []*types.Header) error {
	return d.deliver(d.headerCh, &headerPack{id, headers})
}

// DeliverBodies injects a new batch of block bodies received from a remote node.
func (d *Downloader) DeliverBodies(id string, transactions [][]types.Transaction, uncles [][]*types.Header) error {
	return d.deliver(d.bodyCh, &bodyPack{id, transactions, uncles})
}

// DeliverReceipts injects a new batch of receipts received from a remote node.
func (d *Downloader) DeliverReceipts(id string, receipts [][]*types.Receipt) error {
	return d.deliver(d.receiptCh, &receiptPack{id, receipts})
}

// deliver injects a new batch of data received from a remote node.
func (d *Downloader) deliver(destCh chan dataPack, packet dataPack) (err error) {
	// Deliver or abort if the sync is canceled while queuing
	d.cancelLock.RLock()
	cancel := d.cancelCh
	d.cancelLock.RUnlock()
	if cancel == nil {
		return errNoSyncActive
	}
	select {
	case destCh <- packet:
		return nil
	case <-cancel:
		return errNoSyncActive
	}
}

// qosTuner is the quality of service tuning loop that occasionally gathers the
// peer latency statistics and updates the estimated request round trip time.
func (d *Downloader) qosTuner() {
	for {
		// Retrieve the current median RTT and integrate into the previoust target RTT
		rtt := time.Duration((1-qosTuningImpact)*float64(atomic.LoadUint64(&d.rttEstimate)) + qosTuningImpact*float64(d.peers.medianRTT()))
		atomic.StoreUint64(&d.rttEstimate, uint64(rtt))

		// A new RTT cycle passed, increase our confidence in the estimated RTT
		conf := atomic.LoadUint64(&d.rttConfidence)
		conf = conf + (1000000-conf)/2
		atomic.StoreUint64(&d.rttConfidence, conf)

		// Log the new QoS values and sleep until the next RTT
		log.Debug("Recalculated downloader QoS values", "rtt", rtt, "confidence", float64(conf)/1000000.0, "ttl", d.requestTTL())
		select {
		case <-d.quitCh:
			return
		case <-time.After(rtt):
		}
	}
}

// qosReduceConfidence is meant to be called when a new peer joins the downloader's
// peer set, needing to reduce the confidence we have in out QoS estimates.
func (d *Downloader) qosReduceConfidence() {
	// If we have a single peer, confidence is always 1
	peers := uint64(d.peers.Len())
	if peers == 0 {
		// Ensure peer connectivity races don't catch us off guard
		return
	}
	if peers == 1 {
		atomic.StoreUint64(&d.rttConfidence, 1000000)
		return
	}
	// If we have a ton of peers, don't drop confidence)
	if peers >= uint64(qosConfidenceCap) {
		return
	}
	// Otherwise drop the confidence factor
	conf := atomic.LoadUint64(&d.rttConfidence) * (peers - 1) / peers
	if float64(conf)/1000000 < rttMinConfidence {
		conf = uint64(rttMinConfidence * 1000000)
	}
	atomic.StoreUint64(&d.rttConfidence, conf)

	rtt := time.Duration(atomic.LoadUint64(&d.rttEstimate))
	log.Debug("Relaxed downloader QoS values", "rtt", rtt, "confidence", float64(conf)/1000000.0, "ttl", d.requestTTL())
}

// requestRTT returns the current target round trip time for a download request
// to complete in.
//
// Note, the returned RTT is .9 of the actually estimated RTT. The reason is that
// the downloader tries to adapt queries to the RTT, so multiple RTT values can
// be adapted to, but smaller ones are preferred (stabler download stream).
func (d *Downloader) requestRTT() time.Duration {
	return time.Duration(atomic.LoadUint64(&d.rttEstimate)) * 9 / 10
}

// requestTTL returns the current timeout allowance for a single download request
// to finish under.
func (d *Downloader) requestTTL() time.Duration {
	var (
		rtt  = time.Duration(atomic.LoadUint64(&d.rttEstimate))
		conf = float64(atomic.LoadUint64(&d.rttConfidence)) / 1000000.0
	)
	ttl := time.Duration(ttlScaling) * time.Duration(float64(rtt)/conf)
	if ttl > ttlLimit {
		ttl = ttlLimit
	}
	return ttl
}

func (d *Downloader) SetSyncStatsChainHeight(h uint64) {
	d.syncStatsLock.Lock()
	d.syncStatsChainHeight = h
	d.syncStatsLock.Unlock()
}

func (d *Downloader) setGreaterSyncStatsChainHeight(h, old uint64) {
	d.syncStatsLock.Lock()
	if d.syncStatsChainHeight < old {
		d.syncStatsChainHeight = h
	}
	d.syncStatsLock.Unlock()
}

func (d *Downloader) GetSyncStatsChainHeight() uint64 {
	d.syncStatsLock.RLock()
	h := d.syncStatsChainHeight
	d.syncStatsLock.RUnlock()
	return h
}
