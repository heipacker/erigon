// Copyright 2019 The go-ethereum Authors
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

package trie

/*
import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"

	bloomfilter "github.com/holiman/bloomfilter/v2"
)

var (
	bloomAddMeter   = metrics.NewRegisteredMeter("trie/bloom/add", nil)
	bloomLoadMeter  = metrics.NewRegisteredMeter("trie/bloom/load", nil)
	bloomTestMeter  = metrics.NewRegisteredMeter("trie/bloom/test", nil)
	bloomMissMeter  = metrics.NewRegisteredMeter("trie/bloom/miss", nil)
	bloomFaultMeter = metrics.NewRegisteredMeter("trie/bloom/fault", nil)
	bloomErrorGauge = metrics.NewRegisteredGauge("trie/bloom/error", nil)
)

// SyncBloom is a bloom filter used during fast sync to quickly decide if a trie
// node or contract code already exists on disk or not. It self populates from the
// provided disk database on creation in a background thread and will only start
// returning live results once that's finished.
type SyncBloom struct {
	bloom  *bloomfilter.Filter
	inited uint32
	closer sync.Once
	closed uint32
	pend   sync.WaitGroup
}

// NewSyncBloom creates a new bloom filter of the given size (in megabytes) and
// initializes it from the database. The bloom is hard coded to use 3 filters.
func NewSyncBloom(memory uint64, database ethdb.Database) *SyncBloom {
	// Create the bloom filter to track known trie nodes
	bloom, err := bloomfilter.New(memory*1024*1024*8, 4)
	if err != nil {
		panic(fmt.Sprintf("failed to create bloom: %v", err))
	}
	log.Info("Allocated fast sync bloom", "size", common.StorageSize(memory*1024*1024))

	// Assemble the fast sync bloom and init it from previous sessions
	b := &SyncBloom{
		bloom: bloom,
	}
	b.pend.Add(2)
	go func() {
		defer b.pend.Done()
		b.init(database)
	}()
	go func() {
		defer b.pend.Done()
		b.meter()
	}()
	return b
}

// init iterates over the database, pushing every trie hash into the bloom filter.
func (b *SyncBloom) init(database ethdb.Database) {
	// Iterate over the database, but restart every now and again to avoid holding
	// a persistent snapshot since fast sync can push a ton of data concurrently,
	// bloating the disk.
	//
	// Note, this is fine, because everything inserted into leveldb by fast sync is
	// also pushed into the bloom directly, so we're not missing anything when the
	// iterator is swapped out for a new one.
	var (
		start = time.Now()
		//swap  = time.Now()
	)
	if atomic.LoadUint32(&b.closed) == 0 {
		_ = database.Walk(dbutils.CurrentStateBucket, []byte{}, 0, func(key, val []byte) (bool, error) {
			// If the database entry is a trie node, add it to the bloom
			if len(key) == common.HashLength {
				b.bloom.Add(syncBloomHasher(common.CopyBytes(key)))
				bloomLoadMeter.Mark(1)
			}
			return true, nil
			// FIXME: restore or remove in Turbo-Geth
			// If enough time elapsed since the last iterator swap, restart
			//if time.Since(swap) > 8*time.Second {
			//	key := common.CopyBytes(it.Key())
			//
			//	it.Release()
			//	it = database.NewIteratorWithStart(key)
			//
			//	log.Info("Initializing fast sync bloom", "items", b.bloom.N(), "errorrate", b.errorRate(), "elapsed", common.PrettyDuration(time.Since(start)))
			//	swap = time.Now()
			//}
		})
	}

	// Mark the bloom filter inited and return
	log.Info("Initialized state bloom", "items", b.bloom.N(), "errorrate", b.bloom.FalsePosititveProbability(), "elapsed", common.PrettyDuration(time.Since(start)))
	atomic.StoreUint32(&b.inited, 1)
}

// meter periodically recalculates the false positive error rate of the bloom
// filter and reports it in a metric.
func (b *SyncBloom) meter() {
	for {
		// Report the current error ration. No floats, lame, scale it up.
		bloomErrorGauge.Update(int64(b.bloom.FalsePosititveProbability() * 100000))

		// Wait one second, but check termination more frequently
		for i := 0; i < 10; i++ {
			if atomic.LoadUint32(&b.closed) == 1 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Close terminates any background initializer still running and releases all the
// memory allocated for the bloom.
func (b *SyncBloom) Close() error {
	b.closer.Do(func() {
		// Ensure the initializer is stopped
		atomic.StoreUint32(&b.closed, 1)
		b.pend.Wait()

		// Wipe the bloom, but mark it "uninited" just in case someone attempts an access
		log.Info("Deallocated state bloom", "items", b.bloom.N(), "errorrate", b.bloom.FalsePosititveProbability())

		atomic.StoreUint32(&b.inited, 0)
		b.bloom = nil
	})
	return nil
}

// Add inserts a new trie node hash into the bloom filter.
func (b *SyncBloom) Add(hash []byte) {
	if atomic.LoadUint32(&b.closed) == 1 {
		return
	}
	b.bloom.AddHash(binary.BigEndian.Uint64(hash))
	bloomAddMeter.Mark(1)
}

// Contains tests if the bloom filter contains the given hash:
//   - false: the bloom definitely does not contain hash
//   - true:  the bloom maybe contains hash
//
// While the bloom is being initialized, any query will return true.
func (b *SyncBloom) Contains(hash []byte) bool {
	bloomTestMeter.Mark(1)
	if atomic.LoadUint32(&b.inited) == 0 {
		// We didn't load all the trie nodes from the previous run of Geth yet. As
		// such, we can't say for sure if a hash is not present for anything. Until
		// the init is done, we're faking "possible presence" for everything.
		return true
	}
	// Bloom initialized, check the real one and report any successful misses
	maybe := b.bloom.ContainsHash(binary.BigEndian.Uint64(hash))
	if !maybe {
		bloomMissMeter.Mark(1)
	}
	return maybe
}

// errorRate calculates the probability of a random containment test returning a
// false positive.
//
// We're calculating it ourselves because the bloom library we used missed a
// parentheses in the formula and calculates it wrong. And it's discontinued...
func (b *SyncBloom) errorRate() float64 {
	k := float64(b.bloom.K())
	n := float64(b.bloom.N())
	m := float64(b.bloom.M())

	return math.Pow(1.0-math.Exp((-k)*(n+0.5)/(m-1)), k)
}

*/
