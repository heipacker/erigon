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

package tests

import (
	"runtime"
	"testing"
)

func TestBlockchain(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please") // after remove ChainReader from consensus engine - this test can be changed to create less databases, then can enable on win. now timeout after 20min
	}

	bt := new(testMatcher)
	// General state tests are 'exported' as blockchain tests, but we can run them natively.
	// For speedier CI-runs, the line below can be uncommented, so those are skipped.
	// For now, in hardfork-times (Berlin), we run the tests both as StateTests and
	// as blockchain tests, since the latter also covers things like receipt root
	//bt.skipLoad(`^GeneralStateTests/`)

	// Skip random failures due to selfish mining test
	bt.skipLoad(`.*bcForgedTest/bcForkUncle\.json`)

	// Slow tests
	bt.slow(`.*bcExploitTest/DelegateCallSpam.json`)
	bt.slow(`.*bcExploitTest/ShanghaiLove.json`)
	bt.slow(`.*bcExploitTest/SuicideIssue.json`)
	bt.slow(`.*/bcForkStressTest/`)
	bt.slow(`.*/bcGasPricerTest/RPC_API_Test.json`)
	bt.slow(`.*/bcWalletTest/`)

	// Very slow test
	bt.skipLoad(`.*/stTimeConsuming/.*`)

	// test takes a lot for time and goes easily OOM because of sha3 calculation on a huge range,
	// using 4.6 TGas
	bt.skipLoad(`.*randomStatetest94.json.*`)

	bt.fails(`(?m)^TestBlockchain/InvalidBlocks/bcInvalidHeaderTest/wrongTransactionsTrie.json`, "Validation happens in the fetcher")
	bt.fails(`(?m)^TestBlockchain/InvalidBlocks/bcInvalidHeaderTest/wrongUncleHash.json`, "Validation happens in the fetcher")
	bt.fails(`(?m)^TestBlockchain/InvalidBlocks/bcInvalidHeaderTest/wrongReceiptTrie.json/wrongReceiptTrie_EIP150`, "No receipt validation before Byzantium")
	bt.fails(`(?m)^TestBlockchain/InvalidBlocks/bcInvalidHeaderTest/wrongReceiptTrie.json/wrongReceiptTrie_EIP158`, "No receipt validation before Byzantium")
	bt.fails(`(?m)^TestBlockchain/InvalidBlocks/bcInvalidHeaderTest/wrongReceiptTrie.json/wrongReceiptTrie_Frontier`, "No receipt validation before Byzantium")
	bt.fails(`(?m)^TestBlockchain/InvalidBlocks/bcInvalidHeaderTest/wrongReceiptTrie.json/wrongReceiptTrie_Homestead`, "No receipt validation before Byzantium")

	// FIXME: failing tests after Berlin rebase
	bt.fails(`(?m)^TestBlockchain/InvalidBlocks/bcUncleHeaderValidity/incorrectUncleTimestamp.json.*`, "Needs to be fixed for TG (Berlin)")
	bt.walk(t, blockTestDir, func(t *testing.T, name string, test *BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(false)); err != nil {
			t.Error(err)
		}
	})

	// There is also a LegacyTests folder, containing blockchain tests generated
	// prior to Istanbul. However, they are all derived from GeneralStateTests,
	// which run natively, so there's no reason to run them here.
}
