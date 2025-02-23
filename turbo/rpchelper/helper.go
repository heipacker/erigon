package rpchelper

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter"
)

func GetBlockNumber(blockNrOrHash rpc.BlockNumberOrHash, tx ethdb.Tx, filters *filters.Filters) (uint64, common.Hash, error) {
	var blockNumber uint64
	var err error
	hash, ok := blockNrOrHash.Hash()
	if !ok {
		number := *blockNrOrHash.BlockNumber
		if number == rpc.LatestBlockNumber {
			blockNumber, err = stages.GetStageProgress(tx, stages.Execution)
			if err != nil {
				return 0, common.Hash{}, fmt.Errorf("getting latest block number: %w", err)
			}
		} else if number == rpc.EarliestBlockNumber {
			blockNumber = 0
		} else if number == rpc.PendingBlockNumber {
			pendingBlock := filters.LastPendingBlock()
			if pendingBlock == nil {
				blockNumber, err = stages.GetStageProgress(tx, stages.Execution)
				if err != nil {
					return 0, common.Hash{}, fmt.Errorf("getting latest block number: %w", err)
				}
			} else {
				return pendingBlock.NumberU64(), pendingBlock.Hash(), nil
			}
		} else {
			blockNumber = uint64(number.Int64())
		}
		hash, err = rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return 0, common.Hash{}, err
		}
	} else {
		number := rawdb.ReadHeaderNumber(tx, hash)
		if number == nil {
			return 0, common.Hash{}, fmt.Errorf("block %x not found", hash)
		}
		blockNumber = *number

		ch, err := rawdb.ReadCanonicalHash(tx, blockNumber)
		if err != nil {
			return 0, common.Hash{}, err
		}
		if blockNrOrHash.RequireCanonical && ch != hash {
			return 0, common.Hash{}, fmt.Errorf("hash %q is not currently canonical", hash.String())
		}
	}
	return blockNumber, hash, nil
}

func GetAccount(tx ethdb.Tx, blockNumber uint64, address common.Address) (*accounts.Account, error) {
	reader := adapter.NewStateReader(tx, blockNumber)
	return reader.ReadAccountData(address)
}
