package commands

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	types2 "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/gointerfaces"
	"github.com/ledgerwatch/erigon/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/gointerfaces/types"
	"github.com/ledgerwatch/erigon/rpc"
)

// GetTransactionByHash implements eth_getTransactionByHash. Returns information about a transaction given the transaction's hash.
func (api *APIImpl) GetTransactionByHash(ctx context.Context, hash common.Hash) (*RPCTransaction, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByHash
	txn, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(tx, hash)
	if txn != nil {
		return newRPCTransaction(txn, blockHash, blockNumber, txIndex), nil
	}

	// No finalized transaction, try to retrieve it from the pool
	reply, err := api.txPool.Transactions(ctx, &txpool.TransactionsRequest{Hashes: []*types.H256{gointerfaces.ConvertHashToH256(hash)}})
	if err != nil {
		return nil, err
	}
	if len(reply.RlpTxs[0]) > 0 {
		txn, err = types2.UnmarshalTransactionFromBinary(reply.RlpTxs[0])
		if err != nil {
			return nil, err
		}
		return newRPCPendingTransaction(txn), nil
	}

	// Transaction unknown, return as such
	return nil, nil
}

// GetRawTransactionByHash returns the bytes of the transaction for the given hash.
func (api *APIImpl) GetRawTransactionByHash(ctx context.Context, hash common.Hash) (hexutil.Bytes, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByHash
	txn, _, _, _ := rawdb.ReadTransaction(tx, hash)
	if txn != nil {
		var buf bytes.Buffer
		err = txn.MarshalBinary(&buf)
		return buf.Bytes(), err
	}

	// No finalized transaction, try to retrieve it from the pool
	reply, err := api.txPool.Transactions(ctx, &txpool.TransactionsRequest{Hashes: []*types.H256{gointerfaces.ConvertHashToH256(hash)}})
	if err != nil {
		return nil, err
	}
	if len(reply.RlpTxs[0]) > 0 {
		return reply.RlpTxs[0], nil
	}
	return nil, nil
}

// GetTransactionByBlockHashAndIndex implements eth_getTransactionByBlockHashAndIndex. Returns information about a transaction given the block's hash and a transaction index.
func (api *APIImpl) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, txIndex hexutil.Uint64) (*RPCTransaction, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByBlockHashAndIndex
	block, _, err := rawdb.ReadBlockByHashWithSenders(tx, blockHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
	}

	txs := block.Transactions()
	if uint64(txIndex) >= uint64(len(txs)) {
		return nil, fmt.Errorf("txIndex (%d) out of range (nTxs: %d)", uint64(txIndex), uint64(len(txs)))
	}

	return newRPCTransaction(txs[txIndex], block.Hash(), block.NumberU64(), uint64(txIndex)), nil
}

// GetRawTransactionByBlockHashAndIndex returns the bytes of the transaction for the given block hash and index.
func (api *APIImpl) GetRawTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index hexutil.Uint) (hexutil.Bytes, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getRawTransactionByBlockHashAndIndex
	block, err := rawdb.ReadBlockByHash(tx, blockHash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
	}

	return newRPCRawTransactionFromBlockIndex(block, uint64(index))
}

// GetTransactionByBlockNumberAndIndex implements eth_getTransactionByBlockNumberAndIndex. Returns information about a transaction given a block number and transaction index.
func (api *APIImpl) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, txIndex hexutil.Uint) (*RPCTransaction, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getTransactionByBlockNumberAndIndex
	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}

	block, err := rawdb.ReadBlockByNumber(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
	}

	txs := block.Transactions()
	if uint64(txIndex) >= uint64(len(txs)) {
		return nil, fmt.Errorf("txIndex (%d) out of range (nTxs: %d)", uint64(txIndex), uint64(len(txs)))
	}

	return newRPCTransaction(txs[txIndex], block.Hash(), block.NumberU64(), uint64(txIndex)), nil
}

// GetRawTransactionByBlockNumberAndIndex returns the bytes of the transaction for the given block number and index.
func (api *APIImpl) GetRawTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) (hexutil.Bytes, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// https://infura.io/docs/ethereum/json-rpc/eth-getRawTransactionByBlockNumberAndIndex
	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}

	block, err := rawdb.ReadBlockByNumber(tx, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
	}

	return newRPCRawTransactionFromBlockIndex(block, uint64(index))
}
