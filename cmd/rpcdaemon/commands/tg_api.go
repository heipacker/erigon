package commands

import (
	"context"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/rpc"
)

// TgAPI TurboGeth specific routines
type TgAPI interface {
	// System related (see ./tg_system.go)
	Forks(ctx context.Context) (Forks, error)

	// Blocks related (see ./tg_blocks.go)
	GetHeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error)
	GetHeaderByHash(_ context.Context, hash common.Hash) (*types.Header, error)

	// Receipt related (see ./tg_receipts.go)
	GetLogsByHash(ctx context.Context, hash common.Hash) ([][]*types.Log, error)
	//GetLogsByNumber(ctx context.Context, number rpc.BlockNumber) ([][]*types.Log, error)

	// Issuance / reward related (see ./tg_issuance.go)
	// BlockReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error)
	// UncleReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error)
	Issuance(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error)
}

// TgImpl is implementation of the TgAPI interface
type TgImpl struct {
	*BaseAPI
	db ethdb.RoKV
}

// NewTgAPI returns TgImpl instance
func NewTgAPI(base *BaseAPI, db ethdb.RoKV) *TgImpl {
	return &TgImpl{
		BaseAPI: base,
		db:      db,
	}
}
