package commands

import (
	"context"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/rpc"
)

// BlockReward returns the block reward for this block
// func (api *TgImpl) BlockReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
//	tx, err := api.dbReader.Begin(ctx, ethdb.RO)
//	if err != nil {
//		return Issuance{}, err
//	}
//	defer tx.Rollback()
//
//	return api.rewardCalc(tx, blockNr, "block") // nolint goconst
//}

// UncleReward returns the uncle reward for this block
// func (api *TgImpl) UncleReward(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
//	tx, err := api.dbReader.Begin(ctx, ethdb.RO)
//	if err != nil {
//		return Issuance{}, err
//	}
//	defer tx.Rollback()
//
//	return api.rewardCalc(tx, blockNr, "uncle") // nolint goconst
//}

// Issuance implements tg_issuance. Returns the total issuance (block reward plus uncle reward) for the given block.
func (api *TgImpl) Issuance(ctx context.Context, blockNr rpc.BlockNumber) (Issuance, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return Issuance{}, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return Issuance{}, err
	}
	if chainConfig.Ethash == nil {
		// Clique for example has no issuance
		return Issuance{}, nil
	}

	block, err := api.getBlockByRPCNumber(tx, blockNr)
	if err != nil {
		return Issuance{}, err
	}
	minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, block.Header(), block.Uncles())
	issuance := minerReward
	for _, r := range uncleRewards {
		p := r // avoids warning?
		issuance.Add(&issuance, &p)
	}

	var ret Issuance
	ret.BlockReward = hexutil.EncodeBig(minerReward.ToBig())
	ret.Issuance = hexutil.EncodeBig(issuance.ToBig())
	issuance.Sub(&issuance, &minerReward)
	ret.UncleReward = hexutil.EncodeBig(issuance.ToBig())
	return ret, nil
}

func (api *TgImpl) getBlockByRPCNumber(tx ethdb.Tx, blockNr rpc.BlockNumber) (*types.Block, error) {
	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}
	return rawdb.ReadBlockByNumber(tx, blockNum)
}

// Issuance structure to return information about issuance
type Issuance struct {
	BlockReward string `json:"blockReward,omitempty"`
	UncleReward string `json:"uncleReward,omitempty"`
	Issuance    string `json:"issuance,omitempty"`
}
