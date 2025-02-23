package commands

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/internal/ethapi"
)

func TestEstimateGas(t *testing.T) {
	db, err := createTestKV()
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer db.Close()
	api := NewEthAPI(NewBaseApi(nil), db, nil, nil, nil, 5000000)
	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	if _, err := api.EstimateGas(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, nil); err != nil {
		t.Errorf("calling EstimateGas: %v", err)
	}
}
