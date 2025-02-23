package rpctest

import (
	"fmt"
	"github.com/ledgerwatch/erigon/common"
	"net/http"
	"time"
)

func Bench4(turbogeth_url string) {
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	blockhash := common.HexToHash("0xdf15213766f00680c6a20ba76ba2cc9534435e19bc490039f3a7ef42095c8d13")
	req_id := 1
	template := `{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",true],"id":%d}`
	var b EthBlockByNumber
	if err := post(client, turbogeth_url, fmt.Sprintf(template, 1720000, req_id), &b); err != nil {
		fmt.Printf("Could not retrieve block %d: %v\n", 1720000, err)
		return
	}
	if b.Error != nil {
		fmt.Printf("Error retrieving block: %d %s\n", b.Error.Code, b.Error.Message)
	}
	for txindex := 0; txindex < 6; txindex++ {
		txhash := b.Result.Transactions[txindex].Hash
		req_id++
		template = `{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["%s"],"id":%d}`
		var trace EthTxTrace
		if err := post(client, turbogeth_url, fmt.Sprintf(template, txhash, req_id), &trace); err != nil {
			fmt.Printf("Could not trace transaction %s: %v\n", txhash, err)
			print(client, turbogeth_url, fmt.Sprintf(template, txhash, req_id))
			return
		}
		if trace.Error != nil {
			fmt.Printf("Error tracing transaction: %d %s\n", trace.Error.Code, trace.Error.Message)
		}
		print(client, turbogeth_url, fmt.Sprintf(template, txhash, req_id))
	}
	to := common.HexToAddress("0x8b3b3b624c3c0397d3da8fd861512393d51dcbac")
	sm := make(map[common.Hash]storageEntry)
	start := common.HexToHash("0xa283ff49a55f86420a4acd5835658d8f45180db430c7b0d7ae98da5c64f620dc")

	req_id++
	template = `{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x%x", %d,"0x%x","0x%x",%d],"id":%d}`
	i := 6
	nextKey := &start
	for nextKey != nil {
		var sr DebugStorageRange
		if err := post(client, turbogeth_url, fmt.Sprintf(template, blockhash, i, to, *nextKey, 1024, req_id), &sr); err != nil {
			fmt.Printf("Could not get storageRange: %v\n", err)
			return
		}
		if sr.Error != nil {
			fmt.Printf("Error getting storageRange: %d %s\n", sr.Error.Code, sr.Error.Message)
			break
		} else {
			nextKey = sr.Result.NextKey
			for k, v := range sr.Result.Storage {
				sm[k] = v
			}
		}
	}
	fmt.Printf("storageRange: %d\n", len(sm))
}
