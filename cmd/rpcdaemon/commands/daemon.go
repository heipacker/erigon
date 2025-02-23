package commands

import (
	"context"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/rpc"
)

// APIList describes the list of available RPC apis
func APIList(ctx context.Context, db ethdb.RoKV, eth services.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, filters *filters.Filters, cfg cli.Flags, customAPIList []rpc.API) []rpc.API {
	var defaultAPIList []rpc.API

	base := NewBaseApi(filters)
	ethImpl := NewEthAPI(base, db, eth, txPool, mining, cfg.Gascap)
	tgImpl := NewTgAPI(base, db)
	netImpl := NewNetAPIImpl(eth)
	debugImpl := NewPrivateDebugAPI(base, db, cfg.Gascap)
	traceImpl := NewTraceAPI(base, db, &cfg)
	web3Impl := NewWeb3APIImpl(eth)
	dbImpl := NewDBAPIImpl()   /* deprecated */
	shhImpl := NewSHHAPIImpl() /* deprecated */

	for _, enabledAPI := range cfg.API {
		switch enabledAPI {
		case "eth":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "eth",
				Public:    true,
				Service:   EthAPI(ethImpl),
				Version:   "1.0",
			})
		case "debug":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "debug",
				Public:    true,
				Service:   PrivateDebugAPI(debugImpl),
				Version:   "1.0",
			})
		case "net":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "net",
				Public:    true,
				Service:   NetAPI(netImpl),
				Version:   "1.0",
			})
		case "web3":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "web3",
				Public:    true,
				Service:   Web3API(web3Impl),
				Version:   "1.0",
			})
		case "trace":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "trace",
				Public:    true,
				Service:   TraceAPI(traceImpl),
				Version:   "1.0",
			})
		case "db":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "db",
				Public:    true,
				Service:   DBAPI(dbImpl),
				Version:   "1.0",
			})
		case "shh":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "shh",
				Public:    true,
				Service:   SHHAPI(shhImpl),
				Version:   "1.0",
			})
		case "tg":
			defaultAPIList = append(defaultAPIList, rpc.API{
				Namespace: "tg",
				Public:    true,
				Service:   TgAPI(tgImpl),
				Version:   "1.0",
			})
		}
	}

	return append(defaultAPIList, customAPIList...)
}
