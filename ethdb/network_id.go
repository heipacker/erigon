package ethdb

import (
	"encoding/binary"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

const networkIdLen int = 8

func SetNetworkIdIfNotExist(db Database, networkId uint64) error {
	id, err := db.GetOne(dbutils.DatabaseInfoBucket, dbutils.NetworkIdKey)
	if err != nil {
		return err
	}

	if len(id) != networkIdLen {
		buf := make([]byte, networkIdLen)
		binary.BigEndian.PutUint64(buf[:], networkId)
		return db.Put(dbutils.DatabaseInfoBucket, dbutils.NetworkIdKey, buf)
	}

	return nil
}

func GetNetworkId(db Database) (*uint64, error) {
	id, err := db.GetOne(dbutils.DatabaseInfoBucket, dbutils.NetworkIdKey)
	if err != nil {
		return nil, err
	}

	if len(id) == networkIdLen {
		v := binary.BigEndian.Uint64(id[:])
		return &v, nil
	}

	return nil, nil
}
