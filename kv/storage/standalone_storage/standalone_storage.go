package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	eu "github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	return &StandAloneStorage{db: eu.CreateDB(conf.DBPath, false)}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneReader{txn: s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := new(eu.WriteBatch)
	for _, v := range batch {
		writeBatch.SetCF(v.Cf(), v.Key(), v.Value())
	}
	err := writeBatch.WriteToDB(s.db)
	return err
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := eu.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneReader) IterCF(cf string) eu.DBIterator {
	return eu.NewCFIterator(cf, r.txn)
}

func (r *StandAloneReader) Close() {
	r.txn.Discard()
}
