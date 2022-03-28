package standalone_storage

import (
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db   *badger.DB
	path string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// path which uses "kv" simulates from test file
	path := filepath.Join(conf.DBPath, "kv")
	return &StandAloneStorage{path: path}
}

func (s *StandAloneStorage) Start() error {
	s.db = engine_util.CreateDB(s.path, false)
	// if error happen, it will Fatal
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)
	return &StandAloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// if don't use engine_util, remember to do txn.Commit()
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			if err := engine_util.PutCF(s.db, modify.Cf(), modify.Key(), modify.Value()); err != nil {
				return err
			}
		case storage.Delete:
			if err := engine_util.DeleteCF(s.db, modify.Cf(), modify.Key()); err != nil {
				return err
			}
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	// Because of server_test.go L84, ErrKeyNotFound will not return error.
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}
