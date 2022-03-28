package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawGetResponse{NotFound: true, Error: err.Error()}, err
	}
	defer reader.Close()
	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return &kvrpcpb.RawGetResponse{NotFound: true, Error: err.Error()}, err
	}
	return &kvrpcpb.RawGetResponse{Value: val, NotFound: len(val) == 0}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	batch := []storage.Modify{{Data: storage.Put{Cf: req.GetCf(), Key: req.GetKey(), Value: req.GetValue()}}}
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	modify := storage.Modify{Data: storage.Delete{Cf: req.GetCf(), Key: req.GetKey()}}
	batch := []storage.Modify{modify}
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
	}
	defer reader.Close()
	ret := make([]*kvrpcpb.KvPair, 0)
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()
	iter.Seek(req.GetStartKey())
	batch := uint32(0)
	for iter.Valid() && batch < req.Limit {
		item := iter.Item()
		key := item.Key()
		val, err := item.Value()
		// next after getting key and value
		iter.Next()
		if err != nil {
			continue
		}
		ret = append(ret, &kvrpcpb.KvPair{Key: key, Value: val})
		batch++
	}
	return &kvrpcpb.RawScanResponse{Kvs: ret}, nil
}
