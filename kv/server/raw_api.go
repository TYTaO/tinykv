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
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	res, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawGetResponse{
		Value:    res,
		NotFound: res == nil,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	server.storage.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	})

	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	cf := reader.IterCF(req.Cf)
	cf.Seek(req.StartKey)
	var kvs []*kvrpcpb.KvPair
	i := uint32(0)
	for cf.Valid() && i < req.Limit {
		item := cf.Item()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{Key: item.Key(), Value: value})
		cf.Next()
		i++
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
