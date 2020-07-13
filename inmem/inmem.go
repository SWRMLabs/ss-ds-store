package inmem

import (
	"github.com/StreamSpace/ss-ds-store"
	"github.com/StreamSpace/ss-store"
	"github.com/ipfs/go-datastore"
	syncds "github.com/ipfs/go-datastore/sync"
)

func NewInmemStore() (store.Store, error) {
	bs := syncds.MutexWrap(datastore.NewMapDatastore())
	return ds.NewDataStore(&ds.DSConfig{DS: bs})
}
