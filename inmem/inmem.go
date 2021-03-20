package inmem

import (
	"github.com/SWRMLabs/ss-ds-store"
	"github.com/SWRMLabs/ss-store"
	"github.com/ipfs/go-datastore"
	syncds "github.com/ipfs/go-datastore/sync"
)

func NewInmemStore() (store.Store, error) {
	bs := syncds.MutexWrap(datastore.NewMapDatastore())
	return ds.NewDataStore(&ds.DSConfig{DS: bs})
}
