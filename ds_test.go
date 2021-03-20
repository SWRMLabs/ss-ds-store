package ds

import (
	"os"
	"testing"

	"github.com/SWRMLabs/ss-store/testsuite"
	badger "github.com/ipfs/go-ds-badger"
	logger "github.com/ipfs/go-log/v2"
)

func TestStoreSuite(t *testing.T) {
	logger.SetLogLevel("*", "Debug")
	dsPath := "/tmp/dataStore"

	defer func() {
		os.RemoveAll(dsPath)
	}()

	ds, err := badger.NewDatastore(dsPath, &badger.DefaultOptions)
	if err != nil {
		panic("Failed to initialize ds")
	}
	dsStore, _ := NewDataStore(&DSConfig{
		DS: ds,
	})
	store_testsuite.RunTestsuite(t, dsStore, store_testsuite.Advanced)
}
