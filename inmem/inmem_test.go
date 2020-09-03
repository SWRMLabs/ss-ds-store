package inmem

import (
	"testing"

	"github.com/StreamSpace/ss-store/testsuite"
	logger "github.com/ipfs/go-log/v2"
)

func TestStoreSuite(t *testing.T) {
	logger.SetLogLevel("*", "Debug")

	inmemStore, _ := NewInmemStore()
	store_testsuite.RunTestsuite(t, inmemStore, store_testsuite.Advanced)
}
