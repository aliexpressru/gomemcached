// nolint
package main

import (
	"context"
	"os"

	"golang.org/x/exp/maps"

	"github.com/aliexpressru/gomemcached/memcached"
)

func main() {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	_ = os.Setenv("MEMCACHED_SERVERS", "localhost:11211")

	// Initialize memcached client from environment variables
	mcl, err := memcached.InitFromEnv(
		ctx,
		memcached.WithMaxIdleConns(10),
		memcached.WithAuthentication("admin", "mysecretpassword"),
		memcached.WithDisableLogger(),
	)
	mustInit(err)
	defer mcl.CloseAllConns(ctx)

	// Debug logging example: enable to see which keys go to which nodes
	// Useful for debugging key distribution across memcached shards
	// Works for both single operations (Get, Store, Delete, Delta, Append)
	// and batch operations (MultiGet, MultiStore, MultiDelete)
	//
	// Uncomment the line below to enable debug logging:
	// ctx = memcached.EnableDebugLog(ctx)
	//
	// Or use environment variable:
	// debugEnabled := os.Getenv("DEBUG") == "true"
	// if debugEnabled {
	//     ctx = memcached.EnableDebugLog(ctx)
	// }

	// Single operations
	// With debug enabled, will log: gomemcached: Store 127.0.0.1:11211 - [foo]
	_, err = mcl.Store(ctx, memcached.Set, "foo", 10, []byte("bar"))
	mustInit(err)

	// With debug enabled, will log: gomemcached: Get 127.0.0.1:11211 - [foo]
	_, err = mcl.Get(ctx, "foo")
	mustInit(err)

	_, err = mcl.Delete(ctx, "foo")
	mustInit(err)

	_, err = mcl.Delta(ctx, memcached.Increment, "incappend", 1, 1, 0)
	mustInit(err)

	_, err = mcl.Append(ctx, memcached.Append, "incappend", []byte("add"))
	mustInit(err)

	// Batch operations
	items := map[string][]byte{
		"foo":    []byte("bar"),
		"gopher": []byte("golang"),
		"answer": []byte("42"),
	}

	err = mcl.MultiStore(ctx, memcached.Add, items, 0)
	mustInit(err)

	// With debug enabled, will log something like:
	// gomemcached: MultiGet 127.0.0.1:11211 - [foo, answer]
	// gomemcached: MultiGet 127.0.0.2:11211 - [gopher]
	_, err = mcl.MultiGet(ctx, maps.Keys(items))
	mustInit(err)

	err = mcl.MultiDelete(ctx, maps.Keys(items))
	mustInit(err)

	err = mcl.FlushAll(ctx, 0)
	mustInit(err)
}

func mustInit(e error) {
	if e != nil {
		panic(e)
	}
}
