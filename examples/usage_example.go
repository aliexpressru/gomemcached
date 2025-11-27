// nolint
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"golang.org/x/exp/maps"

	"github.com/aliexpressru/gomemcached/memcached"
)

func main() {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	_ = os.Setenv("MEMCACHED_SERVERS", "localhost:11215")

	// Initialize memcached client from environment variables
	mcl, err := memcached.InitFromEnv(
		ctx,
		memcached.WithMaxIdleConns(10),
		memcached.WithAuthentication("admin", "secret"),
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

	// Demonstration: MultiStore returning multiple errors via errors.Join,
	// and converting the returned error into []error using Unwrap() []error.
	items2 := map[string][]byte{
		"alpha": []byte("one"),
		"beta":  []byte("two"),
		"gamma": []byte("three"),
	}

	// Ensure keys exist first.
	err = mcl.MultiStore(ctx, memcached.Set, items2, 0)
	mustInit(err)

	// Attempt to Add the same keys again; memcached should return per-key errors (e.g., KEY_EEXISTS).
	if err = mcl.MultiStore(ctx, memcached.Add, items2, 0); err != nil {
		fmt.Println("\nMultiStore(Add) returned a joined error. Unwrapping into individual errors:")
		for i, e := range unwrapAllErrors(err) {
			fmt.Printf("  error[%d]: %v\n", i, e)

			// Use UnwrapMemcachedError to extract *Response from each error
			if resp, ok := memcached.UnwrapMemcachedError(e); ok {
				// All Response fields are accessible for detailed error analysis
				fmt.Printf("    -> Memcached Response (all fields):\n")
				fmt.Printf("       Opcode: %v\n", resp.Opcode)
				fmt.Printf("       Status: %v\n", resp.Status)
				fmt.Printf("       Opaque: %v\n", resp.Opaque)
				fmt.Printf("       Cas: %v\n", resp.Cas)
				fmt.Printf("       Key: %q\n", string(resp.Key))
				fmt.Printf("       Body: %q\n", string(resp.Body))
				fmt.Printf("       Extras: %v\n", resp.Extras)
				fmt.Printf("       Error(): %s\n", resp.Error())
				fmt.Printf("       String(): %s\n", resp.String())
			} else {
				fmt.Printf("    -> Non-memcached error (network/transport issue)\n")
			}
		}
	}
}

func mustInit(e error) {
	if e != nil {
		panic(e)
	}
}

// unwrapAllErrors converts an error possibly created by errors.Join into a flat slice.
func unwrapAllErrors(err error) []error {
	if err == nil {
		return nil
	}
	type unwrapper interface{ Unwrap() []error }
	var u unwrapper
	if errors.As(err, &u) {
		return u.Unwrap()
	}
	return []error{err}
}
