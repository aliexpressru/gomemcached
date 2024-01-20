package main

import (
	"os"

	"golang.org/x/exp/maps"

	"github.com/aliexpressru/gomemcached/memcached"
)

func main() {
	_ = os.Setenv("MEMCACHED_SERVERS", "localhost:11211")

	mcl, err := memcached.InitFromEnv(
		memcached.WithMaxIdleConns(10),
		memcached.WithAuthentication("admin", "mysecretpassword"),
		memcached.WithDisableLogger(),
		memcached.WithDisableMemcachedDiagnostic(),
	)
	mustInit(err)
	defer mcl.CloseAllConns()

	_, err = mcl.Store(memcached.Set, "foo", 10, []byte("bar"))
	mustInit(err)

	_, err = mcl.Get("foo")
	mustInit(err)

	_, err = mcl.Delete("foo")
	mustInit(err)

	_, err = mcl.Delta(memcached.Increment, "incappend", 1, 1, 0)
	mustInit(err)

	_, err = mcl.Append(memcached.Append, "incappend", []byte("add"))
	mustInit(err)

	items := map[string][]byte{
		"foo":    []byte("bar"),
		"gopher": []byte("golang"),
		"answer": []byte("42"),
	}

	err = mcl.MultiStore(memcached.Add, items, 0)
	mustInit(err)

	_, err = mcl.MultiGet(maps.Keys(items))
	mustInit(err)

	err = mcl.MultiDelete(maps.Keys(items))
	mustInit(err)

	err = mcl.FlushAll(0)
	mustInit(err)
}

func mustInit(e error) {
	if e != nil {
		panic(e)
	}
}
