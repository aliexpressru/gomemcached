package pool

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const defaultSocketPoolingTimeout = 50 * time.Millisecond

type testConnection struct{}

func newTestConnection() (any, error) {
	return &testConnection{}, nil
}

func closeTestConnection(any) {
	// Do nothing
}

func TestPool(t *testing.T) {
	defer func() {
		if pErr := recover(); pErr != nil {
			t.Fatalf("pool have panic - %v", pErr)
		}
	}()

	p := New(context.Background(), 2, defaultSocketPoolingTimeout, newTestConnection, closeTestConnection)
	defer p.Destroy()

	_, ok := p.Pop()
	assert.False(t, ok, "Pop return ok != false for empty pool")

	assert.Equalf(t, 0, p.Len(), "Expected pool length to be 0, got %d", p.Len())

	conn, err := p.Get()
	assert.Nilf(t, err, "Get from empty pool have error - %v", err)

	assert.Equalf(t, 0, p.Len(), "Expected pool length to be 0 after getting a connection, got %d", p.Len())

	p.Put(conn)
	assert.Equalf(t, 1, p.Len(), "Expected pool length to be 1 after putting back a connection, got %d", p.Len())

	_, ok = p.Pop()
	assert.True(t, ok, "Pop return ok != true for non-empty pool")

	conn, err = p.Get()
	assert.Nilf(t, err, "Get from pool have error - %v", err)

	assert.Equalf(t, 0, p.Len(), "Expected pool length to be 0 after getting a connection from the pool, got %d", p.Len())

	p.Put(conn)
	p.Destroy()
	assert.Equalf(t, 0, p.Len(), "Expected pool length to be 0 after destroying the pool, got %d", p.Len())

	_, err = p.Get()
	assert.ErrorIsf(t, err, ErrClosedPool, "Expected to get an error when getting from a destroyed pool, got %v", err)

	p.Put(conn)
	assert.ErrorIsf(t, err, ErrClosedPool, "Expected to put an error when putting a destroyed pool, got %v", err)
}

func TestPoolConcurrency(t *testing.T) {
	p := New(context.Background(), 10, defaultSocketPoolingTimeout, newTestConnection, closeTestConnection)
	defer p.Destroy()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := p.Get()
			assert.Nilf(t, err, "Get have error %v", err)
			time.Sleep(5 * time.Millisecond)
			p.Put(conn)
		}()
	}
	wg.Wait()

	assert.Equalf(t, 10, p.Len(), "Expected pool length to be 10, got %d", p.Len())
}

func TestCountConns(t *testing.T) {
	const count = 300
	p := New(context.Background(), int32(count), defaultSocketPoolingTimeout, newTestConnection, closeTestConnection)

	conn := atomic.Int32{}
	wg1 := sync.WaitGroup{}

	wg1.Add(3)
	go func() {
		defer wg1.Done()
		for i := 0; i < count/3; i++ {
			_, pErr := p.Get()
			conn.Add(1)
			assert.Nilf(t, pErr, "Get have error - %v", pErr)
		}
	}()
	go func() {
		defer wg1.Done()
		for i := 0; i < count/3; i++ {
			_, pErr := p.Get()
			conn.Add(1)
			assert.Nilf(t, pErr, "Get have error - %v", pErr)
		}
	}()
	go func() {
		defer wg1.Done()
		for i := 0; i < count/3; i++ {
			_, pErr := p.Get()
			conn.Add(1)
			assert.Nilf(t, pErr, "Get have error - %v", pErr)
		}
	}()

	wg1.Wait()

	assert.Equalf(t, conn.Load(), int32(count), "Not equal init and received conns. have - %d, expacted - %d ", conn.Load(), int32(count))

	for i := 0; i < int(conn.Load()); i++ {
		p.Put(testConnection{})
	}

	wg1.Add(2)

	go func() {
		defer wg1.Done()
		p.Destroy()
	}()
	go func() {
		defer wg1.Done()
		p.Destroy()
	}()
	wg1.Wait()

	_, closed := <-p.storeClose
	assert.Falsef(t, closed, "Wrong storeClose flag. have - %t, expacted - %t ", closed, false)

	p2 := New(context.Background(), count, defaultSocketPoolingTimeout, newTestConnection, closeTestConnection)

	var (
		mu    sync.RWMutex
		conns []any
		wg2   sync.WaitGroup
	)

	addToSl := func(c any) {
		mu.Lock()
		defer mu.Unlock()
		conns = append(conns, c)
	}

	getFromSl := func() any {
		mu.Lock()
		defer mu.Unlock()
		return conns[len(conns)-1]
	}

	getSlLen := func() int {
		mu.RLock()
		defer mu.RUnlock()
		return len(conns)
	}

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		for i := 0; i < count/2; i++ {
			c, gErr := p2.Get()
			assert.Nilf(t, gErr, "Get have error")
			//nolint:gosec
			if rand.Int()%2 == 0 {
				addToSl(c)
			} else {
				p2.Put(c)
			}
		}
	}()
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		for i := 0; i < count/2; i++ {
			c, gErr := p2.Get()
			assert.Nilf(t, gErr, "Get have error")
			//nolint:gosec
			if rand.Int()%2 == 0 {
				addToSl(c)
			} else {
				p2.Put(c)
			}
		}
	}()

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		time.Sleep(200 * time.Millisecond)
		c, gErr := p2.Get()
		assert.Nilf(t, gErr, "Get with full cap have error")
		addToSl(c)
	}()

	wg2.Wait()

	for i := 0; i < getSlLen(); i++ {
		go func() {
			p2.Put(getFromSl())
		}()
	}
}
