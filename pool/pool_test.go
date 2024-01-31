package pool

import (
	"context"
	"math/rand"
	"net/http"
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

func newTestConnectionWithErr() (any, error) {
	return nil, http.ErrHandlerTimeout
}

func closeTestConnection(any) {
	// Do nothing
}

func TestPool(t *testing.T) {
	assert.Panics(t, func() {
		_ = New(context.TODO(), 0, defaultSocketPoolingTimeout, newTestConnection, closeTestConnection)
	}, "was expected panic")

	defer func() {
		if pErr := recover(); pErr != nil {
			t.Fatalf("pool have panic - %v", pErr)
		}
	}()

	p := New(context.TODO(), 2, defaultSocketPoolingTimeout, newTestConnection, closeTestConnection)
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
	p := New(context.TODO(), 10, defaultSocketPoolingTimeout, newTestConnection, closeTestConnection)
	defer p.Destroy()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := p.Get()
			assert.Nilf(t, err, "Get have error %v", err)
			<-time.After(5 * time.Millisecond)
			p.Put(conn)
		}()
	}
	wg.Wait()

	assert.Equalf(t, 10, p.Len(), "Expected pool length to be 10, got %d", p.Len())
}

func TestCountConns(t *testing.T) {
	const count = 300
	p := New(context.TODO(), int32(count), defaultSocketPoolingTimeout, newTestConnection, closeTestConnection)

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

	// p.store is full, over-conn
	p.Put(testConnection{})

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

	cn, err := p.Get()
	assert.Nil(t, cn, "Get: after method Destroy, pool is closed and should return cn == nil")
	assert.ErrorIs(t, err, ErrClosedPool, "Get: after method Destroy, pool is closed, want error ErrClosedPool")

	p2 := New(context.TODO(), count, defaultSocketPoolingTimeout, newTestConnection, closeTestConnection)

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
		<-time.After(200 * time.Millisecond)
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

	p3 := New(context.TODO(), 1, defaultSocketPoolingTimeout, newTestConnection, closeTestConnection)

	// maxConns is full
	_, _ = p3.Get()

	cn, err = p3.Get()
	assert.Nil(t, cn, "Get: after a timeout, it should return cn == nil")
	assert.ErrorIsf(t, ErrAcquireTimeout, err, "Get: after a timeout, it should return ErrAcquireTimeout")

	_, ok := p3.Pop()
	assert.False(t, ok, "Pop: pool with empty pool it should return false for second arg")

	p3.Destroy()
	cn, ok = p3.Pop()
	assert.Nil(t, cn, "Pop: after method Destroy, pool is closed and should return cn == nil")
	assert.False(t, ok, "Pop: after method Destroy, pool is closed and should return false for second arg")

	p4 := New(context.TODO(), 1, defaultSocketPoolingTimeout, newTestConnectionWithErr, closeTestConnection)

	cn, err = p4.Get()
	assert.Nil(t, cn, "Get: create new conn returned an error, conn should be nil")
	assert.ErrorIs(t, err, http.ErrHandlerTimeout, "Get: error should be equal - http.ErrHandlerTimeout")

	p5 := New(context.TODO(), 1, time.Second, nil, nil)

	cn, err = p5.Get()
	assert.Nil(t, cn, "Get: newFunc equal nil, conn should be nil")
	assert.ErrorIs(t, err, ErrNewFuncNil, "Get: error should be equal ErrNewFuncNil")

	p6 := New(context.TODO(), 1, defaultSocketPoolingTimeout, newTestConnection, closeTestConnection)
	bcn, err := p6.Get()
	assert.NotNil(t, bcn, "Get: conn cannot be nil")
	assert.Nil(t, err, "Get: error should be nil")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		cn, err = p6.Get()
		assert.Nil(t, cn, "Get: conn should be nil")
		assert.ErrorIs(t, err, ErrClosedPool, "Get: error should be equal ErrClosedPool")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-time.After(10 * time.Millisecond)
		p6.Close(bcn)
		p6.Destroy()
	}()

	wg.Wait()
}
