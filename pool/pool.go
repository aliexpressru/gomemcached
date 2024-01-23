package pool

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/semaphore"
)

const token int64 = 1

var (
	ErrClosedPool     = fmt.Errorf("pool is closed")
	ErrNewFuncNil     = fmt.Errorf("newFunc for pool is nil, can not create connection")
	ErrAcquireTimeout = fmt.Errorf("timeout for Acquire from the pool. Need to increase the maxCap for pool")
)

var _ ConnPool = (*Pool)(nil)

type ConnPool interface {
	Get() (any, error)
	Pop() (any, bool)
	Put(v any)
	Destroy()
	Len() int
}

// Pool common connection pool
type Pool struct {
	ctx context.Context

	// newConn are functions for creating new connections if maxCap is not reached.
	newConn func() (any, error)
	// closeConn is a function for graceful closed connections.
	closeConn func(any)

	// sema is a semaphore implementation for control a max capacity of pool
	sema *semaphore.Weighted
	// aqSemaTimeout is an amount of time to acquire conn from pool
	aqSemaTimeout time.Duration

	// store is a chan with connections.
	store chan any
	// storeClose is a flag indicating that store is closed.
	storeClose chan struct{}
	// maxCap is maximum of total connections used
	maxCap int32
}

// New create a pool with capacity
func New(ctx context.Context, maxCap int32, acquireSemaTimeout time.Duration, newFunc func() (any, error), closeFunc func(any)) *Pool {
	if maxCap <= 0 {
		panic("invalid memcached maxCap")
	}

	return &Pool{
		ctx:           ctx,
		newConn:       newFunc,
		closeConn:     closeFunc,
		sema:          semaphore.NewWeighted(int64(maxCap)),
		aqSemaTimeout: acquireSemaTimeout,
		store:         make(chan any, maxCap),
		storeClose:    make(chan struct{}),
		maxCap:        maxCap,
	}
}

// Len returns current connections in pool
func (p *Pool) Len() int {
	return len(p.store)
}

// Get returns a conn from store or create one
func (p *Pool) Get() (any, error) {
	var aqTimeout bool

	for {
		select {
		case v, ok := <-p.store:
			if ok {
				return v, nil
			}
			return nil, ErrClosedPool
		default:
			if aqTimeout {
				return nil, ErrAcquireTimeout
			}
			if cn, timeout, err := p.create(); timeout {
				// last try get conn after timeout
				aqTimeout = true
				continue
			} else {
				return cn, err
			}
		}
	}
}

// Pop return available conn without block
func (p *Pool) Pop() (any, bool) {
	if p.isClosed() {
		return nil, false
	}

	select {
	case v, ok := <-p.store:
		return v, ok
	default:
		return nil, false
	}
}

// Put set back conn into store again
func (p *Pool) Put(v any) {
	if p.isClosed() {
		return
	}
	select {
	case p.store <- v:
	default:
	}
}

// Destroy close all connections and deactivate the pool
func (p *Pool) Destroy() {
	if p.isClosed() {
		// pool already destroyed
		return
	}

	close(p.storeClose)
	close(p.store)
	for v := range p.store {
		p.close(v)
	}
}

// Close is closed a connection
func (p *Pool) Close(v any) {
	p.close(v)
}

func (p *Pool) create() (any, bool, error) {
	ctx, cancel := context.WithTimeout(p.ctx, p.aqSemaTimeout)
	defer cancel()

	if err := p.sema.Acquire(ctx, token); err != nil {
		return nil, true, nil
	}

	if p.isClosed() {
		p.sema.Release(token)
		return nil, false, ErrClosedPool
	}

	if p.newConn == nil {
		return nil, false, ErrNewFuncNil
	}
	cn, err := p.newConn()
	if err != nil {
		p.sema.Release(token)
		return nil, false, err
	}
	return cn, false, nil
}

func (p *Pool) close(v any) {
	p.sema.Release(token)
	if p.closeConn != nil {
		p.closeConn(v)
	}
}

func (p *Pool) isClosed() bool {
	select {
	case <-p.storeClose:
		return true
	default:
		return false
	}
}
