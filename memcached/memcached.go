package memcached

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kelseyhightower/envconfig"
	"golang.org/x/exp/maps"

	"github.com/aliexpressru/gomemcached/consistenthash"
	"github.com/aliexpressru/gomemcached/logger"
	"github.com/aliexpressru/gomemcached/pool"
	"github.com/aliexpressru/gomemcached/utils"
)

const (
	// DefaultTimeout is the default socket read/write timeout.
	DefaultTimeout = 500 * time.Millisecond

	// DefaultMaxIdleConns is the default maximum number of idle connections
	// kept for any single address.
	DefaultMaxIdleConns = 100

	// DefaultNodeHealthCheckPeriod is the default time period for start check available nods
	DefaultNodeHealthCheckPeriod = 15 * time.Second
	// DefaultRebuildingNodePeriod is the default time period for rebuilds the nodes in hash ring using freshly discovered
	DefaultRebuildingNodePeriod = 15 * time.Second

	// DefaultRetryCountForConn is a default number of connection retries before return i/o timeout error
	DefaultRetryCountForConn = uint8(3)

	// DefaultOfNumberConnsToDestroyPerRBPeriod is number of connections in pool whose needed close in every rebuild node cycle
	DefaultOfNumberConnsToDestroyPerRBPeriod = 1

	// DefaultSocketPoolingTimeout Amount of time to acquire socket from pool
	DefaultSocketPoolingTimeout = 50 * time.Millisecond
)

var _ Memcached = (*Client)(nil)

type (
	Memcached interface {
		Store(ctx context.Context, storeMode StoreMode, key string, exp uint32, body []byte) (*Response, error)
		Get(ctx context.Context, key string) (*Response, error)
		Delete(ctx context.Context, key string) (*Response, error)
		Delta(ctx context.Context, deltaMode DeltaMode, key string, delta, initial uint64, exp uint32) (newValue uint64, err error)
		Append(ctx context.Context, appendMode AppendMode, key string, data []byte) (*Response, error)
		FlushAll(ctx context.Context, exp uint32) error
		MultiDelete(ctx context.Context, keys []string) error
		MultiStore(ctx context.Context, storeMode StoreMode, items map[string][]byte, exp uint32) error
		MultiGet(ctx context.Context, keys []string) (map[string][]byte, error)

		CloseAllConns(ctx context.Context) error
		CloseAvailableConnsInAllShardPools(ctx context.Context, numOfClose int) (int, error)
	}

	// Client is a memcached client.
	// It is safe for unlocked use by multiple concurrent goroutines.
	Client struct {
		ctx context.Context
		nw  *network
		cfg *config

		// opaque - a unique identifier for the request, used to associate the request with its corresponding response.
		opaque *uint32

		// timeout specifies the socket read/write timeout.
		// If zero, DefaultTimeout is used.
		timeout time.Duration

		// maxIdleConns specifies the maximum number of idle connections that will
		// be maintained per address. If less than one, DefaultMaxIdleConns will be
		// used.
		//
		// Consider your expected traffic rates and latency carefully. This should
		// be set to a number higher than your peak parallel requests.
		maxIdleConns int

		// hr - hash ring implementation (can be a custom consistenthash.NewCustomHashRing)
		hr consistenthash.ConsistentHash

		// disableMemcachedDiagnostic - is a flag for turn off write metrics from lib.
		disableMemcachedDiagnostic bool
		// disableNodeProvider - is a flag for turnoff rebuild and health check nodes.
		disableNodeProvider bool
		// disableRefreshConns - is a flag for turn off to refresh conns in the pool.
		disableRefreshConns bool
		// nodeHCPeriod - period for execute nodes health checker
		// if zero, DefaultNodeHealthCheckPeriod is used.
		nodeHCPeriod time.Duration
		// nodeRBPeriod - period for execute rebuilding nodes
		// if zero, DefaultNodeHealthCheckPeriod is used.
		nodeRBPeriod time.Duration

		// fmu - mutex for freeConns
		fmu sync.RWMutex
		// freeConns hashmap with nodes and their open dial connections
		freeConns map[string]*pool.Pool
		// dmu - mutex for deadNodes
		dmu sync.RWMutex
		// deadNodes hashmap with nodes that did not respond to health check
		deadNodes map[string]struct{}

		authEnable bool
		// authData ready body for authentication request
		authData []byte
	}

	network struct {
		dial        func(network string, address string) (net.Conn, error)
		dialTimeout func(network string, address string, timeout time.Duration) (net.Conn, error)
		lookupHost  func(host string) (addrs []string, err error)
	}

	config struct {
		// HeadlessServiceAddress Headless service to lookup all the memcached ip addresses.
		HeadlessServiceAddress string `envconfig:"MEMCACHED_HEADLESS_SERVICE_ADDRESS"`
		// Servers List of servers with hosted memcached (with ports)
		Servers []string `envconfig:"MEMCACHED_SERVERS"`
		// MemcachedPort The optional port override for cases when memcached IP addresses are obtained from headless service.
		MemcachedPort int `envconfig:"MEMCACHED_PORT" default:"11211"`
	}
	conn struct {
		dl      deadliner
		rc      io.ReadCloser
		addr    net.Addr
		c       *Client
		hdrBuf  []byte
		healthy bool
		wrtBuf  *bufio.Writer
		authed  bool
	}
)

// InitFromEnv returns a memcached client using the config.HeadlessServiceAddress or config.Servers
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func InitFromEnv(ctx context.Context, opts ...Option) (*Client, error) {
	var (
		op  = new(options)
		cfg = new(config)
	)
	if err := envconfig.Process("", cfg); err != nil {
		return nil, fmt.Errorf("%s: client init err: %s", libPrefix, err.Error())
	}

	op.cfg = cfg

	for _, opt := range opts {
		opt(op)
	}

	if op.nw == nil {
		op.nw = &network{
			dial:        net.Dial,
			dialTimeout: net.DialTimeout,
			lookupHost:  net.LookupHost,
		}
	}
	if op.hr == nil {
		op.hr = consistenthash.NewHashRing()
	}
	if op.opaque == nil {
		op.opaque = new(uint32)
	}
	if op.disableLogger {
		logger.DisableLogger()
	}
	op.ctx = ctx

	// Initialize metrics with custom or default configuration
	if !op.disableMemcachedDiagnostic {
		initMetrics(op.metricsRegisterer, op.metricsDurationBuckets, op.metricsObjectSizeBuckets)
	}

	return newFromConfig(op)
}

func newFromConfig(op *options) (*Client, error) {
	if op.cfg != nil && (op.cfg.HeadlessServiceAddress == "" && len(op.cfg.Servers) == 0) {
		return nil, fmt.Errorf("%w, you must fill in either MEMCACHED_HEADLESS_SERVICE_ADDRESS or MEMCACHED_SERVERS", ErrNotConfigured)
	}
	nodes, err := getNodes(op.nw.lookupHost, op.cfg)
	if err != nil {
		return nil, fmt.Errorf("%w, %s", ErrInvalidAddr, err.Error())
	}

	mc := &op.Client

	for _, n := range nodes {
		addr, err := utils.AddrRepr(n)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrInvalidAddr, err.Error())
		}
		mc.hr.Add(addr)
	}

	if !mc.disableNodeProvider {
		mc.initNodesProvider()
	}
	return mc, nil
}

// release returns this connection back to the client's free pool
func (cn *conn) release() {
	cn.c.putFreeConn(cn)
}

func (cn *conn) close() {
	if p, ok := cn.c.safeGetFreeConn(cn.addr); ok {
		p.Close(cn)
	} else {
		_ = cn.rc.Close()
	}
}

// condRelease releases this connection if the error pointed to by err
// is nil (not an error) or is only a protocol level error (e.g. a
// cache miss).  The purpose is to not recycle TCP connections that
// are bad.
func (cn *conn) condRelease(err *error) {
	if (*err == nil || resumableError(*err)) && cn.healthy {
		cn.dl.ClearDeadline()
		cn.release()
	} else {
		cn.close()
	}
}

func (c *Client) getOpaque() uint32 {
	return atomic.AddUint32(c.opaque, uint32(1))
}

func (c *Client) safeGetFreeConn(addr net.Addr) (*pool.Pool, bool) {
	c.fmu.RLock()
	defer c.fmu.RUnlock()
	connPool, ok := c.freeConns[addr.String()]
	return connPool, ok
}

func (c *Client) safeGetOrInitFreeConn(addr net.Addr) *pool.Pool {
	c.fmu.Lock()
	defer c.fmu.Unlock()

	connPool, ok := c.freeConns[addr.String()]
	if ok {
		return connPool
	}

	dialConn := func() (any, error) {
		nc, err := c.dial(addr)
		if err != nil {
			return nil, err
		}
		return &conn{
			dl:      newDeadliner(nc),
			rc:      nc,
			addr:    addr,
			c:       c,
			hdrBuf:  make([]byte, HDR_LEN),
			wrtBuf:  bufio.NewWriter(nc),
			healthy: true,
		}, nil
	}

	closeConn := func(cn any) {
		_ = cn.(*conn).rc.Close()
	}

	newPool := pool.New(c.ctx, int32(c.getMaxIdleConns()), DefaultSocketPoolingTimeout, dialConn, closeConn)

	if c.freeConns == nil {
		c.freeConns = make(map[string]*pool.Pool)
	}
	c.freeConns[addr.String()] = newPool

	return newPool
}

func (c *Client) freeConnsIsNil() bool {
	c.fmu.RLock()
	defer c.fmu.RUnlock()
	return c.freeConns == nil
}

func (c *Client) putFreeConn(cn *conn) {
	connPool, ok := c.safeGetFreeConn(cn.addr)
	if ok {
		connPool.Put(cn)
	} else {
		_ = cn.rc.Close()
	}
}

func (c *Client) getFreeConn(ctx context.Context, addr net.Addr) (*conn, error) {
	connPool := c.safeGetOrInitFreeConn(addr)

	connRaw, err := connPool.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: Get from pool error - %w", libPrefix, err)
	}

	cn := connRaw.(*conn)

	if c.authEnable && !cn.authed {
		if ok, aErr := c.authenticate(cn); ok {
			cn.authed = true
			return cn, nil
		} else {
			return nil, fmt.Errorf("%w, %s", ErrAuthFail, aErr)
		}
	}

	return connRaw.(*conn), nil
}

func (c *Client) removeFromFreeConns(addr net.Addr) {
	if c.freeConnsIsNil() {
		return
	}
	connPool, ok := c.safeGetFreeConn(addr)

	c.fmu.Lock()
	defer c.fmu.Unlock()
	if ok {
		connPool.Destroy()
	}
	delete(c.freeConns, addr.String())
}

func (c *Client) netTimeout() time.Duration {
	if c.timeout != 0 {
		return c.timeout
	}
	return DefaultTimeout
}

func (c *Client) getMaxIdleConns() int {
	if c.maxIdleConns > 0 {
		return c.maxIdleConns
	}
	return DefaultMaxIdleConns
}

func (c *Client) getHCPeriod() time.Duration {
	if c.nodeHCPeriod > 0 {
		return c.nodeHCPeriod
	}
	return DefaultNodeHealthCheckPeriod
}

func (c *Client) getRBPeriod() time.Duration {
	if c.nodeRBPeriod > 0 {
		return c.nodeRBPeriod
	}
	return DefaultRebuildingNodePeriod
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "connect timeout to " + cte.Addr.String()
}

func (c *Client) dial(addr net.Addr) (net.Conn, error) {
	if c.netTimeout() > 0 {
		nc, err := c.nw.dialTimeout(addr.Network(), addr.String(), c.netTimeout())
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return nil, &ConnectTimeoutError{addr}
			}
			return nil, err
		}
		return nc, nil
	}
	return c.nw.dial(addr.Network(), addr.String())
}

func (c *Client) getConnForNode(ctx context.Context, node any) (*conn, error) {
	addr, ok := node.(net.Addr)
	if !ok {
		return nil, ErrInvalidAddr
	}
	cn, err := c.getFreeConn(ctx, addr)
	if err != nil {
		return nil, err
	}

	cn.dl.SetDeadline(ctx)
	return cn, nil
}

// Store is a wrote the provided item with expiration.
func (c *Client) Store(ctx context.Context, storeMode StoreMode, key string, exp uint32, body []byte) (_ *Response, err error) {
	const methodName = "Store"
	timer := time.Now()
	defer c.writeMethodDiagnostics(methodName, timer, &err)

	if !legalKey(key) {
		return nil, ErrMalformedKey
	}

	node, find := c.hr.Get(key)
	if !find {
		return nil, ErrNoServers
	}

	logDebugSingleKey(ctx, methodName, node, key)

	cn, err := c.getConnForNode(ctx, node)
	if err != nil {
		return nil, err
	}
	return c.store(cn, storeMode.Resolve(), key, exp, c.getOpaque(), body)
}

func (c *Client) store(cn *conn, opcode OpCode, key string, exp, opaque uint32, body []byte) (_ *Response, err error) {
	req := &Request{
		Opcode: opcode,
		Key:    []byte(key),
		Opaque: opaque,
		Body:   body,
	}
	req.prepareExtras(exp, 0, 0)
	defer func() {
		if err == nil {
			c.writeItemSizeDiagnostics("Store", req.size())
		}
	}()
	return c.send(cn, req)
}

func (c *Client) send(cn *conn, req *Request) (resp *Response, err error) {
	defer cn.condRelease(&err)
	_, err = transmitRequest(cn.wrtBuf, req)
	if err != nil {
		cn.healthy = false
		return
	}

	if err = cn.wrtBuf.Flush(); err != nil {
		return nil, err
	}

	resp, _, err = getResponse(cn.rc, cn.hdrBuf)
	cn.healthy = !isFatal(err)
	return resp, err
}

// Get is return an item for provided key.
func (c *Client) Get(ctx context.Context, key string) (_ *Response, err error) {
	const methodName = "Get"
	timer := time.Now()
	defer c.writeMethodDiagnostics(methodName, timer, &err)

	if !legalKey(key) {
		return nil, ErrMalformedKey
	}

	node, find := c.hr.Get(key)
	if !find {
		return nil, ErrNoServers
	}

	logDebugSingleKey(ctx, methodName, node, key)

	cn, err := c.getConnForNode(ctx, node)
	if err != nil {
		return nil, err
	}

	req := &Request{
		Opcode: GET,
		Opaque: c.getOpaque(),
		Key:    []byte(key),
	}
	req.prepareExtras(0, 0, 0)

	return c.send(cn, req)
}

// Delete is a deletes the element with the provided key.
// If the element does not exist, an ErrCacheMiss error is returned.
func (c *Client) Delete(ctx context.Context, key string) (_ *Response, err error) {
	const methodName = "Delete"
	timer := time.Now()
	defer c.writeMethodDiagnostics(methodName, timer, &err)

	if !legalKey(key) {
		return nil, ErrMalformedKey
	}

	node, find := c.hr.Get(key)
	if !find {
		return nil, ErrNoServers
	}

	logDebugSingleKey(ctx, methodName, node, key)

	cn, err := c.getConnForNode(ctx, node)
	if err != nil {
		return nil, err
	}

	req := &Request{
		Opcode: DELETE,
		Opaque: c.getOpaque(),
		Key:    []byte(key),
	}
	req.prepareExtras(0, 0, 0)

	return c.send(cn, req)
}

// Delta is an atomically increments/decrements value by delta. The return value is
// the new value after being incremented/decrements or an error.
func (c *Client) Delta(ctx context.Context, deltaMode DeltaMode, key string, delta, initial uint64, exp uint32) (newValue uint64, err error) {
	const methodName = "Delta"
	timer := time.Now()
	defer c.writeMethodDiagnostics(methodName, timer, &err)

	if !legalKey(key) {
		return 0, ErrMalformedKey
	}

	node, find := c.hr.Get(key)
	if !find {
		return 0, ErrNoServers
	}

	logDebugSingleKey(ctx, methodName, node, key)

	cn, err := c.getConnForNode(ctx, node)
	if err != nil {
		return 0, err
	}

	req := &Request{
		Opcode: deltaMode.Resolve(),
		Key:    []byte(key),
	}
	req.prepareExtras(exp, delta, initial)

	resp, err := c.send(cn, req)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(resp.Body), nil
}

// Append is an appends/prepends the given item to the existing item, if a value already
// exists for its key. ErrNotStored is returned if that condition is not met.
func (c *Client) Append(ctx context.Context, appendMode AppendMode, key string, data []byte) (_ *Response, err error) {
	const methodName = "Append"
	timer := time.Now()
	defer c.writeMethodDiagnostics(methodName, timer, &err)

	if !legalKey(key) {
		return nil, ErrMalformedKey
	}

	node, find := c.hr.Get(key)
	if !find {
		return nil, ErrNoServers
	}

	logDebugSingleKey(ctx, methodName, node, key)

	cn, err := c.getConnForNode(ctx, node)
	if err != nil {
		return nil, err
	}

	req := &Request{
		Opcode: appendMode.Resolve(),
		Opaque: c.getOpaque(),
		Key:    []byte(key),
		Body:   data,
	}
	req.prepareExtras(0, 0, 0)

	return c.send(cn, req)
}

// FlushAll is a deletes all items in the cache.
// A non-nil error returned by Join implements the Unwrap() []error method.
func (c *Client) FlushAll(ctx context.Context, exp uint32) (err error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	timerMethod := time.Now()
	defer c.writeMethodDiagnostics("FlushAll", timerMethod, &err)

	type retElem struct {
		err error
	}

	var (
		wg = new(sync.WaitGroup)

		nodes = c.hr.GetAllNodes()
		retCh = make(chan retElem, len(nodes))
	)

	for _, node := range nodes {
		wg.Add(1)
		go func(node any, exp uint32) {
			defer wg.Done()

			if ctx.Err() != nil {
				return
			}

			var cnErr error
			cn, nErr := c.getConnForNode(ctx, node)
			if nErr != nil {
				retCh <- retElem{err: nErr}
				return
			}
			defer cn.condRelease(&cnErr)

			req := &Request{
				Opcode: FLUSH,
			}
			req.prepareExtras(exp, 0, 0)

			_, cnErr = transmitRequest(cn.wrtBuf, req)
			if cnErr != nil {
				cn.healthy = false
				retCh <- retElem{err: cnErr}
				return
			}

			if cnErr = cn.wrtBuf.Flush(); cnErr != nil {
				logger.Errorf("%s. %s", ErrServerError.Error(), cnErr.Error())
				return
			}

			_, _, cnErr = getResponse(cn.rc, cn.hdrBuf)
			if cnErr != nil {
				if isFatal(cnErr) {
					cn.healthy = false
					logger.Errorf("%s. %s", ErrServerError.Error(), cnErr.Error())
					return
				}
			}
		}(node, exp)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}

	var multiErr error
	close(retCh)
	for el := range retCh {
		if el.err != nil {
			multiErr = errors.Join(multiErr, el.err)
		}
	}

	return multiErr
}

// MultiGet is a batch version of Get.The returned map from keys to
// items may have fewer elements than the input slice, due to memcached
// cache misses.Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
// A non-nil error returned by Join implements the Unwrap() []error method.
func (c *Client) MultiGet(ctx context.Context, keys []string) (_ map[string][]byte, err error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	ret := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return ret, nil
	}

	const methodName = "MultiGet"
	timerMethod := time.Now()
	defer c.writeMethodDiagnostics(methodName, timerMethod, &err)

	if len(keys) == 1 {
		var res *Response
		res, err = c.Get(ctx, keys[0])
		if res != nil {
			switch res.Status {
			case SUCCESS:
				ret[keys[0]] = res.Body
			case KEY_ENOENT:
				// MultiGet never returns a ENOENT
				err = nil
			}
		}
		return ret, err
	}

	type retElem struct {
		key  string
		body []byte
		err  error
	}

	var (
		wg    = new(sync.WaitGroup)
		retCh = make(chan retElem, len(keys))
	)

	nodes, err := getNodesForKeys(ctx, c.hr, keys)
	if err != nil {
		return ret, err
	}

	logDebugNodes(ctx, methodName, nodes)

	for node, ks := range nodes {
		wg.Add(1)
		go func(node any, keys []string) {
			defer wg.Done()

			if ctx.Err() != nil {
				return
			}

			var cnErr error
			cn, nErr := c.getConnForNode(ctx, node)
			if nErr != nil {
				retCh <- retElem{err: nErr}
				return
			}
			defer cn.condRelease(&cnErr)

			idToKey := make(map[uint32]string, len(keys))

			for _, key := range keys {
				if ctx.Err() != nil {
					return
				}

				opaqueGet := c.getOpaque()
				req := &Request{
					Opcode: GETQ,
					Opaque: opaqueGet,
					Key:    []byte(key),
				}
				req.prepareExtras(0, 0, 0)

				_, cnErr = transmitRequest(cn.wrtBuf, req)
				if cnErr != nil {
					cn.healthy = false
					retCh <- retElem{err: cnErr}
					return
				}

				idToKey[opaqueGet] = key
			}

			opaqueNOOP := c.getOpaque()
			req := &Request{
				Opcode: NOOP,
				Opaque: opaqueNOOP,
			}
			req.prepareExtras(0, 0, 0)

			_, cnErr = transmitRequest(cn.wrtBuf, req)
			if cnErr != nil {
				cn.healthy = false
				retCh <- retElem{err: cnErr}
				return
			}

			if cnErr = cn.wrtBuf.Flush(); cnErr != nil {
				logger.Errorf("%s. %s", ErrServerError.Error(), cnErr.Error())
				return
			}

			for {
				if ctx.Err() != nil {
					return
				}

				var resp *Response
				resp, _, cnErr = getResponse(cn.rc, cn.hdrBuf)
				if isFatal(cnErr) {
					cn.healthy = false
					logger.Errorf("%s. %s", ErrServerError.Error(), cnErr.Error())
					return
				}

				if resp.Opcode == NOOP && resp.Opaque == opaqueNOOP {
					break
				}

				if key, ok := idToKey[resp.Opaque]; ok && cnErr == nil {
					retCh <- retElem{key, resp.Body, nil}
				}
			}
		}(node, ks)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-done:
	}

	var multiErr error
	close(retCh)
	for el := range retCh {
		if el.err != nil {
			multiErr = errors.Join(multiErr, el.err)
		} else {
			ret[el.key] = el.body
		}
	}

	return ret, multiErr
}

// MultiStore is a batch version of Store.
// Writes the provided items with expiration.
//
// A non-nil error returned by Join implements the Unwrap() []error method.
func (c *Client) MultiStore(ctx context.Context, storeMode StoreMode, items map[string][]byte, exp uint32) (err error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if len(items) == 0 {
		return nil
	}

	const methodName = "MultiStore"
	timerMethod := time.Now()
	defer c.writeMethodDiagnostics(methodName, timerMethod, &err)

	type retElem struct {
		err error
	}

	var (
		wg    = new(sync.WaitGroup)
		retCh = make(chan retElem, len(items))
	)

	quietCode := storeMode.Resolve().changeOnQuiet(SETQ)

	keys := maps.Keys(items)
	nodes, err := getNodesForKeys(ctx, c.hr, keys)
	if err != nil {
		return err
	}

	logDebugNodes(ctx, methodName, nodes)

	for node, ks := range nodes {
		kv := utils.PickByKeys(items, ks)
		wg.Add(1)
		go func(node any, keys []string, itemsByNode map[string][]byte, exp uint32) {
			defer wg.Done()

			if ctx.Err() != nil {
				return
			}

			var cnErr error
			cn, nErr := c.getConnForNode(ctx, node)
			if nErr != nil {
				retCh <- retElem{err: nErr}
				return
			}
			defer cn.condRelease(&cnErr)

			idToKey := make(map[uint32]string, len(keys))

			for _, key := range keys {
				if ctx.Err() != nil {
					return
				}

				opaqueStore := c.getOpaque()
				req := &Request{
					Opcode: quietCode,
					Opaque: opaqueStore,
					Key:    []byte(key),
					Body:   itemsByNode[key],
				}
				req.prepareExtras(exp, 0, 0)

				_, cnErr = transmitRequest(cn.wrtBuf, req)
				if cnErr != nil {
					cn.healthy = false
					retCh <- retElem{err: cnErr}
					return
				}

				c.writeItemSizeDiagnostics(methodName, req.size())

				idToKey[opaqueStore] = key
			}

			opaqueNOOP := c.getOpaque()
			req := &Request{
				Opcode: NOOP,
				Opaque: opaqueNOOP,
			}
			req.prepareExtras(0, 0, 0)

			_, cnErr = transmitRequest(cn.wrtBuf, req)
			if cnErr != nil {
				cn.healthy = false
				retCh <- retElem{err: cnErr}
				return
			}

			if cnErr = cn.wrtBuf.Flush(); cnErr != nil {
				logger.Errorf("%s. %s", ErrServerError.Error(), cnErr.Error())
				return
			}

			for {
				if ctx.Err() != nil {
					return
				}

				var resp *Response
				resp, _, cnErr = getResponse(cn.rc, cn.hdrBuf)
				if isFatal(cnErr) {
					cn.healthy = false
					retCh <- retElem{err: cnErr}
					return
				}

				if resp.Opcode == NOOP && resp.Opaque == opaqueNOOP {
					break
				}

				if key, ok := idToKey[resp.Opaque]; ok {
					if resp.Status != SUCCESS {
						retCh <- retElem{err: fmt.Errorf("status - %s ; error - %w ; key - %s", resp.Status, resp, key)}
					}
				}
			}
		}(node, ks, kv, exp)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}

	var multiErr error
	close(retCh)
	for el := range retCh {
		if el.err != nil {
			multiErr = errors.Join(multiErr, el.err)
		}
	}

	return multiErr
}

// MultiDelete is a batch version of Delete.
// Deletes the items with the provided keys.
//
// If there is a key in the provided keys that is missing in the cache,
// the ErrCacheMiss error is ignored.
// A non-nil error returned by Join implements the Unwrap() []error method.
func (c *Client) MultiDelete(ctx context.Context, keys []string) (err error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if len(keys) == 0 {
		return nil
	}

	const methodName = "MultiDelete"
	timerMethod := time.Now()
	defer c.writeMethodDiagnostics(methodName, timerMethod, &err)

	type retElem struct {
		err error
	}

	var (
		wg    = new(sync.WaitGroup)
		retCh = make(chan retElem, len(keys))
	)

	nodes, err := getNodesForKeys(ctx, c.hr, keys)
	if err != nil {
		return err
	}

	logDebugNodes(ctx, methodName, nodes)

	for node, ks := range nodes {
		wg.Add(1)
		go func(node any, keys []string) {
			defer wg.Done()

			if ctx.Err() != nil {
				return
			}

			var cnErr error
			cn, nErr := c.getConnForNode(ctx, node)
			if nErr != nil {
				retCh <- retElem{err: nErr}
				return
			}
			defer cn.condRelease(&cnErr)

			idToKey := make(map[uint32]string, len(keys))

			for _, key := range keys {
				if ctx.Err() != nil {
					return
				}

				opaqueDel := c.getOpaque()
				req := &Request{
					Opcode: DELETEQ,
					Opaque: opaqueDel,
					Key:    []byte(key),
				}
				req.prepareExtras(0, 0, 0)

				_, cnErr = transmitRequest(cn.wrtBuf, req)
				if cnErr != nil {
					cn.healthy = false
					retCh <- retElem{err: cnErr}
					return
				}

				idToKey[opaqueDel] = key
			}

			opaqueNOOP := c.getOpaque()
			req := &Request{
				Opcode: NOOP,
				Opaque: opaqueNOOP,
			}
			req.prepareExtras(0, 0, 0)

			_, cnErr = transmitRequest(cn.wrtBuf, req)
			if cnErr != nil {
				cn.healthy = false
				retCh <- retElem{err: cnErr}
				return
			}

			if cnErr = cn.wrtBuf.Flush(); cnErr != nil {
				logger.Errorf("%s. %s", ErrServerError.Error(), cnErr.Error())
				return
			}

			for {
				if ctx.Err() != nil {
					return
				}

				var resp *Response
				resp, _, cnErr = getResponse(cn.rc, cn.hdrBuf)
				if isFatal(cnErr) {
					cn.healthy = false
					retCh <- retElem{err: cnErr}
					return
				}

				if resp.Opcode == NOOP && resp.Opaque == opaqueNOOP {
					break
				}

				if key, ok := idToKey[resp.Opaque]; ok {
					if resp.Status != SUCCESS && resp.Status != KEY_ENOENT {
						retCh <- retElem{err: fmt.Errorf("status - %s ; error - %w ; key - %s", resp.Status, resp, key)}
					}
				}
			}
		}(node, ks)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}

	var multiErr error
	close(retCh)
	for el := range retCh {
		if el.err != nil {
			multiErr = errors.Join(multiErr, el.err)
		}
	}

	return multiErr
}

// CloseAllConns is close all opened connection per shards.
// Once closed, resources should be released.
func (c *Client) CloseAllConns(ctx context.Context) error {
	c.fmu.Lock()
	defer c.fmu.Unlock()

	for addr, connPool := range c.freeConns {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		connPool.Destroy()
		delete(c.freeConns, addr)
	}

	return nil
}

// CloseAvailableConnsInAllShardPools - removes the specified number of connections from the pools of all shards.
func (c *Client) CloseAvailableConnsInAllShardPools(ctx context.Context, numOfClose int) (int, error) {
	var closed int

	c.fmu.Lock()
	defer c.fmu.Unlock()

	for _, p := range c.freeConns {
		for i := 0; i < numOfClose; i++ {
			if connRaw, ok := p.Pop(); ok {

				if ctx.Err() != nil {
					return closed, ctx.Err()
				}

				p.Close(connRaw)
				closed++
			}
		}
	}

	return closed, nil
}

func (c *Client) writeMethodDiagnostics(methodName string, timer time.Time, err *error) {
	if methodName == "" || c.disableMemcachedDiagnostic {
		return
	}

	observeMethodDurationSeconds(methodName, time.Since(timer).Seconds(), *err == nil)
}

func (c *Client) writeItemSizeDiagnostics(methodName string, size int) {
	if methodName == "" || c.disableMemcachedDiagnostic {
		return
	}

	observeObjectSizeBytes(methodName, float64(size))
}

func (c *Client) authenticate(cn *conn) (ok bool, err error) {
	req := &Request{
		Key:  []byte(SaslMechanism),
		Body: c.authData,
	}

	req.Opcode = SASL_AUTH
	_, err = transmitRequest(cn.wrtBuf, req)
	if err != nil {
		return
	}

	if err = cn.wrtBuf.Flush(); err != nil {
		return
	}

	resp, _, err := getResponse(cn.rc, cn.hdrBuf)
	if errors.Is(err, ErrNoServers) {
		return false, err
	}
	if err == nil {
		return true, nil
	}
	if resp != nil && resp.Status != FURTHER_AUTH {
		return false, fmt.Errorf("error from sasl auth - %s", resp.Error())
	}

	req.Opcode = SASL_STEP
	_, err = transmitRequest(cn.wrtBuf, req)
	if err != nil {
		logger.Errorf("%s, %s", ErrServerError.Error(), err.Error())
		return
	}

	resp, _, err = getResponse(cn.rc, cn.hdrBuf)
	if err != nil {
		logger.Errorf("%s: Error from sasl step - %v", libPrefix, resp)
		return
	}

	if err = cn.wrtBuf.Flush(); err != nil {
		logger.Errorf("%s, %s", ErrServerError.Error(), err.Error())
		return
	}

	return true, nil
}

func legalKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] == 0x7f {
			return false
		}
	}
	return true
}

// getNodesForKeys return a map where key is a node and value is a suitable keys
func getNodesForKeys(ctx context.Context, hr consistenthash.ConsistentHash, keys []string) (map[any][]string, error) {
	resp := make(map[any][]string, hr.GetNodesCount())

	for _, key := range keys {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if !legalKey(key) {
			return nil, fmt.Errorf("%w. Invalid key - %v", ErrMalformedKey, key)
		}
		if node, found := hr.Get(key); found {
			resp[node] = append(resp[node], key)
		}
	}

	return resp, nil
}
