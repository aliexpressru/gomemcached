package memcached

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
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
		Store(storeMode StoreMode, key string, exp uint32, body []byte) (*Response, error)
		Get(key string) (*Response, error)
		Delete(key string) (*Response, error)
		Delta(deltaMode DeltaMode, key string, delta, initial uint64, exp uint32) (newValue uint64, err error)
		Append(appendMode AppendMode, key string, data []byte) (*Response, error)
		FlushAll(exp uint32) error
		MultiDelete(keys []string) error
		MultiStore(storeMode StoreMode, items map[string][]byte, exp uint32) error
		MultiGet(keys []string) (map[string][]byte, error)

		CloseAllConns()
		CloseAvailableConnsInAllShardPools(numOfClose int) int
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

		// disableMemcachedDiagnostic - is flag for turn off write metrics from lib.
		disableMemcachedDiagnostic bool
		// disableNodeProvider - is flag for turn off rebuild and health check nodes.
		disableNodeProvider bool
		// disableRefreshConns - is flag for turn off to refresh conns in the pool.
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
		// Servers List of servers with hosted memcached
		Servers []string `envconfig:"MEMCACHED_SERVERS"`
		// MemcachedPort The optional port override for cases when memcached IP addresses are obtained from headless service.
		MemcachedPort int `envconfig:"MEMCACHED_PORT" default:"11211"`
	}
	conn struct {
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
func InitFromEnv(opts ...Option) (*Client, error) {
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

	if op.Client.nw == nil {
		op.Client.nw = &network{
			dial:        net.Dial,
			dialTimeout: net.DialTimeout,
			lookupHost:  net.LookupHost,
		}
	}
	if op.Client.hr == nil {
		op.Client.hr = consistenthash.NewHashRing()
	}
	if op.Client.ctx == nil {
		op.Client.ctx = context.Background()
	}
	if op.Client.opaque == nil {
		op.Client.opaque = new(uint32)
	}
	if op.disableLogger {
		logger.DisableLogger()
	}

	return newFromConfig(op)
}

func newForTests(servers ...string) (*Client, error) {
	hr := consistenthash.NewHashRing()
	for _, s := range servers {
		addr, err := utils.AddrRepr(s)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrInvalidAddr, err.Error())
		}
		hr.Add(addr)
	}
	cm := &Client{
		ctx:                        context.Background(),
		opaque:                     new(uint32),
		hr:                         hr,
		disableMemcachedDiagnostic: true,
		nw: &network{
			dial:        net.Dial,
			dialTimeout: net.DialTimeout,
			lookupHost:  net.LookupHost,
		},
	}

	return cm, nil
}

func newFromConfig(op *options) (*Client, error) {
	if op.cfg != nil && !(op.cfg.HeadlessServiceAddress != "" || len(op.cfg.Servers) != 0) {
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
		cn.release()
	} else {
		cn.close()
	}
}

func (c *Client) getOpaque() uint32 {
	atomic.CompareAndSwapUint32(c.opaque, math.MaxUint32, uint32(0))
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

func (c *Client) getFreeConn(addr net.Addr) (*conn, error) {
	connPool := c.safeGetOrInitFreeConn(addr)

	connRaw, err := connPool.Get()
	if err != nil {
		return nil, fmt.Errorf("%s: Get from pool error - %w", libPrefix, err)
	}

	cn := connRaw.(*conn)

	if c.authEnable && !cn.authed {
		if c.authenticate(cn) {
			cn.authed = true
			return cn, nil
		} else {
			return nil, ErrAuthFail
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

func (c *Client) getConnForNode(node any) (*conn, error) {
	addr, ok := node.(net.Addr)
	if !ok {
		return nil, ErrInvalidAddr
	}
	cn, err := c.getFreeConn(addr)
	if err != nil {
		return nil, err
	}

	return cn, nil
}

// Store is a wrote the provided item with expiration.
func (c *Client) Store(storeMode StoreMode, key string, exp uint32, body []byte) (_ *Response, err error) {
	timer := time.Now()
	defer c.writeMethodDiagnostics("Store", timer, &err)

	if !legalKey(key) {
		return nil, ErrMalformedKey
	}

	node, find := c.hr.Get(key)
	if !find {
		return nil, ErrNoServers
	}

	cn, err := c.getConnForNode(node)
	if err != nil {
		return nil, err
	}
	return c.store(cn, storeMode.Resolve(), key, exp, c.getOpaque(), body)
}

func (c *Client) store(cn *conn, opcode OpCode, key string, exp, opaque uint32, body []byte) (*Response, error) {
	req := &Request{
		Opcode: opcode,
		Key:    []byte(key),
		Opaque: opaque,
		Body:   body,
	}
	req.prepareExtras(exp, 0, 0)
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
func (c *Client) Get(key string) (_ *Response, err error) {
	timer := time.Now()
	defer c.writeMethodDiagnostics("Get", timer, &err)

	if !legalKey(key) {
		return nil, ErrMalformedKey
	}

	node, find := c.hr.Get(key)
	if !find {
		return nil, ErrNoServers
	}

	cn, err := c.getConnForNode(node)
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
func (c *Client) Delete(key string) (_ *Response, err error) {
	timer := time.Now()
	defer c.writeMethodDiagnostics("Delete", timer, &err)

	if !legalKey(key) {
		return nil, ErrMalformedKey
	}

	node, find := c.hr.Get(key)
	if !find {
		return nil, ErrNoServers
	}

	cn, err := c.getConnForNode(node)
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
func (c *Client) Delta(deltaMode DeltaMode, key string, delta, initial uint64, exp uint32) (newValue uint64, err error) {
	timer := time.Now()
	defer c.writeMethodDiagnostics("Delta", timer, &err)

	if !legalKey(key) {
		return 0, ErrMalformedKey
	}

	node, find := c.hr.Get(key)
	if !find {
		return 0, ErrNoServers
	}

	cn, err := c.getConnForNode(node)
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
func (c *Client) Append(appendMode AppendMode, key string, data []byte) (_ *Response, err error) {
	timer := time.Now()
	defer c.writeMethodDiagnostics("Append", timer, &err)

	if !legalKey(key) {
		return nil, ErrMalformedKey
	}

	node, find := c.hr.Get(key)
	if !find {
		return nil, ErrNoServers
	}

	cn, err := c.getConnForNode(node)
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
func (c *Client) FlushAll(exp uint32) (err error) {
	timerMethod := time.Now()
	defer c.writeMethodDiagnostics("FlushAll", timerMethod, &err)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		multiErr error

		nodes = c.hr.GetAllNodes()
	)

	addToMultiErr := func(e error) {
		mu.Lock()
		defer mu.Unlock()
		multiErr = errors.Join(multiErr, e)
	}

	for _, node := range nodes {
		wg.Add(1)
		go func(node any) {
			defer wg.Done()

			var cn *conn
			cn, err = c.getConnForNode(node)
			if err != nil {
				addToMultiErr(err)
				return
			}
			defer cn.condRelease(&err)

			req := &Request{
				Opcode: FLUSH,
			}
			req.prepareExtras(exp, 0, 0)

			_, err = transmitRequest(cn.wrtBuf, req)
			if err != nil {
				cn.healthy = false
				return
			}

			if err = cn.wrtBuf.Flush(); err != nil {
				logger.Errorf("%s. %s", ErrServerError.Error(), err.Error())
				return
			}

			_, _, err = getResponse(cn.rc, cn.hdrBuf)
			if err != nil {
				if isFatal(err) {
					cn.healthy = false
					return
				}
				addToMultiErr(err)
			}
		}(node)
	}

	wg.Wait()

	return multiErr
}

// MultiGet is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcached
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (c *Client) MultiGet(keys []string) (_ map[string][]byte, err error) {
	var (
		wg sync.WaitGroup
		mu sync.Mutex

		ret = make(map[string][]byte, len(keys))
	)
	if len(keys) == 0 {
		return ret, nil
	}

	timerMethod := time.Now()
	defer c.writeMethodDiagnostics("MultiGet", timerMethod, &err)

	if len(keys) == 1 {
		var res *Response
		res, err = c.Get(keys[0])
		if res != nil {
			if res.Status == SUCCESS {
				ret[keys[0]] = res.Body
			} else if res.Status == KEY_ENOENT {
				// MultiGet never returns a ENOENT
				err = nil
			}
		}
		return ret, err
	}

	var (
		once        sync.Once
		singleError error
	)

	addToRet := func(key string, body []byte) {
		mu.Lock()
		defer mu.Unlock()
		ret[key] = body
	}

	nodes, err := getNodesForKeys(c.hr, keys)
	if err != nil {
		return ret, err
	}

	for node, ks := range nodes {
		wg.Add(1)
		go func(node any, keys []string) {
			defer wg.Done()

			var cnErr error

			cn, nErr := c.getConnForNode(node)
			if nErr != nil {
				once.Do(func() {
					singleError = nErr
				})
				return
			}
			defer cn.condRelease(&cnErr)

			idToKey := make(map[uint32]string, len(keys))

			for _, key := range keys {
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
				return
			}

			if cnErr = cn.wrtBuf.Flush(); err != nil {
				logger.Errorf("%s. %s", ErrServerError.Error(), cnErr.Error())
				return
			}

			for {
				var resp *Response
				resp, _, cnErr = getResponse(cn.rc, cn.hdrBuf)
				if isFatal(cnErr) {
					cn.healthy = false
					return
				}

				if resp.Opcode == NOOP && resp.Opaque == opaqueNOOP {
					break
				}

				if key, ok := idToKey[resp.Opaque]; ok && cnErr == nil {
					addToRet(key, resp.Body)
				}
			}
		}(node, ks)
	}

	wg.Wait()

	return ret, singleError
}

// MultiStore is a batch version of Store.
// Writes the provided items with expiration.
func (c *Client) MultiStore(storeMode StoreMode, items map[string][]byte, exp uint32) (err error) {
	if len(items) == 0 {
		return nil
	}

	timerMethod := time.Now()
	defer c.writeMethodDiagnostics("MultiStore", timerMethod, &err)

	var (
		wg       sync.WaitGroup
		muMErr   sync.Mutex
		multiErr error
	)

	addToMultiErr := func(e error) {
		muMErr.Lock()
		defer muMErr.Unlock()
		multiErr = errors.Join(multiErr, e)
	}

	var muItems sync.RWMutex
	safeGetItems := func(key string) []byte {
		muItems.RLock()
		defer muItems.RUnlock()
		return items[key]
	}

	quietCode := storeMode.Resolve().changeOnQuiet(SETQ)

	keys := maps.Keys(items)
	nodes, err := getNodesForKeys(c.hr, keys)
	if err != nil {
		return err
	}

	for node, ks := range nodes {
		wg.Add(1)
		go func(node any, keys []string, exp uint32) {
			defer wg.Done()

			var cnErr error

			cn, nErr := c.getConnForNode(node)
			if nErr != nil {
				addToMultiErr(nErr)
				return
			}
			defer cn.condRelease(&cnErr)

			idToKey := make(map[uint32]string, len(keys))

			for _, key := range keys {
				opaqueStore := c.getOpaque()
				req := &Request{
					Opcode: quietCode,
					Opaque: opaqueStore,
					Key:    []byte(key),
					Body:   safeGetItems(key),
				}
				req.prepareExtras(exp, 0, 0)

				_, cnErr = transmitRequest(cn.wrtBuf, req)
				if cnErr != nil {
					cn.healthy = false
					return
				}

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
				return
			}

			if cnErr = cn.wrtBuf.Flush(); err != nil {
				logger.Errorf("%s. %s", ErrServerError.Error(), cnErr.Error())
				return
			}

			for {
				var resp *Response
				resp, _, cnErr = getResponse(cn.rc, cn.hdrBuf)
				if isFatal(cnErr) {
					cn.healthy = false
					return
				}

				if resp.Opcode == NOOP && resp.Opaque == opaqueNOOP {
					break
				}

				if key, ok := idToKey[resp.Opaque]; ok {
					if resp.Status != SUCCESS {
						addToMultiErr(fmt.Errorf("%w. Error for key - %s", cnErr, key))
					}
				}
			}
		}(node, ks, exp)
	}

	wg.Wait()

	return multiErr
}

// MultiDelete is a batch version of Delete.
// Deletes the items with the provided keys.
// If there is a key in the provided keys that is missing in the cache,
// the ErrCacheMiss error is ignored.
func (c *Client) MultiDelete(keys []string) (err error) {
	if len(keys) == 0 {
		return nil
	}

	timerMethod := time.Now()
	defer c.writeMethodDiagnostics("MultiDelete", timerMethod, &err)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		multiErr error
	)

	addToMultiErr := func(e error) {
		mu.Lock()
		defer mu.Unlock()
		multiErr = errors.Join(multiErr, e)
	}

	nodes, err := getNodesForKeys(c.hr, keys)
	if err != nil {
		return err
	}

	for node, ks := range nodes {
		wg.Add(1)
		go func(node any, keys []string) {
			defer wg.Done()

			var cnErr error

			cn, nErr := c.getConnForNode(node)
			if nErr != nil {
				addToMultiErr(nErr)
				return
			}
			defer cn.condRelease(&cnErr)

			idToKey := make(map[uint32]string, len(keys))

			for _, key := range keys {
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
				return
			}

			if cnErr = cn.wrtBuf.Flush(); err != nil {
				logger.Errorf("%s. %s", ErrServerError.Error(), cnErr.Error())
				return
			}

			for {
				var resp *Response
				resp, _, cnErr = getResponse(cn.rc, cn.hdrBuf)
				if isFatal(cnErr) {
					cn.healthy = false
					return
				}

				if resp.Opcode == NOOP && resp.Opaque == opaqueNOOP {
					break
				}

				if key, ok := idToKey[resp.Opaque]; ok {
					if resp.Status != SUCCESS && resp.Status != KEY_ENOENT {
						addToMultiErr(fmt.Errorf("%w. Error for key - %s", cnErr, key))
					}
				}
			}
		}(node, ks)
	}

	wg.Wait()

	return multiErr
}

// CloseAllConns is close all opened connection per shards.
// Once closed, resources should be released.
func (c *Client) CloseAllConns() {
	c.fmu.Lock()
	defer c.fmu.Unlock()

	for addr, connPool := range c.freeConns {
		connPool.Destroy()
		delete(c.freeConns, addr)
	}
}

// CloseAvailableConnsInAllShardPools - removes the specified number of connections from the pools of all shards.
func (c *Client) CloseAvailableConnsInAllShardPools(numOfClose int) int {
	var closed int

	c.fmu.Lock()
	defer c.fmu.Unlock()

	for _, p := range c.freeConns {
		for i := 0; i < numOfClose; i++ {
			if connRaw, ok := p.Pop(); ok {
				p.Close(connRaw)
				closed++
			}
		}
	}

	return closed
}

func (c *Client) writeMethodDiagnostics(methodName string, timer time.Time, err *error) {
	if methodName == "" || c.disableMemcachedDiagnostic {
		return
	}

	observeMethodDurationSeconds(methodName, time.Since(timer).Seconds(), *err == nil)
}

func (c *Client) authenticate(cn *conn) (ok bool) {
	req := &Request{
		Key:  []byte(SaslMechanism),
		Body: c.authData,
	}

	req.Opcode = SASL_AUTH
	_, err := transmitRequest(cn.wrtBuf, req)
	if err != nil {
		return
	}

	if err = cn.wrtBuf.Flush(); err != nil {
		return
	}

	resp, _, err := getResponse(cn.rc, cn.hdrBuf)
	if err == nil {
		return true
	}
	if err != nil && resp.Status != FURTHER_AUTH {
		logger.Errorf("%s: Error from sasl auth - %v", libPrefix, resp)
		return
	}

	req.Opcode = SASL_STEP
	_, err = transmitRequest(cn.wrtBuf, req)
	if err != nil {
		return
	}

	resp, _, err = getResponse(cn.rc, cn.hdrBuf)
	if err != nil {
		logger.Errorf("%s: Error from sasl step - %v", libPrefix, resp)
		return
	}

	if err = cn.wrtBuf.Flush(); err != nil {
		return
	}

	return true
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
func getNodesForKeys(hr consistenthash.ConsistentHash, keys []string) (map[any][]string, error) {
	resp := make(map[any][]string, hr.GetNodesCount())

	for _, key := range keys {
		if !legalKey(key) {
			return nil, fmt.Errorf("%w. Invalid key - %v", ErrMalformedKey, key)
		}
		if node, found := hr.Get(key); found {
			resp[node] = append(resp[node], key)
		}
	}

	return resp, nil
}
