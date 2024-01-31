package memcached

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/aliexpressru/gomemcached/consistenthash"
	"github.com/aliexpressru/gomemcached/logger"
	"github.com/aliexpressru/gomemcached/utils"
)

func Test_getNodes(t *testing.T) {
	type args struct {
		cfg  *config
		mock *network
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "Servers",
			args: args{
				mock: &network{lookupHost: func(host string) (addrs []string, err error) {
					return []string{"server1:11211", "server2:11211"}, nil
				}},
				cfg: &config{
					Servers: []string{"server1:11211", "server2:11211"},
				}},
			want: []string{"server1:11211", "server2:11211"},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("getNodes have error - %v", err)
					return false
				}
				return true
			},
		},
		{
			name: "Headless",
			args: args{
				mock: &network{lookupHost: func(host string) (addrs []string, err error) {
					return []string{"93.184.216.34", "123.323.32.11"}, nil
				}},
				cfg: &config{
					HeadlessServiceAddress: "example.com",
					MemcachedPort:          11211,
				}},
			want: []string{"93.184.216.34:11211", "123.323.32.11:11211"},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("getNodes have error - %v", err)
					return false
				}
				return true
			},
		},
		{
			name: "config nil",
			args: args{
				mock: &network{lookupHost: func(_ string) (_ []string, _ error) {
					return
				}},
				cfg: nil},
			want: []string{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					t.Errorf("getNodes have error - %v", err)
					return false
				}
				return true
			},
		},
		{
			name: "error",
			args: args{
				mock: &network{lookupHost: func(host string) (addrs []string, err error) {
					return nil, &net.DNSError{
						Err:  "no such host",
						Name: "fakeaddress.r",
					}
				}},
				cfg: &config{HeadlessServiceAddress: "fakeaddress.r"}},
			want: nil,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					dnsError := new(net.DNSError)
					assert.ErrorAs(t, err, &dnsError, "Error should be as net.DNSError")
					return true
				}
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getNodes(tt.args.mock.lookupHost, tt.args.cfg)
			if !tt.wantErr(t, err) {
				return
			}
			assert.Equalf(t, tt.want, got, "getNodes(%v)", tt.args.cfg)
		})
	}
}

func Test_safeGetDeadNodes(t *testing.T) {
	client := &Client{
		deadNodes: map[string]struct{}{
			"node1": {},
			"node2": {},
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		deadNodes := client.safeGetDeadNodes()

		expectedDeadNodes := map[string]struct{}{
			"node1": {},
			"node2": {},
		}
		assert.Equal(t, expectedDeadNodes, deadNodes)
	}()
	go func() {
		defer wg.Done()
		deadNodes := client.safeGetDeadNodes()

		expectedDeadNodes := map[string]struct{}{
			"node1": {},
			"node2": {},
		}
		assert.Equal(t, expectedDeadNodes, deadNodes)
	}()

	wg.Wait()
}

func Test_safeAddToDeadNodes(t *testing.T) {
	client := &Client{
		deadNodes: map[string]struct{}{},
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		client.safeAddToDeadNodes("node1")
	}()
	go func() {
		defer wg.Done()
		client.safeAddToDeadNodes("node2")
	}()

	wg.Wait()

	assert.Contains(t, client.deadNodes, "node1")
	assert.Contains(t, client.deadNodes, "node2")
}

func Test_safeRemoveFromDeadNodes(t *testing.T) {
	client := &Client{
		deadNodes: map[string]struct{}{
			"node1": {},
			"node2": {},
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		client.safeRemoveFromDeadNodes("node1")
	}()
	go func() {
		defer wg.Done()
		client.safeRemoveFromDeadNodes("node2")
	}()

	wg.Wait()

	assert.NotContains(t, client.deadNodes, "node1")
	assert.NotContains(t, client.deadNodes, "node2")
}

func Test_nodeIsDead(t *testing.T) {
	logger.DisableLogger()
	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}

	mockNetworkError := new(MockNetworkOperations)
	client := &Client{nw: &network{
		dialTimeout: mockNetworkError.DialTimeout,
	}}

	assert.True(t, client.nodeIsDead("wrongarrd.r"), "nodeIsDead: wrong addr should be return true")

	expectedErr := errors.New("mocked dial error")

	mockNetworkError.On("DialTimeout", addr.Network(), addr.String(), client.netTimeout()).Return(nil, expectedErr)

	result := client.nodeIsDead(addr)

	assert.True(t, result)

	mockNetworkError.AssertCalled(t, "DialTimeout", addr.Network(), addr.String(), client.netTimeout())

	mockNetworkRetry := new(MockNetworkOperations)
	client = &Client{nw: &network{
		dialTimeout: mockNetworkRetry.DialTimeout,
	}}

	expectedErr = &ConnectTimeoutError{addr}

	mockNetworkRetry.On("DialTimeout", addr.Network(), addr.String(), client.netTimeout()).Return(nil, expectedErr)
	result = client.nodeIsDead(addr)

	assert.True(t, result)

	// int(DefaultRetryCountForConn)+1 - the default number of retries plus the first execution.
	mockNetworkRetry.AssertNumberOfCalls(t, "DialTimeout", int(DefaultRetryCountForConn)+1)

	mockNetworkSuccess := new(MockNetworkOperations)
	client = &Client{nw: &network{
		dialTimeout: mockNetworkSuccess.DialTimeout,
	}}

	mockNetworkSuccess.On("DialTimeout", addr.Network(), addr.String(), client.netTimeout()).Return(&FakeConn{}, nil)

	result = client.nodeIsDead(addr)

	assert.False(t, result)

	mockNetworkSuccess.AssertCalled(t, "DialTimeout", addr.Network(), addr.String(), client.netTimeout())
}

func Test_initNodesProvider(t *testing.T) {
	var (
		mockNetworkErr = new(MockNetworkOperations)

		period = 10 * time.Millisecond

		ctx, cancel = context.WithCancel(context.TODO())
		expectedErr = errors.New("mocked dial error")
	)
	cl := &Client{
		ctx: ctx,
		nw: &network{
			dial:       mockNetworkErr.Dial,
			lookupHost: mockNetworkErr.LookupHost,
		},
		cfg: &config{
			HeadlessServiceAddress: "example.com",
		},
		nodeHCPeriod: period,
		nodeRBPeriod: period,
	}

	mockNetworkErr.On("LookupHost", cl.cfg.HeadlessServiceAddress).Return(nil, expectedErr)
	mockNetworkErr.On("Dial", mock.Anything, mock.Anything).Return(&FakeConn{}, nil)

	cl.initNodesProvider()

	mockNetworkErr.AssertNotCalled(t, "Dial")

	count := 4
	<-time.After(period * time.Duration(count))
	cancel()

	mockNetworkErr.AssertNumberOfCalls(t, "LookupHost", 2*count-2)
}

func Test_checkNodesHealth(t *testing.T) {
	var (
		mockNetworkErr = new(MockNetworkOperations)

		expectedErr = errors.New("mocked dial error")
	)
	cl := &Client{
		nw: &network{
			dial:       mockNetworkErr.Dial,
			lookupHost: mockNetworkErr.LookupHost,
		},
		cfg: &config{
			HeadlessServiceAddress: "example.com",
		},
	}

	mockNetworkErr.On("LookupHost", cl.cfg.HeadlessServiceAddress).Return(nil, expectedErr)
	mockNetworkErr.On("Dial", mock.Anything, mock.Anything).Return(&FakeConn{}, nil)

	cl.checkNodesHealth()

	mockNetworkErr.AssertNotCalled(t, "Dial")
	mockNetworkErr.AssertNumberOfCalls(t, "LookupHost", 1)

	var (
		currentNodes     = []string{"127.0.0.1:12345", "127.0.0.2:12345", "127.0.0.3:12345", "127.0.0.4:12345", "127.0.0.5:12345"}
		alreadyDeadNodes = []string{"127.0.0.4:12345", "127.0.0.5:12345"}
		disableNodes     = []string{"127.0.0.6:12345"}

		mockNetwork = new(MockNetworkOperations)
	)

	cl = &Client{
		hr:      consistenthash.NewHashRing(),
		timeout: -1,
		nw: &network{
			dial:       mockNetwork.Dial,
			lookupHost: mockNetwork.LookupHost,
		},
		cfg: &config{
			Servers: currentNodes,
		},
	}

	mockNetwork.On("Dial", "tcp", "127.0.0.2:12345").Return(nil, expectedErr).Once()
	mockNetwork.On("Dial", "tcp", "127.0.0.4:12345").Return(nil, expectedErr).Once()
	mockNetwork.On("Dial", mock.Anything, mock.Anything).Return(&FakeConn{}, nil)

	for _, node := range currentNodes {
		addr, _ := utils.AddrRepr(node)
		cl.hr.Add(addr)
	}
	cl.deadNodes = make(map[string]struct{})
	for _, node := range alreadyDeadNodes {
		cl.deadNodes[node] = struct{}{}
	}
	for _, node := range disableNodes {
		cl.deadNodes[node] = struct{}{}
	}

	cl.checkNodesHealth()

	assert.Equal(t, 3, len(cl.hr.GetAllNodes()))
	assert.Equal(t, 2, len(cl.deadNodes))
}

func Test_rebuildNodes(t *testing.T) {
	var (
		mockNetworkErr = new(MockNetworkOperations)

		expectedErr = errors.New("mocked dial error")
	)
	cl := &Client{
		nw: &network{
			dial:       mockNetworkErr.Dial,
			lookupHost: mockNetworkErr.LookupHost,
		},
		cfg: &config{
			HeadlessServiceAddress: "example.com",
		},
	}

	mockNetworkErr.On("LookupHost", cl.cfg.HeadlessServiceAddress).Return(nil, expectedErr)
	mockNetworkErr.On("Dial", mock.Anything, mock.Anything).Return(&FakeConn{}, nil)

	cl.rebuildNodes()

	mockNetworkErr.AssertNotCalled(t, "Dial")
	mockNetworkErr.AssertNumberOfCalls(t, "LookupHost", 1)

	var (
		currentNodes        = []string{"127.0.0.1:12345", "127.0.0.2:12345", "127.0.0.3:12345", "127.0.0.4:12345", "127.0.0.5:12345"}
		alreadyDeadNodes    = []string{"127.0.0.4:12345", "127.0.0.2:12345"}
		expectedNodesInRing = []string{"127.0.0.1:12345", "127.0.0.3:12345", "127.0.0.5:12345"}

		mockNetwork = new(MockNetworkOperations)
	)
	cl = &Client{
		ctx: context.TODO(),
		nw: &network{
			dial:       mockNetwork.Dial,
			lookupHost: mockNetwork.LookupHost,
		},
		cfg: &config{
			Servers: currentNodes,
		},
		timeout:      -1,
		maxIdleConns: 1,
		hr:           consistenthash.NewHashRing(),
	}

	mockNetwork.On("LookupHost", cl.cfg.Servers).Return(currentNodes, nil)
	mockNetwork.On("Dial", mock.Anything, mock.Anything).Return(&FakeConn{}, nil)

	cl.deadNodes = make(map[string]struct{})
	for _, node := range alreadyDeadNodes {
		cl.deadNodes[node] = struct{}{}
	}
	// len(currentNodes)-1 simulates the absence of one node in the hash ring.
	for i := 0; i < len(currentNodes)-1; i++ {
		addr, _ := utils.AddrRepr(currentNodes[i])
		cl.hr.Add(addr)
	}

	// simulating the borrowing of a connection from the connection pool by taking a connection and putting it back.
	// for test CloseAvailableConnsInAllShardPools
	for i := 0; i < len(currentNodes)-1; i++ {
		node, ok := cl.hr.Get(currentNodes[i])
		require.Truef(t, ok, "Not found node (%s) in hash ring", currentNodes[i])
		cn, err := cl.getConnForNode(node)
		require.Nil(t, err, "getConnForNode try get conn")
		cn.condRelease(new(error))
	}

	cl.rebuildNodes()

	assert.Equal(t, 3, cl.hr.GetNodesCount())

	var actualNodesInRing []string
	for _, node := range cl.hr.GetAllNodes() {
		actualNodesInRing = append(actualNodesInRing, node.(net.Addr).String())
	}

	slices.Sort(actualNodesInRing)
	slices.Sort(expectedNodesInRing)
	assert.Equal(t, expectedNodesInRing, actualNodesInRing)

	for _, pool := range cl.freeConns {
		assert.Equal(t, 0, pool.Len())
	}
}

type MockNetworkOperations struct {
	mock.Mock
}

func (m *MockNetworkOperations) Dial(network, address string) (net.Conn, error) {
	args := m.Called(network, address)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(net.Conn), args.Error(1)
}

func (m *MockNetworkOperations) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	args := m.Called(network, address, timeout)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(net.Conn), args.Error(1)
}

func (m *MockNetworkOperations) LookupHost(host string) ([]string, error) {
	args := m.Called(host)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

type FakeConn struct {
	net.TCPConn
}

func (f *FakeConn) Read(_ []byte) (n int, err error) {
	return 0, nil
}

func (f *FakeConn) Write(_ []byte) (n int, err error) {
	return 0, nil
}

func (f *FakeConn) Close() error {
	return nil
}
