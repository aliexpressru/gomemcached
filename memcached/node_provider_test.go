package memcached

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/aliexpressru/gomemcached/logger"
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
	ctx, cancel := context.WithCancel(context.TODO())
	c := &Client{ctx: ctx}
	c.initNodesProvider()

	cancel()
}

func Test_checkNodesHealth(t *testing.T) {
	// addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
	// expectedErr := errors.New("mocked dial error")
	//
	// var (
	// 	currentNodes     = []string{"node1:12345", "node2", "node3:12345", "node4:12345", "node5:12345"}
	// 	alreadyDeadNodes = []string{"node5:12345"}
	// 	disableNodes     = []string{"node6:12345"}
	// )
	//
	// mockNetwork := new(MockNetworkOperations)
	// cl := &Client{
	// 	hr:      consistenthash.NewHashRing(),
	// 	timeout: -1,
	// 	nw: &network{
	// 		dial:       mockNetwork.Dial,
	// 		lookupHost: mockNetwork.LookupHost,
	// 	},
	// 	cfg: &config{
	// 		Servers: currentNodes,
	// 	},
	// }
	//
	// mockNetwork.On("LookupHost", cl.cfg.HeadlessServiceAddress).Return(currentNodes, nil)
	// mockNetwork.On("Dial", addr.Network(), addr.String()).Return(nil, expectedErr)
	//
	// for _, node := range currentNodes {
	// 	cl.hr.Add(node)
	// }
	// cl.deadNodes = make(map[string]struct{})
	// for _, node := range alreadyDeadNodes {
	// 	cl.deadNodes[node] = struct{}{}
	// }
	// for _, node := range disableNodes {
	// 	cl.deadNodes[node] = struct{}{}
	// }
	//
	// cl.checkNodesHealth()
	//
	// assert.Equal(t, 3, len(cl.hr.GetAllNodes()))
	// assert.Equal(t, 2, len(cl.deadNodes))
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
