package memcached

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/stretchr/testify/mock"
)

type mockNetworkOperations struct {
	mock.Mock
}

func (m *mockNetworkOperations) Dial(network, address string) (net.Conn, error) {
	args := m.Called(network, address)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(net.Conn), args.Error(1)
}

func (m *mockNetworkOperations) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	args := m.Called(network, address, timeout)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(net.Conn), args.Error(1)
}

func (m *mockNetworkOperations) LookupHost(host string) ([]string, error) {
	args := m.Called(host)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

type mockReadWriteCloser struct {
	mu sync.Mutex
	mock.Mock
	net.TCPConn
	// --- //
	closed bool
	reader io.Reader
}

func (m *mockReadWriteCloser) Read(b []byte) (int, error) {
	if m.reader != nil {
		return m.reader.Read(b)
	}
	args := m.Called(b)
	if data, ok := args.Get(0).([]byte); ok {
		copy(b, data)
		return len(data), args.Error(1)
	}
	return args.Int(0), args.Error(1)
}

func (m *mockReadWriteCloser) Write(b []byte) (int, error) {
	args := m.Called(b)
	return args.Int(0), args.Error(1)
}

func (m *mockReadWriteCloser) Close() error {
	args := m.Called()
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return args.Error(0)
}

// mockTimeoutError a simple mock for timeout error
type mockTimeoutError struct{}

func (e *mockTimeoutError) Error() string   { return "operation timed out" }
func (e *mockTimeoutError) Timeout() bool   { return true }
func (e *mockTimeoutError) Temporary() bool { return true }
