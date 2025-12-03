// nolint
package memcached

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aliexpressru/gomemcached/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"github.com/aliexpressru/gomemcached/consistenthash"
	"github.com/aliexpressru/gomemcached/utils"
)

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

func TestTransmitReq(t *testing.T) {
	b := bytes.NewBuffer([]byte{})
	buf := bufio.NewWriter(b)

	req := Request{
		Opcode: SET,
		Cas:    938424885,
		Opaque: 7242,
		Extras: []byte{},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	// Verify nil transmit is OK
	_, err := transmitRequest(nil, &req)
	if !errors.Is(err, ErrNoServers) {
		t.Errorf("Expected errNoConn with no conn, got %v", err)
	}

	_, err = transmitRequest(buf, &req)
	if err != nil {
		t.Fatalf("Error transmitting request: %v", err)
	}

	buf.Flush()

	expected := []byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,      // extra length
		0x0,      // reserved
		0x0, 0x0, // reserved
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e',
	}

	if len(b.Bytes()) != req.size() {
		t.Fatalf("Expected %v bytes, got %v", req.size(),
			len(b.Bytes()))
	}

	if !reflect.DeepEqual(b.Bytes(), expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, b.Bytes())
	}
}

func BenchmarkTransmitReq(b *testing.B) {
	bout := bytes.NewBuffer([]byte{})

	req := Request{
		Opcode: SET,
		Cas:    938424885,
		Opaque: 7242,
		Extras: []byte{},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	b.SetBytes(int64(req.size()))

	for i := 0; i < b.N; i++ {
		bout.Reset()
		buf := bufio.NewWriterSize(bout, req.size()*2)
		_, err := transmitRequest(buf, &req)
		if err != nil {
			b.Fatalf("Error transmitting request: %v", err)
		}
	}
}

func BenchmarkTransmitReqLarge(b *testing.B) {
	bout := bytes.NewBuffer([]byte{})

	req := Request{
		Opcode: SET,
		Cas:    938424885,
		Opaque: 7242,
		Extras: []byte{},
		Key:    []byte("somekey"),
		Body:   make([]byte, 24*1024),
	}

	b.SetBytes(int64(req.size()))

	for i := 0; i < b.N; i++ {
		bout.Reset()
		buf := bufio.NewWriterSize(bout, req.size()*2)
		_, err := transmitRequest(buf, &req)
		if err != nil {
			b.Fatalf("Error transmitting request: %v", err)
		}
	}
}

func BenchmarkTransmitReqNull(b *testing.B) {
	req := Request{
		Opcode: SET,
		Cas:    938424885,
		Opaque: 7242,
		Extras: []byte{},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	b.SetBytes(int64(req.size()))

	for i := 0; i < b.N; i++ {
		_, err := transmitRequest(io.Discard, &req)
		if err != nil {
			b.Fatalf("Error transmitting request: %v", err)
		}
	}
}

// BenchmarkMultiGet benchmarks MultiGet with various key counts and value sizes
func BenchmarkMultiGet(b *testing.B) {
	c, err := newForTests("localhost:11211", "localhost:11213", "localhost:11214", "localhost:11215")
	require.NoError(b, err)

	keysCount := []int{1, 5, 10, 25, 50, 100, 250, 500, 1000}
	valueSizes := []int{100, 1024, 10240} // 100B, 1KB, 10KB

	for _, count := range keysCount {
		for _, valueSize := range valueSizes {
			b.Run(fmt.Sprintf("keys=%d/valueSize=%dB", count, valueSize), func(b *testing.B) {
				ctx := context.TODO()

				// Prepare data
				m := make(map[string][]byte, count)
				for i := 0; i < count; i++ {
					k := fmt.Sprintf("benchmark-key-%d", i)
					v := make([]byte, valueSize)
					for j := 0; j < valueSize; j++ {
						v[j] = byte(j % 256)
					}
					m[k] = v
				}
				keys := maps.Keys(m)

				// Store data
				err := c.MultiStore(ctx, Set, m, 0)
				require.NoError(b, err)

				// Benchmark MultiGet
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					resp, err := c.MultiGet(ctx, keys)
					require.NoError(b, err)
					require.Equal(b, len(keys), len(resp))
				}
			})
		}
	}
}

// BenchmarkMultiStore benchmarks MultiStore with various item counts and value sizes
func BenchmarkMultiStore(b *testing.B) {
	c, err := newForTests("localhost:11211", "localhost:11213", "localhost:11214", "localhost:11215")
	require.NoError(b, err)

	itemCounts := []int{1, 5, 10, 25, 50, 100, 250, 500, 1000}
	valueSizes := []int{100, 1024, 10240} // 100B, 1KB, 10KB

	for _, count := range itemCounts {
		for _, valueSize := range valueSizes {
			b.Run(fmt.Sprintf("items=%d/valueSize=%dB", count, valueSize), func(b *testing.B) {
				ctx := context.TODO()

				// Prepare data
				m := make(map[string][]byte, count)
				for i := 0; i < count; i++ {
					k := fmt.Sprintf("benchmark-store-key-%d", i)
					v := make([]byte, valueSize)
					for j := 0; j < valueSize; j++ {
						v[j] = byte(j % 256)
					}
					m[k] = v
				}

				// Benchmark MultiStore
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					err := c.MultiStore(ctx, Set, m, 0)
					require.NoError(b, err)
				}
			})
		}
	}
}

// BenchmarkMultiDelete benchmarks MultiDelete with various key counts
func BenchmarkMultiDelete(b *testing.B) {
	c, err := newForTests("localhost:11211", "localhost:11213", "localhost:11214", "localhost:11215")
	require.NoError(b, err)

	keyCounts := []int{1, 5, 10, 25, 50, 100, 250, 500, 1000}

	for _, count := range keyCounts {
		b.Run(fmt.Sprintf("keys=%d", count), func(b *testing.B) {
			ctx := context.TODO()

			// Prepare keys
			keys := make([]string, count)
			for i := 0; i < count; i++ {
				keys[i] = fmt.Sprintf("benchmark-delete-key-%d", i)
			}

			// Benchmark MultiDelete
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Store data before each delete
				b.StopTimer()
				m := make(map[string][]byte, count)
				for _, k := range keys {
					m[k] = []byte("value")
				}
				err := c.MultiStore(ctx, Set, m, 0)
				require.NoError(b, err)
				b.StartTimer()

				// Delete
				err = c.MultiDelete(ctx, keys)
				require.NoError(b, err)
			}
		})
	}
}

/*
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| 0x81          | 0x00          | 0x00          | 0x00          |
       +---------------+---------------+---------------+---------------+
      4| 0x04          | 0x00          | 0x00          | 0x00          |
       +---------------+---------------+---------------+---------------+
      8| 0x00          | 0x00          | 0x00          | 0x09          |
       +---------------+---------------+---------------+---------------+
     12| 0x00          | 0x00          | 0x00          | 0x00          |
       +---------------+---------------+---------------+---------------+
     16| 0x00          | 0x00          | 0x00          | 0x00          |
       +---------------+---------------+---------------+---------------+
     20| 0x00          | 0x00          | 0x00          | 0x01          |
       +---------------+---------------+---------------+---------------+
     24| 0xde          | 0xad          | 0xbe          | 0xef          |
       +---------------+---------------+---------------+---------------+
     28| 0x57 ('W')    | 0x6f ('o')    | 0x72 ('r')    | 0x6c ('l')    |
       +---------------+---------------+---------------+---------------+
     32| 0x64 ('d')    |
       +---------------+

   Field        (offset) (value)
   Magic        (0)    : 0x81
   Opcode       (1)    : 0x00
   Key length   (2,3)  : 0x0000
   Extra length (4)    : 0x04
   Data type    (5)    : 0x00
   Status       (6,7)  : 0x0000
   Total body   (8-11) : 0x00000009
   Opaque       (12-15): 0x00000000
   CAS          (16-23): 0x0000000000000001
   Extras              :
     Flags      (24-27): 0xdeadbeef
   Key                 : None
   Value        (28-32): The textual string "World"

*/

func TestDecodeSpecSample(t *testing.T) {
	data := []byte{
		0x81, 0x00, 0x00, 0x00, // 0
		0x04, 0x00, 0x00, 0x00, // 4
		0x00, 0x00, 0x00, 0x09, // 8
		0x00, 0x00, 0x00, 0x00, // 12
		0x00, 0x00, 0x00, 0x00, // 16
		0x00, 0x00, 0x00, 0x01, // 20
		0xde, 0xad, 0xbe, 0xef, // 24
		0x57, 0x6f, 0x72, 0x6c, // 28
		0x64, // 32
	}

	buf := make([]byte, HDR_LEN)
	res, _, err := getResponse(bytes.NewReader(data), buf)
	if err != nil {
		t.Fatalf("Error parsing response: %v", err)
	}

	expected := &Response{
		Opcode: GET,
		Status: 0,
		Opaque: 0,
		Cas:    1,
		Extras: []byte{0xde, 0xad, 0xbe, 0xef},
		Body:   []byte("World"),
	}

	if !reflect.DeepEqual(res, expected) {
		t.Fatalf("Expected\n%#v -- got --\n%#v", expected, res)
	}
	unwrappedResp, ok := UnwrapMemcachedError(err)
	assert.False(t, ok, "UnwrapMemcachedError: should return ok=false for success getResponse")
	assert.Nil(t, unwrappedResp, "UnwrapMemcachedError: should return nil response for success getResponse")
}

func TestNilReader(t *testing.T) {
	res, _, err := getResponse(nil, nil)
	if !errors.Is(err, ErrNoServers) {
		t.Fatalf("Expected error reading from nil, got %#v", res)
	}
}

func TestNilConfig(t *testing.T) {
	mcl, err := InitFromEnv(context.TODO())
	assert.Nil(t, mcl, "InitFromEnv without config should be return nil client")
	assert.ErrorIs(t, err, ErrNotConfigured, "InitFromEnv without config should be return error == ErrNotConfigured")
}

func TestInitFromEnvEnvconfigError(t *testing.T) {
	// This test verifies that when envconfig.Process returns an error,
	// InitFromEnv properly wraps and returns it
	t.Setenv("MEMCACHED_PORT", "invalid") // This should cause envconfig.Process to fail

	mcl, err := InitFromEnv(context.TODO())
	require.Nil(t, mcl, "InitFromEnv with invalid env config should return nil client")
	require.NotNil(t, err, "InitFromEnv with invalid env config should return an error")
	assert.Contains(t, err.Error(), "client init err", "Error should contain the expected message")
}

func TestErrWrap(t *testing.T) {
	type args struct {
		resp *Response
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: ENOMEM.String(),
			args: args{resp: &Response{
				Status: ENOMEM,
			}},
			wantErr: ErrServerError,
		},
		{
			name: TMPFAIL.String(),
			args: args{resp: &Response{
				Status: TMPFAIL,
			}},
			wantErr: ErrServerNotAvailable,
		},
		{
			name: UNKNOWN_COMMAND.String(),
			args: args{resp: &Response{
				Status: UNKNOWN_COMMAND,
			}},
			wantErr: ErrUnknownCommand,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapErr := wrapMemcachedResp(tt.args.resp)
			require.ErrorIs(t, wrapErr, tt.wantErr, "wrapMemcachedResp wrap error not equal expected")
		})
	}
}

func TestDecode(t *testing.T) {
	data := []byte{
		RES_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,       // extra length
		0x0,       // reserved
		0x6, 0x2e, // status
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e',
	}

	buf := make([]byte, HDR_LEN)
	res, _, _ := getResponse(bytes.NewReader(data), buf)

	expected := &Response{
		Opcode: SET,
		Status: 1582,
		Opaque: 7242,
		Cas:    938424885,
		Extras: nil,
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	if !reflect.DeepEqual(res, expected) {
		t.Fatalf("Expected\n%#v -- got --\n%#v", expected, res)
	}
}

func BenchmarkDecodeResponse(b *testing.B) {
	data := []byte{
		RES_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,       // extra length
		0x0,       // reserved
		0x6, 0x2e, // status
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e',
	}
	buf := make([]byte, HDR_LEN)
	b.SetBytes(int64(len(buf)))

	for i := 0; i < b.N; i++ {
		getResponse(bytes.NewReader(data), buf)
	}
}

const (
	localhostTCPAddr         = "localhost:11211"
	localhostTCPAddrWithAuth = "localhost:11215"
)

func TestLocalhost(t *testing.T) {
	t.Parallel()
	c, err := net.Dial("tcp", localhostTCPAddr)
	if err != nil {
		t.Skipf("skipping test; no server running at %s", localhostTCPAddr)
	}
	req := Request{
		Opcode: VERSION,
	}

	_, err = transmitRequest(c, &req)
	if err != nil {
		t.Errorf("Expected errNoConn with no conn, got %v", err)
	}

	buf := make([]byte, HDR_LEN)
	resp, _, err := getResponse(c, buf)
	if err != nil {
		t.Fatalf("Error transmitting request: %v", err)
	}

	if resp.Status != SUCCESS {
		t.Errorf("Expected SUCCESS, got %v", resp.Status)
	}
	if err = c.Close(); err != nil {
		t.Fatalf("Error with close connection: %v", err)
	}

	_, err = newForTests("invalidServerAddr")
	require.ErrorIs(t, err, ErrInvalidAddr)

	mc, err := newForTests(localhostTCPAddr)
	if err != nil {
		t.Fatalf("failed to create new client: %v", err)
	}

	ctx := context.TODO()
	t.Cleanup(func() { mc.CloseAllConns(ctx) })
	testWithClient(ctx, t, mc)
}

func TestLocalhostWithAuth(t *testing.T) {
	ctx := context.TODO()

	t.Parallel()
	amc, err := newForTests(localhostTCPAddrWithAuth)
	require.NoError(t, err)

	amc.authEnable = true
	amc.authData = prepareAuthData("admin", "secret")

	aNet, err := utils.AddrRepr(localhostTCPAddrWithAuth)
	require.NoError(t, err)
	cn, err := amc.getFreeConn(ctx, aNet)
	opErr := new(net.OpError)
	if err != nil && assert.ErrorAs(t, err, &opErr, "error is not net.OpError") {
		t.Skipf("skipping test; no server running at %s", localhostTCPAddrWithAuth)
	}
	require.NotNil(t, cn, "conn is nil")
	cn.release()

	_ = amc.CloseAllConns(ctx)
	amc.authData = prepareAuthData("admin", "secret1")
	cn, err = amc.getFreeConn(ctx, aNet)
	require.ErrorIs(t, err, ErrAuthFail)

	t.Cleanup(func() { _ = amc.CloseAllConns(ctx) })
}

func testWithClient(ctx context.Context, t *testing.T, c *Client) {
	resp, err := c.Store(ctx, Set, "bigdata", 0, make([]byte, MaxBodyLen+1))
	assert.ErrorIsf(t, err, ErrDataSizeExceedsLimit, "Store: body > MaxBodyLen, want error ErrDataSizeExceedsLimit")
	unwrapResp, ok := UnwrapMemcachedError(err)
	assert.True(t, ok, "UnwrapMemcachedError: should return ok=true for memcached error")
	if !reflect.DeepEqual(resp, unwrapResp) {
		t.Fatalf("Expected\n%#v -- got --\n%#v", resp, unwrapResp)
	}

	// multi
	err = c.MultiStore(ctx, Set, map[string][]byte{}, 0)
	assert.Nil(t, err, "MultiStore with 0 items should have no errors")
	items, err := c.MultiGet(ctx, []string{})
	assert.Nil(t, err, "MultiGet with 0 keys should have no errors")
	assert.Empty(t, items, "MultiGet with 0 keys should return empty map")
	err = c.MultiDelete(ctx, []string{})
	assert.Nil(t, err, "MultiDelete with 0 keys should have no errors")

	// Set
	_, err = c.Store(ctx, Set, "foo", 0, []byte("fooval-fromset1"))
	assert.Nilf(t, err, "first set(foo): %v", err)
	_, err = c.Store(ctx, Set, "foo", 0, []byte("fooval-fromset2"))
	assert.Nilf(t, err, "second set(foo): %v", err)
	// Add
	_, err = c.Store(ctx, Add, "foo", 0, []byte("fooval-fromset3"))
	assert.ErrorIsf(t, err, ErrNotStored, "Add with exist key - %s, want error - ErrNotStored, have - %v", "foo", err)

	// Get
	resp, err = c.Get(ctx, "foo")
	assert.Nilf(t, err, "get(foo): %v", err)
	// assert.Equalf(t, []byte("foo"), resp.Key, "get(foo) Key = %s, want foo", string(resp.Key)) only for GETK
	assert.Equalf(t, []byte("fooval-fromset2"), resp.Body, "get(foo) Body = %s, want fooval-fromset2", string(resp.Body))
	err = wrapMemcachedResp(resp)
	assert.Nil(t, err, "Get: wrapped success resp should be nil")

	// Get and set a Unicode key
	quxKey := "Hello_世界"
	_, err = c.Store(ctx, Set, quxKey, 0, []byte("hello world"))
	assert.Nilf(t, err, "first set(Hello_世界): %v", err)
	resp, err = c.Get(ctx, quxKey)
	assert.Nilf(t, err, "get(Hello_世界): %v", err)
	// assert.Equalf(t, quxKey, string(resp.Key), "get(Hello_世界) Key = %q, want Hello_世界", quxKey) only for GETK
	assert.Equalf(t, "hello world", string(resp.Body), "get(Hello_世界) Value = %q, want hello world", string(resp.Body))

	// Set malformed keys
	_, err = c.Store(ctx, Set, "foo bar", 0, []byte("foobarval"))
	assert.ErrorIsf(t, err, ErrMalformedKey, "set(foo bar) should return ErrMalformedKey instead of %v", err)
	_, err = c.Store(ctx, Set, "foo"+string(rune(0x7f)), 0, []byte("foobarval"))
	assert.ErrorIsf(t, err, ErrMalformedKey, "set(foo<0x7f>) should return ErrMalformedKey instead of %v", err)

	// Append
	_, err = c.Append(ctx, Append, "append", []byte("appendval"))
	assert.ErrorIsf(t, err, ErrNotStored, "first append(append) want ErrNotStored, got %v", err)

	_, err = c.Store(ctx, Set, "append", 0, []byte("appendval"))
	assert.Nilf(t, err, "Set for append have error - %v", err)
	_, err = c.Append(ctx, Append, "append", []byte("1"))
	assert.Nilf(t, err, "second append(append): %v", err)
	appended, err := c.Get(ctx, "append")
	assert.Nilf(t, err, "after append(append): %v", err)
	assert.Equalf(t, fmt.Sprintf("%s%s", "appendval", "1"), string(appended.Body),
		"Append: want=append1, got=%s", string(appended.Body))

	// Prepend
	_, err = c.Append(ctx, Prepend, "prepend", []byte("prependval"))
	assert.ErrorIsf(t, err, ErrNotStored, "first prepend(prepend) want ErrNotStored, got %v", err)

	_, err = c.Store(ctx, Set, "prepend", 0, []byte("prependval"))
	assert.Nilf(t, err, "Set for prepend have error - %v", err)
	_, err = c.Append(ctx, Prepend, "prepend", []byte("1"))
	assert.Nilf(t, err, "second prepend(prepend): %v", err)
	prepend, err := c.Get(ctx, "prepend")
	assert.Nilf(t, err, "after prepend(prepend): %v", err)
	assert.Equalf(t, fmt.Sprintf("%s%s", "1", "prependval"), string(prepend.Body),
		"Prepend: want=1prependval, got=%s", string(prepend.Body))

	// Replace
	_, err = c.Store(ctx, Replace, "baz", 0, []byte("bazvalue"))
	assert.ErrorIsf(t, err, ErrCacheMiss, "expected replace(baz) to return ErrCacheMiss, got %v", err)
	_, err = c.Store(ctx, Set, "baz", 0, []byte("bazvalue"))
	assert.Nilf(t, err, "Set for Replace have error - %v", err)
	resp, err = c.Store(ctx, Replace, "baz", 0, []byte("42"))
	assert.Nilf(t, err, "Replace have error - %v", err)
	resp, err = c.Get(ctx, "baz")
	assert.Nilf(t, err, "Get for Replace have error - %v", err)
	assert.Equalf(t, "42", string(resp.Body), "Resp after replaces want - 42, have - %s", string(resp.Body))

	// Incr/Decr
	_, err = c.Store(ctx, Set, "num", 0, []byte("42"))
	assert.Nilf(t, err, "Set for Increment have error - %v", err)
	n, err := c.Delta(ctx, Increment, "num", 8, 0, 0)
	assert.Nilf(t, err, "Increment num + 8: %v", err)
	assert.Equalf(t, 50, int(n), "Increment num + 8: want=50, got=%d", n)
	n, err = c.Delta(ctx, Decrement, "num", 49, 0, 0)
	assert.Nilf(t, err, "Decrement: %v", err)
	assert.Equalf(t, 1, int(n), "Decrement 49: want=1, got=%d", n)
	_, err = c.Delete(ctx, "num")
	assert.Nilf(t, err, "Delete for Increment/Decrement have error - %v", err)
	n, err = c.Delta(ctx, Increment, "num", 1, 10, 0)
	assert.Nilf(t, err, "Increment with initial value have error - %v", err)
	assert.Equalf(t, 10, int(n), "Increment with initial value 10: want=10, got=%d", n)
	n, err = c.Delta(ctx, Decrement, "num", 2, 0, 0)
	assert.Nilf(t, err, "Increment with initial value have error - %v", err)
	assert.Equalf(t, 8, int(n), "Increment with initial value 1: want=8, got=%d", n)
	const fakeDeltaMode = DeltaMode(42)
	n, err = c.Delta(ctx, fakeDeltaMode, "num", 2, 0, 0)
	assert.Nilf(t, err, "Increment with fakeDeltaMode have error - %v", err)

	_, err = c.Store(ctx, Set, "num", 0, []byte("not-numeric"))
	assert.Nilf(t, err, "Set for Increment non-numeric value have error - %v", err)
	_, err = c.Delta(ctx, Increment, "num", 1, 0, 0)
	assert.ErrorIs(t, err, ErrInvalidArguments, "Increment not-numeric value")

	// Delete
	_, err = c.Delete(ctx, "foo")
	assert.Nilf(t, err, "Delete: %v", err)
	_, err = c.Get(ctx, "foo")
	assert.ErrorIsf(t, err, ErrCacheMiss, "post-Delete want ErrCacheMiss, got %v", err)

	testExpireWithClient(ctx, t, c)

	// MutliGet
	// Create some test items.
	keys := []string{"foo", "bar", "gopher", "42"}
	input := make(map[string][]byte, len(keys))

	addKeys := func() {
		for i, key := range keys {
			body := []byte(key + strconv.Itoa(i))
			_, err = c.Store(ctx, Set, key, 0, body)
			assert.Nilf(t, err, "Store for MutliGet have error - %v", err)
			input[key] = body
		}
	}

	checkKeyOnExist := func(method string, input map[string][]byte, output map[string][]byte) {
		for key, reqBody := range input {
			if respBody, ok := output[key]; ok {
				assert.Equalf(t, reqBody, respBody, "%s. Request and response body not equal, have - %v, want - %v", method, respBody, reqBody)
			} else {
				t.Errorf("%s. Don't found requset key %v in response", method, key)
			}
		}
	}

	_, err = c.MultiGet(ctx, append(keys, invalidKey))
	assert.ErrorIsf(t, err, ErrMalformedKey, "MultiGet: invalid key, want error ErrMalformedKey")

	addKeys()
	output, err := c.MultiGet(ctx, keys)
	assert.Nilf(t, err, "MultiGet have error: %v", err)
	if len(input) != len(output) {
		t.Errorf("want %d items after MultiGet, have %d", len(input), len(output))
	} else {
		checkKeyOnExist("MultiGet", input, output)
	}

	// Test MultiGet with single non-existent key (should not return ENOENT error)
	nonExistentKeys := []string{"non-existent-key"}
	emptyResult, err := c.MultiGet(ctx, nonExistentKeys)
	assert.Nilf(t, err, "MultiGet with non-existent key should not return error, got: %v", err)
	assert.Emptyf(t, emptyResult, "MultiGet with non-existent key should return empty map, got: %v", emptyResult)

	// Test MultiGet with single existing key
	singleKey := []string{keys[0]}
	singleResult, err := c.MultiGet(ctx, singleKey)
	assert.Nilf(t, err, "MultiGet with single existing key should not return error, got: %v", err)
	assert.Equalf(t, 1, len(singleResult), "MultiGet with single existing key should return one item, got: %v", len(singleResult))
	assert.Equalf(t, input[keys[0]], singleResult[keys[0]], "MultiGet with single existing key should return correct value")

	cCtx, cancel := context.WithCancel(ctx)
	cancel()
	empty, err := c.MultiGet(cCtx, keys)
	assert.ErrorIs(t, err, context.Canceled, "MultiGet have error, want error ErrDeadlineExceeded")
	assert.Empty(t, empty, "MultiGet must be empty map")

	// remove one key from cache
	_, err = c.Delete(ctx, keys[0])
	assert.Nilf(t, err, "Delete for MultiGet have error: %v", err)
	output, err = c.MultiGet(ctx, keys)
	assert.Nilf(t, err, "MultiGet after delete one elem have error: %v", err)
	if len(input)-1 != len(output) {
		t.Errorf("want %d items after MultiStore, have %d", len(input)-1, len(output))
	}

	// MutliStore
	inputMStore := map[string][]byte{
		"foo42": []byte("bar"),
		"hello": []byte("world"),
		"go":    []byte("gopher"),
	}
	inputMStoreExp := map[string][]byte{
		"exp": []byte("needDelete"),
	}
	inputExp := uint32(1)

	err = c.MultiStore(ctx, Set, inputMStore, 0)
	assert.Nilf(t, err, "MultiStore have error: %v", err)
	err = c.MultiStore(ctx, Set, inputMStoreExp, inputExp)
	assert.Nilf(t, err, "MultiStore with exp have error: %v", err)

	time.Sleep(time.Second)
	keyWithExp := maps.Keys(inputMStoreExp)[0]
	_, err = c.Get(ctx, keyWithExp)
	assert.ErrorIsf(t, err, ErrCacheMiss, "Get for item with 1 sec experetion setted in MultiStore. want - %v, have - %v", ErrCacheMiss, err)

	keysInputMStore := maps.Keys(inputMStore)
	outputMStoreOne, err := c.Get(ctx, keysInputMStore[0])
	assert.Nilf(t, err, "Get for MultiStore have error: %v", err)
	assert.NotNil(t, outputMStoreOne.Body, "Get after MultiStore gets item without body")
	outputMStore, err := c.MultiGet(ctx, keysInputMStore)
	assert.Nilf(t, err, "MultiGet for MultiStore have error: %v", err)
	checkKeyOnExist("MultiStore", inputMStore, outputMStore)

	singleMStore, err := c.MultiGet(ctx, []string{keysInputMStore[0]})
	assert.Nilf(t, err, "MultiGet with 1 item have error: %v", err)
	for key, body := range singleMStore {
		assert.Equal(t, keysInputMStore[0], key, "MultiGet with 1 item not equals keys")
		assert.Equal(t, inputMStore[key], body, "MultiGet with 1 item not equals body")
	}

	// Test Flush All
	err = c.FlushAll(ctx, 0)
	assert.Nilf(t, err, "FlushAll: %v", err)
	_, err = c.Get(ctx, "bar")
	assert.ErrorIsf(t, err, ErrCacheMiss, "post-FlushAll want ErrCacheMiss, got %v", err)
}

func testExpireWithClient(ctx context.Context, t *testing.T, c *Client) {
	if testing.Short() {
		t.Log("Skipping testing memcached Touch with testing in Short mode")
		return
	}

	const secondsToExpiry = uint32(1)

	_, err := c.Store(ctx, Set, "foo", secondsToExpiry, []byte("fooval"))
	assert.Nilf(t, err, "Store(Set) with expire have error - %v", err)
	_, err = c.Store(ctx, Add, "bar", secondsToExpiry, []byte("barval"))
	assert.Nilf(t, err, "Store(Add) with expire have error - %v", err)

	time.Sleep(time.Second) // todo use a testing/synctest after upgrade on >go.1.25.0

	_, err = c.Get(ctx, "foo")
	assert.ErrorIsf(t, err, ErrCacheMiss, "Get for expire item - %v", err)

	_, err = c.Get(ctx, "bar")
	assert.ErrorIsf(t, err, ErrCacheMiss, "Get for expire item - %v", err)
}

func TestLocalhost_FlushAll_MultiDelete(t *testing.T) {
	c, err := net.Dial("tcp", localhostTCPAddr)
	if err != nil {
		t.Skipf("skipping test; no server running at %s", localhostTCPAddr)
	}
	req := Request{
		Opcode: VERSION,
	}

	_, err = transmitRequest(c, &req)
	if err != nil {
		t.Errorf("Expected errNoConn with no conn, got %v", err)
	}

	buf := make([]byte, HDR_LEN)
	resp, _, err := getResponse(c, buf)
	if err != nil {
		t.Fatalf("Error transmitting request: %v", err)
	}

	if resp.Status != SUCCESS {
		t.Errorf("Expected SUCCESS, got %v", resp.Status)
	}
	if err = c.Close(); err != nil {
		t.Fatalf("Error with close connection: %v", err)
	}

	mc, err := newForTests(localhostTCPAddr)
	if err != nil {
		t.Fatalf("failed to create new client: %v", err)
	}

	ctx := context.TODO()
	t.Cleanup(func() { mc.CloseAllConns(ctx) })

	keys := []string{"foo", "bar", "gopher", "42"}

	addKeys := func() {
		for i, key := range keys {
			_, err = mc.Store(ctx, Set, key, 0, []byte(key+strconv.Itoa(i)))
			assert.Nil(t, err, fmt.Sprintf("Fail to Store item with key - %s", key))
		}
	}

	checkKeyOnExist := func(meth string) {
		for _, key := range keys {
			_, err = mc.Get(ctx, key)
			assert.ErrorIsf(t, err, ErrCacheMiss, "Get item after %s. want - %v, have - %v", meth, ErrCacheMiss, err)
		}
	}

	addKeys()
	err = mc.MultiDelete(ctx, append(keys, "fake"))
	assert.Nil(t, err, "MultiDelete")
	checkKeyOnExist("MultiDelete")

	addKeys()
	err = mc.FlushAll(ctx, 0)
	assert.Nil(t, err, "FlushAll")
	checkKeyOnExist("FlushAll")
}

func TestClient_CloseAvailableConnsInAllShardPools(t *testing.T) {
	_, err := net.Dial("tcp", localhostTCPAddr)
	if err != nil {
		t.Skipf("skipping test; no server running at %s", localhostTCPAddr)
	}
	mc, err := newForTests(localhostTCPAddr)
	assert.Nilf(t, err, "failed to create new client: %v", err)

	ctx := context.TODO()
	t.Cleanup(func() { mc.CloseAllConns(ctx) })

	// for create conns in pool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := mc.Store(ctx, Set, "foo1", 0, []byte("bar"))
		assert.Nilf(t, err, "Set foo1: %v", err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := mc.Store(ctx, Set, "foo2", 0, []byte("bar"))
		assert.Nilf(t, err, "Set foo2: %v", err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := mc.Store(ctx, Set, "foo3", 0, []byte("bar"))
		assert.Nilf(t, err, "Set foo3: %v", err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := mc.Store(ctx, Set, "foo4", 0, []byte("bar"))
		assert.Nilf(t, err, "Set foo4: %v", err)
	}()

	wg.Wait()

	addr, err := utils.AddrRepr(localhostTCPAddr)
	assert.Nilf(t, err, "AddrRepr: %v", err)

	mc.fmu.RLock()
	p, ok := mc.freeConns[addr.String()]
	mc.fmu.RUnlock()
	assert.Truef(t, ok, "Get from freeConns not found pool for %s", addr.String())

	l := p.Len()

	numOfClose := 1
	c, err := mc.CloseAvailableConnsInAllShardPools(ctx, numOfClose)
	assert.Equal(t, numOfClose, c, "Request for closed not equal actual")
	assert.Nilf(t, err, "CloseAvailableConnsInAllShardPools: %v", err)

	assert.Equalf(t, l-numOfClose, p.Len(), "Resulting pool len not equal expected number")

	ctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = mc.CloseAvailableConnsInAllShardPools(ctx, numOfClose)
	assert.ErrorIsf(t, err, context.Canceled, "CloseAvailableConnsInAllShardPools: %v", err)
}

func TestConn(t *testing.T) {
	c, err := net.DialTimeout("tcp", localhostTCPAddr, time.Second)
	if err != nil {
		t.Skipf("skipping test; no server running at %s", localhostTCPAddr)
	}
	req := Request{
		Opcode: VERSION,
	}

	n, err := transmitRequest(c, &req)
	if err != nil {
		t.Errorf("Expected errNoConn with no conn, got %v", err)
	}

	buf := make([]byte, HDR_LEN)
	resp, _, err := getResponse(c, buf)
	if err != nil {
		t.Fatalf("Error transmitting request: %v", err)
	}

	if n != len(buf) {
		t.Errorf("write bytes - %d != read bytes - %d\n", n, len(buf))
	}
	if resp.Status != SUCCESS {
		t.Errorf("Expected SUCCESS, got %v", resp.Status)
	}
	if err = c.Close(); err != nil {
		t.Fatalf("Error with close connection: %v", err)
	}
}

func TestSafeConnErrors(t *testing.T) {
	ctx := context.TODO()

	var (
		mockNetworkErr = new(mockNetworkOperations)

		mockReadCloser = new(mockReadWriteCloser)

		expectedDialErr = errors.New("mocked dial error")

		addr, _ = utils.AddrRepr("127.0.0.1:11211")
	)
	mockReadCloser.On("Close").Return(nil)

	mockNetworkErr.On("DialTimeout", addr.Network(), addr.String(), DefaultTimeout).Return(nil, expectedDialErr)

	client := &Client{
		nw:        &network{dialTimeout: mockNetworkErr.DialTimeout},
		freeConns: make(map[string]*pool.Pool),
	}

	// Call safeGetOrInitFreeConn which should create a new pool
	p := client.safeGetOrInitFreeConn(addr)

	// Get a connection from the pool, which should return the dial error
	_, err := p.Get(ctx)
	require.NotNil(t, err, "Get from pool should return dial error")
	assert.ErrorIsf(t, err, expectedDialErr, "Error should contain the mocked dial error message")

	// Try to get another connection, which should return an error
	_, err = client.getFreeConn(ctx, addr)
	require.NotNil(t, err, "getFreeConn should return an error when pool.Get fails")
	assert.Contains(t, err.Error(), "Get from pool error", "Error should contain the expected message")

	// Now test getConnForNode with the same address, which should also return an error
	_, err = client.getConnForNode(ctx, addr)
	assert.NotNil(t, err, "getConnForNode should return an error when getFreeConn fails")

	// This test covers the case when freeConns is nil
	// Testing the removeFromFreeConns method when freeConns is nil
	// Call removeFromFreeConns when freeConns is nil - should not panic
	assert.NotPanics(t, func() {
		client.removeFromFreeConns(addr)
	}, "removeFromFreeConns should not panic when freeConns is nil")

	cn := &conn{
		rc:   mockReadCloser,
		addr: addr,
		c:    client,
	}

	// In this case, cn.rc.Close() should be called directly
	cn.close()
	// Verify that the mock ReadCloser's Close method was called
	assert.True(t, mockReadCloser.closed, "ReadCloser.Close should be called when no pool exists for the address")
	// rollback for next test
	mockReadCloser.closed = false

	client.putFreeConn(cn)
	// Verify that the mock ReadCloser's Close method was called
	assert.True(t, mockReadCloser.closed, "ReadCloser.Close should be called when no pool exists for the address")
}

func TestClient_Getters(t *testing.T) {
	type fields struct {
		timeout      time.Duration
		maxIdleConns int
		nodeHCPeriod time.Duration
		nodeRBPeriod time.Duration
	}
	tests := []struct {
		name             string
		fields           fields
		wantTimeout      time.Duration
		wantMaxIdleConns int
		wantNodeHCPeriod time.Duration
		wantNodeRBPeriod time.Duration
	}{
		{
			name:             "Default",
			fields:           fields{},
			wantTimeout:      DefaultTimeout,
			wantMaxIdleConns: DefaultMaxIdleConns,
			wantNodeHCPeriod: DefaultNodeHealthCheckPeriod,
			wantNodeRBPeriod: DefaultRebuildingNodePeriod,
		},
		{
			name: "Custom",
			fields: fields{
				timeout:      5 * time.Second,
				maxIdleConns: 50,
				nodeHCPeriod: time.Second,
				nodeRBPeriod: time.Second,
			},
			wantTimeout:      5 * time.Second,
			wantMaxIdleConns: 50,
			wantNodeHCPeriod: time.Second,
			wantNodeRBPeriod: time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				timeout:      tt.fields.timeout,
				maxIdleConns: tt.fields.maxIdleConns,
				nodeHCPeriod: tt.fields.nodeHCPeriod,
				nodeRBPeriod: tt.fields.nodeRBPeriod,
			}
			assert.Equalf(t, tt.wantTimeout, c.netTimeout(), "netTimeout()")
			assert.Equalf(t, tt.wantMaxIdleConns, c.getMaxIdleConns(), "getMaxIdleConns()")
			assert.Equalf(t, tt.wantNodeHCPeriod, c.getHCPeriod(), "getHCPeriod()")
			assert.Equalf(t, tt.wantNodeRBPeriod, c.getRBPeriod(), "getRBPeriod()")
		})
	}
}

func TestSendErrors(t *testing.T) {
	mockDL := new(MockDeadliner)
	mockDL.On("SetDeadline", mock.Anything)
	mockDL.On("ClearDeadline")

	mockWriter := new(mockReadWriteCloser)
	mockWriter.On("Close").Return(nil)

	addr, _ := utils.AddrRepr("127.0.0.1:11211")

	mc := &Client{
		freeConns: make(map[string]*pool.Pool),
	}

	// Test when transmitRequest returns an error
	t.Run("TransmitRequestError", func(t *testing.T) {

		expectedErr := errors.New("write error")
		mockWriter.On("Write", mock.Anything).Return(0, expectedErr).Once()

		cn := &conn{
			rc:      mockWriter,
			addr:    addr,
			c:       mc,
			healthy: true,
			wrtBuf:  bufio.NewWriterSize(mockWriter, 1), // Small buffer to force immediate writes
			dl:      mockDL,
		}

		req := &Request{
			Opcode: GET,
			Opaque: 1,
			Key:    []byte("test"),
		}

		// Call send which should return an error from transmitRequest
		_, err := mc.send(cn, req)
		assert.NotNil(t, err, "send should return an error when transmitRequest fails")
		assert.ErrorIs(t, err, expectedErr, "send() returned not expected error")
		assert.False(t, cn.healthy, "connection should be marked as unhealthy after transmitRequest error")
	})
}

func TestDialTimeoutError(t *testing.T) {
	var (
		mockNetworkHeadlessErr = new(mockNetworkOperations)

		addr, _ = utils.AddrRepr("127.0.0.1:11211")

		expectedErr = &ConnectTimeoutError{addr}
	)
	mockNetworkHeadlessErr.On("DialTimeout", addr.Network(), addr.String(), DefaultTimeout).Return(nil, &mockTimeoutError{})

	client := &Client{
		nw: &network{dialTimeout: mockNetworkHeadlessErr.DialTimeout},
	}

	// Call dial which should return a ConnectTimeoutError
	_, err := client.dial(addr)

	require.NotNil(t, err, "dial should return an error")
	assert.ErrorAs(t, err, &expectedErr, "Error should be a ConnectTimeoutError")
	assert.Equal(t, addr, expectedErr.Addr, "ConnectTimeoutError should have the correct address")
}

func TestAuthenticate(t *testing.T) {
	var (
		ctx     = context.TODO()
		addr, _ = utils.AddrRepr(localhostTCPAddrWithAuth)
	)

	t.Run("SASL_AUTH transmitRequest error", func(t *testing.T) {
		mockWriter := new(mockReadWriteCloser)
		expectedErr := errors.New("write error")
		mockWriter.On("Write", mock.Anything).Return(0, expectedErr)

		client := &Client{
			authEnable: true,
			authData:   prepareAuthData("user", "pass"),
		}

		cn := &conn{
			rc:      mockWriter,
			addr:    addr,
			c:       client,
			wrtBuf:  bufio.NewWriterSize(mockWriter, 1), // Small buffer to force immediate writes
			hdrBuf:  make([]byte, HDR_LEN),
			healthy: true,
		}

		ok, err := client.authenticate(ctx, cn)
		assert.False(t, ok, "authenticate should return false on transmitRequest error")
		assert.ErrorIs(t, err, expectedErr, "authenticate should return the write error")
	})

	t.Run("SASL_AUTH flush error", func(t *testing.T) {
		mockWriter := new(mockReadWriteCloser)
		mockWriter.On("Write", mock.Anything).Return(10, nil) // Successful write
		mockWriter.On("Flush").Return(errors.New("flush error"))

		client := &Client{
			authEnable: true,
			authData:   prepareAuthData("user", "pass"),
		}

		cn := &conn{
			rc:      mockWriter,
			addr:    addr,
			c:       client,
			wrtBuf:  bufio.NewWriterSize(mockWriter, 1024),
			hdrBuf:  make([]byte, HDR_LEN),
			healthy: true,
		}

		ok, err := client.authenticate(ctx, cn)
		assert.False(t, ok, "authenticate should return false on flush error")
		assert.Error(t, err, "authenticate should return the flush error")
	})

	t.Run("SASL_AUTH ErrNoServers", func(t *testing.T) {
		// We need to make transmitRequest and flush succeed, but getResponse return ErrNoServers
		var (
			mockWriter = new(mockReadWriteCloser)

			authData = prepareAuthData("user", "pass")
			client   = &Client{
				authEnable: true,
				authData:   authData,
			}
			req = &Request{
				Opcode: SASL_AUTH,
				Key:    []byte(SaslMechanism),
				Body:   authData,
			}
		)
		mockWriter.On("Write", req.bytes()).Return(req.size(), nil)
		mockWriter.On("Flush").Return(nil)

		cn := &conn{
			rc:      nil, //
			addr:    addr,
			c:       client,
			wrtBuf:  bufio.NewWriterSize(mockWriter, 1024),
			hdrBuf:  make([]byte, HDR_LEN),
			healthy: true,
		}

		ok, err := client.authenticate(ctx, cn)
		assert.False(t, ok, "authenticate should return false on ErrNoServers")
		assert.ErrorIs(t, err, ErrNoServers, "authenticate should return ErrNoServers")
	})

	t.Run("SASL_STEP transmitRequest error", func(t *testing.T) {
		var (
			mockWriter = new(mockReadWriteCloser)

			authData = prepareAuthData("user", "pass")

			client = &Client{
				authEnable: true,
				authData:   authData,
			}
		)
		reqData := &Request{
			Opcode: SASL_AUTH,
			Key:    []byte(SaslMechanism),
			Body:   authData,
		}
		// First write (SASL_AUTH) succeeds
		mockWriter.On("Write", reqData.bytes()).Return(reqData.size(), nil).Once()
		// Flush succeeds
		mockWriter.On("Flush").Return(nil).Once()
		// Second write (SASL_STEP) fails
		mockWriter.On("Write", mock.Anything).Return(0, errors.New("write error")).Once()

		// Create a mock reader that returns FURTHER_AUTH to trigger SASL_STEP
		mockReader := new(mockReadWriteCloser)
		// Mock response for SASL_AUTH with FURTHER_AUTH status
		respData := &Response{
			Opcode: SASL_AUTH,
			Status: FURTHER_AUTH,
			Opaque: 1,
		}
		respBytes := respData.bytes()
		mockReader.On("Read", mock.Anything).Return(len(respBytes), nil).Once()

		cn := &conn{
			rc:      mockReader,
			addr:    addr,
			c:       client,
			wrtBuf:  bufio.NewWriterSize(mockWriter, 1024),
			hdrBuf:  make([]byte, HDR_LEN),
			healthy: true,
		}

		ok, err := client.authenticate(ctx, cn)
		assert.False(t, ok, "authenticate should return false on SASL_STEP transmitRequest error")
		assert.Error(t, err, "authenticate should return an error")
	})

	t.Run("SASL_AUTH success without FURTHER_AUTH", func(t *testing.T) {
		var (
			mockWriter = new(mockReadWriteCloser)

			authData = prepareAuthData("user", "pass")
			client   = &Client{
				authEnable: true,
				authData:   authData,
			}
		)

		// Mock successful write and flush
		mockWriter.On("Write", mock.Anything).Return(1024, nil)
		mockWriter.On("Flush").Return(nil)

		// Create real response data
		respData := &Response{
			Opcode: SASL_AUTH,
			Status: SUCCESS,
			Opaque: 1,
		}
		respBytes := respData.bytes()
		realReader := &mockReadWriteCloser{reader: bytes.NewReader(respBytes)}

		cn := &conn{
			rc:      realReader,
			addr:    addr,
			c:       client,
			wrtBuf:  bufio.NewWriterSize(mockWriter, 1024),
			hdrBuf:  make([]byte, HDR_LEN),
			healthy: true,
		}

		ok, err := client.authenticate(ctx, cn)
		assert.True(t, ok, "authenticate should return true on successful auth")
		assert.Nil(t, err, "authenticate should not return error on successful auth")
	})

	t.Run("SASL_AUTH error with wrong status", func(t *testing.T) {
		var (
			mockWriter = new(mockReadWriteCloser)

			authData = prepareAuthData("user", "pass")
			client   = &Client{
				authEnable: true,
				authData:   authData,
			}
		)

		// Mock successful write and flush
		mockWriter.On("Write", mock.Anything).Return(1024, nil)
		mockWriter.On("Flush").Return(nil)

		// Create real response data with error status
		respData := &Response{
			Opcode: SASL_AUTH,
			Status: ENOMEM, // Some error status
			Opaque: 1,
		}
		respBytes := respData.bytes()
		realReader := &mockReadWriteCloser{reader: bytes.NewReader(respBytes)}

		cn := &conn{
			rc:      realReader,
			addr:    addr,
			c:       client,
			wrtBuf:  bufio.NewWriterSize(mockWriter, 1024),
			hdrBuf:  make([]byte, HDR_LEN),
			healthy: true,
		}

		ok, err := client.authenticate(ctx, cn)
		assert.False(t, ok, "authenticate should return false on error status")
		assert.Error(t, err, "authenticate should return error on wrong status")
		assert.Contains(t, err.Error(), "error from sasl auth", "Error should contain expected message")
	})

	t.Run("SASL_STEP getResponse error", func(t *testing.T) {
		var (
			mockWriter = new(mockReadWriteCloser)

			authData = prepareAuthData("user", "pass")
			client   = &Client{
				authEnable: true,
				authData:   authData,
			}
		)

		// First SASL_AUTH request succeeds
		mockWriter.On("Write", mock.Anything).Return(1024, nil).Once()
		mockWriter.On("Flush").Return(nil).Once()

		// Create real response data for SASL_AUTH with FURTHER_AUTH status
		respData := &Response{
			Opcode: SASL_AUTH,
			Status: FURTHER_AUTH,
			Opaque: 1,
		}
		respBytes := respData.bytes()
		realReader := &mockReadWriteCloser{reader: bytes.NewReader(respBytes)}

		// Second SASL_STEP request succeeds
		mockWriter.On("Write", mock.Anything).Return(10, nil).Once()

		cn := &conn{
			rc:      realReader,
			addr:    addr,
			c:       client,
			wrtBuf:  bufio.NewWriterSize(mockWriter, 1024),
			hdrBuf:  make([]byte, HDR_LEN),
			healthy: true,
		}

		ok, err := client.authenticate(ctx, cn)
		assert.False(t, ok, "authenticate should return false on SASL_STEP getResponse error")
		assert.Error(t, err, "authenticate should return error on SASL_STEP getResponse error")
	})

	t.Run("SASL_STEP flush error", func(t *testing.T) {
		var (
			mockWriter = new(mockReadWriteCloser)

			authData = prepareAuthData("user", "pass")
			client   = &Client{
				authEnable: true,
				authData:   authData,
			}
		)

		// First SASL_AUTH request succeeds
		mockWriter.On("Write", mock.Anything).Return(1024, nil).Once()
		mockWriter.On("Flush").Return(nil).Once()

		// Create real response data for SASL_AUTH with FURTHER_AUTH status
		respData := &Response{
			Opcode: SASL_AUTH,
			Status: FURTHER_AUTH,
			Opaque: 1,
		}
		respBytes := respData.bytes()
		realReader := &mockReadWriteCloser{reader: bytes.NewReader(respBytes)}

		// Second SASL_STEP request succeeds
		mockWriter.On("Write", mock.Anything).Return(1024, nil).Once()

		// But flush after SASL_STEP fails
		mockWriter.On("Flush").Return(errors.New("flush error")).Once()

		cn := &conn{
			rc:      realReader,
			addr:    addr,
			c:       client,
			wrtBuf:  bufio.NewWriterSize(mockWriter, 1024),
			hdrBuf:  make([]byte, HDR_LEN),
			healthy: true,
		}

		ok, err := client.authenticate(ctx, cn)
		assert.False(t, ok, "authenticate should return false on SASL_STEP flush error")
		assert.Error(t, err, "authenticate should return error on SASL_STEP flush error")
	})

	t.Run("SASL_AUTH with FURTHER_AUTH - full success", func(t *testing.T) {
		var (
			mockWriter = new(mockReadWriteCloser)

			authData = prepareAuthData("user", "pass")
			client   = &Client{
				authEnable: true,
				authData:   authData,
			}
		)

		// First SASL_AUTH request succeeds
		mockWriter.On("Write", mock.Anything).Return(1024, nil).Once()
		mockWriter.On("Flush").Return(nil).Once()

		// Create combined response data for both SASL_AUTH and SASL_STEP
		authRespData := &Response{
			Opcode: SASL_AUTH,
			Status: FURTHER_AUTH,
			Opaque: 1,
		}
		stepRespData := &Response{
			Opcode: SASL_STEP,
			Status: SUCCESS,
			Opaque: 2,
		}

		// Combine both responses
		combinedData := append(authRespData.bytes(), stepRespData.bytes()...)
		realReader := &mockReadWriteCloser{reader: bytes.NewReader(combinedData)}

		// Second SASL_STEP request succeeds
		mockWriter.On("Write", mock.Anything).Return(1024, nil).Once()

		// Final flush succeeds
		mockWriter.On("Flush").Return(nil).Once()

		cn := &conn{
			rc:      realReader,
			addr:    addr,
			c:       client,
			wrtBuf:  bufio.NewWriterSize(mockWriter, 1024),
			hdrBuf:  make([]byte, HDR_LEN),
			healthy: true,
		}

		ok, err := client.authenticate(ctx, cn)
		assert.True(t, ok, "authenticate should return true on successful FURTHER_AUTH flow")
		assert.Nil(t, err, "authenticate should not return error on successful FURTHER_AUTH flow")
	})
}

func TestMethodsErrors(t *testing.T) {
	c := &Client{
		hr:                         consistenthash.NewHashRing(),
		disableMemcachedDiagnostic: true,
	}

	ctx := context.TODO()
	// invalid key
	_, err := c.Store(ctx, Set, invalidKey, 0, []byte("foo"))
	assert.ErrorIsf(t, err, ErrMalformedKey, "Store: invalid key, want error ErrMalformedKey")
	_, err = c.Get(ctx, invalidKey)
	assert.ErrorIsf(t, err, ErrMalformedKey, "Get: invalid key, want error ErrMalformedKey")
	_, err = c.Delete(ctx, invalidKey)
	assert.ErrorIsf(t, err, ErrMalformedKey, "Delete: invalid key, want error ErrMalformedKey")
	_, err = c.Delta(ctx, Increment, invalidKey, 1, 0, 0)
	assert.ErrorIsf(t, err, ErrMalformedKey, "Delta: invalid key, want error ErrMalformedKey")
	_, err = c.Append(ctx, Append, invalidKey, []byte("foo"))
	assert.ErrorIsf(t, err, ErrMalformedKey, "Append: invalid key, want error ErrMalformedKey")
	_, err = c.MultiGet(ctx, []string{invalidKey, "foo", "bar"})
	assert.ErrorIsf(t, err, ErrMalformedKey, "MultiGet: invalid key, want error ErrMalformedKey")
	err = c.MultiDelete(ctx, []string{invalidKey, "foo", "bar"})
	assert.ErrorIsf(t, err, ErrMalformedKey, "MultiDelete: invalid key, want error ErrMalformedKey")
	err = c.MultiStore(ctx, Set, map[string][]byte{"foo": []byte("bar"), invalidKey: []byte("data")}, 0)
	assert.ErrorIsf(t, err, ErrMalformedKey, "MultiDelete: invalid key, want error ErrMalformedKey")

	// empty hash ring
	_, err = c.Store(ctx, Set, "store", 0, []byte("foo"))
	assert.ErrorIsf(t, err, ErrNoServers, "Store: with empty hash ring, want error ErrNoServers")
	_, err = c.Get(ctx, "get")
	assert.ErrorIsf(t, err, ErrNoServers, "Get: with empty hash ring, want error ErrNoServers")
	_, err = c.Delete(ctx, "delete")
	assert.ErrorIsf(t, err, ErrNoServers, "Delete: with empty hash ring, want error ErrNoServers")
	_, err = c.Delta(ctx, Increment, "deltaInc", 1, 0, 0)
	assert.ErrorIsf(t, err, ErrNoServers, "Delta: with empty hash ring, want error ErrNoServers")
	_, err = c.Append(ctx, Append, "append", []byte("foo"))
	assert.ErrorIsf(t, err, ErrNoServers, "Append: with empty hash ring, want error ErrNoServers")

	// add invalid node
	c.hr.Add("node1")

	// invalid node
	_, err = c.Store(ctx, Set, "store", 0, []byte("foo"))
	assert.ErrorIsf(t, err, ErrInvalidAddr, "Store: invalid node, want error ErrInvalidAddr")
	_, err = c.Get(ctx, "get")
	assert.ErrorIsf(t, err, ErrInvalidAddr, "Get: invalid node, want error ErrInvalidAddr")
	_, err = c.Delete(ctx, "delete")
	assert.ErrorIsf(t, err, ErrInvalidAddr, "Delete: invalid node, want error ErrInvalidAddr")
	_, err = c.Delta(ctx, Increment, "deltaInc", 1, 0, 0)
	assert.ErrorIsf(t, err, ErrInvalidAddr, "Delta: invalid node, want error ErrInvalidAddr")
	_, err = c.Append(ctx, Append, "append", []byte("foo"))
	assert.ErrorIsf(t, err, ErrInvalidAddr, "Append: invalid node, want error ErrInvalidAddr")
	_, err = c.MultiGet(ctx, []string{"gopher", "foo", "bar"})
	assert.ErrorIsf(t, err, ErrInvalidAddr, "MutliGet: invalid node, want error ErrInvalidAddr")
	err = c.MultiDelete(ctx, []string{"gopher", "foo", "bar"})
	assert.ErrorIsf(t, err, ErrInvalidAddr, "MutliDelete: invalid node, want error ErrInvalidAddr")
	err = c.MultiStore(ctx, Set, map[string][]byte{"foo": []byte("bar"), "data": []byte("data")}, 0)
	assert.ErrorIsf(t, err, ErrInvalidAddr, "MutliStore: invalid node, want error ErrInvalidAddr")

	var (
		mockNetworkHeadlessErr = new(mockNetworkOperations)

		expectedErr = errors.New("mocked dial error")

		headlessServiceAddress = "example.com"
	)
	mockNetworkHeadlessErr.On("LookupHost", headlessServiceAddress).Return(nil, expectedErr)

	op := &options{
		Client: Client{
			nw:  &network{lookupHost: mockNetworkHeadlessErr.LookupHost},
			cfg: &config{HeadlessServiceAddress: headlessServiceAddress},
		},
	}

	_, err = newFromConfig(ctx, op)
	assert.ErrorIs(t, err, ErrInvalidAddr)

	mockNetworkNodeErr := new(mockNetworkOperations)
	mockNetworkNodeErr.On("LookupHost", headlessServiceAddress).Return([]string{"wrongNode"}, nil)

	op = &options{
		Client: Client{
			nw:  &network{lookupHost: mockNetworkNodeErr.LookupHost},
			cfg: &config{HeadlessServiceAddress: headlessServiceAddress},
		},
	}

	_, err = newFromConfig(ctx, op)
	assert.ErrorIs(t, err, ErrInvalidAddr)
}

const invalidKey = `Loremipsumdolorsitamet,consecteturadipiscingelit.Velelitvoluptateeleifendquisproidentnonfeugaitiriureliberminimveniamillumcupiditataliquid,nihiltefeugiatlobortiseleifendnibhproidenttationatoptionesseconsectetuerdeserunt.Gubergrenveroidsolutaquis.Dignissimlobortisloremveroenimrebumconsetetur.`
