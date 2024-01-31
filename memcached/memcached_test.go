// nolint
package memcached

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	"github.com/aliexpressru/gomemcached/utils"
)

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

	if len(b.Bytes()) != req.Size() {
		t.Fatalf("Expected %v bytes, got %v", req.Size(),
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

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		bout.Reset()
		buf := bufio.NewWriterSize(bout, req.Size()*2)
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

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		bout.Reset()
		buf := bufio.NewWriterSize(bout, req.Size()*2)
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

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		_, err := transmitRequest(ioutil.Discard, &req)
		if err != nil {
			b.Fatalf("Error transmitting request: %v", err)
		}
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
	assert.Nil(t, UnwrapMemcachedError(err), "UnwrapMemcachedError: should be return nil for success getResponse")
}

func TestNilReader(t *testing.T) {
	res, _, err := getResponse(nil, nil)
	if !errors.Is(err, ErrNoServers) {
		t.Fatalf("Expected error reading from nil, got %#v", res)
	}
}

func TestNilConfig(t *testing.T) {
	mcl, err := InitFromEnv()
	assert.Nil(t, mcl, "InitFromEnv without config should be return nil client")
	assert.ErrorIs(t, err, ErrNotConfigured, "InitFromEnv without config should be return error == ErrNotConfigured")
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

const localhostTCPAddr = "localhost:11211"

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

	mc, err := newForTests(localhostTCPAddr)
	if err != nil {
		t.Fatalf("failed to create new client: %v", err)
	}
	t.Cleanup(mc.CloseAllConns)
	testWithClient(t, mc)
}

func testWithClient(t *testing.T, c *Client) {
	_, err := c.Store(Set, invalidKey, 0, []byte("foo"))
	assert.ErrorIsf(t, err, ErrMalformedKey, "Store: invalid key, want error ErrMalformedKey")
	_, err = c.Get(invalidKey)
	assert.ErrorIsf(t, err, ErrMalformedKey, "Get: invalid key, want error ErrMalformedKey")
	_, err = c.Delete(invalidKey)
	assert.ErrorIsf(t, err, ErrMalformedKey, "Delete: invalid key, want error ErrMalformedKey")
	_, err = c.Delta(Increment, invalidKey, 1, 0, 0)
	assert.ErrorIsf(t, err, ErrMalformedKey, "Delta: invalid key, want error ErrMalformedKey")
	_, err = c.Append(Append, invalidKey, []byte("foo"))
	assert.ErrorIsf(t, err, ErrMalformedKey, "Append: invalid key, want error ErrMalformedKey")
	_, err = c.MultiGet([]string{invalidKey, "foo", "bar"})
	assert.ErrorIsf(t, err, ErrMalformedKey, "MultiGet: invalid key, want error ErrMalformedKey")
	err = c.MultiDelete([]string{invalidKey, "foo", "bar"})
	assert.ErrorIsf(t, err, ErrMalformedKey, "MultiDelete: invalid key, want error ErrMalformedKey")
	err = c.MultiStore(Set, map[string][]byte{"foo": []byte("bar"), invalidKey: []byte("data")}, 0)
	assert.ErrorIsf(t, err, ErrMalformedKey, "MultiDelete: invalid key, want error ErrMalformedKey")
	resp, err := c.Store(Set, "bigdata", 0, make([]byte, MaxBodyLen+1))
	assert.ErrorIsf(t, err, ErrDataSizeExceedsLimit, "Store: body > MaxBodyLen, want error ErrDataSizeExceedsLimit")
	unwrapResp := UnwrapMemcachedError(err)
	if !reflect.DeepEqual(resp, unwrapResp) {
		t.Fatalf("Expected\n%#v -- got --\n%#v", resp, unwrapResp)
	}

	err = c.MultiStore(Set, map[string][]byte{}, 0)
	assert.Nil(t, err, "MultiStore with 0 items should have no errors")
	items, err := c.MultiGet([]string{})
	assert.Nil(t, err, "MultiGet with 0 keys should have no errors")
	assert.Empty(t, items, "MultiGet with 0 keys should return empty map")
	err = c.MultiDelete([]string{})
	assert.Nil(t, err, "MultiDelete with 0 keys should have no errors")

	// Set
	_, err = c.Store(Set, "foo", 0, []byte("fooval-fromset1"))
	assert.Nilf(t, err, "first set(foo): %v", err)
	_, err = c.Store(Set, "foo", 0, []byte("fooval-fromset2"))
	assert.Nilf(t, err, "second set(foo): %v", err)
	// Add
	_, err = c.Store(Add, "foo", 0, []byte("fooval-fromset3"))
	assert.ErrorIsf(t, err, ErrNotStored, "Add with exist key - %s, want error - ErrNotStored, have - %v", "foo", err)

	// Get
	resp, err = c.Get("foo")
	assert.Nilf(t, err, "get(foo): %v", err)
	// assert.Equalf(t, []byte("foo"), resp.Key, "get(foo) Key = %s, want foo", string(resp.Key)) only for GETK
	assert.Equalf(t, []byte("fooval-fromset2"), resp.Body, "get(foo) Body = %s, want fooval-fromset2", string(resp.Body))
	err = wrapMemcachedResp(resp)
	assert.Nil(t, err, "Get: wrapped success resp should be nil")

	// Get and set a unicode key
	quxKey := "Hello_世界"
	_, err = c.Store(Set, quxKey, 0, []byte("hello world"))
	assert.Nilf(t, err, "first set(Hello_世界): %v", err)
	resp, err = c.Get(quxKey)
	assert.Nilf(t, err, "get(Hello_世界): %v", err)
	// assert.Equalf(t, quxKey, string(resp.Key), "get(Hello_世界) Key = %q, want Hello_世界", quxKey) only for GETK
	assert.Equalf(t, "hello world", string(resp.Body), "get(Hello_世界) Value = %q, want hello world", string(resp.Body))

	// Set malformed keys
	_, err = c.Store(Set, "foo bar", 0, []byte("foobarval"))
	assert.ErrorIsf(t, err, ErrMalformedKey, "set(foo bar) should return ErrMalformedKey instead of %v", err)
	_, err = c.Store(Set, "foo"+string(rune(0x7f)), 0, []byte("foobarval"))
	assert.ErrorIsf(t, err, ErrMalformedKey, "set(foo<0x7f>) should return ErrMalformedKey instead of %v", err)

	// Append
	_, err = c.Append(Append, "append", []byte("appendval"))
	assert.ErrorIsf(t, err, ErrNotStored, "first append(append) want ErrNotStored, got %v", err)

	_, err = c.Store(Set, "append", 0, []byte("appendval"))
	assert.Nilf(t, err, "Set for append have error - %v", err)
	_, err = c.Append(Append, "append", []byte("1"))
	assert.Nilf(t, err, "second append(append): %v", err)
	appended, err := c.Get("append")
	assert.Nilf(t, err, "after append(append): %v", err)
	assert.Equalf(t, fmt.Sprintf("%s%s", "appendval", "1"), string(appended.Body),
		"Append: want=append1, got=%s", string(appended.Body))

	// Prepend
	_, err = c.Append(Prepend, "prepend", []byte("prependval"))
	assert.ErrorIsf(t, err, ErrNotStored, "first prepend(prepend) want ErrNotStored, got %v", err)

	_, err = c.Store(Set, "prepend", 0, []byte("prependval"))
	assert.Nilf(t, err, "Set for prepend have error - %v", err)
	_, err = c.Append(Prepend, "prepend", []byte("1"))
	assert.Nilf(t, err, "second prepend(prepend): %v", err)
	prepend, err := c.Get("prepend")
	assert.Nilf(t, err, "after prepend(prepend): %v", err)
	assert.Equalf(t, fmt.Sprintf("%s%s", "1", "prependval"), string(prepend.Body),
		"Prepend: want=1prependval, got=%s", string(prepend.Body))

	// Replace
	_, err = c.Store(Replace, "baz", 0, []byte("bazvalue"))
	assert.ErrorIsf(t, err, ErrCacheMiss, "expected replace(baz) to return ErrCacheMiss, got %v", err)
	_, err = c.Store(Set, "baz", 0, []byte("bazvalue"))
	assert.Nilf(t, err, "Set for Replace have error - %v", err)
	resp, err = c.Store(Replace, "baz", 0, []byte("42"))
	assert.Nilf(t, err, "Replace have error - %v", err)
	resp, err = c.Get("baz")
	assert.Nilf(t, err, "Get for Replace have error - %v", err)
	assert.Equalf(t, "42", string(resp.Body), "Resp after replaces want - 42, have - %s", string(resp.Body))

	// Incr/Decr
	_, err = c.Store(Set, "num", 0, []byte("42"))
	assert.Nilf(t, err, "Set for Increment have error - %v", err)
	n, err := c.Delta(Increment, "num", 8, 0, 0)
	assert.Nilf(t, err, "Increment num + 8: %v", err)
	assert.Equalf(t, 50, int(n), "Increment num + 8: want=50, got=%d", n)
	n, err = c.Delta(Decrement, "num", 49, 0, 0)
	assert.Nilf(t, err, "Decrement: %v", err)
	assert.Equalf(t, 1, int(n), "Decrement 49: want=1, got=%d", n)
	_, err = c.Delete("num")
	assert.Nilf(t, err, "Delete for Increment/Decrement have error - %v", err)
	n, err = c.Delta(Increment, "num", 1, 10, 0)
	assert.Nilf(t, err, "Increment with initial value have error - %v", err)
	assert.Equalf(t, 10, int(n), "Increment with initial value 10: want=10, got=%d", n)
	n, err = c.Delta(Decrement, "num", 2, 0, 0)
	assert.Nilf(t, err, "Increment with initial value have error - %v", err)
	assert.Equalf(t, 8, int(n), "Increment with initial value 1: want=8, got=%d", n)
	const fakeDeltaMode = DeltaMode(42)
	n, err = c.Delta(fakeDeltaMode, "num", 2, 0, 0)
	assert.Nilf(t, err, "Increment with fakeDeltaMode have error - %v", err)

	_, err = c.Store(Set, "num", 0, []byte("not-numeric"))
	assert.Nilf(t, err, "Set for Increment non-numeric value have error - %v", err)
	_, err = c.Delta(Increment, "num", 1, 0, 0)
	assert.ErrorIs(t, err, ErrInvalidArguments, "Increment not-numeric value")

	// Delete
	_, err = c.Delete("foo")
	assert.Nilf(t, err, "Delete: %v", err)
	_, err = c.Get("foo")
	assert.ErrorIsf(t, err, ErrCacheMiss, "post-Delete want ErrCacheMiss, got %v", err)

	testExpireWithClient(t, c)

	// MutliGet
	// Create some test items.
	keys := []string{"foo", "bar", "gopher", "42"}
	input := make(map[string][]byte, len(keys))

	addKeys := func() {
		for i, key := range keys {
			body := []byte(key + strconv.Itoa(i))
			_, err = c.Store(Set, key, 0, body)
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

	_, err = c.MultiGet(append(keys, invalidKey))
	assert.ErrorIsf(t, err, ErrMalformedKey, "MultiGet: invalid key, want error ErrMalformedKey")

	addKeys()
	output, err := c.MultiGet(keys)
	assert.Nilf(t, err, "MultiGet have error: %v", err)
	if len(input) != len(output) {
		t.Errorf("want %d items after MultiGet, have %d", len(input), len(output))
	} else {
		checkKeyOnExist("MultiGet", input, output)
	}

	// remove one key from cache
	_, err = c.Delete(keys[0])
	assert.Nilf(t, err, "Delete for MultiGet have error: %v", err)
	output, err = c.MultiGet(keys)
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

	err = c.MultiStore(Set, inputMStore, 0)
	assert.Nilf(t, err, "MultiStore have error: %v", err)
	err = c.MultiStore(Set, inputMStoreExp, inputExp)
	assert.Nilf(t, err, "MultiStore with exp have error: %v", err)

	time.Sleep(time.Second)
	keyWithExp := maps.Keys(inputMStoreExp)[0]
	_, err = c.Get(keyWithExp)
	assert.ErrorIsf(t, err, ErrCacheMiss, "Get for item with 1 sec experetion setted in MultiStore. want - %v, have - %v", ErrCacheMiss, err)

	keysInputMStore := maps.Keys(inputMStore)
	outputMStoreOne, err := c.Get(keysInputMStore[0])
	assert.Nilf(t, err, "Get for MultiStore have error: %v", err)
	assert.NotNil(t, outputMStoreOne.Body, "Get after MultiStore gets item without body")
	outputMStore, err := c.MultiGet(keysInputMStore)
	assert.Nilf(t, err, "MultiGet for MultiStore have error: %v", err)
	checkKeyOnExist("MultiStore", inputMStore, outputMStore)

	singleMStore, err := c.MultiGet([]string{keysInputMStore[0]})
	assert.Nilf(t, err, "MultiGet with 1 item have error: %v", err)
	for key, body := range singleMStore {
		assert.Equal(t, keysInputMStore[0], key, "MultiGet with 1 item not equals keys")
		assert.Equal(t, inputMStore[key], body, "MultiGet with 1 item not equals body")
	}

	// Test Flush All
	err = c.FlushAll(0)
	assert.Nilf(t, err, "FlushAll: %v", err)
	_, err = c.Get("bar")
	assert.ErrorIsf(t, err, ErrCacheMiss, "post-FlushAll want ErrCacheMiss, got %v", err)
}

func testExpireWithClient(t *testing.T, c *Client) {
	if testing.Short() {
		t.Log("Skipping testing memcached Touch with testing in Short mode")
		return
	}

	const secondsToExpiry = uint32(1)

	_, err := c.Store(Set, "foo", secondsToExpiry, []byte("fooval"))
	assert.Nilf(t, err, "Store(Set) with expire have error - %v", err)
	_, err = c.Store(Add, "bar", secondsToExpiry, []byte("barval"))
	assert.Nilf(t, err, "Store(Add) with expire have error - %v", err)

	time.Sleep(time.Second)

	_, err = c.Get("foo")
	assert.ErrorIsf(t, err, ErrCacheMiss, "Get for expire item - %v", err)

	_, err = c.Get("bar")
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
	t.Cleanup(mc.CloseAllConns)

	keys := []string{"foo", "bar", "gopher", "42"}

	addKeys := func() {
		for i, key := range keys {
			_, err = mc.Store(Set, key, 0, []byte(key+strconv.Itoa(i)))
			assert.Nil(t, err, fmt.Sprintf("Fail to Store item with key - %s", key))
		}
	}

	checkKeyOnExist := func(meth string) {
		for _, key := range keys {
			_, err = mc.Get(key)
			assert.ErrorIsf(t, err, ErrCacheMiss, "Get item after %s. want - %v, have - %v", meth, ErrCacheMiss, err)
		}
	}

	addKeys()
	err = mc.MultiDelete(append(keys, "fake"))
	assert.Nil(t, err, "MultiDelete")
	checkKeyOnExist("MultiDelete")

	addKeys()
	err = mc.FlushAll(0)
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
	t.Cleanup(mc.CloseAllConns)

	// for create conns in pool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := mc.Store(Set, "foo1", 0, []byte("bar"))
		assert.Nilf(t, err, "Set foo1: %v", err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := mc.Store(Set, "foo2", 0, []byte("bar"))
		assert.Nilf(t, err, "Set foo2: %v", err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := mc.Store(Set, "foo3", 0, []byte("bar"))
		assert.Nilf(t, err, "Set foo3: %v", err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := mc.Store(Set, "foo4", 0, []byte("bar"))
		assert.Nilf(t, err, "Set foo4: %v", err)
	}()

	wg.Wait()

	addr, err := utils.AddrRepr(localhostTCPAddr)
	assert.Nilf(t, err, "AddrRepr: %v", err)

	pool, ok := mc.safeGetFreeConn(addr)
	assert.Truef(t, ok, "Get from freeConns not found pool for %s", addr.String())

	l := pool.Len()

	numOfClose := 1
	c := mc.CloseAvailableConnsInAllShardPools(numOfClose)
	assert.Equal(t, numOfClose, c, "Request for closed not equal actual")

	assert.Equalf(t, l-numOfClose, pool.Len(), "Resulting pool len not equal expected number")
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

const invalidKey = `Loremipsumdolorsitamet,consecteturadipiscingelit.Velelitvoluptateeleifendquisproidentnonfeugaitiriureliberminimveniamillumcupiditataliquid,nihiltefeugiatlobortiseleifendnibhproidenttationatoptionesseconsectetuerdeserunt.Gubergrenveroidsolutaquis.Dignissimlobortisloremveroenimrebumconsetetur.`
