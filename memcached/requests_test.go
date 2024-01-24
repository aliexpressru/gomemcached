// nolint
package memcached

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"
)

func TestEncodingRequest(t *testing.T) {
	req := Request{
		Opcode: SET,
		Cas:    938424885,
		Opaque: 7242,
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	got := req.Bytes()

	expected := []byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,      // extra length
		0x0,      // reserved
		0x0, 0x0, // vbucket
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e',
	}

	if len(got) != req.Size() {
		t.Fatalf("Expected %v bytes, got %v", got,
			len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}

	exp := `{Request opcode=SET, bodylen=9, key='somekey'}`
	if req.String() != exp {
		t.Errorf("Expected string=%q, got %q", exp, req.String())
	}
}

func TestEncodingRequestWithExtras(t *testing.T) {
	req := Request{
		Opcode: SET,
		Cas:    938424885,
		Opaque: 7242,
		Extras: []byte{1, 2, 3, 4},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	buf := &bytes.Buffer{}
	req.Transmit(buf)
	got := buf.Bytes()

	expected := []byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x4,      // extra length
		0x0,      // reserved
		0x0, 0x0, // vbucket
		0x0, 0x0, 0x0, 0x14, // Length of remainder
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		1, 2, 3, 4, // extras
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e',
	}

	if len(got) != req.Size() {
		t.Fatalf("Expected %v bytes, got %v", got,
			len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}
}

func TestEncodingRequestWithLargeBody(t *testing.T) {
	req := Request{
		Opcode: SET,
		Cas:    938424885,
		Opaque: 7242,
		Extras: []byte{1, 2, 3, 4},
		Key:    []byte("somekey"),
		Body:   make([]byte, 256),
	}

	buf := &bytes.Buffer{}
	req.Transmit(buf)
	got := buf.Bytes()

	expected := append([]byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x4,      // extra length
		0x0,      // reserved
		0x0, 0x0, // vbucket
		0x0, 0x0, 0x1, 0xb, // Length of remainder
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		1, 2, 3, 4, // extras
		's', 'o', 'm', 'e', 'k', 'e', 'y',
	}, make([]byte, 256)...)

	if len(got) != req.Size() {
		t.Fatalf("Expected %v bytes, got %v", got,
			len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}
}

func BenchmarkEncodingRequest(b *testing.B) {
	req := Request{
		Opcode: SET,
		Cas:    938424885,
		Opaque: 7242,
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func BenchmarkEncodingRequest0CAS(b *testing.B) {
	req := Request{
		Opcode: SET,
		Cas:    0,
		Opaque: 7242,
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func BenchmarkEncodingRequest1Extra(b *testing.B) {
	req := Request{
		Opcode: SET,
		Cas:    0,
		Opaque: 7242,
		Extras: []byte{1},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func TestRequestTransmit(t *testing.T) {
	res := Request{Key: []byte("thekey")}
	_, err := res.Transmit(ioutil.Discard)
	if err != nil {
		t.Errorf("Error sending small request: %v", err)
	}

	res.Body = make([]byte, 256)
	_, err = res.Transmit(ioutil.Discard)
	if err != nil {
		t.Errorf("Error sending large request thing: %v", err)
	}
}

func TestReceiveRequest(t *testing.T) {
	req := Request{
		Opcode: SET,
		Cas:    0,
		Opaque: 7242,
		Extras: []byte{1},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	data := req.Bytes()

	req2 := Request{}
	n, err := req2.Receive(bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("Error receiving: %v", err)
	}
	if len(data) != n {
		t.Errorf("Expected to read %v bytes, read %v", len(data), n)
	}

	if !reflect.DeepEqual(req, req2) {
		t.Fatalf("Expected %#v == %#v", req, req2)
	}
}

func TestReceiveRequestNoContent(t *testing.T) {
	req := Request{
		Opcode: SET,
		Cas:    0,
		Opaque: 7242,
	}

	data := req.Bytes()

	req2 := Request{}
	n, err := req2.Receive(bytes.NewReader(data), nil)
	if err != nil {
		t.Fatalf("Error receiving: %v", err)
	}
	if len(data) != n {
		t.Errorf("Expected to read %v bytes, read %v", len(data), n)
	}

	if fmt.Sprintf("%#v", req) != fmt.Sprintf("%#v", req2) {
		t.Fatalf("Expected %#v == %#v", req, req2)
	}
}

func TestReceiveRequestShortHdr(t *testing.T) {
	req := Request{}
	n, err := req.Receive(bytes.NewReader([]byte{1, 2, 3}), nil)
	if err == nil {
		t.Errorf("Expected error, got %#v", req)
	}
	if n != 3 {
		t.Errorf("Expected to have read 3 bytes, read %v", n)
	}
}

func TestReceiveRequestShortBody(t *testing.T) {
	req := Request{
		Opcode: SET,
		Cas:    0,
		Opaque: 7242,
		Extras: []byte{1},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	data := req.Bytes()

	req2 := Request{}
	n, err := req2.Receive(bytes.NewReader(data[:len(data)-3]), nil)
	if err == nil {
		t.Errorf("Expected error, got %#v", req2)
	}
	if n != len(data)-3 {
		t.Errorf("Expected to have read %v bytes, read %v", len(data)-3, n)
	}
}

func TestReceiveRequestBadMagic(t *testing.T) {
	req := Request{
		Opcode: SET,
		Cas:    0,
		Opaque: 7242,
		Extras: []byte{1},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	data := req.Bytes()
	data[0] = 0x83

	req2 := Request{}
	_, err := req2.Receive(bytes.NewReader(data), nil)
	if err == nil {
		t.Fatalf("Expected error, got %#v", req2)
	}
}

func TestReceiveRequestLongBody(t *testing.T) {
	req := Request{
		Opcode: SET,
		Cas:    0,
		Opaque: 7242,
		Extras: []byte{1},
		Key:    []byte("somekey"),
		Body:   make([]byte, MaxBodyLen+5),
	}

	data := req.Bytes()

	req2 := Request{}
	_, err := req2.Receive(bytes.NewReader(data), nil)
	if err == nil {
		t.Fatalf("Expected error, got %#v", req2)
	}
}

func BenchmarkReceiveRequest(b *testing.B) {
	req := Request{
		Opcode: SET,
		Cas:    0,
		Opaque: 7242,
		Extras: []byte{1},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	data := req.Bytes()
	data[0] = REQ_MAGIC
	rdr := bytes.NewReader(data)

	b.SetBytes(int64(len(data)))

	b.ResetTimer()
	buf := make([]byte, HDR_LEN)
	for i := 0; i < b.N; i++ {
		req2 := Request{}
		rdr.Seek(0, 0)
		_, err := req2.Receive(rdr, buf)
		if err != nil {
			b.Fatalf("Error receiving: %v", err)
		}
	}
}

func BenchmarkReceiveRequestNoBuf(b *testing.B) {
	req := Request{
		Opcode: SET,
		Cas:    0,
		Opaque: 7242,
		Extras: []byte{1},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	data := req.Bytes()
	data[0] = REQ_MAGIC
	rdr := bytes.NewReader(data)

	b.SetBytes(int64(len(data)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req2 := Request{}
		rdr.Seek(0, 0)
		_, err := req2.Receive(rdr, nil)
		if err != nil {
			b.Fatalf("Error receiving: %v", err)
		}
	}
}

func TestRequest_prepareExtras(t *testing.T) {
	type fields struct {
		Opcode OpCode
	}
	type args struct {
		expiration uint32
		delta      uint64
		initVal    uint64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		expect []byte
	}{
		{
			name: "GET must not have extras",
			fields: fields{
				Opcode: GET,
			},
			args: args{
				expiration: 256,
				delta:      1,
				initVal:    1,
			},
			expect: nil,
		},
		{
			name: "SET",
			fields: fields{
				Opcode: SET,
			},
			args: args{
				expiration: 256,
				delta:      1,
				initVal:    1,
			},
			expect: []byte{
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x01, 0x00,
			},
		},
		{
			name: "INCREMENT",
			fields: fields{
				Opcode: INCREMENT,
			},
			args: args{
				expiration: 256,
				delta:      1,
				initVal:    42,
			},
			expect: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a,
				0x00, 0x00, 0x01, 0x00,
			},
		},
		{
			name: "FLUSH",
			fields: fields{
				Opcode: FLUSH,
			},
			args: args{
				expiration: 256,
				delta:      0,
				initVal:    0,
			},
			expect: []byte{
				0x00, 0x00, 0x01, 0x00,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Request{
				Opcode: tt.fields.Opcode,
			}
			r.prepareExtras(tt.args.expiration, tt.args.delta, tt.args.initVal)

			if !bytes.Equal(r.Extras, tt.expect) {
				t.Fatalf("Expected %#v == %#v", r.Extras, tt.expect)
			}
		})
	}
}
