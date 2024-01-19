package memcached

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// MaxBodyLen a maximum reasonable body length to expect.
	// Anything larger than this will result in an error.
	MaxBodyLen = int(22 * 1e6) // 22 MB

	BUF_LEN = 256

	// reserved<bit> always 0
	reserved8  = uint8(0)
	reserved16 = uint16(0)
)

// Request a Memcached request
type Request struct {
	// The command being issued
	Opcode OpCode
	// The CAS (if applicable, or 0)
	Cas uint64
	// An opaque value to be returned with this request
	Opaque uint32
	// Command extras, key, and body
	Extras, Key, Body []byte
}

// Size is a number of bytes this request requires.
func (r *Request) Size() int {
	return HDR_LEN + len(r.Extras) + len(r.Key) + len(r.Body)
}

// String is a debugging string representation of this request
func (r Request) String() string {
	return fmt.Sprintf("{Request opcode=%s, bodylen=%d, key='%s'}",
		r.Opcode, len(r.Body), r.Key)
}

func (r *Request) fillHeaderBytes(data []byte) int {
	/*
	   Byte/     0       |       1       |       2       |       3       |
	      /              |               |               |               |
	     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
	     +---------------+---------------+---------------+---------------+
	    0| Magic         | Opcode        | Key length                    |
	     +---------------+---------------+---------------+---------------+
	    4| Extras length | Data type     | vbucket id                    |
	     +---------------+---------------+---------------+---------------+
	    8| Total body length                                             |
	     +---------------+---------------+---------------+---------------+
	   12| Opaque                                                        |
	     +---------------+---------------+---------------+---------------+
	   16| CAS                                                           |
	     |                                                               |
	     +---------------+---------------+---------------+---------------+
	     Total 24 bytes
	*/

	pos := 0
	data[pos] /*0x00*/ = REQ_MAGIC
	pos++ // 1
	data[pos] /*0x01*/ = byte(r.Opcode)
	pos++ // 2
	binary.BigEndian.PutUint16(data[pos:pos+2] /*0x02 - 0x03*/, uint16(len(r.Key)))

	pos += 2 // 4
	data[pos] /*0x04*/ = byte(len(r.Extras))

	pos++ // 5
	data[pos] /*0x05*/ = reserved8

	pos++ // 6
	binary.BigEndian.PutUint16(data[pos:pos+2] /*0x06*/, reserved16)

	pos += 2 // 8
	binary.BigEndian.PutUint32(data[pos:pos+4] /*0x08 - 0x09 - 0x0a - 0x0b*/, uint32(len(r.Body)+len(r.Key)+len(r.Extras)))

	pos += 4 // 12
	binary.BigEndian.PutUint32(data[pos:pos+4] /*0x0c - 0x0d - 0x0e - 0x0f*/, r.Opaque)

	pos += 4 // 16
	if r.Cas != 0 {
		binary.BigEndian.PutUint64(data[pos:pos+8] /*0x10 - 0x11 - 0x12 - 0x13 - 0x14 - 0x16 - 0x17*/, r.Cas)
	}

	pos += 8 // 24
	if len(r.Extras) > 0 {
		copy(data[pos:pos+len(r.Extras)], r.Extras)
		pos += len(r.Extras)
	}

	if len(r.Key) > 0 {
		copy(data[pos:pos+len(r.Key)], r.Key)
		pos += len(r.Key)
	}
	return pos
}

// HeaderBytes is a wire representation of the header (with the extras and key)
func (r *Request) HeaderBytes() []byte {
	data := make([]byte, HDR_LEN+len(r.Extras)+len(r.Key))

	r.fillHeaderBytes(data)

	return data
}

// Bytes is a wire representation of this request.
func (r *Request) Bytes() []byte {
	data := make([]byte, r.Size())

	pos := r.fillHeaderBytes(data)

	if len(r.Body) > 0 {
		copy(data[pos:pos+len(r.Body)], r.Body)
	}

	return data
}

// Transmit is send this request message across a writer.
func (r *Request) Transmit(w io.Writer) (n int, err error) {
	if len(r.Body) < BODY_LEN {
		n, err = w.Write(r.Bytes())
	} else {
		n, err = w.Write(r.HeaderBytes())
		if err == nil {
			m := 0
			m, err = w.Write(r.Body)
			n += m
		}
	}
	return
}

// Receive a fill this Request with the data from this reader.
func (r *Request) Receive(rd io.Reader, hdrBytes []byte) (int, error) {
	/*
	   Byte/     0       |       1       |       2       |       3       |
	      /              |               |               |               |
	     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
	     +---------------+---------------+---------------+---------------+
	    0| Magic         | Opcode        | Key length                    |
	     +---------------+---------------+---------------+---------------+
	    4| Extras length | Data type     | vbucket id                    |
	     +---------------+---------------+---------------+---------------+
	    8| Total body length                                             |
	     +---------------+---------------+---------------+---------------+
	   12| Opaque                                                        |
	     +---------------+---------------+---------------+---------------+
	   16| CAS                                                           |
	     |                                                               |
	     +---------------+---------------+---------------+---------------+
	     Total 24 bytes
	*/

	if len(hdrBytes) < HDR_LEN {
		hdrBytes = make([]byte, HDR_LEN)
	}

	n, err := io.ReadFull(rd, hdrBytes)
	if err != nil {
		return n, err
	}

	if hdrBytes[0] != RES_MAGIC && hdrBytes[0] != REQ_MAGIC {
		return n, fmt.Errorf("bad magic: 0x%02x", hdrBytes[0])
	}
	r.Opcode = OpCode(hdrBytes[1])

	klen := int(binary.BigEndian.Uint16(hdrBytes[2:]))
	elen := int(hdrBytes[4])
	bodyLen := int(binary.BigEndian.Uint32(hdrBytes[8:]) - uint32(klen) - uint32(elen))
	if bodyLen > MaxBodyLen {
		return n, fmt.Errorf("%d is too big (max %d)",
			bodyLen, MaxBodyLen)
	}
	r.Opaque = binary.BigEndian.Uint32(hdrBytes[12:])
	r.Cas = binary.BigEndian.Uint64(hdrBytes[16:])

	buf := make([]byte, klen+elen+bodyLen)
	m, err := io.ReadFull(rd, buf)
	n += m
	if err == nil {
		if elen > 0 {
			r.Extras = buf[0:elen]
		}
		if klen > 0 {
			r.Key = buf[elen : klen+elen]
		}
		if klen+elen > 0 {
			r.Body = buf[klen+elen:]
		}
	}

	return n, err
}

// prepareExtras fills Extras depending on OpCode for Request
func (r *Request) prepareExtras(expiration uint32, delta uint64, initVal uint64) {
	switch r.Opcode {
	case DELETE, DELETEQ, QUIT, QUITQ, NOOP, VERSION, APPEND, APPENDQ, PREPEND, PREPENDQ, STAT, GET, GETQ, GETK, GETKQ: // MUST NOT have extras
	case SET, SETQ, ADD, ADDQ, REPLACE, REPLACEQ:
		/*
		   Byte/     0       |       1       |       2       |       3       |
		      /              |               |               |               |
		     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
		     +---------------+---------------+---------------+---------------+
		    0| Flags                                                         |
		     +---------------+---------------+---------------+---------------+
		    4| Expiration                                                    |
		     +---------------+---------------+---------------+---------------+
		     Total 8 bytes
		*/

		r.Extras = make([]byte, 8)
		// flags always is 0
		binary.BigEndian.PutUint32(r.Extras[:4], uint32(0))
		binary.BigEndian.PutUint32(r.Extras[4:], expiration)
	case INCREMENT, INCREMENTQ, DECREMENT, DECREMENTQ:
		/*

		   Byte/     0       |       1       |       2       |       3       |
		      /              |               |               |               |
		     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
		     +---------------+---------------+---------------+---------------+
		    0| Amount to add / subtract (delta)                              |
		     |                                                               |
		     +---------------+---------------+---------------+---------------+
		    8| Initial value                                                 |
		     |                                                               |
		     +---------------+---------------+---------------+---------------+
		   16| Expiration                                                    |
		     +---------------+---------------+---------------+---------------+
		     Total 20 bytes
		*/
		r.Extras = make([]byte, 20)
		binary.BigEndian.PutUint64(r.Extras[:8], delta)
		binary.BigEndian.PutUint64(r.Extras[8:], initVal)
		binary.BigEndian.PutUint32(r.Extras[16:], expiration)
	case FLUSH, FLUSHQ:
		/*
		   Byte/     0       |       1       |       2       |       3       |
		      /              |               |               |               |
		     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
		     +---------------+---------------+---------------+---------------+
		    0| Expiration                                                    |
		     +---------------+---------------+---------------+---------------+
		   Total 4 bytes
		*/
		r.Extras = make([]byte, 4)
		binary.BigEndian.PutUint32(r.Extras, expiration)
	}
}

type StoreMode uint8

const (
	// Add - Store the data, but only if the server does not already hold data for a given key
	Add StoreMode = iota
	// Set -  Store the data, overwrite if already exists
	Set
	// Replace - Store the data, but only if the server does already hold data for a given key
	Replace
)

func (sm StoreMode) Resolve() OpCode {
	switch sm {
	case Set:
		return SET
	case Replace:
		return REPLACE
	default:
		return ADD
	}
}

type DeltaMode uint8

const (
	// Increment - increases the value by the specified amount
	Increment DeltaMode = iota
	// Decrement - decreases the value by the specified amount
	Decrement
)

func (sm DeltaMode) Resolve() OpCode {
	switch sm {
	case Increment:
		return INCREMENT
	case Decrement:
		return DECREMENT
	default:
		return INCREMENT
	}
}

type AppendMode uint8

const (
	// Append - Appends data to the end of an existing key value.
	Append AppendMode = iota
	// Prepend -Appends data to the beginning of an existing key value.
	Prepend
)

func (sm AppendMode) Resolve() OpCode {
	switch sm {
	case Append:
		return APPEND
	default:
		return PREPEND
	}
}
