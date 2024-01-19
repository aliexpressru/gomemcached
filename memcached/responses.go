package memcached

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Response is a memcached response
type Response struct {
	// The command opcode of the command that sent the request
	Opcode OpCode
	// The status of the response
	Status Status
	// The opaque sent in the request
	Opaque uint32
	// The CAS identifier (if applicable)
	Cas uint64
	// Extras, key, and body for this response
	Extras, Key, Body []byte
}

// String a debugging string representation of this response
func (r Response) String() string {
	return fmt.Sprintf("{Response status=%v keylen=%d, extralen=%d, bodylen=%d}",
		r.Status, len(r.Key), len(r.Extras), len(r.Body))
}

// Error - Response as an error.
func (r *Response) Error() string {
	return fmt.Sprintf("Response status=%v, opcode=%v, opaque=%v, msg: %s",
		r.Status, r.Opcode, r.Opaque, string(r.Body))
}

// isFatal return false if this error isn't believed to be fatal to a connection.
func isFatal(e error) bool {
	if e == nil {
		return false
	}
	switch errStatus(e) {
	case KEY_ENOENT, KEY_EEXISTS, NOT_STORED, TMPFAIL, AUTHFAIL:
		return false
	}
	return true
}

// Size is a number of bytes this response consumes on the wire.
func (r *Response) Size() int {
	return HDR_LEN + len(r.Extras) + len(r.Key) + len(r.Body)
}

func (r *Response) fillHeaderBytes(data []byte) int {
	/*
	   Byte/     0       |       1       |       2       |       3       |
	      /              |               |               |               |
	     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
	     +---------------+---------------+---------------+---------------+
	    0| Magic         | Opcode        | Key Length                    |
	     +---------------+---------------+---------------+---------------+
	    4| Extras length | Data type     | Status                        |
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
	data[pos] /*0x00*/ = RES_MAGIC
	pos++ // 1
	data[pos] /*0x01*/ = byte(r.Opcode)
	pos++ // 2
	binary.BigEndian.PutUint16(data[pos:pos+2] /*0x02 - 0x03*/, uint16(len(r.Key)))

	pos += 2 // 4
	data[pos] /*0x04*/ = byte(len(r.Extras))

	pos++ // 5
	data[pos] /*0x05*/ = reserved8

	pos++ // 6
	binary.BigEndian.PutUint16(data[pos:pos+2] /*0x06*/, uint16(r.Status))

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

// HeaderBytes get just the header bytes for this response.
func (r *Response) HeaderBytes() []byte {
	data := make([]byte, HDR_LEN+len(r.Extras)+len(r.Key))

	r.fillHeaderBytes(data)

	return data
}

// Bytes the actual bytes transmitted for this response.
func (r *Response) Bytes() []byte {
	data := make([]byte, r.Size())

	pos := r.fillHeaderBytes(data)

	copy(data[pos:pos+len(r.Body)], r.Body)

	return data
}

// Transmit send this response message across a writer.
func (r *Response) Transmit(w io.Writer) (n int, err error) {
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

// Receive - fill this Response with the data from this reader.
func (r *Response) Receive(rd io.Reader, hdrBytes []byte) (int, error) {
	/*
	   Byte/     0       |       1       |       2       |       3       |
	      /              |               |               |               |
	     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
	     +---------------+---------------+---------------+---------------+
	    0| Magic         | Opcode        | Key Length                    |
	     +---------------+---------------+---------------+---------------+
	    4| Extras length | Data type     | Status                        |
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
		return n, fmt.Errorf("Bad magic: 0x%02x", hdrBytes[0])
	}

	klen := int(binary.BigEndian.Uint16(hdrBytes[2:4]))
	elen := int(hdrBytes[4])

	r.Opcode = OpCode(hdrBytes[1])
	r.Status = Status(binary.BigEndian.Uint16(hdrBytes[6:8]))
	r.Opaque = binary.BigEndian.Uint32(hdrBytes[12:16])
	r.Cas = binary.BigEndian.Uint64(hdrBytes[16:24])

	bodyLen := int(binary.BigEndian.Uint32(hdrBytes[8:12])) - (klen + elen)

	buf := make([]byte, klen+elen+bodyLen)
	m, err := io.ReadFull(rd, buf)
	if err == nil {
		if elen > 0 {
			r.Extras = buf[0:elen]
		}
		if klen > 0 {
			r.Key = buf[elen : klen+elen]
		}
		if bodyLen > 0 {
			r.Body = buf[klen+elen:]
		}
	}

	return n + m, err
}
