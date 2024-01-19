package memcached

import (
	"errors"
	"io"
)

// UnwrapMemcachedError converts memcached errors to normal responses.
//
// If the error is a memcached response, declare the error to be nil
// so a client can handle the status without worrying about whether it
// indicates success or failure.
func UnwrapMemcachedError(err error) *Response {
	var res *Response
	if errors.As(err, &res) {
		return res
	}
	return nil
}

func getResponse(s io.Reader, hdrBytes []byte) (rv *Response, n int, err error) {
	if s == nil {
		return nil, 0, ErrNoServers
	}

	rv = &Response{}
	n, err = rv.Receive(s, hdrBytes)
	if err == nil && rv.Status != SUCCESS {
		err = wrapMemcachedResp(rv)
	}
	return rv, n, err
}

func transmitRequest(o io.Writer, req *Request) (int, error) {
	if o == nil {
		return 0, ErrNoServers
	}
	n, err := req.Transmit(o)
	return n, err
}
