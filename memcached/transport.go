package memcached

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

// UnwrapMemcachedError converts memcached errors to normal responses.
//
// If the error is a memcached response, declare the error to be nil
// so a client can handle the status without worrying about whether it
// indicates success or failure.
//
// The second return value (ok) indicates whether the error contains a *Response.
// When ok is false, the returned *Response is nil.
func UnwrapMemcachedError(err error) (resp *Response, ok bool) {
	var res *Response
	if errors.As(err, &res) {
		return res, true
	}
	return nil, false
}

func getResponse(s io.Reader, hdrBytes []byte) (rv *Response, n int, err error) {
	if s == nil {
		return nil, 0, ErrNoServers
	}

	rv = &Response{}
	n, err = rv.receive(s, hdrBytes)
	if err == nil && rv.Status != SUCCESS {
		err = wrapMemcachedResp(rv)
	}
	return rv, n, err
}

func transmitRequest(o io.Writer, req *Request) (int, error) {
	if o == nil {
		return 0, ErrNoServers
	}
	n, err := req.transmit(o)
	return n, err
}

type deadliner interface {
	SetDeadline(ctx context.Context)
	ClearDeadline()
}

type dl struct {
	cn net.Conn
}

func newDeadliner(cn net.Conn) deadliner { // nolint:ireturn
	return &dl{cn: cn}
}

func (d *dl) SetDeadline(ctx context.Context) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = d.cn.SetDeadline(deadline)
	}
}

func (d *dl) ClearDeadline() {
	_ = d.cn.SetDeadline(time.Time{})
}
