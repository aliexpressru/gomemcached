// nolint
package memcached

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDeadliner_SetDeadline(t *testing.T) {
	c1, _ := net.Pipe()
	defer c1.Close()

	dl := newDeadliner(c1)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	dl.SetDeadline(ctx)
	_, ok := ctx.Deadline()
	require.True(t, ok, "deadline should be set")
}

func TestDeadliner_ClearDeadline(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	dl := newDeadliner(c1)

	err := c1.SetDeadline(time.Now().Add(1 * time.Second))
	require.NoError(t, err)

	dl.ClearDeadline()

	done := make(chan struct{})
	go func() {
		buf := make([]byte, 10)
		_, _ = c2.Read(buf)
		close(done)
	}()

	_, err = c1.Write([]byte("test"))
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("read timeout")
	}
}

func TestDeadliner_SetDeadline_NoDeadline(t *testing.T) {
	c1, _ := net.Pipe()
	defer c1.Close()

	dl := newDeadliner(c1)

	ctx := context.Background()
	dl.SetDeadline(ctx)
	_, ok := ctx.Deadline()
	assert.False(t, ok, "deadline should not be set")
}

type MockDeadliner struct {
	mock.Mock
}

func (m *MockDeadliner) SetDeadline(ctx context.Context) {
	m.Called(ctx)
}

func (m *MockDeadliner) ClearDeadline() {
	m.Called()
}
