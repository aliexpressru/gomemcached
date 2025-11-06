package memcached

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnableDebugLog(t *testing.T) {
	ctx := context.Background()

	// Initially debug should not be enabled
	assert.False(t, isDebugEnabled(ctx), "Debug should not be enabled by default")

	// Enable debug logging
	ctx = EnableDebugLog(ctx)
	assert.True(t, isDebugEnabled(ctx), "Debug should be enabled after EnableDebugLog")
}

func TestIsDebugEnabled(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{
			name: "nil context",
			ctx:  nil,
			want: false,
		},
		{
			name: "context without debug",
			ctx:  context.Background(),
			want: false,
		},
		{
			name: "context with debug enabled",
			ctx:  EnableDebugLog(context.Background()),
			want: true,
		},
		{
			name: "context with wrong value type",
			ctx:  context.WithValue(context.Background(), debugLogKey, "wrong"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isDebugEnabled(tt.ctx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLogDebugNodeKeys(t *testing.T) {
	ctx := EnableDebugLog(context.Background())

	addr := &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 11211,
	}

	keys := []string{"test1", "test2"}

	// This should not panic
	logDebugNodeKeys(ctx, "TestMethod", addr, keys)

	// Test with disabled debug
	ctx = context.Background()
	logDebugNodeKeys(ctx, "TestMethod", addr, keys)
}

func TestLogDebugSingleKey(t *testing.T) {
	ctx := EnableDebugLog(context.Background())

	addr := &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 11211,
	}

	key := "testkey"

	// This should not panic
	logDebugSingleKey(ctx, "Get", addr, key)

	// Test with disabled debug
	ctx = context.Background()
	logDebugSingleKey(ctx, "Get", addr, key)
}

func TestLogDebugNodes(t *testing.T) {
	ctx := EnableDebugLog(context.Background())

	addr1 := &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 11211,
	}
	addr2 := &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 2),
		Port: 11211,
	}

	nodes := map[any][]string{
		addr1: {"key1", "key2"},
		addr2: {"key3", "key4"},
	}

	// This should not panic
	logDebugNodes(ctx, "TestMethod", nodes)

	// Test with disabled debug
	ctx = context.Background()
	logDebugNodes(ctx, "TestMethod", nodes)
}
