package memcached

import (
	"context"
	"strings"

	"github.com/aliexpressru/gomemcached/logger"
	"github.com/aliexpressru/gomemcached/utils"
)

type contextKey string

const debugLogKey contextKey = "gomemcached:debug"

// EnableDebugLog adds debug logging flag to the context.
// When enabled, it logs information about which keys are sent to which nodes.
//
// Usage:
//
//	if debugOn {
//	    ctx = memcached.EnableDebugLog(ctx)
//	}
//	result, err := client.MultiGet(ctx, keys)
func EnableDebugLog(ctx context.Context) context.Context {
	return context.WithValue(ctx, debugLogKey, true)
}

// isDebugEnabled checks if debug logging is enabled in the context.
func isDebugEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	val, ok := ctx.Value(debugLogKey).(bool)
	return ok && val
}

// logDebugNodeKeys logs which keys are being sent to which node.
func logDebugNodeKeys(ctx context.Context, method string, node any, keys []string) {
	if !isDebugEnabled(ctx) || logger.IsDisable() {
		return
	}

	nodeAddr := utils.Repr(node)
	keysStr := strings.Join(keys, ", ")

	logger.Infof(ctx, "gomemcached: %s %s - [%s]", method, nodeAddr, keysStr)
}

// logDebugSingleKey logs which key is being sent to which node for single operations.
func logDebugSingleKey(ctx context.Context, method string, node any, key string) {
	if !isDebugEnabled(ctx) || logger.IsDisable() {
		return
	}

	nodeAddr := utils.Repr(node)
	logger.Infof(ctx, "gomemcached: %s %s - [%s]", method, nodeAddr, key)
}

// logDebugNodes logs the distribution of keys across nodes.
// It logs one line per node showing which keys are sent to that node.
func logDebugNodes(ctx context.Context, method string, nodes map[any][]string) {
	if !isDebugEnabled(ctx) || logger.IsDisable() {
		return
	}

	for node, keys := range nodes {
		logDebugNodeKeys(ctx, method, node, keys)
	}
}
