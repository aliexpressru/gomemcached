package memcached

import (
	"time"

	"github.com/aliexpressru/gomemcached/consistenthash"
)

type options struct {
	Client
	disableLogger bool
}

type Option func(*options)

// WithMaxIdleConns is sets a custom value of open connections per address.
// By default, DefaultMaxIdleConns will be used.
func WithMaxIdleConns(num int) Option {
	return func(o *options) {
		o.Client.maxIdleConns = num
	}
}

// WithTimeout is sets custom timeout for connections.
// By default, DefaultTimeout will be used.
func WithTimeout(tm time.Duration) Option {
	return func(o *options) {
		o.Client.timeout = tm
	}
}

// WithCustomHashRing for setup use consistenthash.NewCustomHashRing
func WithCustomHashRing(hr *consistenthash.HashRing) Option {
	return func(o *options) {
		o.Client.hr = hr
	}
}

// WithPeriodForNodeHealthCheck is sets a custom frequency for health checker of physical nodes.
// By default, DefaultNodeHealthCheckPeriod will be used.
func WithPeriodForNodeHealthCheck(t time.Duration) Option {
	return func(o *options) {
		o.Client.nodeHCPeriod = t
	}
}

// WithPeriodForRebuildingNodes is sets a custom frequency for resharding and checking for dead nodes.
// By default, DefaultRebuildingNodePeriod will be used.
func WithPeriodForRebuildingNodes(t time.Duration) Option {
	return func(o *options) {
		o.Client.nodeRBPeriod = t
	}
}

// WithDisableNodeProvider is disabled node health cheek and rebuild nodes for hash ring
func WithDisableNodeProvider() Option {
	return func(o *options) {
		o.Client.disableNodeProvider = true
	}
}

// WithDisableRefreshConnsInPool is disabled auto close some connections in pool in NodeProvider.
// This is done to refresh connections in the pool.
func WithDisableRefreshConnsInPool() Option {
	return func(o *options) {
		o.Client.disableRefreshConns = true
	}
}

// WithDisableMemcachedDiagnostic is disabled write library metrics.
//
//	gomemcached_method_duration_seconds
func WithDisableMemcachedDiagnostic() Option {
	return func(o *options) {
		o.Client.disableMemcachedDiagnostic = true
	}
}

// WithDisableLogger is disabled internal library logs.
func WithDisableLogger() Option {
	return func(o *options) {
		o.disableLogger = true
	}
}

// WithAuthentication is turn on authenticate for memcached
func WithAuthentication(user, pass string) Option {
	return func(o *options) {
		o.Client.authEnable = true
		o.Client.authData = prepareAuthData(user, pass)
	}
}
