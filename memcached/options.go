package memcached

import (
	"time"

	"github.com/aliexpressru/gomemcached/consistenthash"
	"github.com/prometheus/client_golang/prometheus"
)

type options struct {
	Client
	disableLogger            bool
	metricsRegisterer        prometheus.Registerer
	metricsDurationBuckets   []float64
	metricsObjectSizeBuckets []float64
}

type Option func(*options)

// WithMaxIdleConns is sets a custom value of open connections per address.
// By default, DefaultMaxIdleConns will be used.
func WithMaxIdleConns(num int) Option {
	return func(o *options) {
		o.maxIdleConns = num
	}
}

// WithTimeout is sets custom timeout for connections.
// By default, DefaultTimeout will be used.
func WithTimeout(tm time.Duration) Option {
	return func(o *options) {
		o.timeout = tm
	}
}

// WithCustomHashRing for setup use consistenthash.NewCustomHashRing
func WithCustomHashRing(hr *consistenthash.HashRing) Option {
	return func(o *options) {
		o.hr = hr
	}
}

// WithPeriodForNodeHealthCheck is sets a custom frequency for health checker of physical nodes.
// By default, DefaultNodeHealthCheckPeriod will be used.
func WithPeriodForNodeHealthCheck(t time.Duration) Option {
	return func(o *options) {
		o.nodeHCPeriod = t
	}
}

// WithPeriodForRebuildingNodes is sets a custom frequency for resharding and checking for dead nodes.
// By default, DefaultRebuildingNodePeriod will be used.
func WithPeriodForRebuildingNodes(t time.Duration) Option {
	return func(o *options) {
		o.nodeRBPeriod = t
	}
}

// WithDisableNodeProvider is disabled node health cheek and rebuild nodes for hash ring
func WithDisableNodeProvider() Option {
	return func(o *options) {
		o.disableNodeProvider = true
	}
}

// WithDisableRefreshConnsInPool is disabled auto close some connections in pool in NodeProvider.
// This is done to refresh connections in the pool.
func WithDisableRefreshConnsInPool() Option {
	return func(o *options) {
		o.disableRefreshConns = true
	}
}

// WithDisableMemcachedDiagnostic is disabled write library metrics.
//
//	gomemcached_method_duration_seconds
//	gomemcached_object_size_bytes
func WithDisableMemcachedDiagnostic() Option {
	return func(o *options) {
		o.disableMemcachedDiagnostic = true
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
		o.authEnable = true
		o.authData = prepareAuthData(user, pass)
	}
}

// WithHeadlessServiceAddress sets the headless service address to lookup memcached IP addresses.
// This option overrides the MEMCACHED_HEADLESS_SERVICE_ADDRESS environment variable.
// Useful when you need to configure multiple memcached clients with different headless services in the same application.
func WithHeadlessServiceAddress(addr string) Option {
	return func(o *options) {
		o.cfg.HeadlessServiceAddress = addr
	}
}

// WithServersList sets the list of memcached servers.
// This option overrides the MEMCACHED_SERVERS environment variable.
// Useful when you need to configure multiple memcached clients with different server lists in the same application.
func WithServersList(servers []string) Option {
	return func(o *options) {
		o.cfg.Servers = servers
	}
}

// WithMemcachedPort sets the memcached port for headless service addresses.
// This option overrides the MEMCACHED_PORT environment variable (default: 11211).
// Useful when you need to configure multiple memcached clients with different ports in the same application.
func WithMemcachedPort(port int) Option {
	return func(o *options) {
		o.cfg.MemcachedPort = port
	}
}

// WithMetricsRegisterer sets a custom Prometheus registerer for the library metrics.
// If not provided, prometheus.DefaultRegisterer will be used.
// This is useful when you want to use a custom registry or when you need to isolate metrics in multi-tenant applications.
func WithMetricsRegisterer(registerer prometheus.Registerer) Option {
	return func(o *options) {
		o.metricsRegisterer = registerer
	}
}

// WithMetricsDurationBuckets sets custom histogram buckets for the method duration metric.
// If not provided, default buckets will be used: [0.0005, 0.001, 0.005, 0.007, 0.015, 0.05, 0.1, 0.2, 0.5, 1]
// This allows you to adjust the buckets to match your expected latency profile.
func WithMetricsDurationBuckets(buckets []float64) Option {
	return func(o *options) {
		o.metricsDurationBuckets = buckets
	}
}

// WithMetricsObjectSizeBuckets sets custom histogram buckets for the object size metric in bytes.
// If not provided, default buckets will be used: [10, 100, 1024, 10_240, 51_200, 102_400, 524_288, 1_048_576, 5_242_880, 10_485_760]
// This allows you to adjust the buckets to match your typical object size distribution.
func WithMetricsObjectSizeBuckets(buckets []float64) Option {
	return func(o *options) {
		o.metricsObjectSizeBuckets = buckets
	}
}
