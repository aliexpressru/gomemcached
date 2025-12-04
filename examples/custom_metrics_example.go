// nolint
package main

import (
	"context"
	"os"

	"github.com/aliexpressru/gomemcached/memcached"
	"github.com/prometheus/client_golang/prometheus"
)

// Example demonstrating custom metrics configuration:
// 1. Using a custom Prometheus registry
// 2. Using custom histogram buckets for latency measurement
// 3. Using custom histogram buckets for object size measurement
// 4. Using custom namespace for environment variables and metrics
func exampleCustomMetrics() {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	// By default, the client uses "aer" namespace for environment variables
	_ = os.Setenv("AER_MEMCACHED_SERVERS", "localhost:11211")

	// Example 1: Use default prometheus registry, default buckets, and default "aer" namespace
	// This is the simplest case - metrics will be automatically registered
	// with prometheus.DefaultRegisterer
	// Metrics will be: aer_gomemcached_method_duration_seconds, aer_gomemcached_object_size_bytes
	mclDefault, err := memcached.InitFromEnv(ctx)
	mustInit(err)
	defer mclDefault.CloseAllConns(ctx)

	// Example 2: Use custom prometheus registry
	// Useful when you need to isolate metrics or use multiple registries
	// Metrics will still use "aer" namespace: aer_gomemcached_*
	customRegistry := prometheus.NewRegistry()

	mclCustomRegistry, err := memcached.InitFromEnv(
		ctx,
		memcached.WithMetricsRegisterer(customRegistry),
	)
	mustInit(err)
	defer mclCustomRegistry.CloseAllConns(ctx)

	// Now you can export metrics from customRegistry separately
	// For example, using different HTTP endpoints for different clients

	// Example 3: Use custom histogram buckets for duration
	// Useful when your expected latency profile is different from defaults
	// Default duration buckets: [0.0005, 0.001, 0.005, 0.007, 0.015, 0.05, 0.1, 0.2, 0.5, 1]
	customDurationBuckets := []float64{
		0.001, // 1ms
		0.005, // 5ms
		0.01,  // 10ms
		0.025, // 25ms
		0.05,  // 50ms
		0.1,   // 100ms
		0.25,  // 250ms
		0.5,   // 500ms
		1.0,   // 1s
		2.5,   // 2.5s
		5.0,   // 5s
	}

	// Default size buckets: [100, 1024, 10240, 51200, 102400, 524288, 1048576, 5242880, 10485760]
	// (100 bytes, 1KB, 10KB, 50KB, 100KB, 512KB, 1MB, 5MB, 10MB)
	customSizeBuckets := []float64{
		1024,    // 1KB
		10240,   // 10KB
		102400,  // 100KB
		1048576, // 1MB
	}

	mclCustomBuckets, err := memcached.InitFromEnv(
		ctx,
		memcached.WithMetricsDurationBuckets(customDurationBuckets),
		memcached.WithMetricsObjectSizeBuckets(customSizeBuckets),
	)
	mustInit(err)
	defer mclCustomBuckets.CloseAllConns(ctx)

	// Example 4: Combine custom registry and custom buckets
	anotherRegistry := prometheus.NewRegistry()

	mclFullyCustom, err := memcached.InitFromEnv(
		ctx,
		memcached.WithMetricsRegisterer(anotherRegistry),
		memcached.WithMetricsDurationBuckets(customDurationBuckets),
		memcached.WithMetricsObjectSizeBuckets(customSizeBuckets),
	)
	mustInit(err)
	defer mclFullyCustom.CloseAllConns(ctx)

	// Example 5: Disable metrics entirely
	mclNoMetrics, err := memcached.InitFromEnv(
		ctx,
		memcached.WithDisableMemcachedDiagnostic(),
	)
	mustInit(err)
	defer mclNoMetrics.CloseAllConns(ctx)
}
