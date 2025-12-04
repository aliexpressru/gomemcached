// nolint
package memcached

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_observeMethodDurationSeconds(t *testing.T) {
	type args struct {
		methodName   string
		duration     float64
		isSuccessful bool
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "60 true",
			args: args{
				methodName:   "TestMeth",
				duration:     60 * time.Millisecond.Seconds(),
				isSuccessful: true,
			},
		},
		{
			name: "15 true",
			args: args{
				methodName:   "TestMeth",
				duration:     15 * time.Millisecond.Seconds(),
				isSuccessful: true,
			},
		},
		{
			name: "39 true",
			args: args{
				methodName:   "TestMeth",
				duration:     39 * time.Millisecond.Seconds(),
				isSuccessful: true,
			},
		},
		{
			name: "100 false",
			args: args{
				methodName:   "TestMeth",
				duration:     100 * time.Millisecond.Seconds(),
				isSuccessful: false,
			},
		},
		{
			name: "66 true",
			args: args{
				methodName:   "TestMeth",
				duration:     66 * time.Millisecond.Seconds(),
				isSuccessful: true,
			},
		},
		{
			name: "11 false",
			args: args{
				methodName:   "TestMeth",
				duration:     11 * time.Millisecond.Seconds(),
				isSuccessful: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observeMethodDurationSeconds(tt.args.methodName, tt.args.duration, tt.args.isSuccessful)

			var success = "0"
			if tt.args.isSuccessful {
				success = "1"
			}

		// Ensure metrics are initialized
		if metricsRegistry.methodDurationSeconds == nil {
			initMetrics(nil, nil, nil, "")
		}

			_, err := metricsRegistry.methodDurationSeconds.GetMetricWith(map[string]string{methodNameLabel: tt.args.methodName, isSuccessfulLabel: success})
			assert.Nil(t, err, "GetMetricWith: returned error is not nil - %v", err)
		})
	}
}

func TestObserveMethodDurationSecondsLazyInit(t *testing.T) {
	// This test covers the case where observeMethodDurationSeconds is called
	// without prior initialization, triggering lazy initialization with defaults

	// Save current state
	oldDurationMetric := metricsRegistry.methodDurationSeconds
	oldSizeMetric := metricsRegistry.objectSizeBytes
	oldMu := metricsRegistry.mu

	// Reset to simulate uninitialized state
	metricsRegistry.methodDurationSeconds = nil
	metricsRegistry.objectSizeBytes = nil
	metricsRegistry.mu = sync.Once{}

	// Restore after test
	defer func() {
		metricsRegistry.methodDurationSeconds = oldDurationMetric
		metricsRegistry.objectSizeBytes = oldSizeMetric
		metricsRegistry.mu = oldMu
	}()

	// Call observeMethodDurationSeconds without prior initialization
	// This should trigger the lazy initialization with defaults (lines 85-88)
	observeMethodDurationSeconds("LazyInitTest", 0.05, true)

	// Verify metrics were initialized
	assert.NotNil(t, metricsRegistry.methodDurationSeconds, "Duration metrics should be auto-initialized")
	assert.NotNil(t, metricsRegistry.objectSizeBytes, "Size metrics should be auto-initialized")

	// Verify the metric was recorded
	metric, err := metricsRegistry.methodDurationSeconds.GetMetricWith(map[string]string{
		methodNameLabel:   "LazyInitTest",
		isSuccessfulLabel: "1",
	})
	assert.NoError(t, err, "Should be able to retrieve recorded metric")
	assert.NotNil(t, metric, "Metric should exist after observation")
}

func TestObserveObjectSizeBytesLazyInit(t *testing.T) {
	// This test covers the case where observeObjectSizeBytes is called
	// without prior initialization, triggering lazy initialization with defaults

	// Save current state
	oldDurationMetric := metricsRegistry.methodDurationSeconds
	oldSizeMetric := metricsRegistry.objectSizeBytes
	oldMu := metricsRegistry.mu

	// Reset to simulate uninitialized state
	metricsRegistry.methodDurationSeconds = nil
	metricsRegistry.objectSizeBytes = nil
	metricsRegistry.mu = sync.Once{}

	// Restore after test
	defer func() {
		metricsRegistry.methodDurationSeconds = oldDurationMetric
		metricsRegistry.objectSizeBytes = oldSizeMetric
		metricsRegistry.mu = oldMu
	}()

	// Call observeObjectSizeBytes without prior initialization
	// This should trigger the lazy initialization with defaults (lines 101-104)
	observeObjectSizeBytes("LazyInitTest", 1024.0)

	// Verify metrics were initialized
	assert.NotNil(t, metricsRegistry.methodDurationSeconds, "Duration metrics should be auto-initialized")
	assert.NotNil(t, metricsRegistry.objectSizeBytes, "Size metrics should be auto-initialized")

	// Verify the metric was recorded (no is_successful label for size metric)
	metric, err := metricsRegistry.objectSizeBytes.GetMetricWith(map[string]string{
		methodNameLabel: "LazyInitTest",
	})
	assert.NoError(t, err, "Should be able to retrieve recorded metric")
	assert.NotNil(t, metric, "Metric should exist after observation")
}

func TestCustomMetricsRegistry(t *testing.T) {
	// Reset metrics for clean state
	metricsRegistry.mu = sync.Once{}
	metricsRegistry.methodDurationSeconds = nil
	metricsRegistry.objectSizeBytes = nil

	// Create a custom registry
	customRegistry := prometheus.NewRegistry()

	// Initialize metrics with custom registry
	initMetrics(customRegistry, nil, nil, "")

	// Observe values to make sure the metrics are created
	observeMethodDurationSeconds("TestMethod", 0.1, true)
	observeObjectSizeBytes("TestMethod", 1024.0)

	// Verify the metrics were registered
	families, err := customRegistry.Gather()
	require.NoError(t, err)

	// Find our metrics
	foundDuration := false
	foundSize := false
	for _, family := range families {
		if family.GetName() == "gomemcached_method_duration_seconds" {
			foundDuration = true
		}
		if family.GetName() == "gomemcached_object_size_bytes" {
			foundSize = true
		}
	}

	assert.True(t, foundDuration, "Expected duration metric should be registered in custom registry")
	assert.True(t, foundSize, "Expected size metric should be registered in custom registry")
}

func TestCustomMetricsBuckets(t *testing.T) {
	customBuckets := []float64{0.001, 0.01, 0.1, 1.0, 10.0}

	// Create a new registry for this test
	testRegistry := prometheus.NewRegistry()

	// Create new histogram directly to test bucket configuration
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "",
		Name:      "test_custom_buckets",
		Help:      "test histogram with custom buckets",
		Buckets:   customBuckets,
	}, []string{
		methodNameLabel,
		isSuccessfulLabel,
	})

	err := testRegistry.Register(histogram)
	require.NoError(t, err, "Should register test histogram")

	// Record some observations
	histogram.WithLabelValues("TestMethod", "1").Observe(0.005)

	// Gather metrics to verify buckets
	families, err := testRegistry.Gather()
	require.NoError(t, err)

	// Find our metric and verify buckets
	for _, family := range families {
		if family.GetName() == "test_custom_buckets" {
			metrics := family.GetMetric()
			require.Greater(t, len(metrics), 0, "Should have at least one metric")

			histogram := metrics[0].GetHistogram()
			require.NotNil(t, histogram, "Should have histogram data")

			buckets := histogram.GetBucket()
			// Prometheus adds +Inf bucket automatically, so we expect customBuckets length
			assert.GreaterOrEqual(t, len(buckets), len(customBuckets), "Should have at least the custom buckets")
		}
	}
}

func TestMetricsWithClientInitialization(t *testing.T) {
	ctx := context.TODO()

	// Test 1: Client initialization with default metrics
	t.Run("default_initialization", func(t *testing.T) {
		client, err := InitFromEnv(
			ctx,
			WithServersList([]string{"localhost:11211"}),
		)
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.CloseAllConns(ctx)

		// Verify metrics were initialized
		assert.NotNil(t, metricsRegistry.methodDurationSeconds, "Metrics should be initialized")
	})

	// Test 2: Client initialization with custom registry option (metrics should still work)
	t.Run("with_custom_registry_option", func(t *testing.T) {
		customRegistry := prometheus.NewRegistry()

		client, err := InitFromEnv(
			ctx,
			WithServersList([]string{"localhost:11211"}),
			WithMetricsRegisterer(customRegistry),
		)
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.CloseAllConns(ctx)

		// Verify client was created successfully
		assert.NotNil(t, client, "Client should be initialized")
	})

	// Test 3: Client initialization with custom buckets option
	t.Run("with_custom_buckets_option", func(t *testing.T) {
		customDurationBuckets := []float64{0.001, 0.01, 0.1, 1.0}
		customSizeBuckets := []float64{100, 1000, 10000, 100000, 1000000}

		client, err := InitFromEnv(
			ctx,
			WithServersList([]string{"localhost:11211"}),
			WithMetricsDurationBuckets(customDurationBuckets),
			WithMetricsObjectSizeBuckets(customSizeBuckets),
		)
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.CloseAllConns(ctx)

		// Verify client was created successfully
		assert.NotNil(t, client, "Client should be initialized")
	})

	// Test 4: Metrics disabled
	t.Run("metrics_disabled", func(t *testing.T) {
		client, err := InitFromEnv(
			ctx,
			WithServersList([]string{"localhost:11211"}),
			WithDisableMemcachedDiagnostic(),
		)
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.CloseAllConns(ctx)

		// Client should work fine even with metrics disabled
		assert.True(t, client.disableMemcachedDiagnostic, "Metrics should be disabled")
	})
}

func TestDefaultBuckets(t *testing.T) {
	expectedDuration := []float64{
		0.0005, 0.001, 0.005, 0.007, 0.015, 0.05, 0.1, 0.2, 0.5, 1,
	}
	assert.Equal(t, expectedDuration, defaultDurationBuckets, "Default duration buckets should match expected values")

	expectedSize := []float64{
		10, 100, 1024, 10240, 51200, 102400, 524288, 1048576, 5242880, 10485760,
	}
	assert.Equal(t, expectedSize, defaultSizeBuckets, "Default size buckets should match expected values")
}

func TestWithNamespaceMetrics(t *testing.T) {
	ctx := context.TODO()

	t.Run("default_namespace_aer_in_metrics", func(t *testing.T) {
		// Reset metrics for clean state
		metricsRegistry.mu = sync.Once{}
		metricsRegistry.methodDurationSeconds = nil
		metricsRegistry.objectSizeBytes = nil

		// Create a custom registry to isolate this test
		customRegistry := prometheus.NewRegistry()

		client, err := InitFromEnv(
			ctx,
			WithServersList([]string{"localhost:11211"}),
			WithMetricsRegisterer(customRegistry),
		)
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.CloseAllConns(ctx)

		// Observe some metrics
		observeMethodDurationSeconds("TestMethod", 0.1, true)
		observeObjectSizeBytes("TestMethod", 1024.0)

		// Gather metrics and verify default aer namespace
		families, err := customRegistry.Gather()
		require.NoError(t, err)

		foundDuration := false
		foundSize := false
		for _, family := range families {
			if family.GetName() == "aer_gomemcached_method_duration_seconds" {
				foundDuration = true
			}
			if family.GetName() == "aer_gomemcached_object_size_bytes" {
				foundSize = true
			}
		}

		assert.True(t, foundDuration, "Expected duration metric with default 'aer' namespace")
		assert.True(t, foundSize, "Expected size metric with default 'aer' namespace")
	})

	t.Run("override_namespace_to_empty", func(t *testing.T) {
		// Reset metrics for clean state
		metricsRegistry.mu = sync.Once{}
		metricsRegistry.methodDurationSeconds = nil
		metricsRegistry.objectSizeBytes = nil

		// Create a custom registry to isolate this test
		customRegistry := prometheus.NewRegistry()

		client, err := InitFromEnv(
			ctx,
			WithServersList([]string{"localhost:11211"}),
			WithNamespace(""), // Override to no prefix
			WithMetricsRegisterer(customRegistry),
		)
		require.NoError(t, err)
		require.NotNil(t, client)
		defer client.CloseAllConns(ctx)

		// Observe some metrics
		observeMethodDurationSeconds("TestMethod", 0.1, true)
		observeObjectSizeBytes("TestMethod", 1024.0)

		// Gather metrics and verify no namespace prefix
		families, err := customRegistry.Gather()
		require.NoError(t, err)

		foundDuration := false
		foundSize := false
		for _, family := range families {
			if family.GetName() == "gomemcached_method_duration_seconds" {
				foundDuration = true
			}
			if family.GetName() == "gomemcached_object_size_bytes" {
				foundSize = true
			}
		}

		assert.True(t, foundDuration, "Expected duration metric without namespace")
		assert.True(t, foundSize, "Expected size metric without namespace")
	})
}
