package memcached

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	methodNameLabel   = "method_name"
	isSuccessfulLabel = "is_successful"
)

var (
	// defaultDurationBuckets are the default histogram buckets for method duration in seconds
	defaultDurationBuckets = []float64{
		0.0005, 0.001, 0.005, 0.007, 0.015, 0.05, 0.1, 0.2, 0.5, 1,
	}

	// defaultSizeBuckets are the default histogram buckets for object size in bytes
	// Memcached typically has a 1MB limit per item, these buckets cover common sizes
	defaultSizeBuckets = []float64{
		10,         // 10 bytes
		100,        // 100 bytes
		1024,       // 1 KB
		10_240,     // 10 KB
		51_200,     // 50 KB
		102_400,    // 100 KB
		524_288,    // 512 KB
		1_048_576,  // 1 MB
		5_242_880,  // 5 MB
		10_485_760, // 10 MB
	}

	// metricsRegistry stores the metrics configuration
	metricsRegistry struct {
		mu                    sync.Once
		methodDurationSeconds *prometheus.HistogramVec
		objectSizeBytes       *prometheus.HistogramVec
	}
)

// initMetrics initializes the metrics with custom configuration
func initMetrics(registerer prometheus.Registerer, durationBuckets []float64, sizeBuckets []float64, namespace string) {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}

	if len(durationBuckets) == 0 {
		durationBuckets = defaultDurationBuckets
	}

	if len(sizeBuckets) == 0 {
		sizeBuckets = defaultSizeBuckets
	}

	metricsRegistry.mu.Do(func() {
		metricsRegistry.methodDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "gomemcached_method_duration_seconds",
			Help:      "counts the execution time of successful and failed gomemcached methods",
			Buckets:   durationBuckets,
		}, []string{
			methodNameLabel,
			isSuccessfulLabel,
		})

		metricsRegistry.objectSizeBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "gomemcached_object_size_bytes",
			Help:      "tracks the size of objects being stored in memcached",
			Buckets:   sizeBuckets,
		}, []string{
			methodNameLabel,
		})

		// Register with provided registerer, ignore if already registered
		_ = registerer.Register(metricsRegistry.methodDurationSeconds)
		_ = registerer.Register(metricsRegistry.objectSizeBytes)
	})
}

// observeMethodDurationSeconds is observing the duration of a method.
func observeMethodDurationSeconds(methodName string, duration float64, isSuccessful bool) {
	if metricsRegistry.methodDurationSeconds == nil {
		// Initialize with defaults if not yet initialized
		initMetrics(nil, nil, nil, "")
	}

	flag := "0"
	if isSuccessful {
		flag = "1"
	}

	metricsRegistry.methodDurationSeconds.
		WithLabelValues(methodName, flag).
		Observe(duration)
}

// observeObjectSizeBytes is observing the size of an object in bytes.
func observeObjectSizeBytes(methodName string, sizeBytes float64) {
	if metricsRegistry.objectSizeBytes == nil {
		// Initialize with defaults if not yet initialized
		initMetrics(nil, nil, nil, "")
	}

	metricsRegistry.objectSizeBytes.
		WithLabelValues(methodName).
		Observe(sizeBytes)
}
