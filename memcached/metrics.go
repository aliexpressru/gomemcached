package memcached

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	methodNameLabel   = "method_name"
	isSuccessfulLabel = "is_successful"
)

var (
	methodDurationSeconds = func() *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "",
			Name:      "gomemcached_method_duration_seconds",
			Help:      "counts the execution time of successful and failed gomemcached methods",
			Buckets: []float64{
				0.0005, 0.001, 0.005, 0.007, 0.015, 0.05, 0.1, 0.2, 0.5, 1,
			},
		}, []string{
			methodNameLabel,
			isSuccessfulLabel,
		})
	}()
)

// observeMultiMethodDurationSeconds is observing the duration of a method.
func observeMethodDurationSeconds(methodName string, duration float64, isSuccessful bool) {
	flag := "0"
	if isSuccessful {
		flag = "1"
	}

	methodDurationSeconds.
		WithLabelValues(methodName, flag).
		Observe(duration)
}
