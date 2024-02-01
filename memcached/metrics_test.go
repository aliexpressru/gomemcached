package memcached

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

			_, err := methodDurationSeconds.GetMetricWith(map[string]string{methodNameLabel: tt.args.methodName, isSuccessfulLabel: success})
			assert.Nil(t, err, "GetMetricWith: returned error is not nil - %v", err)
		})
	}
}
