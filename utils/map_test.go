package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPickByKeys(t *testing.T) {
	tests := []struct {
		name string
		in   map[string][]byte
		keys []string
		want map[string][]byte
	}{
		{
			name: "empty map",
			in:   map[string][]byte{},
			keys: []string{"key1", "key2"},
			want: map[string][]byte{},
		},
		{
			name: "empty keys",
			in:   map[string][]byte{"key1": []byte("value1"), "key2": []byte("value2")},
			keys: []string{},
			want: map[string][]byte{},
		},
		{
			name: "all keys exist",
			in:   map[string][]byte{"key1": []byte("value1"), "key2": []byte("value2"), "key3": []byte("value3")},
			keys: []string{"key1", "key3"},
			want: map[string][]byte{"key1": []byte("value1"), "key3": []byte("value3")},
		},
		{
			name: "no keys exist",
			in:   map[string][]byte{"key1": []byte("value1"), "key2": []byte("value2")},
			keys: []string{"key3", "key4"},
			want: map[string][]byte{},
		},
		{
			name: "some keys exist",
			in:   map[string][]byte{"key1": []byte("value1"), "key2": []byte("value2"), "key3": []byte("value3")},
			keys: []string{"key1", "key4", "key2", "key5"},
			want: map[string][]byte{"key1": []byte("value1"), "key2": []byte("value2")},
		},
		{
			name: "single key exists",
			in:   map[string][]byte{"key1": []byte("value1"), "key2": []byte("value2")},
			keys: []string{"key1"},
			want: map[string][]byte{"key1": []byte("value1")},
		},
		{
			name: "duplicate keys in slice",
			in:   map[string][]byte{"key1": []byte("value1"), "key2": []byte("value2")},
			keys: []string{"key1", "key1", "key2"},
			want: map[string][]byte{"key1": []byte("value1"), "key2": []byte("value2")},
		},
		{
			name: "empty values",
			in:   map[string][]byte{"key1": []byte(""), "key2": []byte("value2")},
			keys: []string{"key1", "key2"},
			want: map[string][]byte{"key1": []byte(""), "key2": []byte("value2")},
		},
		{
			name: "nil byte slices",
			in:   map[string][]byte{"key1": nil, "key2": []byte("value2")},
			keys: []string{"key1", "key2"},
			want: map[string][]byte{"key1": nil, "key2": []byte("value2")},
		},
		{
			name: "nil map",
			in:   nil,
			keys: []string{"key1", "key2"},
			want: map[string][]byte{},
		},
		{
			name: "nil keys",
			in:   map[string][]byte{"key1": []byte("value1"), "key2": []byte("value2")},
			keys: nil,
			want: map[string][]byte{},
		},
		{
			name: "both nil",
			in:   nil,
			keys: nil,
			want: map[string][]byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PickByKeys(tt.in, tt.keys)
			assert.Equal(t, tt.want, got, "PickByKeys(%v, %v)", tt.in, tt.keys)
		})
	}
}

func TestPickByKeysCapacity(t *testing.T) {
	in := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}
	keys := []string{"key1", "key2", "key3", "key4", "key5"}

	result := PickByKeys(in, keys)

	// Verify that only existing keys are in the result
	assert.Len(t, result, 3)
	assert.Equal(t, []byte("value1"), result["key1"])
	assert.Equal(t, []byte("value2"), result["key2"])
	assert.Equal(t, []byte("value3"), result["key3"])
	assert.NotContains(t, result, "key4")
	assert.NotContains(t, result, "key5")
}
