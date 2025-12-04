package utils

import (
	"math"
	"testing"
)

func TestCalcEntropy(t *testing.T) {
	tests := []struct {
		name     string
		m        map[any]int
		expected float64
	}{
		{
			name:     "empty map",
			m:        make(map[any]int),
			expected: 1.0,
		},
		{
			name:     "single value",
			m:        map[any]int{"A": 5},
			expected: 1.0,
		},
		{
			name:     "multiple values with equal distribution",
			m:        map[any]int{"A": 5, "B": 5, "C": 5},
			expected: 1.0,
		},
		{
			name: "different probabilities",
			m:    map[any]int{"A": 1, "B": 3, "C": 6},
			expected: func() float64 {
				m := map[any]int{"A": 1, "B": 3, "C": 6}
				total := 0
				probabilities := make(map[any]float64)
				for _, count := range m {
					total += count
				}
				for key, count := range m {
					probabilities[key] = float64(count) / float64(total)
				}

				entropy := 0.0
				for _, p := range probabilities {
					entropy -= p * math.Log2(p)
				}
				return entropy / math.Log2(float64(len(m)))
			}(),
		},
		{
			name: "covers epsilon branch",
			m: map[any]int{
				"A": 1,
				"B": 1,
				"C": 1000000000,
			},
			expected: -1.0, // special value to indicate we just check it's positive
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalcEntropy(tt.m)

			if tt.expected == -1.0 {
				// Special case for epsilon branch test
				if result <= 0 {
					t.Errorf("Expected positive entropy, got %f", result)
				}
			} else {
				if math.Abs(result-tt.expected) > epsilon {
					t.Errorf("Expected: %f, Got: %f", tt.expected, result)
				}
			}
		})
	}
}
