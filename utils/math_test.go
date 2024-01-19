package utils

import (
	"math"
	"testing"
)

func TestCalcEntropyWithEmptyMap(t *testing.T) {
	m := make(map[any]int)
	result := CalcEntropy(m)
	expected := 1.0

	if math.Abs(result-expected) > epsilon {
		t.Errorf("Expected: %f, Got: %f", expected, result)
	}
}

func TestCalcEntropyWithSingleValue(t *testing.T) {
	m := map[any]int{"A": 5}
	result := CalcEntropy(m)
	expected := 1.0

	if math.Abs(result-expected) > epsilon {
		t.Errorf("Expected: %f, Got: %f", expected, result)
	}
}

func TestCalcEntropyWithMultipleValues(t *testing.T) {
	m := map[any]int{"A": 5, "B": 5, "C": 5}
	result := CalcEntropy(m)
	expected := 1.0

	if math.Abs(result-expected) > epsilon {
		t.Errorf("Expected: %f, Got: %f", expected, result)
	}
}

func TestCalcEntropyWithDifferentProbabilities(t *testing.T) {
	m := map[any]int{"A": 1, "B": 3, "C": 6}
	result := CalcEntropy(m)

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
	expected := entropy / math.Log2(float64(len(m)))

	if math.Abs(result-expected) > epsilon {
		t.Errorf("Expected: %f, Got: %f", expected, result)
	}
}
