// nolint
package memcached

import (
	"strings"
	"testing"
)

func TestCommandCodeString(t *testing.T) {
	if GET.String() != "GET" {
		t.Fatalf("Expected \"GET\" for GET, got \"%v\"", GET.String())
	}

	cc := OpCode(0x80)
	if cc.String() != "0x80" {
		t.Fatalf("Expected \"0x80\" for 0x80, got \"%v\"", cc.String())
	}
}

func TestStatusNameString(t *testing.T) {
	if SUCCESS.String() != "SUCCESS" {
		t.Fatalf("Expected \"SUCCESS\" for SUCCESS, got \"%v\"",
			SUCCESS.String())
	}

	s := Status(0x80)
	if s.String() != "0x80" {
		t.Fatalf("Expected \"0x80\" for 0x80, got \"%v\"", s.String())
	}
}

func TestIsQuiet(t *testing.T) {
	for v, k := range CommandNames {
		isq := strings.HasSuffix(k, "Q")
		if v.IsQuiet() != isq {
			t.Errorf("Expected quiet=%v for %v, got %v",
				isq, v, v.IsQuiet())
		}
	}
}
