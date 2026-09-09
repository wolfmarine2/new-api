package controller

import (
	"testing"
)

func TestParseRequestIds(t *testing.T) {
	cases := []struct {
		raw   string
		count int
	}{
		{"", 0},
		{"a", 1},
		{"a,b,c", 3},
		{" a , , b ", 2},
	}
	for _, tc := range cases {
		if got := len(parseRequestIds(tc.raw)); got != tc.count {
			t.Fatalf("parseRequestIds(%q) returned %d ids, want %d", tc.raw, got, tc.count)
		}
	}
	long := ""
	for i := 0; i < 150; i++ {
		if i > 0 {
			long += ","
		}
		long += "id" + string(rune('a'+i%26))
	}
	if got := len(parseRequestIds(long)); got != maxBatchRequestIds {
		t.Fatalf("parseRequestIds did not cap at %d, got %d", maxBatchRequestIds, got)
	}
}
