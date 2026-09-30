package influence

import (
	"math"
	"testing"
)

func TestTCriticalKnownValues(t *testing.T) {
	cases := []struct {
		conf float64
		df   int
		want float64
	}{
		{0.95, 1, 12.706},
		{0.95, 2, 4.303},
		{0.95, 5, 2.571},
		{0.95, 10, 2.228},
		{0.95, 30, 2.042},
		{0.99, 9, 3.250},
		{0.90, 19, 1.729},
	}
	for _, c := range cases {
		got := tCritical(c.conf, c.df)
		if math.Abs(got-c.want) > 0.01 {
			t.Errorf("tCritical(%v,%d) = %.4f, want %.4f", c.conf, c.df, got, c.want)
		}
	}
}

func TestTCriticalLargeDFApproachesNormal(t *testing.T) {
	got := tCritical(0.95, 2000)
	if math.Abs(got-1.96) > 0.01 {
		t.Fatalf("large-df critical = %.4f, want ~1.96", got)
	}
}
