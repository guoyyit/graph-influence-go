package influence

import (
	"math"
	"strconv"
)

// mix64 and hashString are deterministic, dependency-free 64-bit
// mixers used to derive stable random-stream seeds.

func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func hashString(s string) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}

// floatKey gives a stable textual encoding of a float64 for hashing.
func floatKey(f float64) string {
	return strconv.FormatUint(math.Float64bits(f), 16)
}
