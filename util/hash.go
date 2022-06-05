package util

import (
	"hash/fnv"
)

// HashKey returns 64-bit hash of the given key using FNV-1a.
func HashKey(key []byte) []byte {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum(nil)
}
