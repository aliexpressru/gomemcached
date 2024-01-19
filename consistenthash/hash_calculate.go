package consistenthash

import "github.com/cespare/xxhash"

// Hash returns the hash value of data.
func Hash(data []byte) uint64 {
	return xxhash.Sum64(data)
}
