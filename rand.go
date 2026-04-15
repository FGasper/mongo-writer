package main

import (
	"math/rand/v2"

	pool "github.com/libp2p/go-buffer-pool"
)

// Excluded for clarity: I, l, 1, O, o, 0
const humanCharset = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

var charsetLen = len(humanCharset)

func randomString(n int) string {
	b := pool.Get(n)
	defer pool.Put(b)

	// Use a larger random value and extract multiple characters from it
	// to reduce the number of rand calls (most expensive operation)
	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))

	for i := 0; i < n; {
		// Generate a 64-bit random number and extract characters
		randVal := rng.Uint64()

		// Extract up to ~10 characters per 64-bit number using modulo
		// (works well for our 57-char charset)
		for j := 0; j < 10 && i < n; j++ {
			b[i] = humanCharset[randVal%uint64(charsetLen)]
			randVal /= uint64(charsetLen)
			i++
		}
	}

	return string(b)
}
