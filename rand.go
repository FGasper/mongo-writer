package main

import "math/rand/v2"

// Excluded for clarity: I, l, 1, O, o, 0
const humanCharset = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = humanCharset[rand.IntN(len(humanCharset))]
	}
	return string(b)
}
