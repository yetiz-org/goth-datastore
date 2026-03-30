package datastore

import (
	"os"
	"strconv"
)

func envStr[T ~string](key string, dest *T) {
	if v := os.Getenv(key); v != "" {
		*dest = T(v)
	}
}

func envInt[T ~int](key string, dest *T) {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			*dest = T(n)
		}
	}
}

func envBool(key string, dest *bool) {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			*dest = b
		}
	}
}
