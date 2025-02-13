package util

import (
	li "github.com/takanoriyanagitani/go-avro-log-tag-ipv4"
)

func ComposeErr[T, U, V any](
	f func(T) (U, error),
	g func(U) (V, error),
) func(T) (V, error) {
	return li.ComposeErr(f, g)
}
