package datastore

import (
	"testing"

	kksecret "github.com/kklab-com/goth-kksecret"
)

func TestKKRedisOp_Get(t *testing.T) {
	kksecret.PATH = "./example/"
	KKDB("test").Writer().DB()
}
