package util

import (
	"reflect"
	"testing"

	"github.com/matryer/is"
)

func TestHashKey(t *testing.T) {
	is := is.New(t)
	is.True(reflect.DeepEqual(HashKey([]byte("key1")), HashKey([]byte("key1"))))
	is.True(!reflect.DeepEqual(HashKey([]byte("key1")), HashKey([]byte("key2"))))
}
