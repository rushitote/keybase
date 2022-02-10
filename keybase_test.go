package keybase

import (
	"testing"

	"github.com/matryer/is"
)

func TestGet(t *testing.T) {
	is := is.New(t)

	kb := Open()
	err := kb.Set("key1", "value1")
	is.NoErr(err)
	value, err := kb.Get("key1")
	is.NoErr(err)
	is.Equal(value, "value1")
}
