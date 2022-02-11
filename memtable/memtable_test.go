package memtable

import (
	"testing"

	"github.com/matryer/is"
)

func TestEmptyLogFile(t *testing.T) {
	is := is.New(t)
	m, err := NewMemtable(10000, "/tmp/memtable.log")
	is.NoErr(err)

	err = m.Put("key1", "value1")
	is.NoErr(err)

	val, found := m.Get("key1")
	is.True(found)
	is.Equal(val, "value1")

	_, found = m.Get("key2")
	is.True(!found)

	err = m.Delete("key1")
	is.NoErr(err)

	_, found = m.Get("key1")
	is.True(!found)

	err = m.Clear()
	is.NoErr(err)
}

func TestInitLogFile(t *testing.T){
	is := is.New(t)
	m, err := NewMemtable(10000, "/tmp/memtable.log")
	is.NoErr(err)

	m.Put("key1", "value1")
	m.Put("key2", "value2")
	m.Put("key3", "old_value")
	m.Delete("key2")
	m.Put("key3", "new_value")
}

func TestNonEmptyLogFile(t *testing.T){
	is := is.New(t)
	m, err := NewMemtable(10000, "/tmp/memtable.log")
	is.NoErr(err)

	val, found := m.Get("key1")
	is.True(found)
	is.Equal(val, "value1")

	_, found = m.Get("key2")
	is.True(!found)

	val, found = m.Get("key3")
	is.True(found)
	is.Equal(val, "new_value")

	m.Clear()
}
