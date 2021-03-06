package memtable

import (
	"sort"
	"sync"
	"testing"
	"time"

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

func TestInitLogFile(t *testing.T) {
	is := is.New(t)
	m, err := NewMemtable(10000, "/tmp/memtable.log")
	is.NoErr(err)

	m.Put("key1", "value1")
	m.Put("key2", "value2")
	m.Put("key3", "old_value")
	m.Delete("key2")
	m.Put("key3", "new_value")
}

func TestNonEmptyLogFile(t *testing.T) {
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

func TestToSlice(t *testing.T) {
	is := is.New(t)
	m, err := NewMemtable(10000, "/tmp/memtable.log")
	is.NoErr(err)

	m.Put("key1", "value1")
	m.Put("key2", "value2")
	m.Put("key3", "old_value")
	m.Delete("key2")
	m.Put("key3", "new_value")

	entries := m.ToSlice()
	is.Equal(len(entries), 3)

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	is.Equal(entries[0].Key, "key1")
	is.Equal(entries[0].Value, "value1")
	is.Equal(entries[1].Key, "key2")
	is.Equal(entries[1].Value, "")
	is.Equal(entries[2].Key, "key3")
	is.Equal(entries[2].Value, "new_value")

	m.Clear()
}

func TestConcurrent(t *testing.T) {
	is := is.New(t)
	m, err := NewMemtable(10000, "/tmp/memtable.log")
	is.NoErr(err)
	var wg sync.WaitGroup

	wg.Add(1)
	go checkAfterOneSecond(&wg, is, m, "key1", "value1")
	go m.Put("key1", "value1")

	m.Put("key2", "value2")

	wg.Add(1)
	go checkAfterOneSecond(&wg, is, m, "key2", "")
	go m.Delete("key2")

	wg.Wait()

	m.Clear()
}

func checkAfterOneSecond(wg *sync.WaitGroup, is *is.I, m *Memtable, key string, value string) {
	defer wg.Done()
	time.Sleep(time.Second * 1)
	val, found := m.Get(key)
	if value == "" {
		is.True(!found)
	} else {
		is.True(found)
	}
	is.Equal(val, value)
}
