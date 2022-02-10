package memtable

import "errors"

type Entry struct {
	key   string
	value string
}

type Memtable struct {
	entries map[string]Entry // Map of key to entry
	curSize int              // Current size of the memtable
	maxSize int              // Maximum size of the memtable in bytes
}

func NewMemtable(maxSize int) *Memtable {
	return &Memtable{
		entries: make(map[string]Entry),
		curSize: 0,
		maxSize: maxSize,
	}
}

func (m *Memtable) Put(key string, value string) {
	m.entries[key] = Entry{key, value}
	m.curSize += len(key) + len(value)
}

func (m *Memtable) Get(key string) (string, error) {
	val, ok := m.entries[key]
	if ok {
		return val.value, nil
	}
	return "", errors.New("key not found")
}

func (m *Memtable) Delete(key string) error {
	if _, ok := m.entries[key]; !ok {
		return errors.New("key not found")
	}

	m.curSize -= (len(key) + len(m.entries[key].value))
	delete(m.entries, key)
	return nil
}
