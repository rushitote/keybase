package memtable

import (
	"encoding/binary"
	"os"

	"github.com/edsrzf/mmap-go"
)

type entry struct {
	key       string
	value     string
	isDeleted bool // isDeleted is true if the entry is a delete marker
}

type Memtable struct {
	entries map[string]entry // Map of key to entry
	curSize int              // Current size of the memtable
	maxSize int              // Maximum size of the memtable in bytes

	filePath string   // Memtable log file path
	f        *os.File // Memtable log file
}

func NewMemtable(maxSize int, filePath string) (*Memtable, error) {
	m := Memtable{
		entries:  make(map[string]entry),
		curSize:  0,
		maxSize:  maxSize,
		filePath: filePath,
	}

	err := m.InitMemtable()
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (m *Memtable) InitMemtable() error {
	f, err := os.OpenFile(m.filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	m.f = f

	if fi, _ := f.Stat(); fi.Size() == 0 {
		return nil
	}

	mmmap, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	i := 0 // index of the next entry to read
	for i < len(mmmap) {

		keyLen := binary.BigEndian.Uint32(mmmap[i : i+4])
		key := string(mmmap[i+4 : i+4+int(keyLen)])
		valueLen := binary.BigEndian.Uint64(mmmap[i+4+int(keyLen) : i+4+int(keyLen)+8])

		if valueLen == keyDeleteNum {
			m.entries[key] = entry{key, "", true}
			valueLen = 0
		} else {
			value := string(mmmap[i+4+int(keyLen)+8 : i+4+int(keyLen)+8+int(valueLen)])
			m.entries[key] = entry{key, value, false}
		}

		i += 4 + int(keyLen) + 8 + int(valueLen)
	}
	mmmap.Unmap()

	return nil
}

func (m *Memtable) Put(key string, value string) error {
	m.entries[key] = entry{key, value, false}
	m.curSize += len(key) + len(value)

	keyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLen, uint32(len(key)))

	if _, err := m.f.Write(keyLen); err != nil {
		return err
	}
	if _, err := m.f.WriteString(key); err != nil {
		return err
	}

	valueLen := make([]byte, 8)
	binary.BigEndian.PutUint64(valueLen, uint64(len(value)))

	if _, err := m.f.Write(valueLen); err != nil {
		return err
	}
	if _, err := m.f.WriteString(value); err != nil {
		return err
	}

	return nil
}

func (m *Memtable) Get(key string) (string, bool) {
	val, ok := m.entries[key]
	if ok && !val.isDeleted {
		return val.value, true
	}
	return "", false
}

func (m *Memtable) Delete(key string) error {
	m.entries[key] = entry{key, "", true}
	m.curSize += len(key)

	keyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLen, uint32(len(key)))

	if _, err := m.f.Write(keyLen); err != nil {
		return err
	}
	if _, err := m.f.WriteString(key); err != nil {
		return err
	}

	valueLen := make([]byte, 8)
	binary.BigEndian.PutUint64(valueLen, uint64(keyDeleteNum))

	if _, err := m.f.Write(valueLen); err != nil {
		return err
	}

	return nil
}

// Clear removes all entries from the memtable and the log file
func (m *Memtable) Clear() error {
	m.entries = make(map[string]entry)
	m.curSize = 0

	if err := m.f.Truncate(0); err != nil {
		return err
	}

	return nil
}

const (
	// keyDeleteNum is the value used to indicate a delete marker in the log file
	keyDeleteNum uint64 = 0xffffffffffffffff
)
