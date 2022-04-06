package memtable

import (
	"encoding/binary"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
	"github.com/rushitote/keybase/logger"
)

type MemEntry struct {
	Key       string
	Value     string
	IsDeleted bool // isDeleted is true if the entry is a delete marker
}

type Memtable struct {
	Entries  sync.Map // Map of key to entry
	CurrSize int      // Current size of the memtable
	MaxSize  int      // Maximum size of the memtable in bytes

	FilePath string        // Memtable log file path
	f        *os.File      // Memtable log file
	RW       sync.RWMutex  // R/W lock for the memtable
	Logger   logger.Logger // Logger
}

func NewMemtable(maxSize int, filePath string) (*Memtable, error) {
	m := Memtable{
		Entries:  sync.Map{},
		CurrSize: 0,
		MaxSize:  maxSize,
		FilePath: filePath,
		RW:       sync.RWMutex{},
		Logger:   logger.NewLogger(logger.DEBUG),
	}

	err := m.InitMemtable()
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (m *Memtable) InitMemtable() error {
	m.Logger.Info("Init memtable")

	m.RW.Lock()
	defer m.RW.Unlock()

	f, err := os.OpenFile(m.FilePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	m.f = f

	if fi, _ := f.Stat(); fi.Size() == 0 {
		return nil
	}

	memFile, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	i := 0 // index of the next entry to read
	for i < len(memFile) {

		keyLen := binary.BigEndian.Uint32(memFile[i : i+4])
		key := string(memFile[i+4 : i+4+int(keyLen)])
		valueLen := binary.BigEndian.Uint64(memFile[i+4+int(keyLen) : i+4+int(keyLen)+8])

		if valueLen == KeyDeleteNum {
			m.Entries.Store(key, MemEntry{key, "", true})
			valueLen = 0
		} else {
			value := string(memFile[i+4+int(keyLen)+8 : i+4+int(keyLen)+8+int(valueLen)])
			m.Entries.Store(key, MemEntry{key, value, false})
		}

		i += 4 + int(keyLen) + 8 + int(valueLen)
	}
	memFile.Unmap()

	return nil
}

func (m *Memtable) Put(key string, value string) error {
	m.Logger.Info("Put key: ", key, " value: ", value)

	m.RW.RLock()
	defer m.RW.RUnlock()

	m.Entries.Store(key, MemEntry{key, value, false})
	m.CurrSize += len(key) + len(value)

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
	m.Logger.Info("Get key: ", key)

	m.RW.RLock()
	defer m.RW.RUnlock()

	val, ok := m.Entries.Load(key)
	if ok && !val.(MemEntry).IsDeleted {
		return val.(MemEntry).Value, true
	}
	return "", false
}

func (m *Memtable) Delete(key string) error {
	m.Logger.Info("Delete key: ", key)

	m.RW.RLock()
	defer m.RW.RUnlock()

	m.Entries.Store(key, MemEntry{key, "", true})
	m.CurrSize += len(key)

	keyLen := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLen, uint32(len(key)))

	if _, err := m.f.Write(keyLen); err != nil {
		return err
	}
	if _, err := m.f.WriteString(key); err != nil {
		return err
	}

	valueLen := make([]byte, 8)
	binary.BigEndian.PutUint64(valueLen, uint64(KeyDeleteNum))

	if _, err := m.f.Write(valueLen); err != nil {
		return err
	}

	return nil
}

// Clear removes all entries from the memtable and the log file
func (m *Memtable) Clear() error {
	m.Logger.Info("Clear memtable")

	m.RW.RLock()
	defer m.RW.RUnlock()

	m.Entries = sync.Map{}
	m.CurrSize = 0

	if err := m.f.Truncate(0); err != nil {
		return err
	}

	return nil
}

func (m *Memtable) ToSlice() []MemEntry {
	m.RW.Lock()
	defer m.RW.Unlock()

	entries := make([]MemEntry, 0)
	m.Entries.Range(func(key, value interface{}) bool {
		entries = append(entries, value.(MemEntry))
		return true
	})
	return entries
}

const (
	// KeyDeleteNum is the value used to indicate a delete marker in the log file
	KeyDeleteNum uint64 = 0xffffffffffffffff
)
