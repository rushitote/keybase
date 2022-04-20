package table

import (
	"encoding/binary"
	"sort"

	"github.com/edsrzf/mmap-go"
	"github.com/rushitote/keybase/config"
	"github.com/rushitote/keybase/memtable"
	"github.com/rushitote/keybase/util"
)

/*
Table set will contain list of tables and methods to operate with them (like compaction)
*/

type TableSet struct {
	list     []Table // list of tables
	maxLevel int8    // max current level of the table set
	conf     *config.Options
}

func NewTableSet(conf *config.Options) *TableSet {
	return &TableSet{
		list:     []Table{},
		maxLevel: -1,
		conf:     conf,
	}
}

func (ts *TableSet) CompactMemtable(m *memtable.Memtable) error {
	if ts.maxLevel < 0 {
		tbl, err := NewTable(util.GetLevelPath(ts.conf, 0), uint64(ts.conf.L0Size))
		if err != nil {
			return err
		}
		ts.list = []Table{*tbl}
		ts.maxLevel = 0
	}

	entries := m.ToSlice()
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	// start comparing L0 and memtable using two pointers
	// and put the values as they go in a temp table
	// then rename the temp table to L0
	// and now check if L0 > maxSize

	i := 0 // index of the next memtable entry to read
	j := 0 // offset of the next L0 entry to read

	mmapFile, err := mmap.Map(ts.list[0].File, mmap.RDWR, 0)
	if err != nil {
		return err
	}

	newL0Table, err := NewTable(util.GetLevelPath(ts.conf, 0, true), uint64(ts.conf.L0Size))
	if err != nil {
		return err
	}

	for i < len(entries) && j < int(ts.list[0].CurrSize) {
		memKey := entries[i].Key
		keyLen := binary.BigEndian.Uint32(mmapFile[j : j+4])
		L0key := string(mmapFile[j+4 : j+4+int(keyLen)])

		if L0key < memKey {
			valueLen, err := writeL0EntryToNewTable(newL0Table, keyLen, L0key, mmapFile, j)
			if err != nil {
				return err
			}

			j += util.IncrementFileOffset(keyLen, valueLen)
		} else if L0key > memKey {
			
		} else {

		}
	}

	return nil
}

func writeL0EntryToNewTable(newL0Table *Table, keyLen uint32, L0key string, mmapFile mmap.MMap, j int) (uint64, error) {
	newL0Table.CurrSize += 4 + uint64(keyLen) + 8

	keyLenByteArray := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLenByteArray, keyLen)

	if _, err := newL0Table.File.Write(keyLenByteArray); err != nil {
		return 0, err
	}

	if _, err := newL0Table.File.WriteString(L0key); err != nil {
		return 0, err
	}

	valueLen := binary.BigEndian.Uint64(mmapFile[j+4+int(keyLen) : j+4+int(keyLen)+8])

	valueLenByteArray := make([]byte, 8)
	binary.BigEndian.PutUint64(valueLenByteArray, valueLen)

	if _, err := newL0Table.File.Write(valueLenByteArray); err != nil {
		return 0, err
	}

	if valueLen != memtable.KeyDeleteNum {
		newL0Table.CurrSize += (valueLen)
		value := mmapFile[j+4+int(keyLen)+8 : j+4+int(keyLen)+8+int(valueLen)]
		if _, err := newL0Table.File.Write(value); err != nil {
			return 0, err
		}
	}
	return valueLen, nil
}
