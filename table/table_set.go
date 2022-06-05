package table

import (
	"os"

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
		maxLevel: 0,
		conf:     conf,
	}
}

func (ts *TableSet) CompactMemtable(m *memtable.Memtable) error {
	if ts.maxLevel == 0 {
		if err := ts.AddL1Table(); err != nil {
			return err
		}
	}

	entries := m.GetSortedEntries()
	if err := ts.MergeMemtableToL1(entries); err != nil {
		return err
	}

	return nil
}

func (ts *TableSet) AddL1Table() error {
	tbl, err := NewTable(util.GetLevelPath(ts.conf, 1), uint64(ts.conf.L1Size))
	if err != nil {
		return err
	}
	ts.list = []Table{*tbl}
	ts.maxLevel = 1
	return nil
}

/*
	- Entries on file will be of form:
	| hashed key (8B) | key size (4B) | key | value size (4B) | value |
*/

func (ts *TableSet) MergeMemtableToL1(entries []memtable.MemEntry) error {
	L1Table := ts.list[0]

	oldFileMmap, err := mmap.Map(L1Table.File, mmap.RDWR, 0)
	if err != nil {
		return err
	}

	newFile, err := os.OpenFile(util.GetLevelPath(ts.conf, 1, true), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	oldFileIndex := uint64(0) // index of the entry to read in the old file
	memIndex := 0             // index of the entry to read in the memtable

	// compare memtable entries with the old file entries and merge them
	// if keys are same, use the value from the memtable since it is newer
	for oldFileIndex < uint64(len(oldFileMmap)) && memIndex < len(entries) {
		keyLen := util.GetKeySizeOfNextEntry(&oldFileMmap, oldFileIndex+8)
		key := util.GetKeyOfNextEntry(&oldFileMmap, oldFileIndex+12, uint64(keyLen))

		if key < entries[memIndex].Key {
			valueLen := util.GetValueSizeOfNextEntry(&oldFileMmap, oldFileIndex+12+uint64(keyLen))
			value := make([]byte, 0)

			oldFileIndex += 12 + uint64(keyLen) + 8

			if valueLen != memtable.KeyDeleteNum {
				value = util.GetValueOfNextEntry(&oldFileMmap, oldFileIndex+12+uint64(keyLen)+8, valueLen)
				oldFileIndex += uint64(valueLen)
			}

			util.WriteEntryToFile(newFile, keyLen, key, valueLen, value)

		} else if key == entries[memIndex].Key {
			valueLen := len(entries[memIndex].Value)
			value := []byte(entries[memIndex].Value)

			util.WriteEntryToFile(newFile, keyLen, key, uint64(valueLen), value)

			oldFileIndex += 12 + uint64(keyLen) + 8

			oldFileValueLen := util.GetValueSizeOfNextEntry(&oldFileMmap, oldFileIndex)
			if oldFileValueLen != memtable.KeyDeleteNum {
				oldFileIndex += uint64(oldFileValueLen)
			}

			memIndex++

		} else {
			valueLen := len(entries[memIndex].Value)
			value := []byte(entries[memIndex].Value)

			util.WriteEntryToFile(newFile, keyLen, key, uint64(valueLen), value)

			memIndex++
		}
	}

	return nil
}
