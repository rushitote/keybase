package util

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"

	"github.com/edsrzf/mmap-go"
	"github.com/rushitote/keybase/config"
	"github.com/rushitote/keybase/memtable"
)

func GetLevelPath(conf *config.Options, level int8, isTemp ...bool) string {
	if len(isTemp) > 0 && isTemp[0] {
		return filepath.Join(conf.DBPath, "L"+strconv.Itoa(int(level))+"temp")
	}
	return filepath.Join(conf.DBPath, "L"+strconv.Itoa(int(level)))
}

func GetPathWithoutTemp(filePath string) string {
	if len(filePath) > 4 && filePath[len(filePath)-4:] == "temp" {
		return filePath[:len(filePath)-4]
	}
	return filePath
}

func IncrementFileOffset(keyLen uint32, valueLen uint64) int {
	if valueLen == memtable.KeyDeleteNum {
		return 4 + int(keyLen) + 8
	} else {
		return 4 + int(keyLen) + 8 + int(valueLen)
	}
}

func GetKeySizeOfNextEntry(mmap *mmap.MMap, offset uint64) uint32 {
	return binary.LittleEndian.Uint32((*mmap)[offset : offset+4])
}

func GetKeyOfNextEntry(mmap *mmap.MMap, offset uint64, keySize uint64) string {
	return string((*mmap)[offset+4 : offset+4+keySize])
}

func GetValueSizeOfNextEntry(mmap *mmap.MMap, offset uint64) uint64 {
	return binary.LittleEndian.Uint64((*mmap)[offset : offset+4])
}

func GetValueOfNextEntry(mmap *mmap.MMap, offset uint64, valueSize uint64) []byte {
	return (*mmap)[offset+4+8 : offset+4+8+valueSize]
}

func WriteEntryToFile(f *os.File, keyLen uint32, key string, valueLen uint64, value []byte) error {
	keyLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLenBytes, uint32(len(key)))
	if _, err := f.Write(keyLenBytes); err != nil {
		return err
	}

	if _, err := f.WriteString(key); err != nil {
		return err
	}

	valueLenBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(valueLenBytes, uint64(len(value)))
	if _, err := f.Write(valueLenBytes); err != nil {
		return err
	}

	if valueLen != memtable.KeyDeleteNum {
		if _, err := f.Write(value); err != nil {
			return err
		}
	}

	return nil
}
