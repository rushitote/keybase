package util

import (
	"path/filepath"
	"strconv"

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
