package table

import "os"

type Table struct {
	FilePath string
	File     *os.File
	CurrSize uint64
	MaxSize  uint64
}

type entry struct {
	key       string
	isDeleted bool

	index     uint   // index of the file
	offset    uint64 // entry location offset
	keySize   uint   // byte size of key
	valueSize uint   // byte size of value
}

func NewTable(filePath string, maxSize uint64) (*Table, error) {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return &Table{
		FilePath: filePath,
		File:     f,
		CurrSize: 0,
		MaxSize:  maxSize,
	}, nil
}
