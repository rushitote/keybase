package keybase

import "errors"

type KeyBase struct {
	db map[string]string
}

type KeyBaseMethods interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Delete(key string) error
}

func Open() *KeyBase {
	kb := KeyBase{}
	kb.db = make(map[string]string)
	return &kb
}

func (kb *KeyBase) Get(key string) (string, error) {
	if value, ok := kb.db[key]; ok {
		return value, nil
	}
	return "", errors.New("key doesn't exist")
}

func (kb *KeyBase) Set(key string, value string) error {
	kb.db[key] = value
	return nil
}

func (kb *KeyBase) Delete(key string) error {
	delete(kb.db, key)
	return nil
}

func (kb *KeyBase) Close() {
	for k := range kb.db {
		delete(kb.db, k)
	}
}
