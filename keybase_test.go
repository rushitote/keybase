package keybase

import "testing"

func TestGet(t *testing.T) {
	kb := Open()
	err := kb.Set("key1", "value1")
	if err != nil {
		t.Error(err)
	}
	value, err := kb.Get("key1")
	if err != nil {
		t.Error(err)
	}
	if value != "value1" {
		t.Error(value + " != value1")
	}
}
