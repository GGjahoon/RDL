package index

import (
	"bytes"
	"github.com/GGjahon/bitcask-kv/data"
	"github.com/google/btree"
)

// Index give all method for operating the key of data in memory.Every different could implement own method with this interface
type Index interface {
	// Put the key into memory
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get the key's data store pos in disk
	Get(key []byte) *data.LogRecordPos

	// Delete ,delete the key in memory
	Delete(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
