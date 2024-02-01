package index

import (
	"github.com/GGjahon/bitcask-kv/data"
	"github.com/google/btree"
	"sync"
)

// BTree is a implement of index interface, provides all method of index interface with btree structure
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() Index {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	item := &Item{
		key: key,
		pos: pos,
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(item)
	return true
}

// Get the key's data store pos in disk
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	item := &Item{
		key: key,
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	bit := bt.tree.Get(item)
	if bit == nil {
		return nil
	}
	return bit.(*Item).pos
}

// Delete ,delete the key in memory
func (bt *BTree) Delete(key []byte) bool {
	item := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(item)
	if oldItem == nil {
		return false
	}
	return true
}
