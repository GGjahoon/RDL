package index

import (
	"bytes"
	"sort"
	"sync"

	"github.com/GGjahon/bitcask-kv/data"
	"github.com/google/btree"
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

	return oldItem != nil
}
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}
func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// btreeIterator BTree索引迭代器实例
type btreeIterator struct {
	//当前遍历的下标位置
	currIndex int

	//reverse 表示是否为反向遍历
	reverse bool

	//values 储存从索引内加载的item数据
	values []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) Iterator {
	var idx int
	values := make([]*Item, tree.Len())
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// ReWind() 回到迭代器起点
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

// Seek() 根据传入的key查找到第一个大于（或小于）等于的目标key，从该key开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// Next() 跳转到下一个key
func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

// Valid 验证是否有效，即是否遍历完成所有的key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// Key()当前遍历位置的key数据
func (bti *btreeIterator) Key() []byte {
	if !bti.Valid() {
		return nil
	}
	return bti.values[bti.currIndex].key
}

// Value 当前遍历位置的value数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	if !bti.Valid() {
		return nil
	}
	return bti.values[bti.currIndex].pos
}

// Close() 关闭迭代器，释放占用资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}
